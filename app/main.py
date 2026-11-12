"""FastAPI 服务：计划预览、确认运行、运行记录、分区预览、故障注入、配置与环信息。"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from datetime import date
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .config import ConfigError, load_config
from .metadata import MetaStore
from .runner import Orchestrator
from .warehouse import Warehouse

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = Path(os.environ.get("PIPELINE_CONFIG", BASE_DIR / "config" / "pipeline.yaml"))
DATA_DIR = Path(os.environ.get("DATA_DIR", BASE_DIR / "data"))
STATIC_DIR = BASE_DIR / "frontend" / "dist"


class RunRequest(BaseModel):
    dates: list[str] = Field(..., description="ISO 日期列表，如 ['2026-10-27']")
    kind: str = Field("initial", pattern="^(initial|backfill)$")


class FaultRequest(BaseModel):
    task_name: str
    enabled: bool
    kind: str = Field("fail", pattern="^(fail|delay)$")


def create_app(config_path: str | Path = CONFIG_PATH, data_dir: str | Path = DATA_DIR) -> FastAPI:
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    try:
        cfg = load_config(config_path)
    except ConfigError as exc:
        # 配置非法（含环）时服务仍可启动，并通过 /api/config 暴露错误
        cfg = None
        config_error = str(exc)
    else:
        config_error = None

    meta = MetaStore(data_dir / "orchestrator.db")
    wh = Warehouse(data_dir / "warehouse.duckdb")
    orch = Orchestrator(cfg, meta, wh) if cfg else None

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if orch is not None:
            app.state.recovery = orch.startup_recover()
        yield
        meta.close()
        wh.close()

    app = FastAPI(title="点式补数 SQL 编排工具", lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
    )
    app.state.meta = meta
    app.state.wh = wh
    app.state.orch = orch

    def require_orch() -> Orchestrator:
        if orch is None:
            raise HTTPException(status_code=400, detail=f"配置非法: {config_error}")
        return orch

    @app.get("/api/health")
    def health() -> dict[str, Any]:
        return {"status": "ok", "config_valid": cfg is not None,
                "config_error": config_error}

    @app.get("/api/config")
    def get_config() -> dict[str, Any]:
        if cfg is None:
            return {"valid": False, "error": config_error, "tasks": [], "sources": []}
        return {
            "valid": True,
            "sources": [
                {"name": s.name, "table": s.table, "partition_column": s.partition_column}
                for s in cfg.sources.values()
            ],
            "tasks": [
                {
                    "name": t.name,
                    "sql_file": t.sql_file,
                    "sql_version": t.sql_version,
                    "partition_column": t.partition_column,
                    "inputs": [
                        {"kind": r.kind, "name": r.name, "offset": r.offset}
                        for r in t.inputs
                    ],
                }
                for t in cfg.tasks.values()
            ],
        }

    @app.post("/api/runs/plan")
    def preview_plan(req: RunRequest) -> dict[str, Any]:
        o = require_orch()
        try:
            dates = [date.fromisoformat(d) for d in req.dates]
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=f"日期格式错误: {exc}")
        result = o.create_run(req.kind, dates)
        run = meta.get_run(result["run_id"])
        return {"run": run, "plan": result["plan"], "horizon_end": result["horizon_end"]}

    @app.post("/api/runs/{run_id}/confirm")
    def confirm_run(run_id: int) -> dict[str, Any]:
        o = require_orch()
        try:
            o.confirm_run(run_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc))
        return {"run_id": run_id, "status": "running"}

    @app.get("/api/runs")
    def list_runs() -> list[dict[str, Any]]:
        return meta.list_runs()

    @app.get("/api/runs/{run_id}")
    def get_run(run_id: int) -> dict[str, Any]:
        try:
            run = meta.get_run(run_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        nodes = meta.list_nodes(run_id)
        stats = orch.get_stats(run_id) if orch else {}
        return {"run": run, "nodes": nodes, "stats": stats}

    @app.get("/api/partitions")
    def list_partitions() -> dict[str, Any]:
        return {
            "published": meta.list_published(),
            "sources": meta.list_source_partitions(),
        }

    @app.get("/api/preview/{task_name}/{dt}")
    def preview_partition(task_name: str, dt: str) -> dict[str, Any]:
        o = require_orch()
        if task_name in o.cfg.tasks:
            spec = o.cfg.tasks[task_name]
            table = Warehouse.task_table(task_name)
        elif task_name in o.cfg.sources:
            spec = None
            table = o.cfg.sources[task_name].table
            pcol = o.cfg.sources[task_name].partition_column
            return {"task_name": task_name, "dt": dt,
                    **wh.preview(table, pcol, dt)}
        else:
            raise HTTPException(status_code=404, detail=f"未知任务/源 {task_name}")
        return {"task_name": task_name, "dt": dt,
                **wh.preview(table, spec.partition_column, dt)}

    @app.get("/api/faults")
    def list_faults() -> dict[str, bool]:
        return meta.list_faults()

    @app.post("/api/faults")
    def set_fault(req: FaultRequest) -> dict[str, Any]:
        o = require_orch()
        if req.task_name not in o.cfg.tasks:
            raise HTTPException(status_code=404, detail=f"未知任务 {req.task_name}")
        key = f"{req.kind}:{req.task_name}"
        meta.set_fault(key, req.enabled)
        return {"key": key, "enabled": req.enabled}

    @app.get("/api/sources/scan")
    def scan_sources() -> list[dict[str, Any]]:
        o = require_orch()
        registered = []
        for src in o.cfg.sources.values():
            for dt_str in wh.source_partition_dates(src.table, src.partition_column):
                sig = wh.partition_signature(src.table, src.partition_column, dt_str)
                meta.register_source(src.name, dt_str, sig)
                registered.append({"source": src.name, "dt": dt_str, "signature": sig[:12]})
        return registered

    if STATIC_DIR.is_dir():
        # 构建产物中的静态资源（JS/CSS）
        assets_dir = STATIC_DIR / "assets"
        if assets_dir.is_dir():
            app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")

        @app.get("/")
        def index() -> FileResponse:
            return FileResponse(STATIC_DIR / "index.html")

        # SPA 兜底：非 /api 路径统一返回 index.html，避免刷新后 404
        @app.get("/{full_path:path}")
        def spa_fallback(full_path: str) -> FileResponse:
            if full_path.startswith("api/"):
                raise HTTPException(status_code=404, detail="接口不存在")
            return FileResponse(STATIC_DIR / "index.html")

    return app


def create_app_once() -> FastAPI:
    """uvicorn 工厂入口（--factory）：仅在真正起服务时实例化，导入模块不会建库。"""
    return create_app()
