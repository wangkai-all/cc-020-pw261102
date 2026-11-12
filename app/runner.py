"""执行编排：最多两个就绪节点并发，失败阻塞下游，独立分支继续，支持重试与重启恢复。"""
from __future__ import annotations

import threading
import time
import traceback
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import date, timedelta
from typing import Any

from .config import PipelineConfig
from .metadata import MetaStore, utcnow
from .planner import Planner
from .warehouse import SQLError, Warehouse

TERMINAL_STATES = {"succeeded", "reused", "failed", "blocked"}
ACTIVE_STATES = {"ready", "running"}
MAX_WORKERS = 2


class Orchestrator:
    def __init__(self, cfg: PipelineConfig, meta: MetaStore, wh: Warehouse):
        self.cfg = cfg
        self.meta = meta
        self.wh = wh
        self.planner = Planner(cfg, meta, wh)
        self._run_lock = threading.RLock()
        self._active_run: int | None = None
        self._stats: dict[int, dict[str, Any]] = {}
        self.max_forward_reach = self._compute_forward_reach()

    @staticmethod
    def _new_stats() -> dict[str, Any]:
        return {"max_concurrent": 0, "executed": [], "batches": []}

    def _compute_forward_reach(self) -> int:
        """任务间依赖中，一个上游分区最远会影响多少天后的下游（rolling 的 -2 offset => 2）。"""
        reach = 0
        for spec in self.cfg.tasks.values():
            for ref in spec.inputs:
                if ref.kind == "task":
                    reach = max(reach, max(0, -ref.offset))
        return reach

    # ---------- 计划 ----------
    def create_run(self, kind: str, requested: list[date]) -> dict[str, Any]:
        if not requested:
            raise ValueError("至少需要一个日期")
        requested = sorted(requested)
        horizon_end = requested[-1] + timedelta(days=self.max_forward_reach)
        seeds = {(t, d.isoformat()) for d in requested for t in self.cfg.tasks}
        run_id = self.meta.create_run(
            kind, [d.isoformat() for d in requested], horizon_end.isoformat(),
            seeds=sorted(seeds),
        )
        plan = self.planner.build_plan(
            run_id, requested, horizon_end, seeds=seeds
        )
        self.meta.bulk_upsert_nodes(
            [(r["run_id"], r["task_name"], r["dt"], r["state"], r["reason"]) for r in plan]
        )
        self._stats[run_id] = self._new_stats()
        return {"run_id": run_id, "plan": plan,
                "horizon_end": horizon_end.isoformat()}

    def replan(self, run_id: int) -> list[dict[str, Any]]:
        run = self.meta.get_run(run_id)
        requested = [date.fromisoformat(d) for d in run["requested_dates"]]
        horizon_end = date.fromisoformat(run["horizon_end"])
        seeds = set(tuple(s) for s in run["seeds"]) or {
            (t, d.isoformat()) for d in requested for t in self.cfg.tasks
        }
        plan = self.planner.build_plan(
            run_id, requested, horizon_end, seeds=seeds
        )
        # 终态合并规则：
        # - succeeded 节点保持终态，不重跑；
        # - reused 仅当节点上一轮已经是 succeeded/reused（本运行已产出或确认可复用）才保留，
        #   若上一轮是 blocked/failed/running，说明本运行并未真正完成，必须回到 ready 重算；
        # - 仍缺上游的 blocked（计划新评估为 blocked）保持阻塞，不用空表代替。
        ok_terminal = {"succeeded", "reused"}
        unfinished_states = {"failed", "running", "blocked", "planned"}
        merged = []
        for row in plan:
            current = self.meta.get_node(run_id, row["task_name"], row["dt"])
            cur_state = current["state"] if current else "planned"
            if row["state"] == "blocked":
                # 新计划仍判定缺上游：保持阻塞
                merged.append((run_id, row["task_name"], row["dt"],
                               "blocked", row["reason"]))
            elif cur_state in ok_terminal:
                merged.append((run_id, row["task_name"], row["dt"],
                               cur_state, current["reason"]))
            elif cur_state in unfinished_states:
                merged.append((run_id, row["task_name"], row["dt"],
                               "ready", "重试：重新调度此前未成功的节点"))
            else:
                merged.append((run_id, row["task_name"], row["dt"],
                               row["state"], row["reason"]))
        self.meta.bulk_upsert_nodes(merged)
        return self.meta.list_nodes(run_id)

    # ---------- 启动/恢复 ----------
    def startup_recover(self) -> dict[str, Any]:
        dropped = self.wh.cleanup_temp_tables()
        recovered: list[int] = []
        for run_id in self.meta.find_unfinished_runs():
            for node in self.meta.list_nodes(run_id):
                if node["state"] == "running":
                    self.meta.set_node_state(
                        run_id, node["task_name"], node["dt"], "ready",
                        "服务重启导致中断，确认后从该节点继续（已完成节点不重跑）",
                    )
            self.meta.update_run_status(run_id, "planned", "服务重启，等待恢复确认")
            recovered.append(run_id)
        return {"recovered_runs": recovered, "dropped_temp_tables": dropped}

    def confirm_run(self, run_id: int) -> None:
        run = self.meta.get_run(run_id)
        if run["status"] not in ("planned", "failed", "partial"):
            raise RuntimeError(f"运行 {run_id} 当前状态为 {run['status']}，无法启动")
        with self._run_lock:
            if self._active_run is not None:
                raise RuntimeError(f"运行 {self._active_run} 仍在执行，同一时刻只允许一个活动运行")
            self.replan(run_id)
            self.meta.update_run_status(run_id, "running")
            # 记录本确认批次实际执行的节点（重试时与历史批次区分）
            stats = self._stats.setdefault(run_id, self._new_stats())
            stats.setdefault("batches", [])
            batch: list[dict[str, Any]] = []
            stats["batches"].append(batch)
            stats["_current_batch"] = batch
            self._active_run = run_id
            thread = threading.Thread(
                target=self._run_safely, args=(run_id,), name=f"run-{run_id}", daemon=True
            )
            thread.start()

    def get_stats(self, run_id: int) -> dict[str, Any]:
        stats = self._stats.get(run_id, self._new_stats())
        # 不暴露内部可变指针
        return {
            "max_concurrent": stats.get("max_concurrent", 0),
            "executed": stats.get("executed", []),
            "batches": stats.get("batches", []),
        }

    # ---------- 执行主循环 ----------
    def _run_safely(self, run_id: int) -> None:
        try:
            self._execute(run_id)
        except Exception:
            self.meta.update_run_status(run_id, "failed",
                                        f"调度器异常: {traceback.format_exc()}")
        finally:
            with self._run_lock:
                self._active_run = None

    def _upstream_nodes(self, run_id: int, task_name: str, dt_str: str) -> list[dict[str, Any]]:
        dt = date.fromisoformat(dt_str)
        out = []
        for ref in self.cfg.tasks[task_name].inputs:
            if ref.kind != "task":
                continue
            up_dt = (dt + timedelta(days=ref.offset)).isoformat()
            node = self.meta.get_node(run_id, ref.task, up_dt)
            if node is not None:
                out.append(node)
        return out

    def _execute(self, run_id: int) -> None:
        stats = self._stats.setdefault(run_id, self._new_stats())
        stats.setdefault("batches", [])
        concurrency = {"n": 0}
        conc_lock = threading.Lock()

        def run_node(node: dict[str, Any]) -> dict[str, Any]:
            task_name, dt_str = node["task_name"], node["dt"]
            spec = self.cfg.tasks[task_name]
            with conc_lock:
                concurrency["n"] += 1
                stats["max_concurrent"] = max(stats["max_concurrent"], concurrency["n"])
            try:
                self.meta.update_node(
                    run_id, task_name, dt_str,
                    state="running", reason=None,
                    attempts=node["attempts"] + 1, started_at=utcnow(),
                )
                entry = {"task": task_name, "dt": dt_str,
                         "attempt": node["attempts"] + 1}
                stats["executed"].append(entry)
                current = stats.get("_current_batch")
                if current is not None:
                    current.append(entry)
                # 可选延迟（仅用于演示崩溃恢复时观察 running 状态）
                delay = self.meta.get_fault(f"delay:{task_name}")
                if delay:
                    time.sleep(2.0)
                if self.meta.get_fault(f"fail:{task_name}"):
                    raise SQLError(f"故障注入: 任务 {task_name} 被配置为强制失败")
                fresh = self.planner.evaluate_node(task_name, dt_str)
                if fresh["missing"]:
                    raise SQLError("缺少必要上游分区: " + ", ".join(fresh["missing"]))
                self.wh.execute_node(run_id, spec, self.cfg, date.fromisoformat(dt_str))
                output_sig = self.wh.publish_node(run_id, spec, dt_str)
                self.meta.mark_published(
                    run_id, task_name, dt_str, spec.sql_version,
                    fresh["input_signature"], output_sig,
                )
                self.meta.update_node(
                    run_id, task_name, dt_str, state="succeeded",
                    reason=None, sql_version=spec.sql_version,
                    input_signature=fresh["input_signature"],
                    output_signature=output_sig, finished_at=utcnow(),
                )
            except Exception as exc:
                try:
                    self.wh.drop_temp(run_id, spec, dt_str)
                except Exception:
                    pass
                self.meta.update_node(
                    run_id, task_name, dt_str, state="failed",
                    reason=f"{type(exc).__name__}: {exc}", finished_at=utcnow(),
                )
            finally:
                with conc_lock:
                    concurrency["n"] -= 1
            return self.meta.get_node(run_id, task_name, dt_str)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            pending: dict[Future, dict[str, Any]] = {}
            in_flight: set[tuple[str, str]] = set()
            while True:
                nodes = self.meta.list_nodes(run_id)
                by_key = {(n["task_name"], n["dt"]): n for n in nodes}

                # 失败/阻塞沿边传播，迭代到不动点，确保级联 ready 节点也被阻塞
                propagated = True
                while propagated:
                    propagated = False
                    nodes = self.meta.list_nodes(run_id)
                    by_key = {(n["task_name"], n["dt"]): n for n in nodes}
                    for n in nodes:
                        if n["state"] not in ("failed", "blocked"):
                            continue
                        for consumer in self.downstream_of(n["task_name"]):
                            cdt = (date.fromisoformat(n["dt"])
                                   - timedelta(days=self._edge_offset(consumer, n["task_name"])))
                            cnode = by_key.get((consumer, cdt.isoformat()))
                            if cnode and cnode["state"] in ACTIVE_STATES | {"planned"}:
                                self.meta.set_node_state(
                                    run_id, consumer, cdt.isoformat(), "blocked",
                                    f"上游 {n['task_name']}@{n['dt']} 处于 {n['state']} 状态，下游被阻塞",
                                )
                                propagated = True

                nodes = self.meta.list_nodes(run_id)
                by_key = {(n["task_name"], n["dt"]): n for n in nodes}
                unfinished = [n for n in nodes if n["state"] in ACTIVE_STATES | {"planned"}]
                if not unfinished and not pending:
                    break  # 剩余节点均处于 failed/blocked/succeeded/reused 终态

                # 挑选依赖已满足的 ready 节点，受并发上限约束
                capacity = MAX_WORKERS - len(pending)
                for n in list(unfinished):
                    if capacity <= 0:
                        break
                    if n["state"] != "ready" or (n["task_name"], n["dt"]) in in_flight:
                        continue
                    deps = self._upstream_nodes(run_id, n["task_name"], n["dt"])
                    if any(d["state"] not in ("succeeded", "reused") for d in deps):
                        continue
                    key = (n["task_name"], n["dt"])
                    in_flight.add(key)
                    # 先占位为 running，避免下一轮重复提交；run_node 内部再更新 attempts/时间戳
                    self.meta.set_node_state(run_id, n["task_name"], n["dt"], "running",
                                             "等待工作线程执行")
                    fut = pool.submit(run_node, n)
                    pending[fut] = key
                    capacity -= 1

                if pending:
                    done, _ = _wait_any(pending)
                    for fut in done:
                        in_flight.discard(pending.pop(fut, None))
                        if fut.exception() is not None:
                            # run_node 已在内部把异常落为 failed；此处仅作防御
                            pass
                else:
                    # 没有在途任务。剩下的 ready/planned 节点若依赖了非成功态上游，
                    # 说明其上游已失败或被阻塞，本节点永远无法调度，显式阻塞后退出。
                    stuck = []
                    for n in self.meta.list_nodes(run_id):
                        if n["state"] not in ACTIVE_STATES | {"planned"}:
                            continue
                        deps = self._upstream_nodes(run_id, n["task_name"], n["dt"])
                        bad = [d for d in deps if d["state"] in ("failed", "blocked")]
                        if bad:
                            stuck.append((n, bad))
                    for n, bad in stuck:
                        self.meta.set_node_state(
                            run_id, n["task_name"], n["dt"], "blocked",
                            "上游 " + ", ".join(f"{d['task_name']}@{d['dt']}" for d in bad)
                            + " 已失败/阻塞，本节点无法调度",
                        )
                    if not stuck:
                        # 理论上不可达：无在途、无可推进节点。防御性退出避免空转。
                        break

        self._finalize(run_id)

    def downstream_of(self, task_name: str) -> list[str]:
        return sorted(self.planner.downstream.get(task_name, set()))

    def _edge_offset(self, consumer: str, upstream: str) -> int:
        for ref in self.cfg.tasks[consumer].inputs:
            if ref.kind == "task" and ref.task == upstream:
                return ref.offset
        return 0

    def _finalize(self, run_id: int) -> None:
        nodes = self.meta.list_nodes(run_id)
        if any(n["state"] == "failed" for n in nodes):
            status, err = "failed", "存在失败节点，下游已阻塞；修复后可重试"
        elif any(n["state"] == "blocked" for n in nodes):
            status, err = "partial", "部分节点因缺少上游被阻塞，其余节点已完成"
        else:
            status, err = "succeeded", None
        self.meta.update_run_status(run_id, status, err)


def _wait_any(pending: dict[Future, Any]) -> tuple[list[Future], list[Future]]:
    """轻量等待任意 future 完成，避免依赖 concurrent.futures.wait 的唤醒细节。"""
    from concurrent.futures import wait, FIRST_COMPLETED
    return wait(pending, return_when=FIRST_COMPLETED)
