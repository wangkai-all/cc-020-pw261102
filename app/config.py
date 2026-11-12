"""配置模型与加载：声明任务、SQL 文件、上游日期偏移，并做静态校验（含环检测）。"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from pathlib import Path

import yaml


class ConfigError(Exception):
    """配置非法（引用缺失、环、文件不存在等）。"""


@dataclass(frozen=True)
class InputRef:
    offset: int
    source: str | None = None
    task: str | None = None

    @property
    def kind(self) -> str:
        return "source" if self.source else "task"

    @property
    def name(self) -> str:
        return self.source or self.task or ""


@dataclass
class TaskSpec:
    name: str
    sql_file: str
    sql_text: str
    sql_version: str
    partition_column: str
    inputs: list[InputRef] = field(default_factory=list)


@dataclass
class SourceSpec:
    name: str
    table: str
    partition_column: str


@dataclass
class PipelineConfig:
    sources: dict[str, SourceSpec]
    tasks: dict[str, TaskSpec]
    raw: dict

    def input_refs(self, task_name: str) -> list[InputRef]:
        return self.tasks[task_name].inputs

    def downstream_tasks(self) -> dict[str, set[str]]:
        """task -> 直接下游 task（仅任务间依赖）。"""
        down: dict[str, set[str]] = {name: set() for name in self.tasks}
        for name, spec in self.tasks.items():
            for ref in spec.inputs:
                if ref.kind == "task":
                    down[ref.task].add(name)
        return down


def load_config(config_path: str | Path) -> PipelineConfig:
    config_path = Path(config_path).resolve()
    base_dir = config_path.parent
    with config_path.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}

    if not isinstance(raw, dict):
        raise ConfigError("配置文件顶层必须是映射结构")

    sources: dict[str, SourceSpec] = {}
    for item in raw.get("sources", []) or []:
        spec = SourceSpec(
            name=item["name"],
            table=item.get("table", item["name"]),
            partition_column=item.get("partition_column", "dt"),
        )
        if spec.name in sources:
            raise ConfigError(f"源 {spec.name} 重复定义")
        sources[spec.name] = spec

    tasks: dict[str, TaskSpec] = {}
    for item in raw.get("tasks", []) or []:
        name = item["name"]
        if name in sources:
            raise ConfigError(f"任务名 {name} 与源重名")
        if name in tasks:
            raise ConfigError(f"任务 {name} 重复定义")
        sql_rel = item["sql_file"]
        sql_path = (base_dir / sql_rel).resolve()
        if not sql_path.is_file():
            raise ConfigError(f"任务 {name} 的 SQL 文件不存在: {sql_rel}")
        sql_text = sql_path.read_text(encoding="utf-8")
        refs: list[InputRef] = []
        for inp in item.get("inputs", []) or []:
            offset = int(inp.get("offset", 0))
            if "source" in inp:
                if inp["source"] not in sources:
                    raise ConfigError(f"任务 {name} 引用了不存在的源 {inp['source']}")
                refs.append(InputRef(source=inp["source"], offset=offset))
            elif "task" in inp:
                refs.append(InputRef(task=inp["task"], offset=offset))
            else:
                raise ConfigError(f"任务 {name} 的输入必须声明 source 或 task")
        tasks[name] = TaskSpec(
            name=name,
            sql_file=sql_rel,
            sql_text=sql_text,
            sql_version=hashlib.sha256(sql_text.encode("utf-8")).hexdigest()[:16],
            partition_column=item.get("partition_column", "dt"),
            inputs=refs,
        )

    for spec in tasks.values():
        for ref in spec.inputs:
            if ref.kind == "task" and ref.task not in tasks:
                raise ConfigError(f"任务 {spec.name} 引用了不存在的任务 {ref.task}")

    _detect_cycle(tasks)
    return PipelineConfig(sources=sources, tasks=tasks, raw=raw)


def _detect_cycle(tasks: dict[str, TaskSpec]) -> None:
    WHITE, GRAY, BLACK = 0, 1, 2
    color = {name: WHITE for name in tasks}
    stack: list[str] = []

    def visit(node: str) -> None:
        color[node] = GRAY
        stack.append(node)
        for ref in tasks[node].inputs:
            if ref.kind != "task":
                continue
            nxt = ref.task
            if color[nxt] == GRAY:
                idx = stack.index(nxt)
                chain = stack[idx:] + [nxt]
                raise ConfigError(f"配置存在环: {' -> '.join(chain)}")
            if color[nxt] == WHITE:
                visit(nxt)
        stack.pop()
        color[node] = BLACK

    for name in tasks:
        if color[name] == WHITE:
            visit(name)
