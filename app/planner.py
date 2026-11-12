"""执行计划：闭包扩散、阻塞判定、节点复用判定。

闭包规则：
- 种子节点 = 请求日期 × 全部任务（补数场景下独立分支也会被重新评估）。
- 向后扩散：节点所需的每个上游分区（按 offset）都要进入计划。
- 向前扩散：若某个上游分区被纳入计划，其所有下游同日期分区也要纳入
  （rolling_3d 显式依赖 offset -1/-2，因此补数 d 会带动滚动 d+1、d+2）。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from .config import PipelineConfig
from .metadata import MetaStore
from .warehouse import Warehouse

NODE_STATES = {
    "planned", "ready", "running", "succeeded", "reused",
    "failed", "blocked", "skipped",
}


@dataclass
class Node:
    task_name: str
    dt: str

    @property
    def key(self) -> tuple[str, str]:
        return self.task_name, self.dt


def daterange(start: date, end: date) -> list[date]:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


class Planner:
    def __init__(self, cfg: PipelineConfig, meta: MetaStore, wh: Warehouse):
        self.cfg = cfg
        self.meta = meta
        self.wh = wh
        self.downstream = cfg.downstream_tasks()

    # ---------- 闭包扩散 ----------
    def expand(self, requested: list[date], horizon_end: date,
               seeds: set[tuple[str, str]] | None = None) -> set[tuple[str, str]]:
        """只做种子 + 向前闭包扩散。

        - 种子 = 请求日期 × 全部任务（用户显式要求计算的范围）。
        - 向前：节点的下游消费者按 edge offset 计算消费日期后加入
          （rolling_3d 的 offset -1/-2 使补数 d 带动滚动 d+1、d+2）。
        - 不做后向扩散：已存在的上游分区只是输入签名的一部分，不是本运行节点；
          真正缺失的上游由 evaluate_node 判定为 blocked，不用空表代替。
        """
        nodes: set[tuple[str, str]] = set(seeds or ())
        if not nodes:
            for d in requested:
                for task_name in self.cfg.tasks:
                    nodes.add((task_name, d.isoformat()))

        changed = True
        while changed:
            changed = False
            for task_name, dt_str in list(nodes):
                if task_name not in self.cfg.tasks:
                    continue  # 防御：源节点无任务下游
                dt = date.fromisoformat(dt_str)
                for consumer in self.downstream.get(task_name, ()):
                    consumer_spec = self.cfg.tasks[consumer]
                    for ref in consumer_spec.inputs:
                        if ref.kind != "task" or ref.task != task_name:
                            continue
                        consumer_dt = dt - timedelta(days=ref.offset)
                        if consumer_dt > horizon_end:
                            continue
                        ckey = (consumer, consumer_dt.isoformat())
                        if ckey not in nodes:
                            nodes.add(ckey)
                            changed = True
        return nodes

    # ---------- 输入签名与上游可用性 ----------
    def _input_partition(self, ref, node_dt: date) -> tuple[str, str, str, bool]:
        """返回 (显示名, 物理表, 上游日期, 是否存在)。"""
        up_dt = (node_dt + timedelta(days=ref.offset)).isoformat()
        if ref.kind == "source":
            src = self.cfg.sources[ref.name]
            exists = self.wh.partition_exists(src.table, src.partition_column, up_dt)
            return f"{ref.name}@{up_dt}", src.table, up_dt, exists
        table = Warehouse.task_table(ref.name)
        pcol = self.cfg.tasks[ref.name].partition_column
        exists = self.wh.partition_exists(table, pcol, up_dt)
        return f"{ref.name}@{up_dt}", table, up_dt, exists

    def evaluate_node(self, task_name: str, dt_str: str,
                      in_plan: set[tuple[str, str]] | None = None) -> dict[str, Any]:
        spec = self.cfg.tasks[task_name]
        node_dt = date.fromisoformat(dt_str)
        in_plan = in_plan or set()
        missing: list[str] = []
        pending_inputs: list[str] = []
        input_parts: list[str] = []

        for ref in spec.inputs:
            display, table, up_dt, exists = self._input_partition(ref, node_dt)
            if not exists:
                # 同运行计划内的任务节点会在本批次产出，属于等待而非阻塞；
                # 源分区或计划外的任务分区才是真正缺失，不能用空表代替。
                if ref.kind == "task" and (ref.name, up_dt) in in_plan:
                    pending_inputs.append(display)
                    continue
                missing.append(display)
                continue
            if ref.kind == "source":
                pcol = self.cfg.sources[ref.name].partition_column
                registered = self.meta.get_source_partition(ref.name, up_dt)
                if registered is not None:
                    input_parts.append(f"{ref.name}:{up_dt}:{registered['signature']}")
                else:
                    input_parts.append(
                        f"{ref.name}:{up_dt}:{self.wh.partition_signature(table, pcol, up_dt)}"
                    )
            else:
                pcol = self.cfg.tasks[ref.name].partition_column
                input_parts.append(
                    f"{ref.name}:{up_dt}:{self.wh.partition_signature(table, pcol, up_dt)}"
                )

        import hashlib
        input_signature = hashlib.sha256("|".join(input_parts).encode()).hexdigest()
        published = self.meta.get_published(task_name, dt_str)
        # 故障注入/延迟开关命中的任务必须重新执行（用于演练失败、重试与崩溃恢复），不可复用。
        fault_forced = (
            self.meta.get_fault(f"fail:{task_name}")
            or self.meta.get_fault(f"delay:{task_name}")
        )
        # 复用规则（题目要求）：已发布、输入签名未变化、SQL 版本未变化即复用。
        # 种子只决定计划覆盖范围，不强制重算；源数据补写会改变输入签名，从而自然失效。
        reusable = (
            not fault_forced
            and published is not None
            and not missing
            and published["sql_version"] == spec.sql_version
            and published["input_signature"] == input_signature
        )
        return {
            "task_name": task_name,
            "dt": dt_str,
            "missing": missing,
            "pending_inputs": pending_inputs,
            "input_signature": input_signature,
            "sql_version": spec.sql_version,
            "reusable": reusable,
            "fault_forced": fault_forced,
            "published": published,
        }

    # ---------- 计划构建 ----------
    def build_plan(self, run_id: int, requested: list[date],
                   horizon_end: date,
                   seeds: set[tuple[str, str]] | None = None) -> list[dict[str, Any]]:
        seed_set = seeds or {(t, d.isoformat())
                             for d in requested for t in self.cfg.tasks}
        all_keys = self.expand(requested, horizon_end, seed_set)
        keys = {k for k in all_keys if k[0] in self.cfg.tasks}
        evaluated = {
            k: self.evaluate_node(k[0], k[1], keys) for k in keys
        }

        states: dict[tuple[str, str], str] = {}
        reasons: dict[tuple[str, str], str | None] = {}
        for k in keys:
            ev = evaluated[k]
            if ev["missing"]:
                states[k] = "blocked"
                reasons[k] = "缺少必要上游分区: " + ", ".join(ev["missing"])
            elif ev["reusable"]:
                states[k] = "reused"
                reasons[k] = "输入签名与 SQL 版本均未变化，复用上一版成功结果"
            else:
                states[k] = "ready"
                reasons[k] = None

        # 复用具有传递性：若某节点的计划内任务上游不是 reused（即本运行会重算或已被阻塞），
        # 则该节点也不能复用，必须等待上游新结果后重新计算。
        changed = True
        while changed:
            changed = False
            for (task_name, dt_str) in keys:
                if states[(task_name, dt_str)] != "reused":
                    continue
                dt = date.fromisoformat(dt_str)
                for ref in self.cfg.tasks[task_name].inputs:
                    if ref.kind != "task":
                        continue
                    up_dt = (dt + timedelta(days=ref.offset)).isoformat()
                    ukey = (ref.task, up_dt)
                    if ukey in keys and states[ukey] != "reused":
                        states[(task_name, dt_str)] = "ready"
                        reasons[(task_name, dt_str)] = (
                            f"上游 {ref.task}@{up_dt} 本运行将重新计算，本节点需级联重算"
                        )
                        changed = True
                        break

        # 阻塞沿边向下游传播：只有真正缺失上游的节点才是阻塞源，
        # 其下游（按 offset 精确计算）继承阻塞，原因指向被阻塞的上游。
        propagation: dict[tuple[str, str], str] = {}
        changed = True
        while changed:
            changed = False
            for (task_name, dt_str) in list(keys):
                if states[(task_name, dt_str)] != "blocked":
                    continue
                dt = date.fromisoformat(dt_str)
                for consumer in self.downstream.get(task_name, ()):
                    for ref in self.cfg.tasks[consumer].inputs:
                        if ref.kind != "task" or ref.task != task_name:
                            continue
                        consumer_dt = (dt - timedelta(days=ref.offset)).isoformat()
                        ckey = (consumer, consumer_dt)
                        if ckey not in keys or ckey in propagation:
                            continue
                        reason = f"上游 {task_name}@{dt_str} 被阻塞，本节点不可运行"
                        propagation[ckey] = reason
                        if states[ckey] != "blocked":
                            states[ckey] = "blocked"
                            reasons[ckey] = reason
                            changed = True
                        elif reasons[ckey] and "缺少必要上游分区" in reasons[ckey]:
                            # 自身也缺上游时保留更具体的原因
                            propagation[ckey] = reasons[ckey]

        rows = []
        for (task_name, dt_str) in sorted(keys, key=lambda k: (k[1], k[0])):
            ev = evaluated[(task_name, dt_str)]
            rows.append({
                "run_id": run_id,
                "task_name": task_name,
                "dt": dt_str,
                "state": states[(task_name, dt_str)],
                "reason": reasons[(task_name, dt_str)],
                "missing": ev["missing"],
                "input_signature": ev["input_signature"],
                "sql_version": ev["sql_version"],
                "reusable": ev["reusable"],
            })
        return rows
