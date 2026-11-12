"""DuckDB 数据层：SQL 渲染、临时分区写入、事务化发布、分区签名与预览。

约定：
- 任务正式表名: task_<任务名>；源表名取配置中的 table。
- 临时表名: tmp_r<运行id>_<任务名>_<yyyymmdd>，每个输出分区先在临时位置完成。
- 发布在单个事务内完成（DELETE 旧分区 + INSERT 新分区），失败时旧版本保持可用。
"""
from __future__ import annotations

import hashlib
import re
import threading
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import duckdb

from .config import PipelineConfig, TaskSpec

PLACEHOLDER_RE = re.compile(r"\$\{([a-zA-Z0-9_:\-]+)\}")
TEMP_PREFIX = "tmp_r"


class SQLError(Exception):
    """SQL 执行失败。"""


class Warehouse:
    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        # DuckDB 连接不是为多线程并发写入设计的；统一单连接 + 锁串行化。
        # 编排层的两并发槽位控制节点调度，数据访问在这里排队，避免 native 崩溃。
        self._lock = threading.RLock()
        self._conn = duckdb.connect(self.db_path)

    # ---------- connections ----------
    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @staticmethod
    def task_table(task_name: str) -> str:
        return f"task_{task_name}"

    @staticmethod
    def temp_table(run_id: int, task_name: str, dt: str) -> str:
        return f"{TEMP_PREFIX}{run_id}_{task_name}_{dt.replace('-', '')}"

    # ---------- SQL rendering ----------
    @staticmethod
    def _date_literal(d: date) -> str:
        return f"DATE '{d.isoformat()}'"

    def render(self, sql_text: str, target_table: str, cfg: PipelineConfig,
               dt: date) -> str:
        def repl(match: re.Match) -> str:
            token = match.group(1)
            if token == "target":
                return target_table
            if token == "dt":
                return self._date_literal(dt)
            m = re.fullmatch(r"dt_minus_(\d+)", token)
            if m:
                return self._date_literal(dt - timedelta(days=int(m.group(1))))
            m = re.fullmatch(r"dt_plus_(\d+)", token)
            if m:
                return self._date_literal(dt + timedelta(days=int(m.group(1))))
            m = re.fullmatch(r"table:([a-zA-Z0-9_]+)", token)
            if m:
                name = m.group(1)
                if name in cfg.sources:
                    return cfg.sources[name].table
                if name in cfg.tasks:
                    return self.task_table(name)
                raise SQLError(f"SQL 引用了未知表 {name}")
            raise SQLError(f"SQL 中存在无法解析的占位符 ${{{token}}}")

        return PLACEHOLDER_RE.sub(repl, sql_text)

    # ---------- introspection ----------
    def table_exists(self, table: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_name=?", [table]
            ).fetchone()
            return bool(row and row[0] > 0)

    def partition_exists(self, table: str, partition_column: str, dt: str) -> bool:
        with self._lock:
            if not self.table_exists(table):
                return False
            row = self._conn.execute(
                f"SELECT count(*) FROM {table} WHERE {partition_column}=?", [dt]
            ).fetchone()
            return bool(row and row[0] > 0)

    def partition_signature(self, table: str, partition_column: str, dt: str) -> str:
        """对分区全部行做稳定哈希；空表也有签名，但调用方应先判断分区是否存在。"""
        with self._lock:
            rows = self._conn.execute(
                f"SELECT * FROM {table} WHERE {partition_column}=? ORDER BY ALL", [dt]
            ).fetchall()
        digest = hashlib.sha256()
        digest.update(table.encode())
        digest.update(repr(rows).encode("utf-8"))
        return digest.hexdigest()

    def source_partition_dates(self, table: str, partition_column: str) -> list[str]:
        with self._lock:
            if not self.table_exists(table):
                return []
            rows = self._conn.execute(
                f"SELECT DISTINCT {partition_column}::VARCHAR AS d FROM {table} ORDER BY d"
            ).fetchall()
            return [r[0] for r in rows]

    def preview(self, table: str, partition_column: str, dt: str, limit: int = 50) -> dict[str, Any]:
        with self._lock:
            if not self.partition_exists(table, partition_column, dt):
                return {"columns": [], "rows": [], "available": False}
            cur = self._conn.execute(
                f"SELECT * FROM {table} WHERE {partition_column}=? LIMIT ?", [dt, limit]
            )
            columns = [d[0] for d in cur.description]
            rows = [list(r) for r in cur.fetchall()]
        return {"columns": columns, "rows": rows, "available": True}

    # ---------- write path ----------
    def execute_node(self, run_id: int, spec: TaskSpec, cfg: PipelineConfig,
                     dt: date) -> str:
        """在临时位置构建输出分区，返回临时表名。"""
        dt_str = dt.isoformat()
        target = self.temp_table(run_id, spec.name, dt_str)
        rendered = self.render(spec.sql_text, target, cfg, dt)
        with self._lock:
            conn = self._conn
            conn.execute(f"DROP TABLE IF EXISTS {target}")
            try:
                conn.execute(rendered)
            except Exception as exc:  # duckdb 异常类型多样，统一包装
                conn.execute(f"DROP TABLE IF EXISTS {target}")
                raise SQLError(f"任务 {spec.name}@{dt_str} SQL 执行失败: {exc}") from exc
        return target

    def publish_node(self, run_id: int, spec: TaskSpec, dt: str) -> str:
        """事务化发布：确保正式表存在 -> 替换该分区 -> 删除临时表，返回输出签名。"""
        target = self.task_table(spec.name)
        tmp = self.temp_table(run_id, spec.name, dt)
        col = spec.partition_column
        with self._lock:
            conn = self._conn
            if not self.table_exists(target):
                conn.execute(f"CREATE TABLE {target} AS SELECT * FROM {tmp} WHERE 1=0")
            output_sig = self.partition_signature(tmp, col, dt)
            try:
                conn.execute("BEGIN TRANSACTION")
                conn.execute(f"DELETE FROM {target} WHERE {col}=?", [dt])
                conn.execute(f"INSERT INTO {target} SELECT * FROM {tmp}")
                conn.execute("COMMIT")
            except Exception as exc:
                conn.execute("ROLLBACK")
                raise SQLError(f"任务 {spec.name}@{dt} 发布失败: {exc}") from exc
            conn.execute(f"DROP TABLE IF EXISTS {tmp}")
        return output_sig

    def drop_temp(self, run_id: int, spec: TaskSpec, dt: str) -> None:
        tmp = self.temp_table(run_id, spec.name, dt)
        with self._lock:
            self._conn.execute(f"DROP TABLE IF EXISTS {tmp}")

    def cleanup_temp_tables(self) -> list[str]:
        """服务重启时清理上一进程遗留的临时表。"""
        rows = self._conn.execute(
            "SELECT table_name FROM information_schema.tables "
            f"WHERE table_name LIKE '{TEMP_PREFIX}%'"
        ).fetchall()
        dropped = [r[0] for r in rows]
        with self._lock:
            for name in dropped:
                self._conn.execute(f"DROP TABLE IF EXISTS {name}")
        return dropped
