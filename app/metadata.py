"""SQLite 元数据存储：运行/节点状态、输入输出签名、源分区登记、故障注入。

SQLite 只承担编排状态，实际数据与分区内容都在 DuckDB 中。
"""
from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,                 -- initial | backfill
    requested_dates TEXT NOT NULL,      -- JSON 数组
    seeds TEXT NOT NULL DEFAULT '[]',   -- JSON: 用户显式要求计算的 (task, dt) 种子
    horizon_end TEXT NOT NULL,
    status TEXT NOT NULL,               -- planned | running | succeeded | partial | failed
    error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS run_nodes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES runs(id),
    task_name TEXT NOT NULL,
    dt TEXT NOT NULL,
    state TEXT NOT NULL,                -- planned|ready|running|succeeded|reused|failed|blocked|skipped
    reason TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    sql_version TEXT,
    input_signature TEXT,
    output_signature TEXT,
    started_at TEXT,
    finished_at TEXT,
    UNIQUE(run_id, task_name, dt)
);

CREATE TABLE IF NOT EXISTS published_partitions (
    task_name TEXT NOT NULL,
    dt TEXT NOT NULL,
    run_id INTEGER NOT NULL,
    sql_version TEXT NOT NULL,
    input_signature TEXT NOT NULL,
    output_signature TEXT NOT NULL,
    published_at TEXT NOT NULL,
    PRIMARY KEY (task_name, dt)
);

CREATE TABLE IF NOT EXISTS source_partitions (
    source_name TEXT NOT NULL,
    dt TEXT NOT NULL,
    signature TEXT NOT NULL,
    registered_at TEXT NOT NULL,
    PRIMARY KEY (source_name, dt)
);

CREATE TABLE IF NOT EXISTS fault_switches (
    key TEXT PRIMARY KEY,
    enabled INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL
);
"""


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class MetaStore:
    def __init__(self, path: str | Path):
        self.path = str(path)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(SCHEMA)
        self._migrate()
        self._conn.commit()

    def _migrate(self) -> None:
        cols = {r["name"] for r in
                self._conn.execute("PRAGMA table_info(runs)").fetchall()}
        if "seeds" not in cols:
            self._conn.execute(
                "ALTER TABLE runs ADD COLUMN seeds TEXT NOT NULL DEFAULT '[]'"
            )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ---------- runs ----------
    def create_run(self, kind: str, dates: list[str], horizon_end: str,
                   seeds: list[tuple[str, str]] | None = None) -> int:
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO runs(kind, requested_dates, seeds, horizon_end, status, "
                "created_at, updated_at) VALUES (?,?,?,?,?,?,?)",
                (kind, json.dumps(dates), json.dumps(
                    [list(s) for s in (seeds or [])]),
                 horizon_end, "planned", utcnow(), utcnow()),
            )
            self._conn.commit()
            return int(cur.lastrowid)

    def update_run_status(self, run_id: int, status: str, error: str | None = None) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE runs SET status=?, error=?, updated_at=? WHERE id=?",
                (status, error, utcnow(), run_id),
            )
            self._conn.commit()

    def get_run(self, run_id: int) -> dict[str, Any]:
        with self._lock:
            row = self._conn.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
            if row is None:
                raise KeyError(f"run {run_id} 不存在")
            d = dict(row)
            d["requested_dates"] = json.loads(d["requested_dates"])
            d["seeds"] = [tuple(x) for x in json.loads(d.get("seeds") or "[]")]
            return d

    def list_runs(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM runs ORDER BY id DESC").fetchall()
            out = []
            for row in rows:
                d = dict(row)
                d["requested_dates"] = json.loads(d["requested_dates"])
                d["seeds"] = [tuple(x) for x in json.loads(d.get("seeds") or "[]")]
                out.append(d)
            return out

    def find_unfinished_runs(self) -> list[int]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT id FROM runs WHERE status IN ('planned','running') ORDER BY id"
            ).fetchall()
            return [r["id"] for r in rows]

    # ---------- nodes ----------
    def upsert_node(self, run_id: int, task_name: str, dt: str, state: str,
                    reason: str | None = None) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO run_nodes(run_id, task_name, dt, state, reason) VALUES (?,?,?,?,?) "
                "ON CONFLICT(run_id, task_name, dt) DO UPDATE SET state=excluded.state, "
                "reason=COALESCE(excluded.reason, run_nodes.reason)",
                (run_id, task_name, dt, state, reason),
            )
            self._conn.commit()

    def bulk_upsert_nodes(self, nodes: Iterable[tuple[int, str, str, str, str | None]]) -> None:
        with self._lock:
            self._conn.executemany(
                "INSERT INTO run_nodes(run_id, task_name, dt, state, reason) VALUES (?,?,?,?,?) "
                "ON CONFLICT(run_id, task_name, dt) DO UPDATE SET state=excluded.state, "
                "reason=excluded.reason",
                nodes,
            )
            self._conn.commit()

    def update_node(self, run_id: int, task_name: str, dt: str, **fields: Any) -> None:
        if not fields:
            return
        cols = list(fields.keys())
        assignments = ", ".join(f"{c}=?" for c in cols)
        vals = [fields[c] for c in cols]
        with self._lock:
            self._conn.execute(
                f"UPDATE run_nodes SET {assignments} WHERE run_id=? AND task_name=? AND dt=?",
                (*vals, run_id, task_name, dt),
            )
            self._conn.commit()

    def set_node_state(self, run_id: int, task_name: str, dt: str, state: str,
                       reason: str | None = None) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE run_nodes SET state=?, reason=? WHERE run_id=? AND task_name=? AND dt=?",
                (state, reason, run_id, task_name, dt),
            )
            self._conn.commit()

    def list_nodes(self, run_id: int) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM run_nodes WHERE run_id=? ORDER BY id", (run_id,)
            ).fetchall()
            return [dict(r) for r in rows]

    def get_node(self, run_id: int, task_name: str, dt: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM run_nodes WHERE run_id=? AND task_name=? AND dt=?",
                (run_id, task_name, dt),
            ).fetchone()
            return dict(row) if row else None

    # ---------- published partitions ----------
    def get_published(self, task_name: str, dt: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM published_partitions WHERE task_name=? AND dt=?",
                (task_name, dt),
            ).fetchone()
            return dict(row) if row else None

    def mark_published(self, run_id: int, task_name: str, dt: str, sql_version: str,
                       input_signature: str, output_signature: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO published_partitions(task_name, dt, run_id, sql_version, "
                "input_signature, output_signature, published_at) VALUES (?,?,?,?,?,?,?) "
                "ON CONFLICT(task_name, dt) DO UPDATE SET run_id=excluded.run_id, "
                "sql_version=excluded.sql_version, input_signature=excluded.input_signature, "
                "output_signature=excluded.output_signature, published_at=excluded.published_at",
                (task_name, dt, run_id, sql_version, input_signature, output_signature, utcnow()),
            )
            self._conn.commit()

    def list_published(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM published_partitions ORDER BY task_name, dt"
            ).fetchall()
            return [dict(r) for r in rows]

    # ---------- source partitions ----------
    def register_source(self, source_name: str, dt: str, signature: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO source_partitions(source_name, dt, signature, registered_at) "
                "VALUES (?,?,?,?) ON CONFLICT(source_name, dt) DO UPDATE SET "
                "signature=excluded.signature, registered_at=excluded.registered_at",
                (source_name, dt, signature, utcnow()),
            )
            self._conn.commit()

    def get_source_partition(self, source_name: str, dt: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM source_partitions WHERE source_name=? AND dt=?",
                (source_name, dt),
            ).fetchone()
            return dict(row) if row else None

    def list_source_partitions(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM source_partitions ORDER BY source_name, dt"
            ).fetchall()
            return [dict(r) for r in rows]

    # ---------- fault injection ----------
    def set_fault(self, key: str, enabled: bool) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO fault_switches(key, enabled, updated_at) VALUES (?,?,?) "
                "ON CONFLICT(key) DO UPDATE SET enabled=excluded.enabled, updated_at=excluded.updated_at",
                (key, 1 if enabled else 0, utcnow()),
            )
            self._conn.commit()

    def get_fault(self, key: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT enabled FROM fault_switches WHERE key=?", (key,)
            ).fetchone()
            return bool(row["enabled"]) if row else False

    def list_faults(self) -> dict[str, bool]:
        with self._lock:
            rows = self._conn.execute("SELECT key, enabled FROM fault_switches").fetchall()
            return {r["key"]: bool(r["enabled"]) for r in rows}
