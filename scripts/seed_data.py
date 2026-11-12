"""准备演示数据：在 DuckDB 中生成连续多天的原始订单。

用法:
  python -m scripts.seed_data                 # 生成默认 10 天订单
  python -m scripts.seed_data --days 8
  python -m scripts.seed_data --backfill 2026-10-30   # 重写某一天的源数据（签名随之变化）
"""
from __future__ import annotations

import argparse
import hashlib
from datetime import date, timedelta
from pathlib import Path

import duckdb

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = Path(__file__).resolve().parent.parent / "data"
DB_PATH = DATA_DIR / "warehouse.duckdb"

STORES = ["S01", "S02", "S03"]
PRODUCTS = ["P100", "P200", "P300", "P400"]
STATUSES = ["paid", "shipped", "done", "cancelled"]
START_DATE = date(2026, 10, 27)


def _rng(seed_text: str) -> int:
    return int(hashlib.sha256(seed_text.encode()).hexdigest(), 16)


def orders_for_day(day: date, variant: int = 0) -> list[tuple]:
    rows = []
    base = _rng(f"seed-{day.isoformat()}-{variant}")
    order_seq = 0
    for store in STORES:
        for product in PRODUCTS:
            # 每个门店×商品每天 2~4 单，确定性生成
            n = 2 + (base >> (order_seq % 24)) % 3
            for k in range(n):
                h = _rng(f"{day.isoformat()}-{store}-{product}-{k}-{variant}")
                status = STATUSES[h % 4]
                amount = round(10 + (h % 9000) / 10.0, 2)
                order_id = f"O{day.strftime('%Y%m%d')}{store}{product}{order_seq:03d}"
                rows.append((order_id, store, product, amount, status, day))
                order_seq += 1
    return rows


def ensure_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS orders_raw (
            order_id VARCHAR,
            store_id VARCHAR,
            product_id VARCHAR,
            amount DOUBLE,
            status VARCHAR,
            dt DATE
        )
        """
    )


def write_day(conn: duckdb.DuckDBPyConnection, day: date, variant: int = 0) -> int:
    rows = orders_for_day(day, variant)
    conn.execute("DELETE FROM orders_raw WHERE dt=?", [day])
    conn.executemany(
        "INSERT INTO orders_raw VALUES (?, ?, ?, ?, ?, ?)", rows
    )
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=10)
    parser.add_argument("--backfill", type=str, default=None,
                        help="重写指定日期（ISO）的源数据，模拟补齐/修正")
    parser.add_argument("--db", type=str, default=str(DB_PATH))
    args = parser.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(args.db)
    ensure_table(conn)

    if args.backfill:
        day = date.fromisoformat(args.backfill)
        n = write_day(conn, day, variant=7)
        print(f"backfilled {day.isoformat()}: {n} rows rewritten (new content/signature)")
    else:
        total = 0
        for i in range(args.days):
            day = START_DATE + timedelta(days=i)
            total += write_day(conn, day)
        print(f"seeded {args.days} days ({START_DATE} ~ "
              f"{START_DATE + timedelta(days=args.days - 1)}), {total} rows")
    conn.close()


if __name__ == "__main__":
    main()
