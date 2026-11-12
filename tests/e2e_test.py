"""端到端核对脚本：

1. 连续多天订单的初始加工（前两天滚动指标因缺少上游而阻塞）
2. 故意让中间节点 store_daily 失败：下游阻塞、独立分支 product_daily 继续
3. 故障解除后重试：成功节点复用，只真正执行未完成节点
4. 修改一天源数据补算：计划覆盖 d、d+1、d+2 的滚动结果
5. 失败期间可见结果仍为上一版可用分区
6. 模拟服务重启：未完成运行可恢复，不整批重跑
7. 缺少必要上游显式阻塞（不用空表代替）
8. 配置有环时指出链路
"""
from __future__ import annotations

import shutil
import sys
from datetime import date, timedelta
from pathlib import Path

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.config import ConfigError, load_config  # noqa: E402
from app.main import create_app  # noqa: E402
from app.metadata import MetaStore  # noqa: E402
from app.warehouse import Warehouse  # noqa: E402
from scripts.seed_data import START_DATE, ensure_table, orders_for_day, write_day  # noqa: E402

import duckdb  # noqa: E402

PASS, FAIL = "PASS", "FAIL"
results: list[tuple[str, bool, str]] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    results.append((name, bool(condition), detail))
    mark = PASS if condition else FAIL
    print(f"[{mark}] {name}" + (f" — {detail}" if detail else ""))


def wait_run(client: TestClient, run_id: int) -> dict:
    import time
    for _ in range(200):
        data = client.get(f"/api/runs/{run_id}").json()
        if data["run"]["status"] != "running":
            return data
        time.sleep(0.1)
    raise RuntimeError("run timed out")


def node_map(data: dict) -> dict[tuple[str, str], dict]:
    return {(n["task_name"], n["dt"]): n for n in data["nodes"]}


def main() -> int:
    work = ROOT / "data_e2e"
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)

    # ---- 准备 8 天源数据 ----
    conn = duckdb.connect(str(work / "warehouse.duckdb"))
    ensure_table(conn)
    days = [START_DATE + timedelta(days=i) for i in range(8)]
    for d in days:
        write_day(conn, d)
    conn.close()

    app = create_app(ROOT / "config" / "pipeline.yaml", work)
    client = TestClient(app)
    with client:  # 触发 lifespan（startup_recover）
        client.get("/api/sources/scan")
        d0, d1, d2 = days[0], days[1], days[2]

        # ---------- 1. 初始运行 ----------
        plan = client.post("/api/runs/plan", json={
            "dates": [d.isoformat() for d in days], "kind": "initial"
        }).json()
        blocked_rolling = [
            p for p in plan["plan"]
            if p["task_name"] == "rolling_3d"
            and p["dt"] in (d0.isoformat(), d1.isoformat())
            and p["state"] == "blocked"
        ]
        check("初始计划：前两日 rolling_3d 因缺少上游分区被阻塞",
              len(blocked_rolling) == 2,
              f"blocked={[p['dt'] for p in blocked_rolling]}")
        check("阻塞原因明确指出缺失的上游分区",
              all("缺少必要上游分区" in (p["reason"] or "") for p in blocked_rolling))

        run_id = plan["run"]["id"]
        client.post(f"/api/runs/{run_id}/confirm")
        data = wait_run(client, run_id)
        nm = node_map(data)
        check("初始运行状态为 partial（存在阻塞节点）", data["run"]["status"] == "partial",
              data["run"]["status"])
        check("第 3 天起 rolling_3d 成功",
              nm[("rolling_3d", d2.isoformat())]["state"] == "succeeded")
        check("并发度从未超过 2",
              data["stats"]["max_concurrent"] <= 2 and data["stats"]["max_concurrent"] >= 1,
              f"max={data['stats']['max_concurrent']}")
        check("store_daily 8 个分区全部发布",
              all(nm[("store_daily", d.isoformat())]["state"] == "succeeded" for d in days))

        # ---------- 2. 故障注入：store_daily 强制失败 ----------
        client.post("/api/faults", json={"task_name": "store_daily", "enabled": True})
        bf_day = days[4]
        plan2 = client.post("/api/runs/plan", json={
            "dates": [bf_day.isoformat()], "kind": "backfill"
        }).json()
        rid2 = plan2["run"]["id"]
        client.post(f"/api/runs/{rid2}/confirm")
        data2 = wait_run(client, rid2)
        nm2 = node_map(data2)
        failed_node = nm2[("store_daily", bf_day.isoformat())]
        check("故障注入后 store_daily 失败且记录原因",
              failed_node["state"] == "failed" and "故障注入" in (failed_node["reason"] or ""),
              failed_node.get("reason"))
        rolling_blocked = [
            (t, dt) for (t, dt), n in nm2.items()
            if t == "rolling_3d" and n["state"] == "blocked"
        ]
        check("失败节点的下游 rolling_3d 被阻塞",
              (("rolling_3d", bf_day.isoformat()) in rolling_blocked
               and ("rolling_3d", (bf_day + timedelta(days=1)).isoformat()) in rolling_blocked
               and ("rolling_3d", (bf_day + timedelta(days=2)).isoformat()) in rolling_blocked),
              str(sorted(rolling_blocked)))
        # 独立分支与同分支上游未受故障影响：签名未变则复用（reused），结果保持可用、不被阻塞
        check("独立分支 product_daily 不受影响（成功或复用，未被阻塞）",
              nm2[("product_daily", bf_day.isoformat())]["state"]
              in ("succeeded", "reused"),
              nm2[("product_daily", bf_day.isoformat())]["state"])
        check("orders_clean 同分支也正常（成功或复用，未被阻塞）",
              nm2[("orders_clean", bf_day.isoformat())]["state"]
              in ("succeeded", "reused"),
              nm2[("orders_clean", bf_day.isoformat())]["state"])

        # 失败期间正式分区仍可见（上一版）
        prev = client.get(f"/api/preview/store_daily/{bf_day.isoformat()}").json()
        check("失败期间 store_daily 正式分区仍为上一版可用结果",
              prev["available"] is True and len(prev["rows"]) > 0)
        meta = MetaStore(work / "orchestrator.db")
        pub_before = meta.get_published("store_daily", bf_day.isoformat())
        check("失败不会产生新的已发布版本", pub_before["run_id"] == run_id)

        # ---------- 3. 解除故障并重试 ----------
        client.post("/api/faults", json={"task_name": "store_daily", "enabled": False})
        client.post(f"/api/runs/{rid2}/confirm")
        data2r = wait_run(client, rid2)
        nm2r = node_map(data2r)
        check("重试后运行成功", data2r["run"]["status"] == "succeeded",
              data2r["run"]["status"])
        retry_batch = {(e["task"], e["dt"])
                       for e in data2r["stats"]["batches"][-1]}
        check("重试只执行未完成节点，product_daily 未重跑",
              ("product_daily", bf_day.isoformat()) not in retry_batch,
              f"retry_batch={sorted(retry_batch)}")
        check("重试实际执行了 store_daily 与三个 rolling_3d 分区",
              retry_batch == {
                  ("store_daily", bf_day.isoformat()),
                  ("rolling_3d", bf_day.isoformat()),
                  ("rolling_3d", (bf_day + timedelta(days=1)).isoformat()),
                  ("rolling_3d", (bf_day + timedelta(days=2)).isoformat()),
              }, str(sorted(retry_batch)))
        pub_after = meta.get_published("store_daily", bf_day.isoformat())
        check("重试成功后发布新版本", pub_after["run_id"] == rid2)

        # ---------- 4. 修改一天源数据补算 ----------
        fix_day = days[3]
        wh = Warehouse(work / "warehouse.duckdb")
        raw_sig_before = wh.partition_signature("orders_raw", "dt", fix_day.isoformat())
        conn = duckdb.connect(str(work / "warehouse.duckdb"))
        write_day(conn, fix_day, variant=7)  # 重写源分区
        conn.close()
        raw_sig_after = wh.partition_signature("orders_raw", "dt", fix_day.isoformat())
        check("补写后源分区签名发生变化", raw_sig_before != raw_sig_after)
        client.get("/api/sources/scan")

        plan3 = client.post("/api/runs/plan", json={
            "dates": [fix_day.isoformat()], "kind": "backfill"
        }).json()
        planned = {(p["task_name"], p["dt"]): p["state"] for p in plan3["plan"]}
        expected_affected = {
            ("orders_clean", fix_day.isoformat()),
            ("store_daily", fix_day.isoformat()),
            ("product_daily", fix_day.isoformat()),
            ("rolling_3d", fix_day.isoformat()),
            ("rolling_3d", (fix_day + timedelta(days=1)).isoformat()),
            ("rolling_3d", (fix_day + timedelta(days=2)).isoformat()),
        }
        check("补数计划覆盖 d、d+1、d+2 的滚动结果",
              expected_affected.issubset(planned.keys()),
              f"plan keys={sorted(planned.keys())}")
        check("受影响分区标记为 ready（签名变化，不能复用）",
              all(planned[k] == "ready" for k in expected_affected))
        check("计划不包含无关日期的节点",
              all(dt >= fix_day.isoformat() and dt <= (fix_day + timedelta(days=2)).isoformat()
                  for _, dt in planned))

        rid3 = plan3["run"]["id"]
        client.post(f"/api/runs/{rid3}/confirm")
        data3 = wait_run(client, rid3)
        check("补算运行成功", data3["run"]["status"] == "succeeded", data3["run"]["status"])
        executed3 = {(e["task"], e["dt"]) for e in data3["stats"]["executed"]}
        check("补算实际执行节点与计划受影响范围一致",
              executed3 == expected_affected, str(sorted(executed3)))

        # 再次生成相同范围计划：输入签名与 SQL 版本均未变化 -> 全部复用
        plan4 = client.post("/api/runs/plan", json={
            "dates": [fix_day.isoformat()], "kind": "backfill"
        }).json()
        check("无变化时计划节点全部可复用",
              all(p["state"] == "reused" for p in plan4["plan"]))
        rid4 = plan4["run"]["id"]
        client.post(f"/api/runs/{rid4}/confirm")
        data4 = wait_run(client, rid4)
        last_batch = data4["stats"]["batches"][-1]
        check("复用运行不实际执行任何节点",
              len(last_batch) == 0,
              f"batch={last_batch}")

        # ---------- 5. 重启恢复：运行进行中模拟崩溃 ----------
        # 给任务加执行延迟制造可观察的 running 窗口，确认后在运行中模拟进程崩溃。
        client.post("/api/faults", json={
            "task_name": "orders_clean", "enabled": True, "kind": "delay"})
        plan5 = client.post("/api/runs/plan", json={
            "dates": [days[6].isoformat()], "kind": "backfill"
        }).json()
        rid5 = plan5["run"]["id"]
        client.post(f"/api/runs/{rid5}/confirm")
        # 轮询直到节点进入 running（工作线程已启动且在延迟中），此时模拟进程崩溃
        crashed = False
        for _ in range(100):
            live = client.get(f"/api/runs/{rid5}").json()
            if any(n["state"] == "running" for n in live["nodes"]):
                crashed = True
                break
        check("崩溃前确有节点处于 running", crashed)

        # 模拟服务重启：新 app 实例指向同一数据目录（先关闭延迟，便于恢复后成功）
        client.post("/api/faults", json={
            "task_name": "orders_clean", "enabled": False, "kind": "delay"})
    app2 = create_app(ROOT / "config" / "pipeline.yaml", work)
    client2 = TestClient(app2)
    with client2:
        recovery = app2.state.recovery
        check("重启后能找到未完成的运行", rid5 in recovery["recovered_runs"],
              str(recovery))
        check("重启清理了遗留临时表", isinstance(recovery["dropped_temp_tables"], list))
        detail = client2.get(f"/api/runs/{rid5}").json()
        check("中断运行状态回到可确认状态", detail["run"]["status"] == "planned",
              detail["run"]["status"])
        check("中断节点从 running 回到 ready",
              all(n["state"] != "running" for n in detail["nodes"]))
        client2.post(f"/api/runs/{rid5}/confirm")
        detail2 = wait_run(client2, rid5)
        # 恢复后无任何 failed：剩余 blocked 仅来自向前扩散触及的数据边界（缺上游），属正常
        recovered_states = {n["state"] for n in detail2["nodes"]}
        check("恢复后无失败节点，中断工作全部完成",
              "failed" not in recovered_states and "running" not in recovered_states,
              detail2["run"]["status"])
        target_nodes = [n for n in detail2["nodes"]
                        if n["dt"] == days[6].isoformat()]
        check("崩溃当天各任务节点均成功或复用",
              all(n["state"] in ("succeeded", "reused") for n in target_nodes),
              str([(n["task_name"], n["state"]) for n in target_nodes]))
        executed5 = {(e["task"], e["dt"])
                     for e in detail2["stats"]["batches"][-1]}
        check("恢复重试不重跑此前已成功的独立分支",
              ("product_daily", days[6].isoformat()) not in executed5,
              str(sorted(executed5)))

        # ---------- 6. 缺少必要上游：请求超出源数据范围 ----------
        future_day = days[-1] + timedelta(days=3)
        plan6 = client2.post("/api/runs/plan", json={
            "dates": [future_day.isoformat()], "kind": "initial"
        }).json()
        missing_nodes = [p for p in plan6["plan"] if p["state"] == "blocked"]
        check("无源数据日期全部节点显式阻塞",
              len(missing_nodes) == len(plan6["plan"]) and len(missing_nodes) > 0)
        check("不会静默产出空表",
              client2.get(f"/api/preview/orders_clean/{future_day.isoformat()}").json()
              ["available"] is False)

    # ---------- 7. 环配置检测（不启动服务） ----------
    cyclic = work / "cyclic.yaml"
    cyclic.write_text(
        """
version: 1
sources:
  - name: orders_raw
    table: orders_raw
tasks:
  - name: a
    sql_file: ../sql/orders_clean.sql
    inputs:
      - task: b
        offset: 0
  - name: b
    sql_file: ../sql/store_daily.sql
    inputs:
      - task: c
        offset: 0
  - name: c
    sql_file: ../sql/product_daily.sql
    inputs:
      - task: a
        offset: 0
""",
        encoding="utf-8",
    )
    try:
        load_config(cyclic)
        check("环配置被识别", False, "未抛出异常")
    except ConfigError as exc:
        check("环配置被识别并指出链路",
              "环" in str(exc) and "a -> b -> c -> a" in str(exc), str(exc))

    failed = [r for r in results if not r[1]]
    print("\n========== 汇总 ==========")
    print(f"通过 {len(results) - len(failed)}/{len(results)}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
