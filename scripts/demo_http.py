"""通过真实 HTTP 接口走一遍完整演示流程，供人工核对与文档引用。

前置：
  1. 服务已启动：
     .venv/bin/python -m uvicorn app.main:create_app_once --factory --port 8000
  2. 源数据已 seed（python -m scripts.seed_data --days 8）且 /api/sources/scan 已登记。
"""
import json
import time
import urllib.request

BASE = "http://127.0.0.1:8000"


def call(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def wait_run(rid):
    for _ in range(100):
        d = call("GET", f"/api/runs/{rid}")
        if d["run"]["status"] != "running":
            return d
        time.sleep(0.2)
    raise RuntimeError("timeout")


print("== 健康检查与配置 ==")
print(call("GET", "/api/health"))
cfg = call("GET", "/api/config")
print("任务:", [t["name"] for t in cfg["tasks"]])

print("\n== 1. 初始加工 8 天 ==")
days = [f"2026-10-{d}" for d in range(27, 32)] + [f"2026-11-0{d}" for d in range(1, 4)]
plan = call("POST", "/api/runs/plan", {"dates": days, "kind": "initial"})
rid = plan["run"]["id"]
blocked = [(p["task_name"], p["dt"]) for p in plan["plan"] if p["state"] == "blocked"]
print(f"计划节点 {len(plan['plan'])} 个，阻塞 {len(blocked)} 个: {blocked}")
call("POST", f"/api/runs/{rid}/confirm")
d = wait_run(rid)
from collections import Counter
print("运行结果:", d["run"]["status"], dict(Counter(n["state"] for n in d["nodes"])),
      "峰值并发:", d["stats"]["max_concurrent"])

print("\n== 2. 故障注入：store_daily 强制失败，补算 10-31 ==")
call("POST", "/api/faults", {"task_name": "store_daily", "enabled": True})
p2 = call("POST", "/api/runs/plan", {"dates": ["2026-10-31"], "kind": "backfill"})
rid2 = p2["run"]["id"]
call("POST", f"/api/runs/{rid2}/confirm")
d2 = wait_run(rid2)
for n in d2["nodes"]:
    if n["state"] in ("failed", "blocked"):
        print(f"  {n['state']:8s} {n['task_name']}@{n['dt']} — {n['reason']}")
prev = call("GET", "/api/preview/store_daily/2026-10-31")
print("失败期间正式分区仍可见，行数:", len(prev["rows"]))

print("\n== 3. 解除故障并重试 ==")
call("POST", "/api/faults", {"task_name": "store_daily", "enabled": False})
call("POST", f"/api/runs/{rid2}/confirm")
d2r = wait_run(rid2)
print("重试结果:", d2r["run"]["status"])
print("重试批次实际执行:", [(e["task"], e["dt"]) for e in d2r["stats"]["batches"][-1]])

print("\n== 4. 修改一天源数据后补算 10-30 ==")
print("（请在另一个终端执行: python -m scripts.seed_data --backfill 2026-10-30，"
      "然后重新扫描源分区）")
print("演示脚本到此结束，补算可通过页面或再次调用 /api/runs/plan 完成。")
