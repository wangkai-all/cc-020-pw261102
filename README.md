# 点式补数 SQL 加工编排工具

某一天的源数据补齐后，只重算真正受影响的分区。通过 YAML 声明任务、SQL 文件与上游日期偏移；
页面先展示执行计划，确认后再运行。

技术栈：**Python + DuckDB（数据）+ SQLite（编排元数据）+ FastAPI + React/Vite**。

## 一、核心能力

- **配置声明式 DAG**：`config/pipeline.yaml` 声明源、任务、SQL 文件与上游 `offset`；
  启动时做引用校验和**环检测**，有环会明确报出链路（如 `a -> b -> c -> a`）。
- **点式补数的闭包扩散**：滚动指标显式依赖当天及前两天门店分区（offset 0/-1/-2），
  补数日期 `d` 会自动把滚动结果 `d、d+1、d+2` 纳入计划，无关日期不重算。
- **计划先于执行**：`/api/runs/plan` 只生成计划（就绪/阻塞/复用），页面确认后才真正运行。
- **缺上游显式阻塞**：缺少必要上游分区时节点标记 `blocked` 并列出缺失项，
  **不会用空表悄悄代替**。
- **两并发调度**：最多同时运行两个就绪节点；节点失败后阻塞其下游，独立分支继续。
- **临时分区 + 事务发布**：每个输出分区先写临时表，成功后在单事务内替换正式分区；
  失败则保留上一版可用结果。
- **签名复用**：以「输入分区签名 + SQL 版本」判定复用，两者都没变的成功节点重试时不重跑。
- **重启恢复**：运行状态持久化在 SQLite，服务重启后能找到未完成运行，
  清理遗留临时表，把中断节点从 `running` 复位为 `ready`，而不是整批重跑。

## 二、目录结构

```
config/pipeline.yaml     # 任务/SQL/上游偏移声明
sql/*.sql                # 各任务 SQL（占位符见下）
app/
  config.py              # 配置加载、校验、环检测
  planner.py             # 闭包扩散、阻塞判定、签名与复用
  warehouse.py           # DuckDB：SQL 渲染、临时分区、事务发布、签名
  metadata.py            # SQLite：运行/节点/发布版本/源分区/故障开关
  runner.py              # 两并发编排、失败阻塞、重试、重启恢复
  main.py                # FastAPI 路由与静态托管
scripts/
  seed_data.py           # 生成连续多日订单；--backfill 重写某一天源数据
  demo_http.py           # 通过 HTTP 走完整演示流程
tests/e2e_test.py        # 35 项端到端核对
frontend/                # React + Vite 页面
```

## 三、快速开始

### 1. 后端

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

# 准备 8 天连续订单
.venv/bin/python -m scripts.seed_data --days 8

# 启动服务（工厂模式，导入模块不会提前建库）
.venv/bin/python -m uvicorn app.main:create_app_once --factory --host 127.0.0.1 --port 8000
```

### 2. 前端

```bash
cd frontend
npm install
npm run build        # 产物在 frontend/dist，由后端直接托管
# 或开发模式（带 /api 代理）：npm run dev
```

打开 http://127.0.0.1:8000 ，先点「扫描源分区签名」，再构建计划。

### 3. SQL 占位符

SQL 文件支持以下占位符，由 `Warehouse.render` 渲染：

| 占位符 | 含义 |
| --- | --- |
| `${target}` | 本次输出的临时表/正式表名 |
| `${dt}` | 节点分区日期（DATE 字面量） |
| `${dt_minus_1}` / `${dt_minus_2}` | 前 1 / 2 天日期 |
| `${table:任务或源名}` | 对应物理表名（源表或 `task_<name>`） |

## 四、建议的演示核对流程

1. 准备连续多日订单并扫描源签名，对全部日期做**初始加工**：
   前两天的 `rolling_3d` 因缺更早的门店分区而**阻塞**，第 3 天起成功，峰值并发为 2。
2. 在「故障注入」面板打开 `store_daily` 的强制失败，对某天做 **backfill**：
   - `store_daily` 失败并记录原因，`rolling_3d` 的 d/d+1/d+2 被阻塞；
   - 独立分支 `product_daily` 不受影响（成功或复用）；
   - 正式表里 `store_daily` 仍是上一版可用结果。
3. 关闭故障开关并**重试同一运行**：只重跑失败的 `store_daily` 与三个滚动分区，
   已成功/复用节点不重跑。
4. 停服务后执行 `python -m scripts.seed_data --backfill 2026-10-30` 重写一天源数据，
   重启、重新扫描，再对该日 backfill：
   - 计划覆盖当天全部任务 + 滚动 `d/d+1/d+2`；
   - 因源签名变化，受影响节点全部 `ready`（不复用）；
   - 再做一次相同补算，计划全部 `reused`、实际执行 0 个节点。
5. 打开某任务的 `delay` 开关制造长时间 `running`，运行中重启服务：
   重启后该运行出现在恢复列表，中断节点复位 `ready`，确认后继续而不整批重跑。

> DuckDB 同一时刻只允许一个写连接，因此重写源数据（`seed_data --backfill`）前需先停服务。

## 五、关键 API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/runs/plan` | 预览执行计划（不执行） |
| POST | `/api/runs/{id}/confirm` | 确认运行 / 重试未完成节点 |
| GET | `/api/runs` `/api/runs/{id}` | 运行记录与节点明细 |
| GET | `/api/partitions` | 已发布分区与源分区签名 |
| GET | `/api/preview/{task}/{dt}` | 分区数据预览 |
| POST | `/api/faults` | 故障/延迟开关（kind=fail|delay） |
| GET | `/api/sources/scan` | 扫描并登记源分区签名 |
| GET | `/api/config` | 任务图配置（含环错误） |

## 六、测试

```bash
.venv/bin/python -m tests.e2e_test     # 期望输出「通过 35/35」
```
