# 仪器 CSV 入仓 SQL 错误 · 最小复现样本缩减服务

仪器 CSV 触发入仓 SQL 错误时，质检数仓工程师需要把批次**缩小**后交给维护者复现。
本服务用 Flask 提供网页，对触发同一 SQLite 错误的 CSV 批次做 **Delta 缩减（delta debugging）**，
返回一个**非空、1-最小**的复现样本。

## 它做什么

输入三件套：

| 输入 | 要求 |
| --- | --- |
| SQLite 基线数据库 | 有效 `.db/.sqlite/.sqlite3`；服务全程只读，**永远不会被修改** |
| CSV | 表头恰好为 `sample_id,reading`，两列均按文本处理，数据行 **1–40 行**，保持原行序 |
| SQL 脚本 | 确定性 `.sql`；从 `staging(sample_id TEXT, reading TEXT)` 读取并自行建表写入 |

输出结论之一：

- `reduced`：缩减成功。返回的非空样本仍报出与原批**相同错误码 + 相同消息**的 SQLite 错误，
  且**删除其中任意一行后都不再报同一错误**（1-最小性逐行校验 + 最终独立复核）。页面可下载 `reduced_sample.csv`。
- `no_error`：原批执行成功、没有 SQLite 错误 —— 说明原因，**不缩减**。
- `empty_reproduces`：空样本（0 行 staging）执行脚本就报同一错误，说明故障在脚本/基线而非 CSV 行 —— 说明原因，**不缩减**。
- `incomplete_timeout`：有探测超过 **2 秒**被强制终止。超时既不算目标错误也不算最小性证据，
  缩减标记为**未完成**；已取得的**目标错误仍会保留展示**，但**不提供最小样本下载**。
- `failed`：最终复核时样本不再复现目标错误（例如脚本非确定性）。不返回样本，也不冒充超时。

## 关键正确性保证

1. **每次探测独立基线副本**：每次探测把基线复制进全新的私有临时目录，在副本上重建
   `staging` 并执行脚本；所有 SQL 写入只落在该次副本，结束即删除整个临时目录。
   探测之间、任务之间互不污染，上传基线保持字节不变（页面展示基线 SHA-256，可比对）。
2. **2 秒硬超时且真正终止执行**：每次探测在**独立 worker 进程**中运行。SQLite 里有些工作
   （例如灾难性回溯的 `GLOB` 匹配）在**单个 C 层 VM 操作码**内完成，进度处理器与
   `Connection.interrupt()` 都无法介入；因此到点由父进程对 worker 发送 `SIGTERM`、必要时
   `SIGKILL`，无条件终止实际执行，随后清理私有副本。超时返回 `timeout` 结果，绝不与真实
   错误混淆；任何阶段超时都会把整个缩减标记为未完成。超时**不**算成功、目标错误或最小性证据。
   worker 使用**独立解释器进程**而非 `fork`：Web 层在多线程中运行缩减，多线程进程 fork 可能
   继承他线程持有的锁而假性超时；`spawn`/`forkserver` 又要求宿主主模块可安全重导入。
   另设 `PR_SET_PDEATHSIG`，服务进程被回收时不会有孤儿探测进程残留。
3. **最小性**：先做经典 ddmin 缩减，再显式逐行删除验证（删除任一行均不复现），
   最后对返回样本再做一次独立探测复核，确认仍报目标错误。
4. **错误匹配严格**：必须同为真实 SQL 错误（非超时、非成功），且 SQLite 错误码与消息文本都一致才算复现。
5. **执行期间网页可响应**：缩减在后台线程运行，gunicorn 多线程受理请求；轮询进度、打开页面、提交新任务均不阻塞。

## 目录结构

```
.
├── app/
│   ├── reducer.py          # 缩减引擎：独立副本探测、硬超时终止、ddmin、最小性校验
│   ├── probe_worker.py     # 单次探测的独立 worker 进程（可被杀死的硬隔离边界）
│   ├── web.py              # Flask 路由、上传校验、后台任务与进度/下载 API
│   ├── templates/index.html
│   └── static/{app.js,style.css}
├── tests/                  # 30 个单元/端到端测试（含超时、并发与崩溃回归）
├── demo/                   # 可演示固件（6 个场景）+ 一键演示脚本
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

## 用 Docker Compose 启动（交付方式）

```bash
docker compose up --build
# 打开 http://localhost:8000
```

- Web 入口：<http://localhost:8000>
- 健康检查：<http://localhost:8000/healthz>
- 任务产物持久化在命名卷 `reducer-data`（容器内 `/data/jobs`），探测临时目录在容器内用完即删。
- 单进程多线程 gunicorn（任务注册表在内存中），`PROBE_TIMEOUT_SECONDS` 可在 compose 中调整（默认 2.0）。

## 本地直接运行（开发/测试）

```bash
python3 -m pip install -r requirements.txt
python -m app                      # http://localhost:8000
python -m unittest discover -s tests -v
```

## 一键演示

先生成 6 套演示固件，再对运行中的实例依次提交：

```bash
python3 demo/make_fixtures.py
bash demo/run_demo.sh              # 默认打 http://127.0.0.1:8000
```

| 场景目录 | 构造 | 预期结论 |
| --- | --- | --- |
| `demo/check_constraint/` | 40 行中 1 行 `reading=N/A` 违反 CHECK | `reduced`，样本 1 行 `S-BAD,N/A` |
| `demo/unique_duplicate/` | 21 行中藏一对重复主键 | `reduced`，样本恰为那 2 行（保持原序） |
| `demo/success_batch/` | 全部合法 | `no_error`，不缩减 |
| `demo/empty_reproduces/` | 脚本自身插入与基线预置行冲突的固定 ID | `empty_reproduces`，不缩减 |
| `demo/timeout_probe/` | 巨型递归 CTE，每次探测都跑不完 | `incomplete_timeout`，2 秒终止 |
| `demo/glob_backtrack/` | 两行 `sample_id` 相同（原批报 UNIQUE 冲突）；单行候选触发灾难性 `GLOB` 回溯，进度处理器/`interrupt()` 均无法中断 | `incomplete_timeout`，2 秒由杀进程终止；保留目标错误；无下载 |

网页操作：打开首页 → 选择某场景目录下的 `baseline.sqlite3`、`input.csv`、`script.sql`
→ 开始缩减，页面实时显示阶段、探测次数、候选行数、目标错误与日志，完成后出现下载按钮与样本表格。

## HTTP API

| 方法与路径 | 说明 |
| --- | --- |
| `POST /api/jobs` | multipart 上传 `baseline`、`csvfile`、`script`，返回 `{job_id}` |
| `GET /api/jobs/<id>` | 返回状态、进度事件、目标/空样本探测结果（含仅用于诊断的 `detail`）、最终结论与样本行 |
| `GET /api/jobs/<id>/download` | 下载 `reduced_sample.csv`（仅 `reduced` 状态存在） |
| `GET /jobs/<id>` | 任务页（可深链） |
| `GET /healthz` | 存活探针 |

## SQL 脚本约定

- 每次探测前服务已在副本上执行 `DROP TABLE IF EXISTS staging; CREATE TABLE staging (sample_id TEXT, reading TEXT);`
  并按原行序插入候选行，脚本直接 `SELECT ... FROM staging ORDER BY rowid` 即可保持顺序。
- 脚本应自行 `DROP TABLE IF EXISTS`/`CREATE TABLE` 其目标表，保证确定性、可重复执行。
- 脚本对同一输入必须产生确定结果；非确定性脚本可能让最小性校验失效。
