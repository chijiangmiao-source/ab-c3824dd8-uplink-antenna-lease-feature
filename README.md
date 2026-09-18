# 卫星天线控制租约服务（Antenna Control Lease Service）

纯后端服务。卫星过站期间，两套（或多套）上行控制程序可能同时争抢同一副天线。
本服务保证：

- **任一时刻每副天线至多存在一个未到期租约**；
- **数据库时间（PostgreSQL `clock_timestamp()`）是唯一时钟**，应用服务器从不读取本机时间；
- 成功响应即使在链路中丢失，控制端用**同一幂等键 + 相同参数**重试也只会拿回
  **原来的令牌和原来到期时间**，绝不会产生第二份控制权；
- 同一幂等键携带**不同参数**重试返回**稳定冲突**；
- 租约到期的**边界归新请求**所有：`expires_at <= clock_timestamp()` 即视为已交接，
  未到期的争抢不会改写任何现有记录；
- 未知天线、越界租期返回稳定错误，且**不落任何库记录**。

技术栈：Python 3.12 · FastAPI · SQLAlchemy 2 · PostgreSQL 16（`pgcrypto`）· Alembic · pytest。

---

## 1. 目录结构

```
.
├── app/                    # FastAPI 应用
│   ├── main.py             # 应用入口、异常处理、/health
│   ├── config.py           # 环境变量配置（DATABASE_URL、租期边界）
│   ├── db.py               # SQLAlchemy 引擎
│   ├── errors.py           # 统一错误信封 {error:{code,message,details}}
│   ├── schemas.py          # Pydantic 请求/响应模型
│   ├── services.py         # 原子获取租约、指令进度上报的核心事务逻辑
│   └── routers/            # HTTP 路由（leases、catalog）
├── alembic/                # 迁移脚本（初始迁移含 6 副预置天线种子数据，
│                           # 0002 增加租约指令进度列）
├── tests/                  # 连接真实 PostgreSQL 的验收测试
├── Dockerfile              # 多阶段：api 镜像 / verify 镜像
├── docker-compose.yml      # db + api + 一次性 verify
├── entrypoint.sh           # 先迁移后启动 API
├── requirements.txt
└── requirements-dev.txt    # 额外含 pytest、httpx
```

预置天线（迁移写入 `antennas` 表，`GET /antennas` 可查）：

| 编号 | 名称 |
| --- | --- |
| ANT-01 … ANT-06 | Beijing / Sanya / Kashgar / Kunming / Urumqi / Harbin uplink array |

---

## 2. 一键启动（Docker Compose）

```bash
# 默认宿主端口 8000；可用 API_PORT 覆盖
docker compose build
docker compose up -d db api

# 查看 API
curl http://localhost:8000/health
curl http://localhost:8000/antennas
```

换宿主端口：

```bash
API_PORT=18080 docker compose up -d db api
curl http://localhost:18080/health
```

API 容器启动时由 `entrypoint.sh` **自动执行 `alembic upgrade head`**，
等数据库健康检查通过后才开始迁移与启动。

### 一次性验收服务 verify

`verify` 服务会等待 `db` 与 `api` 都健康，然后针对**真实 API + 真实 PostgreSQL**
跑完整 pytest（并发争抢、幂等重放、参数冲突、到期交接、输入拒绝），结束即退出，
退出码即验收结论，**不会重启**：

```bash
docker compose up --build verify
# 或在已启动栈的基础上单独运行：
docker compose run --build verify
```

验收测试会通过独立的数据库连接截断 `leases` / `idempotency_keys` 表以保证用例独立，
因此请在测试环境运行（预置天线目录不会被清除）。

---

## 3. 数据库迁移（手动执行）

本地不使用容器入口脚本时：

```bash
python3.12 -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
export DATABASE_URL="postgresql+psycopg2://satctl:satctl@localhost:5432/satctl"

alembic upgrade head      # 建表、启用 pgcrypto、写入预置天线
alembic current           # 查看当前版本
alembic downgrade base    # 回滚全部迁移
```

迁移内容：

- `alembic/versions/0001_initial.py`：
  - `antennas(id PK, name, created_at)` + 6 行预置天线；
  - `leases(id BIGINT PK, antenna_id FK, controller, token UNIQUE,
    acquired_at, expires_at)`，校验 `expires_at > acquired_at`，
    索引 `(antenna_id, expires_at)` 服务活跃租约查询；
  - `idempotency_keys(idempotency_key PK, lease_id FK NOT NULL,
    request_params, created_at)`。
- `alembic/versions/0002_lease_progress.py`：`leases` 增加可空的
  `last_command_sequence`（BIGINT，校验 `>= 0`）与
  `last_progress_at`（timestamptz），并约束两列同时为空/同时非空。
  历史行与未上报的新租约均为 `NULL`，**无需补造进度**。

---

## 4. 调用方法

### 4.1 获取租约 `POST /leases`

请求体：

| 字段 | 类型 | 约束 |
| --- | --- | --- |
| `antenna_id` | string | 必须为预置天线编号 |
| `controller` | string | 控制者标识，1–128 字符 |
| `duration_seconds` | int | 租期，闭区间 **[5, 120]** 秒 |
| `idempotency_key` | string | 调用方生成的幂等键，1–128 字符 |

成功 `200`：

```json
{
  "antenna_id": "ANT-01",
  "controller": "gs-beijing-A",
  "lease_token": "k3J9...（32 随机字节的 base64url 无填充编码，43 个字符，仅含 A–Z a–z 0–9 - _，可直接放进 URL 路径段；PostgreSQL CSPRNG 生成，不可预测）",
  "acquired_at": "2026-09-12T04:00:00.123456+00:00",
  "expires_at": "2026-09-12T04:00:30.123456+00:00",
  "replay": false
}
```

时间戳在所有接口（获取、重放、令牌查询）中使用**完全一致的 ISO-8601 带显式偏移量**
表示（`+00:00`，不混用 `Z`），可逐字节比较；同一个 `expires_at` 在获取与查询响应中
字符串相同。

- `lease_token` 不可预测且 URL 安全：由数据库 `gen_random_bytes(32)` 生成后做
  base64url 转换（`+→-`、`/→_`、去掉 `=` 填充，43 字符），可直接用于
  `GET /leases/{lease_token}` 路径；
- `expires_at = clock_timestamp() + duration_seconds`，完全由数据库计算；
- 同键同参重试返回 `200` 且 `replay: true`，令牌与到期时间与首次完全一致。

curl：

```bash
curl -sS -X POST http://localhost:8000/leases \
  -H 'Content-Type: application/json' \
  -d '{
    "antenna_id": "ANT-01",
    "controller": "gs-beijing-A",
    "duration_seconds": 30,
    "idempotency_key": "pass-2026-09-12-ANT01-7f3a"
  }'
```

### 4.2 上报指令进度 `POST /leases/{lease_token}/progress`

一次过站中控制程序会连续下发多条指令，值班人员需要确认当前持有方执行到哪一条。
持有方每执行完一条即上报其序号；**上报不改变租约的占用期限**（`expires_at` 不变）。

请求体：

| 字段 | 类型 | 约束 |
| --- | --- | --- |
| `sequence` | int | 已执行到的指令序号，**非负整数**（不接受文本数字），只允许递增 |

成功 `200`：

```json
{
  "lease_token": "k3J9...",
  "last_command_sequence": 3,
  "last_progress_at": "2026-09-12T04:00:05.123456+00:00",
  "replay": false
}
```

- `last_progress_at` 由数据库时钟 `clock_timestamp()` 生成，格式与其他时间戳完全一致；
- **相同序号视为重放**：返回 `replay: true` 与**原记录时间**，不写库；
- **更小序号**返回 `409 PROGRESS_REGRESSION`，不写库；
- 上报事务锁定租约所属天线行（与获取租约同一把锁），并确认该令牌仍为
  **当前未到期租约**；已到期令牌返回 `409 LEASE_EXPIRED`（即使是重放），未知令牌
  返回 `404 LEASE_NOT_FOUND`，均不落库。

curl：

```bash
curl -sS -X POST http://localhost:8000/leases/k3J9.../progress \
  -H 'Content-Type: application/json' \
  -d '{"sequence": 3}'
```

### 4.3 错误响应

统一信封：`{"error": {"code", "message", "details"}}`

| HTTP | code | 触发条件 | 是否落库 |
| --- | --- | --- | --- |
| 404 | `ANTENNA_NOT_FOUND` | 未知天线编号 | 否 |
| 422 | `VALIDATION_ERROR` | 租期越界（非 5–120 整数）、缺字段、空白、多余字段；`sequence` 为负数/文本/非整数 | 否 |
| 409 | `ANTENNA_BUSY` | 存在未到期租约（`details.expires_at` 给出交接时间） | 否 |
| 409 | `IDEMPOTENCY_CONFLICT` | 同幂等键但参数与首次不同 | 否（首次成功请求的记录保留） |
| 404 | `LEASE_NOT_FOUND` | `GET` / 进度上报的令牌未知 | 否 |
| 409 | `LEASE_EXPIRED` | 进度上报时租约已到期、控制权已交接 | 否 |
| 409 | `PROGRESS_REGRESSION` | 上报序号小于已确认序号（`details` 给出双方序号） | 否 |

`ANTENNA_BUSY` 示例：

```json
{
  "error": {
    "code": "ANTENNA_BUSY",
    "message": "天线 ANT-01 已被未到期租约占用。",
    "details": {
      "antenna_id": "ANT-01",
      "held_by_lease": "原令牌…",
      "expires_at": "2026-09-12T04:00:30.123456+00:00"
    }
  }
}
```

客户端策略建议：拿到 `ANTENNA_BUSY` 后若要在到期后争抢，**必须更换幂等键**再重试；
若是“响应可能丢失”的重试，则保持原键原参数直接重发即可安全重放。

### 4.4 其他接口

- `GET /leases/{lease_token}` — 查询租约与 `active` 状态（以数据库时间实时计算）；
  响应含可空的 `last_command_sequence` / `last_progress_at`，尚未上报过进度的
  租约（含迁移前的历史记录）两者均为 `null`；
- `GET /antennas` — 预置天线目录；
- `GET /health` — 存活探针，返回数据库时钟 `database_time`；
- 交互式文档：`GET /docs`（Swagger UI）。

---

## 5. 并发与正确性如何保证

获取租约在单个 READ COMMITTED 事务内按固定顺序加锁：

1. `pg_advisory_xact_lock(hashtext(:idempotency_key))`
   —— 相同幂等键的事务串行化，杜绝“丢响应后重试”产生第二份租约；
2. 查 `idempotency_keys`：参数一致 → 重放原令牌/原到期时间；不一致 → 稳定冲突；
3. `SELECT ... FROM antennas WHERE id=:id FOR UPDATE`
   —— 串行化同天线的所有争抢者，同时完成“天线必须存在”的校验（未知天线在此之前无任何写入）；
4. 以 `expires_at > clock_timestamp()` 判定活跃租约：未到期 → `ANTENNA_BUSY`；
   `expires_at` 已到达（相等或已过去）→ 边界归新请求；
5. 插入新租约与幂等记录并提交（同事务原子完成）。

进度上报在单个 READ COMMITTED 事务内复用同一把天线行锁：

1. 按令牌找到租约（未知令牌 → `LEASE_NOT_FOUND`，无任何写入）；
2. `SELECT ... FROM antennas WHERE id=:id FOR UPDATE`
   —— 与获取租约、以及其他上报串行化；
3. 锁内以 `expires_at > clock_timestamp()` 确认该令牌仍是当前未到期租约，
   已到期 → `LEASE_EXPIRED`（每副天线任一时刻至多一个未到期租约，故未到期即当前）；
4. 序号大于已确认值 → 更新 `last_command_sequence` 并令
   `last_progress_at = clock_timestamp()`；相等 → 重放原记录时间；
   更小 → `PROGRESS_REGRESSION`。上报绝不改写 `expires_at`。

所有“当前时间”和到期时间都在 SQL 内由 PostgreSQL 产生，应用层没有任何时间判断。

---

## 6. 本地运行测试（不使用 verify 容器）

需要一个真实 PostgreSQL（例如 `docker compose up -d db`）：

```bash
pip install -r requirements-dev.txt
alembic upgrade head
uvicorn app.main:app --port 8000 &           # 或直接 docker compose up -d api

export API_BASE_URL="http://localhost:8000"
export DATABASE_URL="postgresql+psycopg2://satctl:satctl@localhost:5432/satctl"
pytest
```

测试内容：

- `tests/test_input_validation.py` — 未知天线、租期越界/非整数（含文本 `"30"`）、缺字段/空白、
  边界值（5 与 120）、拒绝路径零落库；
- `tests/test_token_safety.py` — 令牌为 URL 安全的 base64url（无 `/ + =`）、
  2000 次抽样数据库令牌生成器、刚获取的租约可经路径查询详情、获取/查询/重放三端
  `expires_at`/`acquired_at` 逐字节同格式、文本租期拒绝零落库；
- `tests/test_idempotency.py` — 原令牌/原到期重放、令牌不可预测、三类参数冲突稳定、
  过期后同键仍重放；
- `tests/test_concurrency.py` — 12 路屏障并发争抢空闲天线仅 1 胜、
  在任者不被改写、同键并发自身重试只有一份租约、发散参数 fanout、
  多天线互不干扰、连续争抢波；
- `tests/test_expiry.py` — SQL 造到期租约验证原子交接、到期边界归新请求、
  以及经 API 的 5 秒最短租约端到端到期交接；
- `tests/test_progress.py` — 获取后连续上报并查询最新进度、同序号重放记录时间
  逐字节稳定、序号回退稳定拒绝、并发上报最终保留最大序号、同序号并发仅一人落库、
  过期令牌被拒绝且数据不变、未知令牌零落库、非法序号 422、
  上报期间获取/争抢/幂等行为不受干扰。

测试不使用任何固定响应或假接口：全部通过 HTTP 打向真实服务，并直连真实
PostgreSQL 制造并发、播种到期数据和断言提交结果。

---

## 7. 配置项（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `API_PORT` | `8000` | compose 映射到宿主的端口（容器内固定 8000） |
| `DATABASE_URL` | compose 内自动指向 `db` | SQLAlchemy/psycopg2 连接串 |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` | `satctl` | 数据库初始化凭据 |
| `API_BASE_URL`（仅 verify） | compose 内 `http://api:8000` | 验收测试访问的 API 地址 |
