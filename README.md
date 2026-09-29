# 任务收尾屏障 API（Task Barrier API）

基于 **Python 3.12 + FastAPI + Redis** 的并行任务协调收尾服务：

- 创建屏障时指定**参与者名单**、**达标人数（quorum）**和**截止时间**；
- 可选启用**分组配额达标策略**：创建时传 `participant_groups`（每个参与者恰属一组）与 `group_thresholds`（各组配额），启用后只有**总报到数达到 `threshold` 且每个分组的报到数都达到对应配额**才进入 `reached`；
- 参与者以自己的标识**幂等报到**；
- 支持**批量幂等报到**：`POST /barriers/{id}/batch-checkins` 一次提交参与者列表与**幂等键**，名单校验、截止/取消校验与整批写入在**同一段 Redis Lua 脚本**内一次原子完成——列表含**未知成员**或屏障**已过期/已取消**时**整批不产生任何报到副作用**（不写报到集合、不推进分组计数、不追加事件、不占用幂等键，可随后用同一幂等键安全重试）；同一幂等键的重试**逐字段回放首次响应**（含首次时刻的视图快照），同键不同名单返回 `409`；本批使屏障达标时与单人报到接口**共用同一冻结键**，跨接口并发也只有一个请求冻结 quorum 并唤醒等待者；
- 并发报到中**首次同时满足总人数与各组配额的瞬间，达标成员与分组计数在 Redis 内被原子冻结**为快照，之后的任何报到（哪怕来自其他名单成员）都不会改写 `quorum` / `frozen_at` / `group_progress`；跨组并发报到也**只有一个请求** `triggered=true`；
- 支持**主动取消**：取消与达标报到在 Redis 中**原子竞争**，先发生者成为唯一终态（`cancelled` 与 `reached` 互斥）；取消保存 `reason` / `cancelled_at` 并唤醒全部等待者；
- 支持**运行中延期**：`PATCH /barriers/{id}/deadline` 传入新的**绝对截止时间**，仅当屏障仍处于 `waiting` 且新时间**严格晚于**当前 deadline 时，在一段 Redis Lua 脚本内原子完成状态校验、deadline 写回与**全部相关键存活期刷新**；终态（reached/cancelled/expired）绝不复活；并发延期、报到与取消保持唯一一致结果；延期成功立即通知已挂起的 `/wait` **按最新 deadline 继续等待**，不会按旧截止时间误报 `expired`；
- 支持**运行中参与者替补**：`POST /barriers/{id}/replace` 在原参与者**尚未报到**且屏障仍 `waiting` 时，把其**原子地替换为名单外的新标识**并**继承原分组**；替补与报到、取消、延期及其他替补并发时按 Redis 实际生效顺序**唯一生效**，并记录可分页查询的 `replaced` 审计事件、唤醒 `/wait`；达标、取消或截止后一律拒绝替补；
- 提供**阻塞等待接口**：达标 / 取消时立即返回终态快照；截止时返回已报到成员与缺席名单；多个等待者通过 Redis Pub/Sub 一并被可靠唤醒（轮询兜底，消息丢失也不会睡死）；
- 提供**达标结果交接（handoff）**：屏障达标后可 `POST /handoffs` 建立一份**独立持久保存**的交接单，复制达标快照 `quorum` / `frozen_at`（屏障到期回收不影响交接单）；交接单支持**栅栏令牌（fencing token）租约**——空闲或租约到期可被 `claim` 认领并获得**单调递增的 fence 与到期时间**，`renew` 续租、`complete` 完成均须携带 `owner` + `fence` 且租约仍有效；竞争认领、到期接管与旧凭证拒绝全部在 **Redis Lua 脚本内以 Redis 时间原子裁决**，完成后永久拒绝再次认领。

## 一条命令启动（含 Redis）

```bash
docker compose up --build
```

该命令会构建 API 镜像并启动 Redis 7（带健康检查与 AOF 持久化），API 等 Redis 就绪后才启动。停止：`Ctrl+C`（或 `docker compose down`，加 `-v` 同时删除数据卷）。

## 访问地址与端口

| 用途 | 地址 |
| --- | --- |
| API Base URL | <http://localhost:8000> |
| Swagger 交互文档 | <http://localhost:8000/docs> |
| OpenAPI schema | <http://localhost:8000/openapi.json> |
| 健康检查（含 Redis ping） | <http://localhost:8000/health> |

Redis 仅在内网暴露：服务名 `redis`，端口 `6379`，不发布到宿主机。

## 必要配置

全部通过环境变量注入（`docker-compose.yml` 中已给默认值，一般无需修改）：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `REDIS_URL` | `redis://redis:6379/0` | Redis 连接地址 |
| `BARRIER_TTL_BUFFER_MS` | `86400000`（24h） | 屏障键在截止时间之后额外保留多久，之后由 Redis 自动回收 |
| `BARRIER_POLL_INTERVAL_S` | `0.5` | 等待接口的轮询兜底间隔（秒），Pub/Sub 为主唤醒通道 |
| `BARRIER_MAX_WAIT_S` | `3600` | 单次等待请求最长挂起时间（秒） |
| `HANDOFF_LEASE_S` | `30` | 达标结果交接单的认领租约时长（秒）；到期可被他人接管（fence 递增），不影响交接单持久保存 |

宿主机端口可在 `docker-compose.yml` 的 `api.ports` 修改（如 `"9000:8000"`）。

## API 一览

### 1. 创建屏障 `POST /barriers`

`threshold` 缺省时等于参与者总人数（全员到齐才达标）。`deadline` 为 ISO-8601，不带时区时按 UTC 处理。

```bash
curl -s -X POST http://localhost:8000/barriers \
  -H 'Content-Type: application/json' \
  -d '{"participants":["node-a","node-b","node-c"],"threshold":2,"deadline":"2026-09-20T15:00:00Z"}'
```

返回（`201`，节选）：

```json
{
  "barrier_id": "8f3c…",
  "status": "waiting",
  "participants": ["node-a", "node-b", "node-c"],
  "threshold": 2,
  "deadline": "2026-09-20T15:00:00Z",
  "checked_in": [],
  "checked_count": 0
}
```

校验：`threshold <= 参与者人数`；`deadline` 必须是未来时间；名单会自动去重、去空白。

#### 分组配额达标（可选）

在创建请求中同时传入 `participant_groups`（**每个参与者恰属一组**，覆盖全部名单成员）与 `group_thresholds`（每个出现过的分组都要有**正整数**配额）。下例要求 3 个机房共至少 4 人报到，且每个机房至少 1 人：

```bash
curl -s -X POST http://localhost:8000/barriers \
  -H 'Content-Type: application/json' \
  -d '{
    "participants":["dc1-a","dc1-b","dc2-a","dc2-b","dc3-a"],
    "threshold":4,
    "deadline":"2026-09-21T15:00:00Z",
    "participant_groups":{
      "dc1-a":"dc1","dc1-b":"dc1",
      "dc2-a":"dc2","dc2-b":"dc2",
      "dc3-a":"dc3"
    },
    "group_thresholds":{"dc1":1,"dc2":1,"dc3":1}
  }'
```

返回（`201`，节选）：

```json
{
  "barrier_id": "8f3c…",
  "status": "waiting",
  "participants": ["dc1-a", "dc1-b", "dc2-a", "dc2-b", "dc3-a"],
  "threshold": 4,
  "checked_count": 0,
  "participant_groups": {"dc1-a": "dc1", "dc1-b": "dc1", "dc2-a": "dc2", "dc2-b": "dc2", "dc3-a": "dc3"},
  "group_progress": [
    {"group": "dc1", "threshold": 1, "checked_count": 0, "satisfied": false},
    {"group": "dc2", "threshold": 1, "checked_count": 0, "satisfied": false},
    {"group": "dc3", "threshold": 1, "checked_count": 0, "satisfied": false}
  ]
}
```

达标规则：**总报到数 ≥ `threshold` 且每个分组的去重报到数都达到其配额**，二者同时满足的瞬间才进入 `reached`。因此上例中即使先有 4 人报到，只要其中没有任何 `dc3` 成员，屏障仍保持 `waiting`。

- `group_progress` 出现在查询（`GET /barriers/{id}`）与报到响应的 `view` 中，逐组给出 `threshold` / `checked_count` / `satisfied`；`reached` 状态下为**冻结瞬间的快照**，迟到报到不再改变。
- 校验（任一不满足返回 `422`）：两个字段必须**同时提供或同时省略**；分组映射不得含名单外（未知）成员、不得漏掉任何参与者；分组名不得为空；配额必须为正整数、不得超过该组人数、不得引用不存在的分组、不得漏掉任何已出现的分组；**各组配额之和不得超过总 `threshold`**。
- 不传这两个字段时响应不含 `group_progress` / `participant_groups`，行为与旧版完全一致。

### 2. 参与者报到 `POST /barriers/{barrier_id}/checkin`

```bash
curl -s -X POST http://localhost:8000/barriers/<id>/checkin \
  -H 'Content-Type: application/json' \
  -d '{"participant_id":"node-a"}'
```

`status` 取值：

- `checked_in`：报到成功，屏障继续等待；
- `already_checked_in`：同一参与者重复报到（幂等，无副作用）；
- `reached`：**本次报到使屏障达标**（同时满足总人数与所有分组配额），`triggered=true`，`quorum` 为冻结成员快照；
- `late`：屏障已达标后才报到，仅记入 `late`，**冻结快照不变**。

非名单成员 → `403`；屏障不存在/已回收 → `404`；截止后或取消后报到 → `409`。

响应中的 `view` 与查询接口同一结构：启用分组配额时附带 `group_progress`；只有总报到数达到 `threshold` 且各组均达到配额时 `triggered` 才会为 `true`。

### 3. 批量幂等报到 `POST /barriers/{barrier_id}/batch-checkins`

一次提交多个参与者与客户端生成的**幂等键**（`idempotency_key`，1–256 字符；参与者列表自动去除首尾空白，不得含空标识或批内重复标识，否则 `422`）。名单校验、截止/取消状态校验、整批报到写入、达标冻结、逐成员审计事件与幂等结果记录在**同一段 Redis Lua 脚本**内一次原子完成：

```bash
curl -s -X POST http://localhost:8000/barriers/<id>/batch-checkins \
  -H 'Content-Type: application/json' \
  -d '{"participants":["node-a","node-b"],"idempotency_key":"deploy-2026-09-22-rollup-7"}'
```

返回（`200`）：

```json
{
  "barrier_id": "8f3c…",
  "status": "checked_in",
  "triggered": false,
  "replayed": false,
  "idempotency_key": "deploy-2026-09-22-rollup-7",
  "accepted_count": 2,
  "accepted": [
    {"participant_id": "node-a", "result": "checked_in"},
    {"participant_id": "node-b", "result": "checked_in"}
  ],
  "quorum": null,
  "frozen_at": null,
  "view": { "status": "waiting", "checked_in": ["node-a", "node-b"], "checked_count": 2 }
}
```

字段语义：

- `status`：`checked_in`（本批已受理，屏障继续等待）| `reached`（**本批使屏障达标**）| `late`（屏障已达标，本批全部记为迟到，冻结快照不变）；
- `triggered`：本批是否首次使屏障达标并冻结 quorum（与单人报到接口跨接口竞争，全局至多一个请求为 `true`）；
- `replayed`：`true` 表示本次响应由同一幂等键的**首次结果回放**，未再次执行报到；
- `accepted[]`：逐成员结果，顺序与首次请求的 `participants` 一致，元素 `result` 为 `checked_in`（新报到）/ `already_checked_in`（此前已报到）/ `reached`（批内承载达标标记的成员，每批至多一个）/ `late`（达标后报到）。

**幂等语义**：

- 同一 `idempotency_key` 的重试（参与者**集合相同**，顺序可以不同）返回**首次响应的逐字段回放**：`status` / `triggered` / `quorum` / `frozen_at` / `accepted` 以及响应里的 `view` 都是**首次执行时刻**的快照，`replayed=true`；即使首次执行后屏障又被其他请求推到达标 / 取消 / 截止，回放视图也不随之改变；
- 同一幂等键携带**另一份参与者集合** → `409`，首次结果不可改写，新名单不产生任何副作用；
- 幂等记录与首次报到结果在同一原子操作内落盘（`barrier:{id}:idem:{sha256(key)}`），随屏障一起 TTL；屏障运行中延期时，全部幂等记录的存活期随新 deadline 一并顺延，延期后仍可回放。

**整批原子（全有或全无）**：

- 列表中**任一成员不在当前名单内**（含已被运行中替补换下的原成员）→ `403`，整批一起失败：不写报到集合、不推进分组计数、不追加审计事件，**也不占用幂等键**——随后可用**同一个幂等键**携带修正后的名单安全重试；
- 屏障**已过截止时间**或**已被取消** → `409`，同样整批零副作用、不占用幂等键；
- 屏障已达标后的批量报到不是错误：全部成员记为 `late` 并追加迟到事件，但 `quorum` / `frozen_at` / 分组计数快照永不改变。

**并发与冻结**：批量脚本与单人报到脚本共用同一 `frozen` 冻结键，由 Redis 单线程串行化——批量与单人报到、多个不同幂等键的批量报到并发时，**必有且仅有一个请求**在首次同时满足「总达标人数 + 各组配额」的瞬间冻结 quorum；其余请求得到 `late`（达标后）或继续 `waiting`。冻结成功后发布 Pub/Sub `reached`，全部 `/wait` 等待者被立即唤醒；同一幂等键自身的并发重试由「先查幂等记录再写入」的原子保证，只有一次实际执行，其余全部回放。

### 4. 主动取消 `POST /barriers/{barrier_id}/cancel`

请求体必须携带**非空** `reason`（纯空白字符串返回 `422`，首尾空白会被去除后保存）：

```bash
curl -s -X POST http://localhost:8000/barriers/<id>/cancel \
  -H 'Content-Type: application/json' \
  -d '{"reason":"上游已放弃等待，提前收尾"}'
```

返回（`200`）：

```json
{
  "barrier_id": "8f3c…",
  "status": "cancelled",
  "reason": "上游已放弃等待，提前收尾",
  "cancelled_at": "2026-09-20T14:58:00.123000Z",
  "view": { "status": "cancelled", "cancel_reason": "…", "cancelled_at": "…", "checked_in": ["node-a"] }
}
```

终态竞争与冲突规则：

- 取消与达标报到由两段 Redis Lua 脚本原子竞争，**先发生者成为唯一终态**：
  - 取消先发生 → 状态 `cancelled`，保存 `reason` 与 `cancelled_at`，此后任何报到返回 `409`（连 `checked_in` 都不会被追加）；
  - 达标先发生 → 取消返回 `409`，`quorum` / `frozen_at` 不变；
  - 已截止 → 取消返回 `409`，缺席名单不变。**任何路径都不得改写既有终态。**
- **重复取消幂等**：返回 `200` 且 `status` 为 `already_cancelled`，`reason` / `cancelled_at` 始终是首次保存的同一份结果（即使本次请求体里的 `reason` 不同）。
- 取消成功会向 Pub/Sub 频道发布 `cancelled`，**唤醒全部已在 `/wait` 挂起的等待者**；发布丢失时仍有轮询兜底。
- 取消时已有的报到名单作为快照保留在 `checked_in` 中；取消本身不是报到行为，不产生报到审计事件。
- 屏障不存在/已回收 → `404`。

### 5. 运行中延期 `PATCH /barriers/{barrier_id}/deadline`

请求体传入新的**绝对截止时间**（ISO-8601，不带时区按 UTC 处理）。只有屏障仍处于 `waiting` 且新时间**严格晚于当前 deadline** 时才生效；成功后返回更新后的完整 `BarrierView`（200）：

```bash
curl -s -X PATCH http://localhost:8000/barriers/<id>/deadline \
  -H 'Content-Type: application/json' \
  -d '{"deadline":"2026-09-21T16:30:00Z"}'
```

返回（`200`，节选）：

```json
{
  "barrier_id": "8f3c…",
  "status": "waiting",
  "threshold": 2,
  "deadline": "2026-09-21T16:30:00Z",
  "checked_in": ["node-a"],
  "checked_count": 1
}
```

冲突与状态语义：

- 新 deadline **早于或等于**当前 deadline → `409`（`新 deadline 必须严格晚于当前 deadline`）；
- 屏障已 `reached` / `cancelled` / `expired` → 一律 `409`，文案分别标明达标 / 取消 / 已截止，**终态绝不因延期复活**，快照、缺席名单、取消原因均保持不变；
- 请求体里的时间本身已是过去时间 → `422`；屏障不存在/已回收 → `404`；
- 延期只改 deadline 与键存活期，**不是报到行为**，不产生报到审计事件，也不改变任何报到/分组计数。

**并发语义（以 Redis 实际生效顺序为准）**：终态校验（frozen / cancel / 是否已过当前 deadline）、新旧 deadline 比较、deadline 写回与**全部 8 个相关键**（meta、roster、checked、frozen、events、cancel、gmap、gcheck）的 `PEXPIRE` 存活期刷新在**同一段 Lua 脚本**内原子完成。因此：

- 多个延期请求并发：Redis 单线程串行执行，按实际生效顺序逐个比较；每次都比执行瞬间的当前值更晚的请求成功，**较早或相同的请求返回 `409`**，最终 deadline 唯一确定为最大生效值；
- 延期与达标报到 / 取消并发：与既有三段（报到、取消、延期）脚本互相串行，先提交者决定唯一结果——若报到先达标或取消先生效，延期返回 `409`；若延期先生效，后续报到按**新 deadline** 判定，取消仍可正常进行；
- 成功延期后，全部键的回收时间按新 deadline 顺延（`新 deadline − now + BARRIER_TTL_BUFFER_MS`），不会在新截止时间之前被 Redis 提前回收。

**对 `/wait` 的影响**：延期成功后向频道 `barrier:{id}:events` 发布 `deadline_extended`，所有已在 `/wait` 挂起的等待者被立即唤醒并**回读最新 deadline** 后继续等待——即使旧截止时间已过，也不会误报 `expired`；等待者最终在新的截止时间返回 `expired`，或在那之前达标 / 被取消时立即返回终态。发布丢失时轮询兜底同样会回读最新 deadline。

可连续多次延期，每次都必须严格晚于上一次生效后的值：

```bash
# 第一次成功
curl -s -X PATCH http://localhost:8000/barriers/<id>/deadline \
  -H 'Content-Type: application/json' -d '{"deadline":"2026-09-21T16:30:00Z"}'
# 相同或更早 -> 409
curl -s -X PATCH http://localhost:8000/barriers/<id>/deadline \
  -H 'Content-Type: application/json' -d '{"deadline":"2026-09-21T16:30:00Z"}'
# 更晚 -> 200
curl -s -X PATCH http://localhost:8000/barriers/<id>/deadline \
  -H 'Content-Type: application/json' -d '{"deadline":"2026-09-21T17:00:00Z"}'
```

### 6. 运行中参与者替补 `POST /barriers/{barrier_id}/replace`

某个名单成员确认无法按时报到时，可在屏障仍处于 `waiting` 期间把其**原子地替换为名单外的新标识**。请求体传入原参与者与替补标识（首尾空白会被去除；两者去除空白后均不得为空且不得相同，否则 `422`）：

```bash
curl -s -X POST http://localhost:8000/barriers/<id>/replace \
  -H 'Content-Type: application/json' \
  -d '{"participant_id":"node-b","replacement_id":"node-b-standby"}'
```

返回（`200`）：

```json
{
  "barrier_id": "8f3c…",
  "status": "replaced",
  "participant_id": "node-b",
  "replacement_id": "node-b-standby",
  "group": null,
  "replaced_at": "2026-09-21T14:57:30.123000Z",
  "view": {
    "status": "waiting",
    "participants": ["node-a", "node-b-standby", "node-c"],
    "checked_in": ["node-a"],
    "checked_count": 1
  }
}
```

生效条件与语义：

- 仅当**屏障仍 `waiting`、原参与者在当前名单内且尚未报到、替补标识不在当前名单内**时才生效；
- 换人后**原标识立即退出名单**（再报到按非名单成员返回 `403`），**新标识成为正式名单成员**，可正常报到；屏障的 `threshold` 与截止时间均不变；
- 启用**分组配额**时，新成员**继承原成员的分组**（响应中的 `group` 为继承到的分组名，未启用分组配额时为 `null`）；换人不改变任何已有报到 / 分组计数——原成员本就未报到，其分组计数为 0，新成员报到后按继承到的分组正常计数；
- 同一席位可被**连续替补**（`b → b1 → b2`），已被换下的旧标识此时属于名单外标识，也可以在后续替补中被再次换入。

冲突与状态规则：

- 原参与者**已报到** → `409`（已报到席位不可替换）；替补标识**已在名单内**（含已被其他替补先换入）→ `409`；
- 屏障已 `reached` / `cancelled` / `expired` → 一律 `409`，文案分别标明达标 / 取消 / 已截止，**终态绝不因替补复活**，名单、quorum、缺席名单、取消原因均保持不变；
- 原参与者**从未在名单内或已被先发生的替补换下** → `404`（屏障本身不存在 / 已回收同样 `404`）；
- 被拒绝的替补**不产生任何审计事件，也不改变名单、分组与计数**。

**并发语义（以 Redis 实际生效顺序为准）**：原成员「在名单内且未报到」校验、新标识「名单外」校验、终态校验（frozen / cancel / 当前截止）、名单 `SREM + SADD`、`gmap` 分组继承（`HDEL` 原成员 + `HSET` 新成员）与审计事件 `RPUSH` 在**同一段 Lua 脚本**内原子完成，与报到、取消、延期脚本在同一 Redis 上串行竞争：

- 替补 vs 原成员报到：替补先生效则原成员立刻失去名单身份，其报到返回 `403`；报到先生效则替补返回 `409`，二者必有且仅有一个赢家；
- 两个替补争抢同一未报到成员：先执行者换入成功，后执行者因原成员已不在名单返回 `404`；并发替补**不同成员**则各自独立成功；
- 替补与取消 / 达标 / 延期并发：终态先落地则替补返回 `409` 且不复活终态；替补先生效后再取消 / 达标 / 延期的，按替换后的名单继续正常进行（取消保留换人后的名单快照，延期正常顺延）。

成功替补后向频道 `barrier:{id}:events` 发布 `participant_replaced`，所有已在 `/wait` 挂起的等待者被立即唤醒并**回读替换后的名单与分组映射**；屏障仍未达标时继续等待（不会被错误提前返回），随后达标 / 取消 / 截止时照常返回终态。发布丢失时轮询兜底同样会回读最新名单。

### 7. 阻塞等待 `GET /barriers/{barrier_id}/wait`

```bash
curl -s http://localhost:8000/barriers/<id>/wait   # 挂起，直到达标、取消或截止
```

- 达标时立即返回 `status: "reached"`、冻结的 `quorum` 与 `frozen_at`；
- 取消时立即返回 `status: "cancelled"`、`cancel_reason` 与 `cancelled_at`；
- 截止时返回 `status: "expired"`、`checked_in` 已报到名单与 `missing` 缺席名单；
- 任意数量的等待者并发挂起：达标 / 取消 / 延期 / 替补各发布一条 Pub/Sub 消息全部唤醒；订阅采用“先订阅后复查 + 轮询兜底”，消息丢失或错过发布也不会漏唤醒（自然截止不发布消息，由轮询在截止点到期兜底）。等待期间 deadline 被延期时，等待者以回读到的**最新 deadline** 为准继续等待，绝不在旧截止点误报 `expired`；等待期间发生参与者替补时，等待者回读到的即是**替换后的名单与分组映射**，并继续等待新成员报到。

### 8. 非阻塞查询 `GET /barriers/{barrier_id}`

随时查看当前 `waiting / reached / expired / cancelled` 状态、报到人数、冻结快照、缺席与迟到名单、取消原因与时间。

启用分组配额的屏障还会返回 `participant_groups` 与 `group_progress`：

```json
{
  "status": "waiting",
  "threshold": 4,
  "checked_count": 4,
  "group_progress": [
    {"group": "dc1", "threshold": 1, "checked_count": 2, "satisfied": true},
    {"group": "dc2", "threshold": 1, "checked_count": 2, "satisfied": true},
    {"group": "dc3", "threshold": 1, "checked_count": 0, "satisfied": false}
  ]
}
```

上例中总人数已达 `threshold`，但 `dc3` 配额未满足，屏障仍为 `waiting`。`reached` 后该数组是**冻结快照**（与 `quorum` 同生共死），迟到报到不会推进任何计数；`expired` / `cancelled` 下如实反映终态发生时的进度。未启用分组配额的屏障不返回该字段。

### 9. 审计事件 `GET /barriers/{barrier_id}/events`

```bash
curl -s 'http://localhost:8000/barriers/<id>/events?cursor=0&limit=100'
```

每次名单成员报到（`checked_in` / `already_checked_in` / `reached` / `late`）都会在与状态变更**同一 Redis 原子操作**中追加一条有序事件；**批量报到按成员顺序逐条追加**（每批至多一条 `reached`，批内重复成员记 `already_checked_in`）；每次成功的运行中替补也在换人脚本内追加一条 `replaced` 事件。非名单成员、截止后报到、**被整批拒绝的批量报到**、被拒绝的替补等错误请求不产生事件；**批量报到的幂等重试只回放首次结果，不重复追加事件**。

返回（节选；第 3 条为替补事件）：

```json
{
  "barrier_id": "8f3c…",
  "events": [
    {"seq": 1, "participant_id": "node-a", "result": "checked_in", "ts": "2026-09-21T14:59:50.123000Z", "original_participant_id": null},
    {"seq": 2, "participant_id": "node-b-standby", "original_participant_id": "node-b", "result": "replaced", "ts": "2026-09-21T14:59:51.000000Z"},
    {"seq": 3, "participant_id": "node-b-standby", "result": "checked_in", "ts": "2026-09-21T14:59:52.456000Z", "original_participant_id": null}
  ],
  "next_cursor": 3
}
```

替补事件（`result: "replaced"`）字段：`participant_id` 为**换上的新标识**，`original_participant_id` 为**被换下的原成员**；报到事件的 `original_participant_id` 为 `null`。

- `cursor`：已读到的最大事件序号（`0` 表示从头开始）；`limit`：单页条数（1–1000，默认 100）；
- 用上一页返回的 `next_cursor` 作为下一页的 `cursor` 即可顺序读完；无新事件时返回空页且 `next_cursor` 不变；
- 事件只增不改、`seq` 连续递增（报到与替补共用同一序号序列），游标分页在并发写入下稳定、不重不漏；
- 事件与屏障其他数据同生命周期：回收前可查询，回收后一并消失（`404`）。

## 达标结果交接（handoff）

屏障达标后，可把达标结果交给一个**带租约的交接单**继续处理（例如让恰好一个执行器接管收尾动作）。交接单在创建时**复制**屏障的达标快照 `quorum` / `frozen_at`，此后与屏障完全解耦：交接单键**不设置 TTL**，屏障到期回收（或删除）后交接单仍完整可查、可认领、可完成；“屏障 → 交接单”映射同样独立持久保存。

租约采用经典的 **fencing token（栅栏令牌）** 模型：每次成功认领（含到期接管）得到一个**单调递增的整数 `fence`**；续租与完成必须同时携带正确的 `owner` 与 `fence`，且租约在裁决瞬间仍有效。全部判定在 **Redis Lua 脚本**中以 `TIME` 命令返回的 **Redis 自身时间**一次原子完成，不依赖任何应用进程时钟。

租约时长由环境变量 `HANDOFF_LEASE_S` 配置（默认 **30 秒**）。

### 10.1 建立交接单 `POST /handoffs`

请求体只携带 `barrier_id`。仅**已达标（reached）**屏障可以建立：屏障不存在 / 已过期回收 → `404`；尚未达标（`waiting` / `expired` / `cancelled`）→ `409`。

```bash
curl -s -X POST http://localhost:8000/handoffs \
  -H 'Content-Type: application/json' \
  -d '{"barrier_id":"8f3c…"}'
```

首次创建返回 `201`，响应即交接单视图（初始 `idle`）：

```json
{
  "handoff_id": "1d2e…",
  "barrier_id": "8f3c…",
  "status": "idle",
  "quorum": ["node-a", "node-b"],
  "frozen_at": "2026-09-21T14:59:52.456000Z",
  "owner": null,
  "fence": null,
  "leased_at": null,
  "expires_at": null,
  "completed_at": null
}
```

- **同一屏障重复创建返回原单**：HTTP `200`，`handoff_id` 与 `quorum` / `frozen_at` 与首次完全一致（即使屏障随后已被回收，独立保存的映射仍把重复创建导向原单）；
- 并发重复创建由创建脚本原子裁决，**绝不可能为同一屏障产生两份交接单**；
- 快照在创建时自屏障 `frozen` 键复制，之后永不回读、永不改写。

### 10.2 查询交接单 `GET /handoffs/{handoff_id}`

返回达标快照、状态与当前租约：

- `idle`：从未认领或租约已到期，**可被认领**；不暴露上一任持有者的失效凭证（`owner` / `fence` 为 `null`）；
- `leased`：租约有效，附带 `owner` / `fence` / `leased_at` / `expires_at`；
- `completed`：已完成终态，附带 `completed_at`。

租约是否到期以查询脚本内同一次 Redis `TIME` 为准。交接单不存在 → `404`。

### 10.3 认领 `POST /handoffs/{handoff_id}/claim`

空闲或租约到期才可认领；请求体携带 `owner`（非空，去首尾空白）：

```bash
curl -s -X POST http://localhost:8000/handoffs/1d2e…/claim \
  -H 'Content-Type: application/json' \
  -d '{"owner":"worker-7"}'
```

成功返回 `200`：

```json
{
  "handoff_id": "1d2e…",
  "status": "leased",
  "owner": "worker-7",
  "fence": 1,
  "leased_at": "2026-09-21T15:00:00.000000Z",
  "expires_at": "2026-09-21T15:00:30.000000Z"
}
```

- 首次认领 `fence=1`；租约到期后被他人接管，`fence` 严格 `+1`（之后 2、3、…），单调递增；
- `expires_at = 认领时刻（Redis 时间）+ HANDOFF_LEASE_S`；
- 他人租约仍有效时认领 → `409`（文案给出当前持有者、其 fence 与到期时间）；
- 交接单 `completed` 后认领 → `409`，**永久拒绝再次认领**；不存在 → `404`。

### 10.4 续租 `POST /handoffs/{handoff_id}/renew`

必须携带 `owner` 与认领时获得的 `fence`：

```bash
curl -s -X POST http://localhost:8000/handoffs/1d2e…/renew \
  -H 'Content-Type: application/json' \
  -d '{"owner":"worker-7","fence":1}'
```

续租不更换持有者、`fence` 不变，`expires_at` 从裁决时刻起再顺延一个 `HANDOFF_LEASE_S`。以下情况一律 `409`：

- 租约已到期（须重新认领，接管后 fence 已前进）；
- `owner` 与当前持有者不符（例如别的执行器已接管）；
- `fence` 与当前栅栏不符（旧持有者拿着被接管前的旧 fence）；
- 交接单已完成。不存在 → `404`。

### 10.5 完成 `POST /handoffs/{handoff_id}/complete`

凭证规则与续租完全相同（`owner` + `fence` 且租约仍有效）：

```bash
curl -s -X POST http://localhost:8000/handoffs/1d2e…/complete \
  -H 'Content-Type: application/json' \
  -d '{"owner":"worker-7","fence":1}'
```

成功返回 `200` 与 `completed_at`，状态永久为 `completed`。此后**再次认领、续租或完成一律 `409`**（终态不可改写、不可重新认领）；旧凭证、到期凭证同样 `409`；不存在 → `404`。

**典型接管时序**（`HANDOFF_LEASE_S=30`）：

1. `worker-A` 认领 → `fence=1`，租约 30s；
2. A 崩溃未续租，30s 后 `worker-B` 认领成功 → `fence=2`；
3. A 恢复后续租 / 完成（`fence=1`）全部 `409`——旧栅栏被原子拒绝，A 无法再干扰交接单；
4. B 用 `fence=2` 完成 → `completed`，任何人都不能再认领。

### 10.6 原子性与持久化语义

- 创建、认领、续租、完成各是一段 Redis Lua 脚本：竞争认领（空闲抢占 / 到期接管）、`fence` 递增、`owner` / `fence` 凭证校验、终态写定均在脚本内一次完成，Redis 单线程串行化保证并发下结果唯一；
- 所有时间判定（租约是否到期、到期时间计算、完成时刻）都使用脚本内 `redis.call('TIME')` 的 **Redis 时钟**，应用服务器时钟回拨 / 偏移不影响裁决；
- 交接单本体 `handoff:{id}` 与索引 `handoff:bybarrier:{barrier_id}` **从不设置 TTL**，与有回收周期的 `barrier:*` 键空间完全分离；compose 中 Redis 开启 AOF，交接单随 Redis 数据一起持久化。

## 并发与一致性保证

- 报到判定由一段 Lua 脚本完成：`名单校验 → 终态校验（frozen/cancel/截止）→ 去重 SADD（新成员才同步 HINCRBY 对应分组计数）→ 人数与分组配额判定 → 写入冻结快照（quorum + 分组计数）` 整体原子执行；取消由另一段 Lua 脚本完成：`终态校验（cancel/frozen/截止）→ 写入取消快照` 整体原子执行。Redis 单线程串行执行脚本，因此达标与取消**必有且仅有一个赢家**：并发报到（包括不同分组的成员同时补齐各自短板）只有一个请求得到 `reached`，或者取消请求得到 `cancelled`，互斥终态的成员集合、时间点、分组计数、取消原因均唯一确定。
- 运行中延期由第三段 Lua 脚本完成：`终态校验（frozen/cancel/当前截止）→ 新旧 deadline 严格比较 → 写回 deadline 与重算 ttl_ms → 对全部 8 个键 PEXPIRE` 整体原子执行。它与报到、取消脚本在同一 Redis 上串行竞争：终态已落地（reached / cancelled / expired）时延期返回 `409` 且不复活终态；并发延期以 Redis 实际生效顺序为准，只有新时间严格晚于执行瞬间当前值的请求成功，相同或更早的请求返回 `409`，最终 deadline 唯一确定。延期不触碰 checked / gcheck / frozen / cancel 的任何业务数据，仅更新 meta 中的 deadline 与各键存活期。
- 运行中替补由第四段 Lua 脚本完成：`原成员在名单内 → 原成员尚未报到 → 新标识在名单外 → 终态校验（frozen/cancel/当前截止）→ roster SREM 原成员 + SADD 新标识 → gmap 分组继承（HDEL/HSET）→ RPUSH replaced 审计事件 → 对全部 8 个键 PEXPIRE` 整体原子执行。它与报到、取消、延期脚本在同一 Redis 上串行竞争：替补与原成员的报到并发时，先执行者唯一胜出（替补先则原成员报到按非名单成员拒绝，报到先则替补返回 `409`）；两个替补争抢同一成员时只有先执行者成功，后执行者收到 `404`；终态已落地时替补返回 `409` 且不复活终态。替补不触碰 checked / gcheck / frozen / cancel 的任何业务数据（未报到成员本就没有计数），仅在 roster 与 gmap 中把原标识改写为新标识。
- **批量报到由第五段 Lua 脚本完成**：`幂等记录查询（已有同键记录且名单指纹一致 → 直接回放首次结果；同键不同名单 → conflict）→ 全员名单校验（任一未知成员立即返回，整批零写入）→ 终态/截止校验（frozen → 整批按迟到处理；cancel / 已过截止 → 整批零写入返回）→ 整批 SADD（仅新成员 HINCRBY 分组计数）→ 总数与分组配额判定 → 必要时冻结 quorum → 逐成员 RPUSH 审计事件 → 首次结果快照与幂等记录同事务落盘 → PEXPIRE（含全部幂等记录）` 整体原子执行。它与单人报到、取消、延期、替补脚本在同一 Redis 上串行竞争：批量与单人（或多个不同幂等键的批量之间）同时补齐最后席位时，Redis 串行顺序决定**唯一**一次冻结，跨接口也只有一个 `triggered=true`；幂等键维度的并发重试因为「记录查询与写入同脚本原子」而不可能重复执行——先到者写入首次快照，后到者只回放。整批失败（未知成员 / 截止 / 取消）发生在任何业务写入之前，天然满足「不产生任何报到副作用」，且因未写入幂等记录，同一幂等键可随修正后的请求安全复用。
- 总数与分组计数都只统计**去重后的唯一报到**：重复报到（`already_checked_in`）与达标后的迟到报到（`late`）均不执行 `HINCRBY`，分组计数不重复累计；达标冻结之后连 `gcheck` 计数也一并冻结。
- 审计事件在同一 Lua 脚本内追加（`RPUSH` 到 `events` 列表，序号 = 列表长度 + 1）：报到事件（含批量报到的逐成员事件）与替补事件共用同一序号序列，均与各自的状态变更/换人操作同生共死，要么一起成功，要么都不发生；脚本串行执行保证 `seq` 从 1 连续递增、无空洞无重复。被拒绝的请求（非名单成员、终态后操作、冲突的延期/替补、**含未知成员或终态后被整批拒绝的批量报到**）不产生事件；批量幂等重试只回放记录，同样不重复产生事件。
- 终态落地后脚本的所有写路径都不会触碰对方的终态键：迟到报到只追加到 `checked` 集合（反映在 `late` 字段）；取消后的报到不写任何键；已达标 / 已截止后的取消直接返回冲突；已达标 / 已取消 / 已截止后的延期与替补直接返回冲突。
- 报到响应里的 `PUBLISH reached`、取消响应里的 `PUBLISH cancelled`、延期响应里的 `PUBLISH deadline_extended` 与替补响应里的 `PUBLISH participant_replaced` 都在脚本执行成功之后进行；即使发布失败，等待者每 `BARRIER_POLL_INTERVAL_S` 秒回读一次真实状态（含 meta 中可能已被延期的最新 deadline，以及 roster / gmap 中可能已被替补改写的名单与分组）。
- 自然截止不发布消息（无后台扫描进程），等待者在剩余截止时间上自然到期回读；该剩余时间每轮都按回读到的最新 deadline 重算，因此延期后旧截止点不会产生误报，避免引入额外调度组件。

## 本地不使用容器开发

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
REDIS_URL=redis://localhost:6379/0 HANDOFF_LEASE_S=30 \
    uvicorn app.main:app --reload --port 8000
```

需要自行准备一个 Redis 7。

## 项目结构

```
app/
  main.py           FastAPI 路由与错误映射（屏障 + 交接单）
  barrier_store.py  Redis 键模型 + 原子报到/批量报到/取消/延期/替补 Lua 脚本（分组配额计数、审计事件、批量幂等记录、TTL 顺延）+ Pub/Sub
  handoff_store.py  达标结果交接单：独立持久键模型 + 原子创建/认领/续租/完成 Lua 脚本（Redis 时间裁决租约、单调递增 fence、终态拒绝）
  service.py        状态机视图组装、批量首次结果快照回放、交接单视图组装、阻塞等待（按最新 deadline / 替换后名单回读）
  models.py         请求/响应模型与校验
  config.py         环境变量配置（含 HANDOFF_LEASE_S）
Dockerfile          API 镜像（沿用，8000 端口）
docker-compose.yml  api + redis（健康检查、依赖顺序、数据卷、租约环境变量）
```
