# Hermes 多实例部署改造设计

> 状态：设计稿（讨论中，未开工）
> 分支：`wmaxai/multi-instance`（从 `hotfix` 切出）
> 日期：2026-09-12
> 作者：wmaxai 服务端

## 1. 目标与范围

### 1.1 要解决的问题

当前 Hermes 是单机有状态服务：一个网关进程、一份 SQLite、一份本地文件工作区。单点故障意味着网关挂掉后所有机器人全部不可用。对未来对外提供多租户服务而言，这个架构无法支撑。

### 1.2 目标形态（关键定义）

**要的不是"无状态"，是"有状态但可迁移"。**

**本质：把协调组件从进程内换成数据库——租约就是"数据库层的分布式锁"。**

这与用 ZK / etcd 做分布式锁是同一类方案，区别只在于协调组件选的是 PG。由于 PG 同时承载会话数据，**锁与数据同源，反而少了一个需要单独保证高可用的组件**。

| 维度 | 目标 |
|---|---|
| 会话粘性 | 一个会话同一时刻只由一个实例处理（**由 PG 租约实现，非路由层**） |
| 灵活性 | 但不能绑死在一台机器上——实例故障后另一台能接管 |
| 长任务 | 优先在同一节点跑完（避免无谓迁移） |
| 故障恢复 | 实例挂了，另一实例能从持久化状态恢复继续工作 |
| 可接受损失 | 未刷盘的数据可以丢（可重建），不追求 turn 级精确恢复 |
| 恢复粒度 | **粗粒度（会话级）即可**，不做工具调用级的细粒度重放 |

用一句话概括这个中间态：**长请求是介于"短请求无状态"和"有状态选举"之间的租约租赁模型。**

### 1.3 明确不做的事

- **不追求真正的无状态**：正在执行的 `AIAgent` 对象、工具执行上下文无法也无必要序列化
- **不做 turn 级精确恢复**：即使持久化了中间状态，很多工具调用本身不可重入（已发出的消息、已推送的代码）
- **不引入 Redis**：见第 6 节论证
- **不上 k3s 改造前置**：本改造在单机可验证，稳定后再同步 stable

---

## 2. 现状调研结论（全部基于源码与实测）

### 2.1 已有的机制（重要：地基已经存在）

Hermes **不是**完全没有为多实例做准备。它已经有一套完整的跨进程租约机制。

#### 2.1.1 会话轮次租约（session turn lease）

实现在 `hermes_state_compression.py:519-605`，表结构：

```sql
CREATE TABLE session_turn_leases (
    conversation_id TEXT,
    holder          TEXT,
    acquired_at     REAL,
    expires_at      REAL
);
```

核心 API：

| 函数 | 语义 |
|---|---|
| `try_acquire_session_turn_lease(session_id, holder, ttl_seconds)` | 原子抢占（单写事务内完成查找+插入+回收） |
| `acquire_session_turn_lease(...)` | 带等待的获取（默认等 1800s，支持 abort 回调） |
| `refresh_session_turn_lease(session_id, holder)` | 续租，仅 holder 可续 |
| `release_session_turn_lease(session_id, holder)` | 释放，仅 holder 可释放，幂等 |

源码注释原文：`Atomically acquire the cross-process turn lease for a conversation (keyed by the lineage root)`。

**关键设计点**：
1. **按会话血统根加锁**：压缩产生的子会话会向上走 parent 链，归到同一个 `conversation_id`，避免压缩期间出现锁分裂
2. **回收条件**：`expires_at <= now OR holder 的本地进程已死`
3. **所有权严格校验**：`refresh`/`release` 都带 `WHERE holder = ?`，旧主的迟到操作无法影响新主

#### 2.1.2 交接状态机（handoff）

实现在 `hermes_state_gateway.py`，状态流转：

```
pending → running → completed | failed
```

- `claim_handoff`：对 `handoff_state` 做**原子 CAS**（多个实例同时抢，只有一个成功）
- `list_pending_handoffs`：gateway handoff watcher 扫描待处理交接
- `reap_stuck_handoffs`：清理卡死的交接（把 running 置为 failed）

#### 2.1.3 消息持久化时机

实现在 `agent/session_persistence.py`：

- `_db_flush_write` 把一轮新增消息**在一个事务里批量写入**（`append_messages_batch`）
- **append-only 语义**：只在安全点写，避免提交合成轮次
- **写库时同时续租**：调用时传入 `turn_lease_holder` / `turn_lease_ttl_seconds=300`

即：**不需要自己实现"定时刷盘"，现有机制已经是"每轮安全点落库"。**

#### 2.1.4 关机兜底刷盘

`gateway/shutdown_flush.py`：关机前把 `_pending_messages` 和 agent 历史原子写入 `<hermes_home>/pending_messages/`，重启后 `recover_pending_to_db` 回放进 DB。

#### 2.1.5 `hosted_rooms` 的复制与 epoch 接管机制（重要参考实现）

除租约外，Hermes 还有第二套多实例协调机制，实现在 `gateway/hosted_room_replicas.py`（**官方实现，非 hack**）：

| 能力 | 实现 |
|---|---|
| 状态复制 | `ingest_page()` —— 分页拉取房间日志，**幂等、防缺口、防 epoch 回退** |
| 故障接管 | `promote_replica()` —— 在 `epoch + 1` 上恢复，写入 `authority.claimed` 事件作为血统证明 |
| 防脑裂 | `demote_room()` —— 旧权威看到更新 epoch 时记录 `authority.lost` 并降级 |
| 版本化 | **`authority_epoch`** —— 每次接管 +1（即 fencing token 的官方实现） |

**两个重要结论**：

1. **不需要自定义 fencing token** —— 官方已有 `authority_epoch` 机制，且覆盖了"旧权威回归"这个最难场景。改造时应**参考它而非自造**。
2. **同步是拉模式（pull），不是推模式（push）** —— 副本主动按序列拉日志页，**不需要 Redis/Valkey 这类广播中间件**。这正是 Hermes 与 FNS 在基础设施需求上的根本差异（见 3.0）。

**适用范围说明**：`hosted_rooms` 服务于**群聊房间**场景；普通单会话走的是租约机制（2.1.1）。**两套机制并存**：

- **普通会话** → 租约（互斥，active/standby 语义）
- **群聊房间** → 复制 + epoch 接管（多副本高可用语义）

### 2.2 租约机制的历史与定位（决定了它为什么只跑 SQLite）

从 git 历史可查：`19527db731 fix(gateway): per-session turn lease + conversation-scope funnel (#64934) (#67401)`

引入原因（commit message 原文摘要）：
- 多个 routing key 会映射到**同一个 session_id**（`/resume` 从另一聊天续接、CLI 续接重绑定、async-delegation 绑定、topic-binding tip-walk）
- 两个 routing key 跑同一会话的并发 turn，对**所有按 key 的守卫都不可见**，导致 flush 顺序错乱、去重吞行、陈旧历史基座

**演进路径**：第一版是**进程内 asyncio 锁**（`gateway/turn_lease.py`），后发现不够，commit 的 "Known limits" 明确写了 `CLI-continuity cross-process pairs need a DB-level lease`，于是补上 DB 级租约。

**结论：租约不是为"多机集群"设计的，是为"单机多入口/多进程并发同一会话"设计的。** 这也解释了为什么它跑在 SQLite 上——作者假设的是单机。但机制本身是通用的，换 PG 后语义原样成立。

### 2.3 实测验证（本机，2026-09-12）

用两个独立 Python 进程、共享一份 `state.db` 副本（`/tmp/lease_test/`），验证了 8 项语义，**全部通过**：

| 语义 | 结果 | 证据 |
|---|---|---|
| 原子抢占 | ✅ | 进程 A（PID 2919514）抢到 |
| **跨进程互斥** | ✅ | 独立进程 B（PID 2919515）被挡 |
| 按会话隔离 | ✅ | 不同会话不互斥 |
| 释放后接管 | ✅ | 无死锁残留 |
| 所有权校验 | ✅ | 冒充者不能释放、不能续租 |
| 续租 | ✅ | 仅 holder 可续期 |
| **进程死亡立即回收** | ✅ | **0.26s 接管**（不等 600s TTL） |
| TTL 过期回收 | ✅ | 2s 过期，3s 后被成功回收 |

**实测边界（不可外推的部分）**：
1. 验证的是**同机多进程共享本地文件**，不是跨机多副本——SQLite 的文件锁在 NFS/CephFS 上不可靠
2. **`_compression_lock_holder_process_is_dead` 依赖本地 PID**，跨机器失效，只能等 TTL
3. 未压测（100 进程级并发抢同一会话）

**发现的现象**：生产库中存在一条**过期 331 秒仍未清理**的租约记录。租约是**惰性回收**的（下次竞争者来时重建），不主动清理。单机无害，多副本下需关注脑裂窗口。

---

## 3. 架构设计

### 3.0 改造的本质：与 FNS 的对比

**本质：Hermes 的多实例改造与 FNS 高度同构——PG 是核心，其余按需可选。**

| 维度 | FNS | Hermes | 差异原因 |
|---|---|---|---|
| **数据库** | SQLite → PostgreSQL | SQLite → PostgreSQL | **核心且必需**，两者完全一致 |
| **分布式锁** | 内存锁 → Valkey | 进程内 asyncio 锁 → **PG 租约表** | Hermes **不需要 Redis**——PG 的 `ON CONFLICT` 提供同等原子性，且锁与数据同源 |
| **全文搜索** | Bleve → Typesense | FTS5 → PG 内置全文检索 | Hermes **不需要独立搜索引擎**，PG 自带能力足够 |
| **文件存储** | 本地文件 → COS | 本地文件 → PVC / git 拉取 | 可选，第一阶段可用挂载解决 |
| **跨实例广播** | WebSocket 通道 → Valkey Pub/Sub | **无此需求** | FNS 是多实例共享同一 WS 通道；Hermes 每实例独立持有平台连接 |

**一句话总结**：FNS 需要引入 Redis/Valkey 是因为它有"多实例共享 WebSocket 通道"这个需求；**Hermes 没有这个需求，所以 PG 一个组件就能承担全部协调职责**。

### 3.1 目标架构

```
                    ┌──────────────┐
   Feishu ──────────▶│  Ingress /   │  (长连接粘性 或 webhook 直连)
   (webhook/WS)      │   Route      │
                    └──────┬───────┘
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
         ┌────────┐   ┌────────┐   ┌────────┐
         │ Hermes │   │ Hermes │   │ Hermes │   每 pod 一个网关进程
         │  pod 1 │   │  pod 2 │   │  pod N │   可承载多个 bot profile
         └───┬────┘   └───┬────┘   └───┬────┘
             │            │            │
             └────────────┼────────────┘
                          ▼
              ┌───────────────────────┐
              │  PostgreSQL           │  ← 会话/租约/handoff/队列/用量
              └───────────────────────┘
                          │
              ┌───────────────────────┐
              │  对象存储 (COS)        │  ← workspace / 附件 / 技能快照
              └───────────────────────┘
```

### 3.2 三种入口模式的处理策略

**核心结论：不需要粘性路由（session affinity）。**

会话互斥由 PG 租约保证——**谁抢到谁跑，抢不到的排队，与 pod 无关**。路由层不需要再做一次。

| 模式 | 特性 | 策略 |
|---|---|---|
| **webhook** | 无状态入站，HTTP 请求即路由 | **不需要粘性**，且粘性有害（破坏负载均衡）。入站即落库 + 租约决定谁执行 |
| **长连接（WS/飞书）** | 连接有物理归属，谁连谁收 | **不是应用层粘性路由**，而是连接层的物理事实。收到消息后照样走租约 |
| **API server / local** | 调用方指定 | 无状态 |

**为什么"连接归属"不等于"粘性路由"**：飞书 WebSocket 连在哪个 pod，消息就从哪个 pod 进——这是连接层的客观事实，不需要任何配置。这个 pod 收到消息后，**仍要竞争租约**：若该会话正被其他 pod 处理，它排队等待，等对方释放后接管。**所以实例与会话之间是"租约关系"，不是"绑定关系"。**

**唯一与粘性相关的场景**：长连接 pod 崩了，飞书重连到另一个 pod。此时该 pod 能从 PG 读出完整会话历史继续处理——这正是"不能绑死在一台机器"的体现。若原 pod 仍持有租约，需等 TTL 超时才能接管（见 4.5，**这是故障接管速度的旋钮，不是粘性路由的补丁**）。

**webhook 下的租约等待处理**（重要）：

问题：会话正在 pod A 上跑，pod B 收到新消息，`acquire_session_turn_lease` 会等（默认 1800s），但 HTTP 请求挂不了 30 分钟。

方案：**入站即落库 + 排队消费**
1. pod B 收到消息 → 尝试抢租约
2. 抢不到 → **不等待**，写入 DB 队列，立即返回 200
3. pod A 释放租约时 → 从队列捞该会话的待处理消息继续跑

**这比粘性路由更健壮**——粘性的本质是"把有状态伪装成无状态"，pod 挂了粘性就断；而落库排队是真的无状态。

### 3.3 状态分类与存储归属

| 类别 | 内容 | 现状 | 目标 |
|---|---|---|---|
| **必须持久化（DB）** | sessions / messages / session_model_usage / gateway_routing / async_delegations / session_turn_leases / handoff_* / delivery_obligations | SQLite `state.db` | **PostgreSQL** |
| **必须持久化（对象存储）** | `profiles/*/workspace`、附件、`skills/` 快照 | 本地文件 | **COS**（可选，见 3.4） |
| **可丢弃（缓存）** | `models_dev_cache.json`、`cache/`、`state-snapshots/`、`lsp/`、`bin/` | 本地文件 | **不进持久卷**（启动重建） |
| **不应进容器** | `backups/`（当前 190M，占总量 1/4） | 本地文件 | 独立 CronJob + 对象存储 |

### 3.4 文件系统状态的处置（建议分阶段）

`HERMES_HOME` 下的文件语义改造代价大（代码里到处是 `Path` 拼接和 `open()`），建议：

- **第一阶段（必须）**：`profiles/` 与 `skills/` 通过 **initContainer 从 git 拉取**或 **PVC 挂载**解决，不改代码
- **第二阶段（可选）**：workspace 产物（如 `drafts/`、`attachs/`）改走现有 `wmaxai-storage` 抽象层（fns/lfs/local 三引擎），代码改动集中在技能层
- **不建议**：把整个 `HERMES_HOME` 对象存储化。收益低、改动面大

### 3.5 部署形态

- **一个 pod 承载多个 bot profile**（保持现有 multiplex 形态），而不是一个 bot 一个 pod
- 理由：`config_env.py` 的 provider 发现逻辑会**跨 profile 扫描 `.env`**，同一 pod 内的 bot 必须共享 `HERMES_HOME`；且现有 `state.db` 是**单一账本**，打散会导致账本/会话/技能沉淀全部分裂
- 分组方式：在 `wmaxai-agents` 仓按目录组织（如 `groups/<业务线>/`），initContainer 按环境变量指定的分组铺开 profile

### 3.6 codex 隔离：必须指定 `CODEX_HOME`

**问题**：`codex-sdk` 插件拉起 codex 子进程时**不设 `CODEX_HOME`**（源码 `plugins/codex-sdk/__init__.py` 用 `os.environ.copy()`），子进程因此读取默认的 `~/.codex`，与宿主机上可能存在的 codex CLI **共用同一份配置与状态库**。

**后果**：插件与 CLI 的 `config.toml`、rollout DB、auth 互相污染（本机已实测确认二者共用同一状态库）。

**改造方案**：让插件显式注入 `CODEX_HOME`（如 `HERMES_HOME/codex-home/`），使 codex 配置、state、rollout 全部落在 Hermes 自己的目录内。

**收益**：
- codex 配置、state、rollout、auth 全部与外部 CLI 隔离
- 容器化后天然成立（pod 内 `~/.codex` 本就是 pod 私有），但**显式指定更干净**，且能避免与镜像内其他工具冲突

**注意**：若容器内直接使用 pod 的 `~/.codex`，需保证 `model_reasoning_effort` 等设置随镜像或 ConfigMap 提供。

### 3.7 多端同步（未来需求，当前未触发）

**现状**：Hermes 支持多端接入同一 agent，实测会话来源分布为 `feishu: 23 / cli: 4 / cron: 1`，`handoff_state` 状态机具备但**全部未使用**（28 个会话均为 `None`）。

**两个层次**：

| 层次 | 语义 | 现状 |
|---|---|---|
| 会话切换 | 同一会话从 A 端切到 B 端继续（`/resume`、CLI 续接） | **已实现**，租约正为此设计 |
| **并发访问** | 用户同时在两端操作同一会话 | **需设计**——租约保证不并发执行，但"第二端的体验"是产品问题 |

**如果要做多端实时同步**（如飞书与桌面同时观看一个正在跑的会话）：会话在 pod A 执行，桌面端连在 pod B，**pod B 需要获知 pod A 的实时输出**——这需要跨实例通知。

**实现优先级（成本从低到高）**：

1. **轮询 PG** —— 秒级延迟，零新组件
2. **PG `LISTEN/NOTIFY`** —— 实时，仍不需要新组件
3. Redis Pub/Sub —— 实时，但引入新组件（**不推荐**，前两者已够）

**结论**：即使未来做多端同步，**仍然不需要 Redis**。当前无此场景，列为未来需求。

---

## 4. 改造清单（按优先级）

### P0：必须做

#### 4.1 租约表迁 PostgreSQL

**目标**：把 `session_turn_leases` 的原子抢占换成 PG 语义。

```sql
-- 抢占（原子，等价于现有 _claim_lease_row）
INSERT INTO session_turn_leases (conversation_id, holder, acquired_at, expires_at)
VALUES ($1, $2, $3, $4)
ON CONFLICT (conversation_id) DO UPDATE
SET holder = EXCLUDED.holder,
    acquired_at = EXCLUDED.acquired_at,
    expires_at = EXCLUDED.expires_at
WHERE session_turn_leases.expires_at <= $3        -- 已过期才可抢
   OR session_turn_leases.holder = EXCLUDED.holder;  -- 自己可续
```

配套的 `refresh` / `release` 保持 `WHERE holder = ?` 的所有权校验。

**注意**：现有 `_claim_lease_row` 还有"holder 进程已死"的回收分支，跨机器时该分支失效（PID 是本地概念），PG 版本应删除或限制为本地模式。

#### 4.2 消息持久化迁 PostgreSQL

工作量评估（实测数据）：
- **80 个文件、581 处**直接引用 `sqlite3`
- `state.db` 有 **39 张表**，其中 **12 张是 FTS5 全文索引表**（`messages_fts*`）
- 无现成 DB 抽象层，`connect()` 到处直接返回 `sqlite3.Connection`
- 方言差异散落：`INSERT OR REPLACE`、`PRAGMA`、`AUTOINCREMENT`、`last_insert_rowid()`

**建议做法（关键）**：**不要散改 581 处**，而是在 `hermes_state` 层做**连接与方言适配层**：
1. 抽出 `SessionDB` 的统一执行入口（现有 `_execute_write` / `_read_ctx` 已是切入点）
2. 在入口处按 backend 分流 SQL 方言
3. FTS5 → PG 的 `tsvector` + `pg_trgm`（中文需 `zhparser` 或 `pgroonga`，见 4.4）

#### 4.3 PG 高可用是本方案的前提（原"fencing token"降级说明）

**结论先行：不需要应用层 fencing token。脑裂问题由 PG 的高可用保证来消除。**

**推理**：租约的抢占与释放都是 PG 上的**原子写**（`INSERT ... ON CONFLICT` / `SELECT FOR UPDATE`）。

- 网络分区的那台 pod **写不进 PG 就干不了活**——它本地"以为自己持有租约"没有意义，PG 不认
- 租约行**只有一份**，谁 UPDATE 成功谁持有，另一个必然失败

**PG 的写冲突本身就是天然的 fencing**。应用层额外加单调递增 token 的场景，是**存储允许双写**（多主、或应用自己缓存了状态）——本架构不属于此类。

**因此本方案的前置依赖是：**

| 要求 | 说明 |
|---|---|
| PG 主从复制 | 保证数据不丢 |
| 自动 failover | 主库故障能自动切换 |
| 同步复制 + 仲裁 | 避免 failover 瞬间的双主窗口 |
| failover 期间应用退避 | pod 拿不到租约时应**退避重试**，而非报错或降级放行 |

**立场（与分布式锁的通用原则一致）**：PG 作为协调组件，**它挂了服务就不该可用**——这与"用 ZK 做分布式锁就不能让 ZK 挂"是同一个道理。**PG 的高可用是本方案乃至未来运维的核心**，不属于 Hermes 应用层要兜底的问题。

> 注：极短的双写窗口由 PG 自身（同步复制 + 仲裁）解决，不应由应用层补偿。

#### 4.4 中文全文检索方案

`messages_fts*` 这 12 张表依赖 SQLite FTS5。PG 侧选项：

| 方案 | 优点 | 缺点 |
|---|---|---|
| `tsvector` + `zhparser` | 原生 PG，中文分词 | 需装扩展，k3s 镜像要自定义 |
| `pgroonga` | 中文/CJK 效果好 | 需装扩展，运维成本高 |
| `pg_trgm` 模糊匹配 | 无需扩展 | 无语义分词，检索质量降级 |

**建议**：第一版用 `pg_trgm` 兜底（保证功能可用），后续按需引入 `zhparser`。

### P1：强烈建议

#### 4.5 调整租约 TTL 与续租频率

**问题**：跨机器时 `_compression_lock_holder_process_is_dead` 失效（它依赖**本地 PID**，跨机器检测不到），故障接管只能等 TTL（默认 300s）。5 分钟的接管延迟对生产服务偏长。

**这个旋钮与路由无关**，它决定的是**故障接管速度**：
- 进程死亡（同机可检测）→ 实测 0.26s 接管
- 进程假死 / 跨机故障 → 最坏等 TTL

**方案**：
- TTL 从 300s 降到 **60s 量级**
- 续租频率相应提高（建议 TTL/3）
- 现有机制已支持续租（`refresh_session_turn_lease`），改的是参数与节奏

#### 4.6 重新审视 fail-open 语义

**现状**（commit message 原文）：卡住的租约在 `agent.gateway_timeout` 后**降级为无串行行为**（fail-open），只打 ERROR，绝不把会话卡死。

**单机下合理**（宁可并发也别卡死用户），**多副本下危险**（降级放行 = 允许双跑 = 数据损坏）。

**方案**：
- 增加配置项 `gateway.turn_lease_fail_mode: open | closed`
- 多实例模式下默认 `closed`（宁可排队/报错，不可并发写）
- 或按部署模式自动选择

### P2：可选/延后

#### 4.7 webhook 入站排队

按 3.2 的方案，实现"入站即落库 + 排队消费"。现有 `async_delegations` 表与 `_pending_messages` 机制是地基。

#### 4.8 指定 `CODEX_HOME` 隔离 codex 运行时

按 3.6，让插件显式注入 `CODEX_HOME`，使 codex 的配置/state/rollout/auth 落在 Hermes 自己的目录内，与外部 codex CLI 彻底隔离。

#### 4.9 可观测性

- 租约表增加定期清理（清理长期过期记录）
- 暴露指标：租约抢占成功率、平均等待时长、接管次数、TTL 超时次数

---

## 5. 恢复语义与预期

### 5.1 故障恢复流程

```
实例 A 故障
    │
    ├─ 进程死亡（同机检测）  → 0.26s 内另一实例可接管
    │
    └─ 进程假死/网络分区    → 等 TTL 超时（建议调到 60s）
              │
              ▼
        实例 B 通过 list_pending_handoffs 发现该会话
              │
              ▼
        claim_handoff（原子 CAS 抢占）
              │
              ▼
        从 PG 读出消息历史 → 重建 agent → 继续执行
```

### 5.2 会丢什么（明确接受）

| 内容 | 是否丢失 | 说明 |
|---|---|---|
| 已落库的会话消息 | ❌ 不丢 | 每轮安全点批量提交 |
| 正在进行的 LLM 调用 | ✅ 丢 | 重新发起 |
| 工具执行中间态 | ✅ 丢 | 该轮从头重跑 |
| 未落库的最后一小段 | ✅ 丢 | 可接受，可重建 |
| 已产生的副作用 | ⚠️ 不可逆 | 已发的消息/已推的代码不会回滚，需业务层确保幂等 |

### 5.3 恢复粒度

**会话级（粗粒度）**——这是明确的选择：
- 恢复后从该会话的历史重新发起当轮
- 用户感知是"任务重跑了"，但上下文不丢
- 业务上可接受：写文档、审核、发布等流程都有确认环节，可重试

---

## 6. 为什么不需要 Redis

对照 `fast-note-sync-service` 的三件套（`locker` / `broadcaster` / `oidc-state-store`，各有 `local | redis` 开关）：

| FNS 的 Redis 用途 | Hermes 的对应物 | 是否需要 Redis |
|---|---|---|
| **分布式锁**（locker） | `session_turn_leases` 表 + `claim_handoff` CAS | **不需要**——PG 的 `ON CONFLICT` / `SELECT FOR UPDATE` 提供同等原子性 |
| **跨实例广播**（broadcaster） | 消息投递 | **不需要**——见下 |
| **OIDC state 共享** | `auth.json` / token 缓存 | **不需要**——非多实例共享场景 |

**关于广播**：FNS 需要它是因为**多个实例都要能主动推消息给同一个 WebSocket 客户端**（通道是共享资源）。而 Hermes 的推送通道是**平台侧长连接**，本质是"谁连谁推"：
- **长连接模式**：连接归属明确，不需要广播
- **webhook 模式**：请求即路由，天然无状态，不需要广播
- **需要跨实例通知的场景**（如"A 跑完让 B 知道"）：可用 PG 的 `LISTEN/NOTIFY` 或轮询替代

**结论：Redis 可以整个不进这套架构。** 这是与 FNS 改造最大的不同点，因为 FNS 是"多实例共享 WebSocket 通道"，而 Hermes 是"每实例独立持有平台连接"。

---

## 7. 实施路径（分阶段验证）

### 阶段 0：已完成（本文档依据）

- [x] 源码调研：租约机制、handoff 机制、持久化时机
- [x] git 历史追溯：机制引入原因
- [x] **同机多进程实测：8 项租约语义全部验证通过**

### 阶段 1：最小验证闭环（建议下一步）

**目标**：在不改生产代码的前提下，验证"两个实例共享状态"端到端可行。

1. 复制一份 `HERMES_HOME` 到沙箱（`HERMES_HOME` 环境变量已实测生效）
2. 两个网关进程指向**同一个 PG 实例**（或先用共享 SQLite 验证租约语义）
3. 对同一 bot 并发发两条消息，观察：
   - 第二个是**被正确挡住**还是双跑
   - 会话历史是否一致
   - 故障注入（kill 一个实例）后另一个能否接管

**验收项（含 10.2 的压缩链清理项，必须一并覆盖）**：

| # | 验收项 | 判据 |
|---|---|---|
| 1 | 跨实例互斥 | 同一会话并发时，第二个被正确挡住 |
| 2 | 故障接管 | kill 实例后，另一实例在 TTL 内接管 |
| 3 | 会话历史一致性 | 接管后历史完整，无重复写入 |
| 4 | **孤儿父会话** | 清理后不存在 `parent_session_id` 指向已删除会话的行 |
| 5 | **谱系完整性** | 压缩链祖先链可完整上溯到根，无断裂 |
| 6 | **租约键一致性** | 清理前后 `conversation_id`（血统根）解析一致 |
| 7 | **活跃会话不受清理** | 进行中（非 ENDED）会话及其压缩链整体保留 |

**这一步不动生产，风险可控，结论是二元的。**

### 阶段 2：数据层改造（分支开发）

1. 引入 DB 抽象层，收敛 `sqlite3` 直接调用
2. PG backend 实现（含迁移脚本、FTS 方案）
3. 移除 `_compression_lock_holder_process_is_dead` 的本地 PID 分支（跨机失效且危险）
4. TTL（300s → 60s）/ fail-mode（open → closed）参数化
5. 指定 `CODEX_HOME`（3.6）
6. 思考链不落库（10.3）
7. `retention_days` 改为 7（10.2）

> 注：**不需要自定义 fencing token**——官方 `hosted_rooms` 已有 `authority_epoch` 机制可参考（2.1.5）。

### 阶段 3：部署验证

1. k3s 部署（1 副本先跑通）→ 2 副本 → 故障注入
2. 与现有单机部署并行验证
3. 稳定后同步 stable 分支

---

## 8. 未决问题

1. **PG 迁移的具体路径**：是先做双写（SQLite + PG 并行）还是停机迁移？现有 39 张表 + 12 张 FTS 表的数据迁移方案待定
2. **FTS 扩展的选择**：k3s 节点是否需要自定义 PG 镜像（装 zhparser/pgroonga）
3. **`HERMES_HOME` 文件状态的最终形态**：PVC / git 拉取 / 对象存储，三者边界待定
4. **长连接粘性的实现层**：由 Ingress 做（session affinity）还是应用层做（入站落库）
5. **多租户隔离**：未来对外服务时，租户间的数据隔离策略（按 profile / 按 schema / 按库）
6. **对 opensourse 上游的跟进策略**：数据层改造后，每次 merge upstream 的冲突成本评估

---

## 9. PG 承载能力评估（基于生产数据实测）

### 9.1 当前真实负载（9/1 - 9/12 生产库）

| 指标 | 数值 |
|---|---|
| 消息总量 | 5,162 |
| 会话总量 | 28 |
| **峰值单日消息** | **1,291 条**（9/11） |
| user 消息数 | 314 |
| **每条 user 消息产生的记录数** | **16.4 条**（含工具调用往返） |
| 峰值日折算 turn 数 | ≈ **79 个** |

### 9.2 PG 负载推算

每轮 turn 涉及的 DB 操作（按现有源码机制）：

1. 租约抢占 1 次（`INSERT ... ON CONFLICT`）
2. 租约释放 1 次（`DELETE`）
3. 若干次续租（TTL/3 频率）
4. 批量消息写 1 次（`append_messages_batch`，单事务写 N 条）
5. 若干次会话读取

按 4 小时峰值窗口保守估算：

| 指标 | 数值 |
|---|---|
| 峰值 turn 速率 | 0.3 turn/min |
| **推算峰值 DB 操作** | **≈ 0.03 ops/s** |
| PG 单实例典型能力 | 1,000 - 10,000 TPS |
| **当前负载占比** | **≈ 0.0027%** |

### 9.3 多租户放大推演

| 放大倍数 | 峰值 DB 操作 | 结论 |
|---|---|---|
| 1x（现状） | 0.03 ops/s | 毫无压力 |
| 100x | 3 ops/s | 毫无压力 |
| **1000x** | **27 ops/s** | **仍在单 PG 实例舒适区** |

### 9.4 数据量增长

**注意：内部使用数据不能直接外推对外服务规模。** 下方区分两种模型。

**内部现状（9/1-9/12，实测）**：

| 项 | 数值 |
|---|---|
| 当前 DB（10 天） | 38.8 MB |
| 日均增长 | ≈ 3.88 MB/天 |

**对外服务模型（按单会话特征推算）**：

平均单会话内容量 ≈ 636 KB（含思考链），永久保留模型下：

| 规模 | 日均增长 | 一年 | 三年 |
|---|---|---|---|
| 100 用户 | 186 MB | 66 GB | 199 GB |
| 1,000 用户 | 1.86 GB | 664 GB | ~2 TB |
| 10,000 用户 | 18.6 GB | 6.6 TB | ~20 TB |

**结论：永久保留在对外服务规模下不可行，必须配保留策略（见第 10 节）。** 引入 7 天保留 + 不存思考链后，实际数据量将下降一至两个数量级。

### 9.5 关于热点行竞争

**不需要特殊处理。** 现有机制已经把它收敛了：

- `try_acquire_session_turn_lease` 抢不到 → **立即入队返回**，不做阻塞式自旋
- 这正是**数据库实现分布式锁的标准逻辑**——竞争被转化为"排队"，而非长时间行锁争抢
- 行锁的判定是毫秒级操作，不存在长事务持有

**唯一需要注意的**：租约必须在 turn 结束时及时释放。若 turn 卡死，该行会被占用到 TTL 超时——这也是 TTL 建议调到 60s 的另一个理由（**限制"卡住租约"的影响面**）。

---

## 10. 数据生命周期与保留策略

### 10.1 定性：会话历史是过程数据，可再生

**核心判断**：Hermes 不存最终产出数据，那是 FNS 的职责（见 3.0）。Hermes 的会话历史是**生产过程记录**，属于**可再生的中间态**。

**重建路径**：会话历史丢失后，从 FNS 拉取有价值的数据，即可重建工作。

**由此得出的策略**：

| 数据类型 | 价值 | 处置 |
|---|---|---|
| FNS 中的最终产出 | **真资产** | 严格保护（FNS 自己的 PG/COS） |
| Hermes 会话历史 | 过程记录 | **只保留热数据，不做冷备/归档** |
| Hermes 思考链 | **无价值** | **不落库**（见 10.3） |
| Hermes 配置数据 | 可重现 | git 即可重建 |

**结论：不做冷热分离、不做归档回捞、不做长期查询优化——按时间窗口删除即可。**

### 10.2 保留窗口：7 天

**决策：会话历史保留 7 天。**

理由：
- 真正有价值的数据在 FNS，会话历史仅用于**近期上下文连续性**
- 当前资源紧张（2C4G，HDD 云盘），不能设长
- 7 天不够时再调整（可配置，非硬编码）

**参照**：线上日志通常保留 15 天，会话历史的可丢弃性高于日志，7 天保守合理。

**已有机制（不需要自己实现）**：

Hermes 内置会话清理，配置项位于 `hermes_cli/config_defaults.py:2040-2053`：

```
# Prune ENDED sessions inactive for retention_days (activity = latest message, else ...)
"retention_days": 90,
```

注释原文：`Default true since #54189: without it state.db grows without bound`——上游已确认"state.db 无限增长"问题并内置了清理。

**语义**：
- 只清理 **ENDED** 状态的会话（不影响进行中会话）
- 按**最后活跃时间**判定（activity = latest message）
- 定期执行（`min_interval_hours` 控制频率）

**要做的只是把 `retention_days` 从 90 改为 7。**

**改造时需验证的点**：清理逻辑对**压缩链**（`parent_session_id` 串联的会话）的处置——只删子会话可能留下孤儿父会话，删父会话可能影响子会话谱系。**在 PG 上验证，不要在测试以外的环境直接试。**

> **验收项（未来 Hermes 多实例验证时必须覆盖）**：
> 1. **孤儿父会话** —— 清理后不得存在 `parent_session_id` 指向已删除会话的行
> 2. **谱系完整性** —— 压缩链的祖先链可完整上溯到根，无断裂
> 3. **租约键一致性** —— 清理前后，`session_turn_leases` 的 conversation_id（血统根）解析结果一致，不因清理产生错误归属
> 4. **活跃会话不受影响** —— 进行中（非 ENDED）会话及其压缩链整体不被清理

### 10.3 思考链：不落库

**决策：思考链不存储。**

**实测依据（为什么这条最值得做）**：

| 列 | 非空条数 | 占用 |
|---|---|---|
| `reasoning` | 1,656 | 5.17 MB |
| `reasoning_content` | 2,285 | 5.17 MB |
| `reasoning_details` | 0 | 0 |
| `codex_reasoning_items` | 0 | 0 |

**合计约 10.34 MB，占全库内容字节的 59.4%**——是最大的单项存储消耗。

对单个会话更明显：某会话 content 仅 967 KB，reasoning 却有 4.8 MB，**思考链是正文的 5 倍**。

**为什么毫无意义**：
- 思考链**不进入上下文**（压缩后更不会），运行时完全不读
- 只对调试有微弱价值，而调试主要看日志与会话正文
- 用户不可见，产品上无意义

**注意**：16 个 bot 已全部关闭思考模式（`agent.reasoning_effort: "none"`），**新增数据不再产生思考链**。本节针对的是**落库路径**——即使模型返回思考内容，也不应写入 DB。

**改造点**：在消息持久化路径（`agent/session_persistence.py` 的 `_db_flush_row`）移除 `_ROW_REASONING_KEYS` 相关列的写入。

### 10.4 预期效果

引入 7 天保留 + 不存思考链后：

| 措施 | 效果 |
|---|---|
| 不存思考链 | 内容量减少 **~59%** |
| 7 天保留（替代永久） | 稳态数据量 = 7 天累积，而非无限增长 |
| 两者叠加 | **数据量下降一至两个数量级** |

配合 9.4 的对外服务模型，1,000 用户规模下的稳态数据量将落在**百 MB 量级**（7 天窗口），而非 TB 级累积。

### 10.5 与多实例改造的关系

**保留策略不是"运维附加项"，是架构的一部分**：

- 会话历史会自动消失 → **故障恢复的时间窗口也是有限的**（超过 7 天的会话本来就没了）
- 这与"粗粒度恢复"的定位一致——**恢复的是近期工作状态，不是永久档案**
- **PG 中不保留长期历史，也降低了对 PG 存储容量的要求**，间接降低 PG 高可用的运维成本

---

## 附录 A：关键源码位置

| 文件 | 内容 |
|---|---|
| `hermes_state_compression.py:485-605` | 租约的 key 解析、抢占、续租、释放 |
| `hermes_state_gateway.py:619-690` | handoff 状态机（CAS 抢占、watcher、清理） |
| `agent/session_persistence.py:151-222` | 消息落库时机（批量事务 + 与租约绑定） |
| `gateway/shutdown_flush.py` | 关机兜底刷盘与重启回放 |
| `gateway/turn_lease.py` | 第一版进程内 asyncio 锁 |
| `gateway/config.py:275-350` | 平台模式定义（webhook / api_server / feishu） |
| `gateway/platforms/webhook.py` | webhook 适配器 |

## 附录 B：实测脚本

- `/tmp/lease_test/run_lease_test.py` —— 基础 5 项（互斥、隔离、释放、所有权）
- `/tmp/lease_test/run_lease_test2.py` —— 续租、TTL、进程死亡回收
- `/tmp/lease_test/run_lease_test3.py` —— TTL 过期回收补测
- `/tmp/lease_test/lease_child.py` —— 子进程操作器

## 附录 C：关键数据（2026-09-12 快照）

| 项 | 数值 |
|---|---|
| `state.db` 大小 | 38.8 MB |
| 表数量 | 39（含 12 张 FTS5） |
| sessions 行数 | 26 |
| messages 行数 | 4,935 |
| 引用 `sqlite3` 的文件数 | 80 |
| `sqlite3` 调用点 | 581 |
| `HERMES_HOME` 总大小 | 741 MB（其中 backups 190M） |
