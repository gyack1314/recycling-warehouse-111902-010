# 无业务主键集市的差量共享与县仓入仓

在 `materials` 物理校验之上新增的产品能力：提供方共享、净变化生成、本地文件交接、
接收方原子入仓与双方对账。两个仓各自落 **真实 Parquet + DuckDB 元数据库**，
独立进程可查；“已发送/已接收”只以磁盘账本与确认文件裁决，不使用进程内变量。

## 1. 核心语义

- **共享视图是行多重集，不是整行去重集合。** 没有业务主键，相同
  `(day, district, material, weight)` 的两行是两笔合法交投，重复份数全程保留；
  `private_ticket` 不共享，也不能当主键。
- **行身份 = 规范化行哈希。** 按契约列顺序做长度前缀二进制编码，`weight` 以
  `DECIMAL(18,3)` 定点值入哈希，不转浮点；NULL 与类型标记参与编码。
- **视图指纹 = 排序后 `(行哈希, 重数)` 列表的 SHA-256。** 与文件路径、行组、
  行顺序完全无关：原件物理重排（S1→S2）得到相同指纹，不产生假删除轮次。
- **净变化 = 多重集 Counter 相减。** 投影后两行撞同哈希、字段更正导致一行退出
  筛选，都只表现为重数的净增/净减；减量使重数为负一律拒绝。
- **传递身份确定性、内容指纹独立。** 同身份同内容重收幂等；同身份内容不同即
  停止并写 `conflicts`，绝不用后到者覆盖。
- **同一轮增删原子发布。** 暂存回读核验通过后一次 `rename` 切换公开 Parquet，
  DuckDB 账本在单事务提交；查询永远看不到“只删未增”的中间状态。
- **代际守卫。** 完整替换（rebase）开启新代际；新基线开放后旧代际差量拒绝入仓。
  退休基线无法再算差量时，提供方拒绝生成并要求完整替换。
- **契约身份。** 筛选、投影列、类型任一变化都会改变契约哈希；新契约必须由县仓
  明确接纳（`adopt`），旧传递不能用于新条件。

## 2. 目录与持久化

```
work/（默认工作目录，可用 CLI 参数改到任意位置）
├── handoff/                     本地文件交接区（不依赖网络）
│   ├── applications/*.json      共享申请（含完整契约）
│   ├── packages/<delivery_id>/  自校验传递包：manifest.json + payloads/*.parquet
│   └── acks/<delivery_id>.json  县仓持久化确认
├── provider_store/
│   ├── provider.duckdb          契约/视图/代际/传递账本（含 acked 进度）
│   ├── views/<契约>/<快照>.parquet   每个快照的共享视图真实 Parquet
│   └── packages/<delivery_id>/  已生成传递包的持久化原件（重发只复制，不重算）
└── receiver_store/
    ├── receiver.duckdb          接纳契约、ledger、rounds、conflicts、refused、current_state
    ├── current.parquet          当前开放版本（原子切换，查询视图 current_view）
    ├── current_by_contract/     按契约隔离的开放版本
    ├── rounds/round_NNNNNN.parquet  每一轮留档（可查该轮针对哪个基线）
    ├── staging/                 暂存区（发布前崩溃则由 recover 清理，天然幂等）
    └── packages/<delivery_id>/  接收包原件留档，供全链路重放对账
```

## 3. 五个交付操作（CLI）

所有命令均为独立进程，入口 `python3 -I run.py <子命令>`（`-I` 隔离模式，
脚本自行把仓库根加入 `sys.path`）。

### 3.1 共享申请

```bash
python3 -I run.py apply --contract inputs/share-contract.json
# 县仓明确接纳；提供方接纳后才能生成该契约的传递
python3 -I run.py adopt     --application work/handoff/applications/app-xxxx.json
python3 -I run.py accept    --application work/handoff/applications/app-xxxx.json
```

### 3.2 净变化生成（提供方）

```bash
# 首轮完整基线
python3 -I run.py snapshot --contract inputs/share-contract.json --snapshot S2
# 两个开放基线之间的差量（added/removed 各自带重复份数）
python3 -I run.py delta    --contract inputs/share-contract.json \
                           --from-snapshot S2 --to-snapshot S3
# 有限保留：退休旧版本（退休后基于它的差量被拒绝）
python3 -I run.py retire   --contract inputs/share-contract.json --snapshots S1 S2
# 完整替换：建立新基线代际
python3 -I run.py rebase   --contract inputs/share-contract.json --snapshot S4
```

### 3.3 本地文件交接

```bash
python3 -I run.py send        --delivery-id <id>   # 持久化包复制进交接区
python3 -I run.py resend 等价于 send，重发字节一致、身份一致，不生成新一轮
python3 -I run.py collect-ack --delivery-id <id>   # 提供方只认磁盘 ack 文件
```

### 3.4 接收入仓（县仓）

```bash
python3 -I run.py receive --delivery-id <id>
# 验收用断点注入：
#   --crash-hook before_publish  暂存核验后、发布前崩溃（仓内不变，重收即可）
#   --crash-hook before_ack      入仓提交后、确认写出前崩溃（模拟确认丢失）
```

接收顺序：静态核验（清单指纹/载荷 sha256/行数）→ 契约接纳检查 → 账本幂等/冲突
裁决 → 代际与基线守卫 → 按类型独立重算指纹 → 暂存回读 → 原子发布 → 事务记账
→ 留接收包 → 写确认。

### 3.5 对账与查询

```bash
python3 -I run.py query                 # 查当前开放版本（DuckDB current_view）
python3 -I run.py query --sql "SELECT district, count(*) FROM current_view GROUP BY 1"
python3 -I run.py query-round --round-id 2          # 查某轮留档针对的基线内容
python3 -I run.py receiver-rounds                    # 每轮 from_view -> to_view
python3 -I run.py receiver-state
python3 -I run.py receiver-conflicts                 # 同身份内容冲突留痕
python3 -I run.py receiver-reconcile                 # 全链路重放+交叉核对
python3 -I run.py provider-ledger
python3 -I run.py provider-status
python3 -I run.py provider-reconcile
```

外部进程也可直接只读挂载，不经过本工具：

```bash
python3 -I -c "import duckdb; c=duckdb.connect(); \
  c.execute(\"ATTACH 'work/receiver_store/receiver.duckdb' AS rw (READ_ONLY)\"); \
  print(c.execute('SELECT * FROM rw.rounds').fetchall())"
```

## 4. 端到端验收

```bash
python3 -I run_tests.py     # 25 个 unittest（协议基元 + 端到端，含全部原始版本）
python3 -I scenario.py      # 严格对应 inputs/actions.json 五步的演示
```

`scenario.py` 覆盖：物理重排无假删除、重复行少一份检测、断线重送不多加轮、
退休与完整替换衔接、旧代际拒绝、双方对账与 DuckDB 直查。

## 5. 拒绝码

| 异常 | 触发条件 | 县仓动作 |
| --- | --- | --- |
| `VerificationFailed` | 清单/载荷摘要、行数、重算指纹不符 | 拒绝，不写账本 |
| `ContentConflict` | 同传递身份内容不同 | 写 `conflicts`，停止，保留原数据 |
| `StaleGeneration` | 旧代际差量在新基线后到达 / 重复初始快照 | 写 `refused`，拒绝 |
| `MissingBaseline` | 差量 `from_view` 与县仓当前基线不符 / 基线已退休 | 要求补传或完整重建 |
| `ContractMismatch` | 契约未接纳或不属于该仓 | 拒绝 |
