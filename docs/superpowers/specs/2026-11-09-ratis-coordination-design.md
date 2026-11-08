# Ratis Coordination 重写设计规格

**日期：** 2026-11-09  
**状态：** 已批准，待实施计划  
**范围：** 新建 `ratis-coordination`，不读取、不迁移、不兼容现有 `ratis-metadata` layout

## 1. 目标与边界

`ratis-coordination` 提供一个由 Ratis 复制的、可恢复的元数据服务。它支持两种互斥的 group role：

- `COORDINATION`：严格路径树、普通节点、ephemeral 节点、sequential 节点、租约、typed lock、分页、change feed/watch 和有界递归删除。
- `KEYSPACE`：任意 bytes key 的 put/get/delete/CAS/scan。不使用路径语义，不创建父节点，不提供租约或锁。

两个 role 使用同一套协议、身份、存储事务和 Ratis adapter，但每个 group 只能选择一个 role。服务端再次校验 role；客户端的 typed API 不能替代服务端校验。

本版目标是正确性优先和最小可证明实现：一个 group 一个状态机、一个 RocksDB、一个原子提交点；没有跨 group 事务、旧 layout 读取、自动 takeover、无限重试、可插拔存储后端或通用插件系统。

## 2. Maven 与包结构

根 `pom.xml` 新增 `<module>ratis-coordination</module>`。模块只依赖现有 `ratis-common`、`ratis-proto`、`ratis-client`、`ratis-server-api`、Ratis shaded protobuf、RocksDB 和测试依赖；不依赖 `ratis-metadata`。现有 `ratis-metadata` 保留在 reactor 中，直到迁移工作显式删除，但新模块的生产代码和测试不得引用其类。

目录固定为：

```text
ratis-coordination/
  pom.xml
  src/main/proto/Coordination.proto
  src/main/java/org/apache/ratis/coordination/protocol/
  src/main/java/org/apache/ratis/coordination/storage/
  src/main/java/org/apache/ratis/coordination/server/
  src/main/java/org/apache/ratis/coordination/client/
  src/test/java/org/apache/ratis/coordination/
```

`protocol` 只放生成消息的不可变包装和值类型；`storage` 只负责 RocksDB handle、编码、快照和 read lease；`server` 负责确定性状态转换和 Ratis `StateMachine`；`client` 负责 typed facade、writer pipeline、watch 和 lease 生命周期。生产代码不公开 protobuf builder、RocksDB 对象、内部锁、内部 future 或可变集合。

## 3. 公共 API

所有公共类为 `final`，所有 `byte[]` 入参和出参都做 defensive copy。公共同步 API 不在内部线程调用阻塞等待；异步 API 返回只读的 `CompletionStage` 镜像。

### 3.1 基础类型

```java
public enum CoordinationRole { COORDINATION, KEYSPACE }

public final class GroupId { public static GroupId valueOf(UUID id); public byte[] toBytes(); }
public final class WriterId { public static WriterId random(); public byte[] toBytes(); }
public final class WriterRef { WriterId writerId(); long generation(); }
public final class OperationId { WriterRef writer(); long sequence(); }
public final class Stamp { long revision(); int ordinal(); }
public final class LockToken { GroupId groupId(); long revision(); }
public final class GroupConfig {
  long resultRetentionMillis();
  long changeRetentionRevisions();
  int maxWriters();
  int maxLeases();
  int maxPathBytes();                 // fixed at 8192 in this version
  int maxPageEntries();
  int maxPageBytes();
  int maxRecursiveDeleteEntries();    // fixed at 1024 in this version
}

public enum WriterLane { BUSINESS, CONTROL }
public enum OpenMode { NEW, RESUME, TAKEOVER }
public final class WriterCredentials {
  WriterId writerId();
  long generation();
  WriterLane lane();
}
public final class LeaseRef { long leaseId(); }
public final class ExpectedVersion {
  static ExpectedVersion absent();
  static ExpectedVersion exact(Stamp stamp);
}
public final class CreateOptions {
  boolean createParents();
  boolean sequential();
  NodeKind kind();                 // NORMAL or EPHEMERAL_LOCK
  LeaseRef lease();                // required for EPHEMERAL_LOCK
}
public final class AcquireOptions {
  Duration ttl();
  Duration deadline();
  boolean sequential();
}
public final class PageRequest {
  int maxEntries();                // zero means configured default
  int maxBytes();                  // zero means configured default
}
public final class Page<T> {
  List<T> entries();
  PageCursor nextCursor();
  boolean hasMore();
}
public final class PageCursor { public String toBase64Url(); }
public final class ChangeCursor { public String toBase64Url(); }
public final class NodeRecord {
  String path(); byte[] value(); NodeKind kind();
  Stamp createStamp(); Stamp modifyStamp(); LeaseRef lease();
}
public final class KeyValue { byte[] key(); byte[] value(); }
public final class KeyRange {
  byte[] startInclusive(); byte[] endExclusive();
}
public final class WriteResult { OperationId operationId(); Status status(); byte[] body(); }
public final class MultiResult { List<WriteResult> results(); Status status(); }
public interface ChangeListener { CompletionStage<?> onChange(ChangeEvent event); }
public interface WatchHandle extends Closeable { ChangeCursor lastAckedCursor(); }

public final class ClientOptions {
  OpenMode openMode();
  RequestJournal requestJournal();       // nullable means memory-only
  Duration retryBudget();
  int maxPendingEntries();
  int maxPendingBytes();
  Executor notificationExecutor();
}

public interface Clock { long millis(); }
```

`GroupId` 和 `WriterId` 必须恰好 16 bytes、非全零。`generation`、`sequence`、revision、TTL、logical time 均解析为非负 signed `long`；业务 sequence 的合法范围为 `1..Long.MAX_VALUE - 1`。不会把 protobuf `uint64` 直接转换为可能为负的 Java `long`。

### 3.2 Group host 与 state machine

```java
public final class CoordinationGroupSpec {
  GroupId groupId();
  CoordinationRole role();
  GroupConfig config();
  Path storageDirectory();
}

public final class CoordinationStateMachine extends BaseStateMachine {
  public CoordinationStateMachine(CoordinationGroupSpec spec, Clock clock);
}

public final class CoordinationGroupHost implements Closeable {
  public static CoordinationGroupHost create(CoordinationGroupSpec spec,
      RaftServer server, Clock clock) throws IOException;
  public CoordinationClient coordinationClient(ClientOptions options);
  public KeyspaceClient keyspaceClient(ClientOptions options);
}
```

`CoordinationGroupHost` 只构造与 `spec.role()` 匹配的 facade；调用另一 role 的 factory 立即抛 `IllegalArgumentException`。一个 host 关闭只关闭本 group 的 state machine、scheduler 和 client，不影响同进程其它 host。

首次启动必须先提交 `INITIALIZE_GROUP(groupId, role, fullConfig, configFingerprint)`；空 DB 接受它之外的任何写入都会得到 `GROUP_NOT_INITIALIZED`。同 group 再次初始化只有在 fingerprint 完全相同才返回已初始化状态；不同 fingerprint 返回 `CONFIG_MISMATCH`。已删除的 group id 不复用。客户端 open 的第一条操作是线性 `HELLO(wireVersion, expectedRole, retryBudget)`，只有 HELLO 成功后才注册 writer 和开放业务 API；HELLO 返回 role、完整 config、fingerprint、applied index 和 logical time。

### 3.3 Coordination facade

同步方法只列语义入口；每个方法均有对应 `Async` 版本，异步版本不复制用户对象。

```java
public final class CoordinationClient implements Closeable {
  NodeRecord get(String path);
  Page<NodeRecord> list(String prefix, PageRequest page);
  ChangePage pollChanges(String prefix, ChangeCursor cursor, PageRequest page);
  WatchHandle watchFromNow(String prefix, ChangeListener listener);
  WatchHandle watch(String prefix, ChangeCursor cursor, ChangeListener listener);
  LeaseSession openLease(Duration ttl);
  Acquisition acquire(String path, AcquireOptions options);
  long increment(String name, long delta);
  WriteResult put(String path, byte[] value, ExpectedVersion version, LeaseRef lease);
  WriteResult create(String path, CreateOptions options, byte[] value);
  WriteResult delete(String path, boolean recursive);
  MultiResult multi(List<Mutation> mutations);
}
```

`put/create/delete/multi/increment` 的业务身份由内部 pipeline 分配；用户不能传入 sequence，也不能通过一个通用 `send(Operation)` 绕过 role 和操作矩阵。`Acquisition` 的成功交付类型是不可变 `Hold`；历史 CREATE reply 必须先执行线性 `READ_HOLD` 才能交付 Hold。

### 3.4 Keyspace facade

```java
public final class KeyspaceClient implements Closeable {
  byte[] get(byte[] key);
  WriteResult put(byte[] key, byte[] value, ExpectedVersion version);
  WriteResult delete(byte[] key);
  Page<KeyValue> scan(KeyRange range, PageRequest page);
}
```

key 长度为 1..8192 bytes，允许任意 bytes，包括 `/`、NUL 和非法 UTF-8。范围为 `[startInclusive, endExclusive)`；空 start 是最小 key，空 end 是正无穷，反向范围拒绝。

`PageCursor` 和 `ChangeCursor` 都是版本化 protobuf 的 base64url 表示，原始 bytes 最多 16384。cursor 必须包含 group id、role、config fingerprint、查询摘要和最后检查的 key/event；改变查询范围、prefix、filter、group 或版本返回 `BAD_CURSOR`。分页使用 exclusive last examined key；达到 visit budget 可以返回空页加 `hasMore=true` 和前进 cursor，不能把空页当作 EOF。

## 4. Wire schema 与冻结规则

`Coordination.proto` 只定义消息，不定义 gRPC service。消息继续放入 Ratis `Message`。schema 版本固定为 `1`；未知字段、`UNRECOGNIZED` enum、缺失 oneof、非法组合和递归深度超过 32 一律拒绝。

### 4.1 Envelope

每个写入 Ratis 的 payload 为：

```text
Envelope {
  uint32 wire_version = 1;       // exactly 1
  bytes group_id = 2;            // exactly 16
  Role role = 3;
  uint64 logical_time_millis = 4;
  oneof body {
    InitializeGroup initialize = 10;
    RegisterWriter register_writer = 11;
    TakeoverWriter takeover_writer = 12;
    BusinessWrite business = 13;
    LeaseWrite lease = 14;
    Maintenance maintenance = 15;
  }
}
```

`BusinessWrite` 和 `LeaseWrite` 均携带：`writer_id[16]、generation、sequence、retry_budget_millis、digest[32]、Operation operation`。digest 为：

```text
SHA-256("RCO1" || 0x00 || group_id || config_fingerprint || role || canonical_operation_bytes)
```

retry budget、Ratis call id、logical time 不进入 digest。客户端在冻结后只重发同一 canonical bytes；服务端重新冻结并比较 digest。

查询不进入写 envelope。`QueryEnvelope` 包含 `wire_version、group_id、role、config_fingerprint、query_digest` 和一个 query oneof：`HELLO、GET、LIST、SCAN、READ_HOLD、READ_SUBTREE_SNAPSHOT、POLL_CHANGES、READ_WRITER、READ_RESULT`。query digest 覆盖 query 的语义参数，不覆盖 page size；查询通过 Ratis linearizable read 或 stale-read API 执行，绝不修改 writer、lease、result 或 change 状态。`HELLO` 是唯一允许在 group 未初始化时执行的 query，但它只返回初始化状态，不创建任何记录。

### 4.2 路径与节点

COORDINATION path 必须以 `/` 开头，不能含 NUL、空 segment、尾 `/`（根 `/` 除外）、`.` 或 `..`；不做 Unicode NFC/NFD 归一化。Java 未配对 surrogate 直接拒绝。长度按 UTF-8 bytes，深度按 segment；比较按 unsigned UTF-8 bytes。

根 `/` 是虚拟持久节点，只能 GET/LIST，不能 PUT/CAS/CREATE/DELETE 或绑定 lease。普通节点保存 `kind、value、createStamp、modifyStamp、leaseId`。`Stamp` 的 ordinal 在同一 log entry 内按实际变更顺序从 1 开始，用于阻止 delete/recreate 的 CAS ABA。

节点操作固定为：`PUT`、`CREATE`、`CAS`、`DELETE`、`DELETE_SUBTREE`、`MULTI`、`INCREMENT`。`CREATE` 可 `createParents`；自动创建的祖先为空值持久节点。ephemeral 节点不能有子节点；LOCK 只能由单独的 typed CREATE 产生；普通 PUT/CAS 不能改变 LOCK kind。

Sequential 名称使用父节点 create stamp 绑定的单调计数器：`base + "~" + 20 位十进制序号`，从 1 开始。显式 CREATE/PUT/CAS 禁止保留后缀，失败 MULTI 和 replay 不推进计数。

### 4.3 Lease 和 lock

lease id 等于 `LEASE_OPEN` 所在的 Raft log index，由服务端产生，调用方不能指定或复用。`LEASE_RENEW` 必须 owner 和当前 generation 都匹配，且 `logicalTime < expireAt`；新过期时间为 `max(oldExpireAt, logicalTime + ttl)`。过期后不可复活。`LEASE_REVOKE` 可撤销旧 generation 的 lease，但必须 owner 匹配；删除绑定节点、反向索引和计数在同一 batch 完成。

LOCK CREATE 必须携带有效 lease，返回 `LockToken(groupId, createRevision)`。释放必须带 owner、leaseId、token；旧 owner 不能删除新锁。`EXPIRE` 只在当前 lease 的 `lastRenewIndex` 与候选值完全相等且 logical time 已到期时生效，否则是确定性 no-op。

## 5. 服务端状态模型与接纳顺序

状态转换器不维护第二份可变业务状态。它从 `ReadView` 读取当前记录，构造临时 `Overlay`，把全部变更写入 `PreparedBatch`。唯一可写路径是 `PreparedBatch.commit()`。

每个 entry 的顺序固定为：

1. 校验 envelope、group、role、wire version、config fingerprint、budget、operation shape 和 digest。
2. 从持久结果表查 `OperationId`。结果存在且 digest 相同则返回原始 BusinessResult bytes 的 `REPLAY`；digest 不同为 `INVOCATION_MISMATCH`。replay 不重新执行前置条件，不延长 retention。
3. 结果不存在时检查 writer、generation、sequence：未知 writer、旧/未来 generation、sequence 不连续、`Long.MAX_VALUE`、结果已过期均返回相应 admission 状态，不消费 sequence。
4. 对新连续 sequence 先运行固定预算的有界 maintenance（结果/change GC、到期 lease 候选）；maintenance 只按 unsigned key 顺序推进，超过预算就停止，不循环提交多个 batch。
5. 在 overlay 上执行操作。确定性业务失败消费 sequence 并保存失败 BusinessResult；业务副作用清零。MULTI 子操作按顺序看到同一 overlay，任一子操作失败时整个 MULTI 无业务副作用，仅保存 `MULTI_FAILED(index,status)`。
6. 检查结果和 change 容量；容量不足返回 `RESULT_CAPACITY` 或 `CHANGE_CAPACITY`，不消费 sequence，但允许只提交 marker/maintenance 的 batch。
7. 将业务效果、原始结果、digest、固定 expiry、writer highWater、change events、logical time、applied marker 写入同一个 batch。commit 成功后才发布内存水位和 reply。

结构/role/config/budget 错误若已进入日志，写入确定性 `MALFORMED` 控制结果但不消费业务 sequence；存储错误不写 BusinessResult，直接让 group `FAILED`。

## 6. 角色操作矩阵

| 操作 | COORDINATION | KEYSPACE | 关键原子性 |
|---|---:|---:|---|
| PUT/CREATE/CAS/DELETE | 是 | 否 | 节点/值与 lease reverse index 同 batch |
| DELETE_SUBTREE | 是 | 否 | 先完整预扫描，超 1024 或 iterator error 整体失败 |
| MULTI | 是 | 否 | 只允许 PUT/CREATE/CAS/DELETE/INCREMENT，禁止 nested/read/lease/LOCK |
| INCREMENT | 是 | 否 | checked signed add；user/sequential counter key tag 分离 |
| LEASE_OPEN/RENEW/REVOKE | 是，CONTROL lane | 否 | lease、绑定节点、计数同 batch |
| EXPIRE | 是，CONTROL lane | 否 | owner/lastRenewIndex/logicalTime CAS |
| put/get/delete/scan | 否 | 是 | bytes key unsigned 排序 |

COORDINATION 的 BUSINESS writer 只处理节点、MULTI、counter；CONTROL writer 只处理 lease 和 expiry。KEYSPACE 不注册 CONTROL writer。跨 role 或跨 lane 请求返回 `WRONG_ROLE/NOT_ALLOWED`，不把错误转成普通业务失败。

## 7. RocksDB layout 与生命周期

每个 group 一个 DB，CF 固定为：`meta、nodes、kv、writers、leases、results、changes`。所有 key 使用明确的 tag 和 big-endian 数字；不依赖 Java `HashMap` 或 RocksDB 默认字符串排序。

- `meta`: group identity、role、config fingerprint、codec version、applied `(term,index)`、logical time、change floor。
- `nodes`: unsigned UTF-8 path -> `NodeRecord`。
- `kv`: length-prefixed bytes key -> `KeyValueRecord`。
- `writers`: `(lane,writerId)` -> generation/highWater/registered flag。
- `leases`: lease id -> owner/ttl/expireAt/lastRenewIndex。
- `results`: `(lane,writerId,generation,sequence)` -> digest + original BusinessResult + expiry。
- `changes`: `(revision,ordinal)` -> event record；revision 与 ordinal 均 big-endian。

每种 value 使用统一 `codecVersion=1、type marker、payload length、payload、SHA-256(payload)` 编码。长度、marker、checksum、字段范围全部在 decode 时检查；损坏、缺失 marker、尾随字节或 iterator.status error 都抛 `StorageCorruptionException`，不能当作空结果。

`ReadView` 持有 RocksDB snapshot、read options 和 active-read lease。query 完成先关闭 iterator，再释放 snapshot，最后释放 read lease。close 进入 `CLOSING` 后不再接受新 view，但已持有 view 必须继续可读；超时 close 不释放仍被 view 使用的 native handle。

所有 apply、checkpoint、initialize、restore 使用同一 apply mutex；业务批次只有一个 `db.write`。失败的 transition 只丢弃 staging，不推进 marker、counter、writer highWater 或 change floor。

## 8. Ratis StateMachine、logical time 与 snapshot

leader `startTransaction` 将客户端 payload 冻结成 envelope，并从注入的 `Clock` 采样 logical time；该值进入 state-machine log data。follower 从日志 envelope 读取同一 logical time，apply 不读取本机时钟。logical time 持久化为 `max(previous, entry.logicalTime)`，不允许倒退。

`preAppendTransaction` 只做轻量解析和大小/预算/role 校验；不写 RocksDB、不修改 writer、不推进任何内存业务状态。`applyTransaction` 在 apply mutex 内执行状态转换，提交成功后调用 `updateLastAppliedTermIndex`。query 走 `queryStale` 或线性 read-index，再在 ReadView 中返回。

snapshot 流程为：

1. 在 apply mutex 下 flush checkpoint，写 manifest（group id、role、config fingerprint、codec version、applied term/index、每个文件 SHA-256）。
2. fsync manifest 和目录；通过 `StateMachineStorage` 发布 checkpoint。
3. restore 时先校验 manifest，再复制/硬链接到 sibling staging 目录；校验所有文件后一次 rename 替换 working DB。
4. reopen DB，校验 identity，最后提升 last-applied floor。working DB ahead 不被覆盖；marker 小于 snapshot index 时才 restore。

恢复失败、manifest 损坏、group/config 不匹配、缺文件、rename 前后目录不一致都 fail closed。旧 snapshot 不作为业务结果回退；本版只读取新 layout。

## 9. Client pipeline 与并发规则

每个 facade 有独立 `ClientCore`、`stateLock`、有限 pending 队列和通知 dispatcher。COORDINATION 创建 BUSINESS/CONTROL 两条 lane，KEYSPACE 只创建 BUSINESS；不共享 writer id、sequence、lease 集合、close 状态或 transport。

赋号规则：先校验操作并预留 pending bytes/entries 和通知槽；随后在同一个 stateLock 临界区冻结 payload、分配 sequence、推进 nextAllocated、插入 FIFO。任何资源不足在赋号前返回。取消、超时和 close 不能删除已赋号项或复用 sequence。

transport 失败后最多三次有界恢复，使用新 Ratis call id 重放原 FrozenCommand；不自动改变 generation，不从 server highWater 猜 next sequence。收到 `STALE_GENERATION、INVOCATION_MISMATCH、RESULT_EXPIRED、SEQUENCE_EXHAUSTED` 立即暂停 lane，暴露明确 outcome。

默认 outbox 只在内存中。可选 `RequestJournal` 只有两个原子操作：

```java
interface RequestJournal {
  void append(FrozenRequest request) throws IOException;
  void recordResult(OperationId id, byte[] originalBusinessResult) throws IOException;
}
```

`append` 成功才允许发送；`recordResult` 失败时原业务结果仍对调用方可见，但 lane 进入 `SUSPENDED`，不能继续接收新业务。没有 journal 的重启只能显式 TAKEOVER，不能声称恢复旧 payload。

所有内部状态转换生成有限 Action 列表，在 stateLock 外执行 RPC、journal、timer、transport close 和用户 callback。close 设置 `CLOSING`、停止续租、取消 timer、保留未决 identity；使用一个统一 10 秒单调 deadline，不为每个资源重复等待。

## 10. LeaseSession、Acquisition 与 watch

`LeaseSession` 状态为 `OPENING -> ACTIVE -> RETIRING -> CLOSED`，异常为 `LOST`。每个 lease 同时最多一个 renew；renew timer 默认 TTL/3，续租和撤销互斥。revoke 前先移出续租集合，迟到 renew 只能得到 `UNKNOWN_LEASE`，不能复活 lease。

Acquisition 的资源状态为 `PENDING/WAITING/VALIDATING/DELIVERY_QUEUED/SUCCEEDED/CANCELLED/FAILED`。CREATE 成功后必须在同一个线性 `READ_HOLD` 中确认 path、owner、lease、token 和当前 generation；确认成功才把候选 Hold 放入通知队列。通知任务真正开始时再次在 stateLock 下与 timeout/cancel 竞争，赢得 `SUCCEEDED` 后才转移 lease 所有权。用户 callback 不在 stateLock、timer 或 transport callback 中运行。

watch 从 `watchFromNow` 的 end cursor 或用户显式 cursor 开始。每次 callback 成功才 ack cursor；失败最多重试三次后进入 `FAILED`，暴露最后成功 cursor。callback 慢时最多保留一页事件，不并行追加 poll。`NEED_SNAPSHOT` 直接使 watch failed，要求调用方重新建立 snapshot，不自动跳过历史。

## 11. 错误分类

- **Stored terminal**：`OK、NOT_FOUND、NODE_EXISTS、NO_PARENT、HAS_CHILDREN、VERSION_MISMATCH、NOT_OWNER、NOT_ALLOWED、UNKNOWN_LEASE、LEASE_EXPIRED、MULTI_FAILED、NUMERIC_OVERFLOW、LIMIT_EXCEEDED`。消费 sequence，保存原结果。
- **Admission retryable**：`SEQUENCE_GAP、RESULT_CAPACITY、CHANGE_CAPACITY`。不消费 sequence，以新 Ratis call id 重试原 identity。
- **Identity terminal**：`UNKNOWN_WRITER、STALE_GENERATION、FUTURE_GENERATION、INVOCATION_MISMATCH、RESULT_EXPIRED、SEQUENCE_EXHAUSTED`。不重做业务，lane 暂停或要求显式 takeover。
- **Read dynamic**：`BAD_CURSOR、PAGE_TOO_SMALL、SNAPSHOT_TOO_LARGE、NEED_SNAPSHOT、NOT_HELD、REPLAY_UNAVAILABLE`。不分配 sequence。
- **Infrastructure**：RPC 超时/不可解码、RocksDB/ checksum/manifest 错误。RPC 可能已经提交，客户端报告 `OUTCOME_UNKNOWN`；存储错误使 group `FAILED`。

Admission 与 BusinessResult 分层：REPLAY 携带原始业务结果 bytes；拒绝只带 admission code 和诊断，不伪造历史业务结果。原结果中的 path、token、first revision 和业务状态不会因重放改变。

## 12. 测试与验收

测试必须使用手动 clock、手动 executor、barrier/latch 和可控 fake transport；禁止用 `Thread.sleep` 证明竞态。

协议层固定 golden canonical bytes/digest、unknown field、非法 UTF-8、oneof、边界长度和 defensive copy。存储层覆盖首次初始化、每类 record checksum、iterator error、未提交 transition、read lease/close 竞争、snapshot manifest 和 staging rename。引擎层覆盖 replay/mismatch、generation/sequence gap、业务失败消费、GC 后迟到重试、MULTI 全回滚、sequential 生命周期、lease renew/expire CAS、锁 token 和 change floor。

客户端层覆盖多线程赋号无 duplicate/missing sequence、FIFO gap recovery、旧 transport 迟到回复、journal append/result 失败、显式 takeover、lease close/revoke、Acquisition cancel/timeout/late success、watch callback 重投递和 NEED_SNAPSHOT。随机参考模型至少 100 个 seed、每 seed 1000 步，模型不调用生产状态 helper。

真实 Ratis 测试覆盖：leader/follower apply、query read-index、snapshot/reinitialize、同 group 重启、working DB ahead、manifest/record 损坏、SIGKILL 于 checkpoint publish 各阶段、两个 group 互不影响、COORDINATION 与 KEYSPACE role 拒绝。验收只接受 surefire XML 中明确执行且 `failures=0、errors=0` 的测试；不得用 skip 或 `failIfNoSpecifiedTests=false` 掩盖未匹配测试。

发布前必须运行：

```text
mvn -pl ratis-coordination -am verify
mvn -pl ratis-server -am test -Dtest='TestRetryCacheImpl,TestPendingRequests'
```

并生成 `docs/metadata-redesign/validation-report` 的等价验收报告，明确哪些 crash/power-loss 场景未在当前平台实测。任何“正确性已保证”的结论都必须能回溯到协议不变量、实现路径和测试证据。

