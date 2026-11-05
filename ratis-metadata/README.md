<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# ratis-metadata

A raft-replicated metadata store on stock Apache Ratis: a RocksDB-backed state machine speaking
a ZooKeeper-flavored command set (hierarchical records, versions, compare-and-swap, sequential
creation, ephemeral records bound to leases, counters), plus the client, lease and bootstrap
layers around it. Ported from the Apache Fluss raft-metadata prototype; the coordination layer
only - the embedder owns the network and the service wiring.

## Layout

```
org.apache.ratis.metadata
├── MetadataServer          assembles one group member: raft server + state machine + lease
│                           scheduler + local client (builder-based)
├── statemachine/           MetadataStateMachine (RocksDB, WAL-off: the raft log is the WAL),
│                           RaftCommand/RaftCommandCodec (hand-written TLV), RaftResult,
│                           snapshot = RocksDB checkpoint + atomic publish + identity manifest,
│                           POLL_CHANGES change stream with retention
├── client/                 RaftMetadataClient (sync surface + never-throwing async twins),
│                           LeaseSession (scheduler-driven renewal), MetadataWatcher (a strictly
│                           sequential async poll chain), exception family: RaftMetadataException
│                           (rejection), NeedSnapshotException (retention line),
│                           MetadataTransportException
├── lease/                  LeaseExpiryScheduler - the leader proposes one expiry command per
│                           due lease through submitClientRequestAsync
├── lock/                   MetadataLock - a mutual-exclusion lock recipe (builder-based): an
│                           ephemeral record guards the path, waiters follow the change stream,
│                           and per-acquire tokens fence stale holders
└── util/                   self-contained helpers (Preconditions, files, clock) and
                           AsyncDriver - the one shared daemon scheduler behind every periodic
                           background step (lease renewals, watcher poll pacing, lock watchdogs);
                           its contract pairs with the client's sender thread - see the
                           threading model
```

## Dependencies

Main scope depends on ratis-common/client/server only - **no transport artifact** (the
ratis-server/ratis-client convention). The transport jar (ratis-grpc for the default
`SupportedRpcType.GRPC`, ratis-netty for `NETTY`) is supplied by whoever runs the module:
tests pull both as test-scoped dependencies.

## Wiring

```java
MetadataServer server = MetadataServer.newBuilder()
    .setSelfId("n0")
    .setQuorum("n0@localhost:9000,n1@localhost:9001,n2@localhost:9002")
    .setDataDir(dataDir)                       // holds raft/ and rocksdb/
    .setPropertiesCustomizer(props ->          // transport keys live with the transport artifact
        GrpcConfigKeys.Server.setPort(props, 9000))
    .build();
server.start();
RaftMetadataClient client = server.getClient();
```

Notes:

- The group id derives deterministically from the sorted membership (`ratis-metadata:` salt);
  every member and every restart computes the same uuid.
- Port 0 is legal only on this member's own quorum entry: the resolved port (probed, or
  `setSelfPort`) is pinned as the transport listen port - written before the properties
  customizer runs, so the customizer keeps the last word - and `getSelfEndpoint()`
  exposes the resolved endpoint. Remote members need concrete ports.
- The ratis grpc message cap defaults to 64 MB, ample headroom over the POLL_CHANGES default
  byte budget (4 MB) - no transport tuning needed for the metadata traffic.
- MetadataServer wires synchronous log flush, no per-commit METADATA log entry, linearizable
  reads by default (a customizer may deliberately override), a 5-minute memory retry cache
  fast path over the durable replicated result store, and a bounded snapshot lifecycle; see
  `MetadataServer#raftProperties()`.
- `start()` never exits the embedding JVM: the advertised port is probed before any ratis
  resource exists - with exactly the listener semantics the transport itself uses, so a
  same-port restart over draining sockets still passes - and ratis' grpc
  exit-on-start-failure (`ExitUtils.terminate`) is disarmed process-wide once a
  `MetadataServer` starts. Any failure of the raft start - either transport's bind
  conflict, or a division-start failure - closes what `start()` opened and fails the call
  with an `IOException`; co-hosted services keep running. Because a disarmed exit means
  a fatal ratis background failure now kills only its thread, `getTerminateException()`
  exposes the recorded reason for a member that goes silent.

## Failure contract (summary)

- `RaftMetadataException` - the committed command was rejected (status + sub-command detail).
- `NeedSnapshotException` - polled change records were garbage collected; resynchronize.
- `MetadataTransportException` - the request never went through (transport broke, the reply was
  undecodable, or the client's send queue was full - safe to retry once the pipeline drains). Its
  subclass `PossiblyCommittedException` marks a mutation whose outcome is
  unknown: its retry budget ran out, or the ordered pipeline it shared was torn down by another
  request's exhausted budget - some attempt may have committed, so reconcile by reading state
  before re-executing; every new call is a fresh invocation, not a retry. Two shapes stay on
  the plain type with an unknown outcome for mutations: a first attempt that gives up (the
  builder refuses a retry budget below one request attempt) and a teardown at `close()` -
  close clients only after their in-flight mutations settle, or reconcile before re-running.
  For a watcher the transport failure splits by recovery: while its client is open with a
  rebuildable pipeline, a poisoned window (one exhausted budget makes ratis fail every later
  async call of that client) is swapped for a fresh client and the watcher polls on through
  it; when there is no way back (the client closed, or its raft client cannot be rebuilt), the
  watcher stops instead - `isRunning()` false, `getFailure()` set, checkpoint kept - and it
  stays bound to that client, so resuming takes a rebuilt client with a NEW watcher started
  from the old one's `lastDeliveredRevision()` (`restartFrom` cannot revive it). Every other
  transport failure keeps the watcher polling at its interval - including a server
  persistently replying MALFORMED, whose replies still leave the pipeline healthy and can heal
  by waiting: `isRunning()` stays true with no failure set; close the watcher from the outside
  if that state is unwanted.
- Programmer-error arguments - an unencodable string (an unpaired surrogate) or an over-cap MULTI -
  throw `IllegalArgumentException` on the sync surface and settle the async twins exceptionally;
  they never reach the replicated apply.

## Write invariants (enforced by the replicated apply)

- The root `/` is never a record: CREATE, DELETE, PUT and CAS all reject it with
  NOT_ALLOWED. Every rule below lives in the replicated apply, not the client - raw log
  entries and MULTI children hit the same gates (the client does no path validation). The one
  client-side gate is wire-representability, not policy: an unpaired-surrogate string is
  rejected at encode because the UTF-8 wire cannot carry it losslessly - silently substituting
  `?` would alias distinct names - and raw crafted log entries cannot smuggle one in either
  (malformed bytes decode to U+FFFD).
- A PUT that would create a missing path is held to CREATE's parent rule (the parent must
  exist and must not be lease-bound); updating an existing record is ungated by parents.
- Sequential CREATE validates the final candidate path (base + `%010d`) before any counter
  or record effect; a candidate that fails validation advances to the next number, and an
  exhausted range fails NODE_EXISTS with no effects staged.
- A lease-bound (ephemeral) record can carry no descendants: binding a lease to a path with
  live descendants is rejected (committed records and same-batch MULTI overlay both count),
  and creating under a lease-bound parent is rejected symmetrically.
- A lease-guarded INCREMENT (a non-zero leaseId on the command) requires that lease to exist
  and to own the record at the counter's path when the command applies; otherwise it rejects
  with UNKNOWN_LEASE or STALE_LEASE and moves no counter. Unguarded increments (leaseId 0)
  stay plain counter arithmetic - the guard is what keeps a lock acquire's fencing token from
  overtaking the current holder's.
- POLL_CHANGES returns only records at or below the poll's sampled watermark (the reply's
  `revision`); a record committed after the sample is left for the next poll to return.

## Threading model

No component owns a blocking thread. The one deliberate exception is the leader's lease-expiry
scan loop (`lease/LeaseExpiryScheduler`): its scans park on the state machine's database read
lock behind seconds-long state-machine transitions - a wait the shared scheduler must never
make, so the loop runs on its own thread. One JVM-wide daemon scheduler (`util/AsyncDriver`) drives
every periodic step, and each step runs on the client's asynchronous twins - every async send is
issued from the client's single FIFO sender thread, so ratis' outstanding-request backpressure
parks that thread and nobody else: a wedged pipeline delays its own sends and eventually fails
new ones fast as `MetadataTransportException` instead of freezing the shared scheduler, the
completion executor or the calling thread. A returned async call means the send is enqueued,
not yet issued. The send queue is bounded (256); lease-lifecycle commands (register/renew/expire)
keep a reserved share, but a saturated pipeline still defers renewals by the backlog queued
ahead of them - keep client backlogs shallow when leases run short ttls. Lease sessions renew
from 50 ms due-check ticks, and each renewal rides the client's dedicated raft client, sized by
the most urgent live session's ttl (a third of it per attempt, a dispatch budget of that ttl
plus the expiry grace), so renewals never queue behind user operations and a leader failover
resolving inside the ttl is transparent for sessions at or above that sizing - renewals of
sessions sharing one client may still queue briefly behind one another, and sub-750 ms ttls
remain best-effort against the pacing floors. The watcher keeps at most one poll in flight and
schedules the next poll from each completion, and a lock's acquire is an event-driven state
machine (wake, watchdog and timeout events) - any number of sessions, watchers or lock
contenders costs no extra threads. `close()` on a session, watcher or lock never blocks;
`close()` on the client never
blocks either - sends still queued behind a wedged pipeline are settled by a bounded drain.
Listener callbacks that set no executor run on their client's small delivery pool - isolated
from the completion executor, so a blocked callback delays only other callbacks of the same
client, never the client's replies (a `MetadataLock`'s wake-ups ride that pool too). Keep
callbacks short, and never let one block on another watcher's delivery or a lock acquire -
default watchers of one client share the pool and can starve each other. Handlers attached to
the futures returned by the `*Async` methods run on the completion executor itself: never block
there either - two blocked handlers starve every reply of the client, lease renewals included.
Hand the watcher an explicit executor for full control over where callbacks run.

## Operations

- Startup pairs `rocksdb/` with `raft/`: a kept working database whose raft log is gone is
  refused, never auto-restored - keep the pair together or rebuild both, an explicit
  operator decision. A torn working database still heals itself from the newest local
  snapshot; the raft log replays everything above it. A snapshot area whose listing fails,
  or a snapshot directory whose manifest cannot even be verified, likewise refuses startup
  (after a few short retries for transient disk errors) - and deliberately so even when a
  leader could have re-installed a snapshot into the broken area: an unverifiable local
  snapshot is an operator decision, and a member coming up degraded and empty can strand a
  single-member group. Only an already-running server treats a transient scan failure as
  "no snapshot found".
- Exactly-once retries hit a five-minute in-memory fast path; the durable, replicated
  result store (default 7-day retention) is the fallback. Client retry budgets must be
  finite (`setRetryForever` is rejected), must fit inside every server's result retention,
  and retries arriving after retention are unsupported (they legitimately re-execute).
- Lease expiry spans leader clocks: the leader that sequences a renewal stamps its expiry with
  its own wall clock, and whoever leads later judges due-ness with theirs. The expiry scan
  tolerates one renewal interval (ttl / 3) of cross-leader skew on top of the ttl / 3 renewal
  grace (`MetadataServer.Builder#setLeaseClockSkewTolerance` adjusts or disables it). Keep
  member clocks synchronized (NTP or equivalent) with skew well under the lease ttl: skew
  reaching the ttl plus the tolerance lets a new leader expire leases whose holders still
  renew on schedule.
- All components must run the same version: identity manifests (codec and layout versions)
  reject older working databases, snapshots and log data on sight - there is no rolling
  downgrade.

## Tests

`Test*` classes only (surefire include rule): state-machine unit suites on built-not-started
members, client mechanics on hand-written rpc stubs, and in-process cluster suites via the
`MetadataCluster` harness (grpc main line, netty variant smoke). Run with:

```
./mvnw -pl ratis-metadata clean test
```

`MetadataBenchmark` (same tree, no `Test` prefix, so surefire never picks it up) is the manual
performance harness of the metadata path - sequential/pipelined writes at several payload sizes,
sequential reads, 1000-child listings, and leader failover under a write stream - against raft-1
and raft-3 in-process groups. Run it from the test classpath:

```
./mvnw -pl ratis-metadata test-compile dependency:build-classpath \
    -Dmdep.outputFile=target/cp.txt
java -cp target/test-classes:target/classes:$(cat target/cp.txt) \
    org.apache.ratis.metadata.MetadataBenchmark [--quick] [--csv=<path>]
```
