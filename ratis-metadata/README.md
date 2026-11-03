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
│                           LeaseSession (background renewal), MetadataWatcher (poll loop),
│                           exception family: RaftMetadataException (rejection),
│                           NeedSnapshotException (retention line), MetadataTransportException
├── lease/                  LeaseExpiryScheduler - the leader proposes one expiry command per
│                           due lease through submitClientRequestAsync
└── util/                   self-contained helpers (Preconditions, files, clock, threads)
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
- Port 0 is legal only on this member's own quorum entry (an ephemeral port is probed) -
  remote members need concrete ports.
- The ratis grpc message cap defaults to 64 MB, ample headroom over the POLL_CHANGES default
  byte budget (4 MB) - no transport tuning needed for the metadata traffic.
- MetadataServer wires async log flush, no per-commit METADATA log entry, a 5-minute retry
  cache (outliving the client's retry budget, for exactly-once) and a bounded snapshot
  lifecycle; see `MetadataServer#raftProperties()`.

## Failure contract (summary)

- `RaftMetadataException` - the committed command was rejected (status + sub-command detail).
- `NeedSnapshotException` - polled change records were garbage collected; resynchronize.
- `MetadataTransportException` - the request never went through (retries exhausted, transport
  broke, or the reply was undecodable).

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

