/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.metadata;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.metadata.client.RaftMetadataClient;
import org.apache.ratis.metadata.protocol.RaftCommand;
import org.apache.ratis.metadata.protocol.RaftCommandCodec;
import org.apache.ratis.metadata.protocol.RaftResult;
import org.apache.ratis.metadata.statemachine.MetadataStateMachine;
import org.apache.ratis.metadata.util.Clock;
import org.apache.ratis.metadata.util.ManualClock;
import org.apache.ratis.metadata.util.TestWait;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.GroupInfoReply;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.SizeInBytes;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * An in-process metadata cluster of {@link MetadataServer}s on stock transports (grpc by
 * default): probed per-node ports, one shared {@link ManualClock}, per-node directories, an
 * admin raft client and leadership helpers - the test-side replacement of the Fluss prototype's
 * RpcServer-based harness.
 */
public final class MetadataCluster implements AutoCloseable {

    private static final Duration LEADER_TIMEOUT = Duration.ofSeconds(60);
    private static final long SNAPSHOT_CALL_TIMEOUT_MS = 60_000L;

    private final File tempDir;
    private final int numServers;
    private final boolean purgeForSnapshot;
    private final SupportedRpcType rpcType;
    private final Duration leaseScanInterval;
    private final Duration leaseClockSkewTolerance;
    private final Duration resultRetention;
    private final Duration retryCacheExpiry;
    private final UUID groupUuid;

    public final ManualClock clock = new ManualClock(100_000L);

    private final int[] ports;
    private final List<String> quorum;
    private final MetadataServer[] servers;
    private RaftClient adminClient;

    private MetadataCluster(Builder builder, File tempDir) {
        this.tempDir = tempDir;
        this.numServers = builder.numServers;
        this.purgeForSnapshot = builder.purgeForSnapshot;
        this.rpcType = builder.rpcType;
        this.leaseScanInterval = builder.leaseScanInterval;
        this.leaseClockSkewTolerance = builder.leaseClockSkewTolerance;
        this.resultRetention = builder.resultRetention;
        this.retryCacheExpiry = builder.retryCacheExpiry;
        this.groupUuid = builder.groupUuid;
        this.ports = new int[numServers];
        this.quorum = new ArrayList<>(numServers);
        this.servers = new MetadataServer[numServers];
    }

    /** The group id - fixed up front so every member and the admin client agree before start. */
    public RaftGroupId groupId() {
        return groupUuid != null
                ? RaftGroupId.valueOf(groupUuid)
                : RaftGroupId.valueOf(
                        UUID.nameUUIDFromBytes(
                                ("ratis-metadata-test:" + numServers + "-nodes").getBytes()));
    }

    /** Starts every member, then the admin client; call once before any kill/restart. */
    public void start() throws Exception {
        final List<RaftPeer> peers = new ArrayList<>(numServers);
        for (int i = 0; i < numServers; i++) {
            ports[i] = probedPort();
            quorum.add("n" + i + "@localhost:" + ports[i]);
            peers.add(
                    RaftPeer.newBuilder()
                            .setId("n" + i)
                            .setAddress("localhost:" + ports[i])
                            .build());
        }
        final RaftGroup fullGroup = RaftGroup.valueOf(groupId(), peers);
        for (int i = 0; i < numServers; i++) {
            newServer(i, fullGroup);
            servers[i].start();
        }
        adminClient = newAdminClient();
    }

    /**
     * A fresh admin raft client on the cluster's transport. Raw unordered sends need one per
     * connection era: a client's unordered stream to a server dies permanently when that server
     * restarts or answers LeaderNotReady.
     */
    public RaftClient newAdminClient() {
        final RaftProperties adminProperties = new RaftProperties();
        RaftConfigKeys.Rpc.setType(adminProperties, rpcType);
        final List<RaftPeer> peers = new ArrayList<>(numServers);
        for (int i = 0; i < numServers; i++) {
            peers.add(
                    RaftPeer.newBuilder()
                            .setId("n" + i)
                            .setAddress("localhost:" + ports[i])
                            .build());
        }
        return RaftClient.newBuilder()
                .setRaftGroup(RaftGroup.valueOf(groupId(), peers))
                .setProperties(adminProperties)
                .build();
    }

    /** (Re)builds member {@code i} against the given group; visible for restart flows. */
    private void newServer(int i, RaftGroup fullGroup) {
        final File dataDir = new File(tempDir, "node-n" + i);
        final MetadataStateMachine stateMachine =
                new MetadataStateMachine(new File(dataDir, "rocksdb"));
        // Pin retention to zero: the suites assert retention line = snapshotIndex + 1 exactly.
        stateMachine.setChangeRecordRetention(0, Long.MAX_VALUE);
        MetadataServer.Builder builder =
                MetadataServer.newBuilder()
                        .setSelfId("n" + i)
                        .setQuorum(quorum)
                        .setDataDir(dataDir)
                        .setClock(clock)
                        .setRpcType(rpcType)
                        .setLeaseScanInterval(leaseScanInterval)
                        .setStateMachine(stateMachine)
                        .setGroupId(fullGroup.getGroupId().getUuid());
        // Test-only overrides; unset means the production defaults (7-day retention, 5-minute
        // retry-cache expiry, per-lease ttl/3 skew tolerance) so these knobs never change
        // default harness behavior.
        if (resultRetention != null) {
            builder.setResultRetention(resultRetention);
        }
        if (retryCacheExpiry != null) {
            builder.setRetryCacheExpiry(retryCacheExpiry);
        }
        if (leaseClockSkewTolerance != null) {
            builder.setLeaseClockSkewTolerance(leaseClockSkewTolerance);
        }
        builder.setPropertiesCustomizer(
                properties -> {
                    setTransportPort(properties, ports[i]);
                    if (purgeForSnapshot) {
                        // 256b segments: one entry per segment, so a purge
                        // advances the retention line in lock-step.
                        RaftServerConfigKeys.Log.setPurgeUptoSnapshotIndex(properties, true);
                        RaftServerConfigKeys.Log.setPurgeGap(properties, 1);
                        RaftServerConfigKeys.Log.setSegmentSizeMax(
                                properties, SizeInBytes.valueOf("256b"));
                    }
                });
        servers[i] = builder.build();
    }

    /** The transport's listen-port key - the one piece of wiring the transport artifact owns. */
    private void setTransportPort(RaftProperties properties, int port) {
        if (rpcType == SupportedRpcType.NETTY) {
            NettyConfigKeys.Server.setPort(properties, port);
        } else {
            GrpcConfigKeys.Server.setPort(properties, port);
        }
    }

    /** The admin raft client of the harness (group management, snapshot creation). */
    public RaftClient adminClient() {
        return adminClient;
    }

    /** A fresh metadata client wired to every member, on the cluster's transport. */
    public RaftMetadataClient newMetadataClient() {
        return newMetadataClient(clock);
    }

    /**
     * A fresh metadata client as {@link #newMetadataClient()}, but on the caller's clock - for
     * suites that pin the client's time source independently of the servers'.
     */
    public RaftMetadataClient newMetadataClient(Clock clientClock) {
        final RaftMetadataClient.Builder builder =
                RaftMetadataClient.newBuilder()
                        .setGroupId(groupUuid != null ? groupUuid : groupId().getUuid())
                        .setClock(clientClock)
                        .setRpcType(rpcType);
        for (int i = 0; i < numServers; i++) {
            builder.addQuorumMember(
                    "n" + i, new InetSocketAddress("localhost", ports[i]));
        }
        return builder.build();
    }

    /** Closes member {@code i} (its scheduler, client and raft server); restartable afterwards. */
    public void killServer(int i) throws Exception {
        if (servers[i] != null) {
            servers[i].close();
            servers[i] = null;
        }
    }

    /** Rebuilds and starts member {@code i} on its existing directories and port. */
    public void restartServer(int i) throws Exception {
        killServer(i);
        final List<RaftPeer> peers = new ArrayList<>(numServers);
        for (int j = 0; j < numServers; j++) {
            peers.add(
                    RaftPeer.newBuilder()
                            .setId("n" + j)
                            .setAddress("localhost:" + ports[j])
                            .build());
        }
        newServer(i, RaftGroup.valueOf(groupId(), peers));
        servers[i].start();
    }

    /** Member {@code i}'s server, when running. */
    public MetadataServer server(int i) {
        return servers[i];
    }

    /** Member {@code i}'s state machine, when its server is running. */
    public MetadataStateMachine stateMachine(int i) {
        final MetadataServer server = servers[i];
        return server != null ? server.getStateMachine() : null;
    }

    public int numServers() {
        return numServers;
    }

    /** Takes a snapshot on the leader; the reply's log index is the snapshot's last index. */
    public long takeSnapshotOnLeader() throws IOException {
        final RaftClientReply reply =
                adminClient
                        .getSnapshotManagementApi(leaderId())
                        .create(true, SNAPSHOT_CALL_TIMEOUT_MS);
        if (!reply.isSuccess()) {
            throw new AssertionError("Snapshot creation failed: " + reply);
        }
        return reply.getLogIndex();
    }

    public List<RaftPeerId> peerIds() {
        final List<RaftPeerId> ids = new ArrayList<>(numServers);
        for (int i = 0; i < numServers; i++) {
            ids.add(RaftPeerId.valueOf("n" + i));
        }
        return ids;
    }

    /**
     * The ids of members whose server is still up. Leadership polls must never address a killed
     * member: grpc fails the connect fast, but a netty connect to a dead endpoint parks in the
     * client's retry loop for far longer than any election.
     */
    public List<RaftPeerId> livePeerIds() {
        final List<RaftPeerId> ids = new ArrayList<>(numServers);
        for (int i = 0; i < numServers; i++) {
            if (servers[i] != null) {
                ids.add(RaftPeerId.valueOf("n" + i));
            }
        }
        return ids;
    }

    /** Waits for a leader among the live members only (a killed member never gets asked). */
    public RaftPeerId awaitLeader() {
        return awaitLeader(livePeerIds());
    }

    /** Waits for a leader among the given peers only (dead peers must never be queried). */
    public RaftPeerId awaitLeader(List<RaftPeerId> candidates) {
        return TestWait.waitValue(
                () -> findLeader(candidates),
                LEADER_TIMEOUT,
                "No leader elected among " + candidates + " within " + LEADER_TIMEOUT);
    }

    /** The current leader among the live members, or null while an election is in flight. */
    public RaftPeerId leaderId() {
        return findLeader(livePeerIds()).orElse(null);
    }

    /**
     * Waits until a restarted member has fully rejoined the group: it answers group info again
     * (its transport is bound and its raft server runs, so it grants votes), and its local state
     * has replayed the whole log - a direct query answers with the revision the current leader
     * applies. Killing a leader while a restart is still catching up parks the next election
     * far past any lease ttl - a failover outside the transparency the session contract
     * describes.
     */
    public void awaitRejoined(int i) {
        final RaftPeerId peerId = peerIds().get(i);
        TestWait.waitUntil(
                () -> {
                    final RaftPeerId leader = leaderId();
                    if (leader == null) {
                        return false;
                    }
                    try {
                        adminClient.getGroupManagementApi(peerId).info(groupId());
                        final long applied =
                                directQuery(stateMachine(indexOf(leader)), RaftCommand.revision())
                                        .getRevision();
                        return directQuery(stateMachine(i), RaftCommand.revision()).getRevision()
                                >= applied;
                    } catch (IOException rejoining) {
                        // A restarting member answers late, not never.
                        return false;
                    } catch (RuntimeException notOpenYet) {
                        // The restarted state machine replays its log before it answers queries.
                        return false;
                    }
                },
                LEADER_TIMEOUT,
                "Server " + peerId + " did not rejoin the group within " + LEADER_TIMEOUT);
    }

    private Optional<RaftPeerId> findLeader(List<RaftPeerId> candidates) {
        for (RaftPeerId candidate : candidates) {
            try {
                final GroupInfoReply info =
                        adminClient.getGroupManagementApi(candidate).info(groupId());
                if (info.getRoleInfoProto().getRole() == RaftPeerRole.LEADER) {
                    return Optional.of(candidate);
                }
            } catch (IOException serverBusy) {
                // A server still starting or re-electing answers late, not never.
            } catch (RuntimeException serverBusy) {
                // A killed member's transport answers UNAVAILABLE mid-teardown - late, not never.
            }
        }
        return Optional.empty();
    }

    public int indexOf(RaftPeerId peerId) {
        return Integer.parseInt(peerId.toString().substring(1));
    }

    @Override
    public void close() {
        if (adminClient != null) {
            try {
                adminClient.close();
            } catch (Exception ignored) {
                // best-effort teardown
            }
        }
        for (int i = numServers - 1; i >= 0; i--) {
            if (servers[i] != null) {
                try {
                    servers[i].close();
                } catch (Exception ignored) {
                    // best-effort teardown
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // static helpers shared by the cluster suites
    // ------------------------------------------------------------------

    /**
     * A wait whose reads may fail while the group re-elects: a failed read counts as "not yet".
     */
    public static void waitThroughTransientFailure(
            SupplierWithIo<Boolean> condition, Duration timeout, String what) {
        TestWait.waitUntil(
                () -> {
                    try {
                        return condition.get();
                    } catch (IOException transientFailure) {
                        return false;
                    }
                },
                timeout,
                what);
    }

    /** A condition that may fail with {@link IOException} while the group re-elects. */
    public interface SupplierWithIo<T> {
        T get() throws IOException;
    }

    /** Direct query: a stopped or catching-up replica has no leader role but queryable state. */
    public static RaftResult directQuery(
            MetadataStateMachine stateMachine, RaftCommand command) {
        final CompletableFuture<Message> future =
                stateMachine.query(
                        Message.valueOf(
                                ByteBuffer.wrap(RaftCommandCodec.encodeCommand(command))));
        final RaftResult result =
                RaftCommandCodec.decodeResult(future.join().getContent().toByteArray());
        if (result == null) {
            throw new AssertionError("Query " + command.getOp() + " does not decode");
        }
        return result;
    }

    /** A bind-and-release free port. */
    static int probedPort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("Probing a free port failed", e);
        }
    }

    /** The cluster builder; defaults: 3 grpc members, no purge, 50ms lease scans. */
    public static Builder newBuilder(File tempDir) {
        return new Builder(tempDir);
    }

    /** The builder of {@link MetadataCluster}. */
    public static final class Builder {

        private final File tempDir;
        private int numServers = 3;
        private boolean purgeForSnapshot = false;
        private SupportedRpcType rpcType = SupportedRpcType.GRPC;
        private Duration leaseScanInterval = Duration.ofMillis(50);
        private Duration leaseClockSkewTolerance;
        private Duration resultRetention;
        private Duration retryCacheExpiry;
        private UUID groupUuid;

        private Builder(File tempDir) {
            this.tempDir = tempDir;
        }

        public Builder setNumServers(int numServers) {
            this.numServers = numServers;
            return this;
        }

        /** 256b segments, purge gap 1, purge up to snapshot: retention line advances per write. */
        public Builder setPurgeForSnapshot(boolean purgeForSnapshot) {
            this.purgeForSnapshot = purgeForSnapshot;
            return this;
        }

        public Builder setRpcType(SupportedRpcType rpcType) {
            this.rpcType = rpcType;
            return this;
        }

        public Builder setLeaseScanInterval(Duration leaseScanInterval) {
            this.leaseScanInterval = leaseScanInterval;
            return this;
        }

        /**
         * Forwards the lease clock-skew tolerance of every member's expiry scan (default: the
         * production per-lease ttl/3) so suites can pin an absolute tolerance deterministically.
         */
        public Builder setLeaseClockSkewTolerance(Duration leaseClockSkewTolerance) {
            this.leaseClockSkewTolerance = leaseClockSkewTolerance;
            return this;
        }

        /**
         * Forwards the result-retention window every member's leader proposes (default: the
         * production 7 days); short windows plus the shared {@link ManualClock} let suites drive
         * deterministic collection.
         */
        public Builder setResultRetention(Duration resultRetention) {
            this.resultRetention = resultRetention;
            return this;
        }

        /**
         * Forwards the memory retry-cache expiry of every member (default: the production
         * 5 minutes) so suites can expire the in-memory cache and reach the durable store.
         */
        public Builder setRetryCacheExpiry(Duration retryCacheExpiry) {
            this.retryCacheExpiry = retryCacheExpiry;
            return this;
        }

        public Builder setGroupUuid(UUID groupUuid) {
            this.groupUuid = groupUuid;
            return this;
        }

        public MetadataCluster build() {
            return new MetadataCluster(this, tempDir);
        }
    }
}
