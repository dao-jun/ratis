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

import org.apache.ratis.metadata.client.RaftMetadataClient;
import org.apache.ratis.metadata.lease.LeaseExpiryScheduler;
import org.apache.ratis.metadata.statemachine.MetadataStateMachine;
import org.apache.ratis.metadata.util.Clock;
import org.apache.ratis.metadata.util.FileUtils;
import org.apache.ratis.metadata.util.SystemClock;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.ratis.util.ExitUtils;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.apache.ratis.metadata.util.Preconditions.checkArgument;
import static org.apache.ratis.metadata.util.Preconditions.checkNotNull;
import static org.apache.ratis.metadata.util.Preconditions.checkState;

/**
 * Assembles one member of a raft metadata group: raft server, RocksDB state machine, lease
 * scheduler and the local metadata client - transport-agnostic except for the rpc type knob, so
 * the matching transport artifact (ratis-grpc by default) must be on the runtime classpath.
 */
public final class MetadataServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataServer.class);

    /** Default replicated result-retention window proposed by each leader. */
    static final java.time.Duration DEFAULT_RESULT_RETENTION = java.time.Duration.ofDays(7);

    /**
     * Default memory retry-cache expiry. The in-memory cache is a five-minute fast path; the
     * durable, replicated result store the state machine commits is the fallback that keeps
     * retries exactly-once after memory expires. The builder setter exists so test harnesses
     * can shorten it and drive the durable store below the in-memory cache.
     */
    private static final TimeDuration DEFAULT_RETRY_CACHE_EXPIRY =
            TimeDuration.valueOf(5, TimeUnit.MINUTES);

    private final RaftPeerId selfId;
    private final RaftGroupId groupId;
    private final List<RaftPeer> peers;
    private final InetSocketAddress selfEndpoint;
    /** The port a port-0 self entry resolved to, pinned as the transport listen port; else -1. */
    private final int selfListenPort;
    private final File raftDir;
    private final File rocksdbDir;
    private final Clock clock;
    private final SupportedRpcType rpcType;
    private final java.time.Duration requestTimeout;
    private final java.time.Duration resultRetention;
    private final TimeDuration retryCacheExpiry;
    private final java.time.Duration leaseScanInterval;
    private final java.time.Duration leaseClockSkewTolerance;
    private final boolean snapshotAutoTrigger;
    private final long snapshotAutoTriggerThreshold;
    private final int snapshotRetentionFileNum;
    private final java.util.function.Consumer<RaftProperties> propertiesCustomizer;

    private RaftServer raftServer;
    private MetadataStateMachine stateMachine;
    private LeaseExpiryScheduler leaseScheduler;
    private RaftMetadataClient client;
    /** Injected instead of built when the embedder (tests) owns the state machine. */
    private final MetadataStateMachine injectedStateMachine;

    private MetadataServer(Builder builder) {
        this.injectedStateMachine = builder.stateMachine;
        this.selfId = RaftPeerId.valueOf(builder.selfId);
        this.peers =
                parseQuorumPeers(
                        builder.quorum, selfId, builder.selfPort != null ? builder.selfPort : 0);
        // Deterministic from static membership: identical on every member and across restarts.
        final TreeSet<String> sortedIds = new TreeSet<>();
        for (RaftPeer peer : peers) {
            sortedIds.add(peer.getId().toString());
        }
        this.groupId =
                builder.groupId != null
                        ? RaftGroupId.valueOf(builder.groupId)
                        : RaftGroupId.valueOf(
                                UUID.nameUUIDFromBytes(
                                        ("ratis-metadata:" + String.join(",", sortedIds))
                                                .getBytes()));
        this.selfEndpoint = selfEndpointOf(peers, selfId);
        // A port-0 self entry had its endpoint substituted (probed or builder-set): that port
        // is also this member's listen port - raftProperties() pins it into the transport key.
        this.selfListenPort =
                selfEntryCarriesPortZero(builder.quorum, selfId) ? selfEndpoint.getPort() : -1;
        this.raftDir = new File(builder.dataDir, "raft");
        this.rocksdbDir = new File(builder.dataDir, "rocksdb");
        this.clock = builder.clock;
        this.rpcType = builder.rpcType;
        this.requestTimeout = builder.requestTimeout;
        this.resultRetention = builder.resultRetention;
        this.retryCacheExpiry = builder.retryCacheExpiry;
        this.leaseScanInterval = builder.leaseScanInterval;
        this.leaseClockSkewTolerance = builder.leaseClockSkewTolerance;
        this.snapshotAutoTrigger = builder.snapshotAutoTrigger;
        this.snapshotAutoTriggerThreshold = builder.snapshotAutoTriggerThreshold;
        this.snapshotRetentionFileNum = builder.snapshotRetentionFileNum;
        this.propertiesCustomizer = builder.propertiesCustomizer;
    }

    /** This member's id in the group. */
    public RaftPeerId getId() {
        return selfId;
    }

    /** The group id all members replicate. */
    public RaftGroupId getGroupId() {
        return groupId;
    }

    /**
     * This member's advertised endpoint - concrete even when its quorum entry carried port 0
     * (probed, or {@link Builder#setSelfPort(int)}); that port is what the transport listens
     * on and what peers and external clients dial. Available before {@link #start()}.
     */
    public InetSocketAddress getSelfEndpoint() {
        return selfEndpoint;
    }

    /** Starts the raft metadata group; the ordering of the steps is load-bearing. */
    public synchronized void start() throws IOException {
        checkState(raftServer == null, "metadata server already started");

        // Ratis' grpc transport answers a server-start failure with ExitUtils.terminate - a
        // System.exit that takes every co-hosted service of the embedding JVM down
        // (GrpcServicesImpl.startImpl is the only terminate call site in main-scope ratis).
        // This module embeds by contract, so disarm the process exit process-wide (ratis'
        // own test harnesses flip the same switch); the guard on the raft start below keeps
        // the failure on this call stack instead. One-way and JVM-global - a member gone
        // silent is observable via getTerminateException().
        ExitUtils.disableSystemExit();

        // A fresh log over a kept RocksDB would alias generations: the applied-revision
        // marker would swallow the new log's early commands. Refuse before ratis formats.
        if (!FileUtils.isDirectoryEmpty(rocksdbDir) && !holdsRaftStorage(raftDir)) {
            throw new IOException(
                    "The metadata RocksDB at "
                            + rocksdbDir
                            + " holds state from a raft log that is gone ("
                            + raftDir
                            + " does not exist or holds no raft storage). Restoring the raft"
                            + " directory or deleting the RocksDB with it requires an explicit"
                            + " decision: keep the pair together or rebuild both.");
        }

        // The advertised port is probed before any ratis resource exists, so the common
        // conflict fails fast here instead of inside the transport's own start.
        checkPortAvailable(selfEndpoint);

        final RaftProperties properties = raftProperties();

        this.stateMachine =
                injectedStateMachine != null
                        ? injectedStateMachine
                        : new MetadataStateMachine(rocksdbDir);
        stateMachine.configureBeforeOpen(clock, resultRetention);

        final RaftServer.Builder builder =
                RaftServer.newBuilder()
                        .setServerId(selfId)
                        .setGroup(RaftGroup.valueOf(groupId, peers))
                        .setStateMachine(stateMachine)
                        .setProperties(properties);
        if (raftDir.exists()) {
            // Rebuild-in-place of a used storage directory must recover, not reformat.
            builder.setOption(RaftStorage.StartupOption.RECOVER);
        }
        this.raftServer = builder.build();

        this.leaseScheduler =
                new LeaseExpiryScheduler(
                        raftServer,
                        stateMachine,
                        groupId,
                        clock,
                        leaseScanInterval,
                        leaseClockSkewTolerance);
        leaseScheduler.start();

        try {
            raftServer.start();
        } catch (IOException | RuntimeException e) {
            // The raft start failed: grpc routes e.g. a bind conflict through
            // ExitUtils.terminate (with the exit disarmed above it lands here as an
            // ExitException); netty's bind failure arrives as a plain IOException; a
            // division-start failure - e.g. the state machine's storage refusing the
            // snapshot area - arrives wrapped in a CompletionException (the proxy starts its
            // divisions asynchronously and joins them). Close what start() opened - the
            // client is still null here - and fail this call, not the process. The lease
            // scheduler's close joins a scan loop only if a raft-1 division already
            // self-elected before the failure; it stops promptly, bounded by the scheduler's
            // 10s join timeout.
            LOG.error(
                    "Starting the raft server of group {} failed; closing the metadata server.",
                    groupId,
                    e);
            throw closeAndSuppress(toStartFailure(e));
        }
        LOG.info(
                "Started the metadata group {} (self={}) on {}.",
                groupId,
                selfId,
                peerAddress(selfEndpoint));

        try {
            final RaftMetadataClient.Builder clientBuilder =
                    RaftMetadataClient.newBuilder()
                            .setGroupId(groupId.getUuid())
                            .setRpcType(rpcType)
                            .setRequestTimeout(requestTimeout)
                            // One clock with the server - an injected ManualClock must reach both.
                            .setClock(clock);
            for (RaftPeer peer : peers) {
                clientBuilder.addQuorumMember(
                        peer.getId().toString(), peerEndpoint(peer.getAddress()));
            }
            this.client = clientBuilder.build();
        } catch (RuntimeException e) {
            // The raft server is already running here; close what start() opened and rethrow.
            LOG.error(
                    "Building the metadata client of group {} failed; closing the raft server.",
                    groupId,
                    e);
            throw closeAndSuppress(e);
        }
    }

    /** The metadata client; null until start() finished. */
    public synchronized RaftMetadataClient getClient() {
        return client;
    }

    /** The state machine this server applies commands on; null until start(). */
    public synchronized MetadataStateMachine getStateMachine() {
        return stateMachine;
    }

    /**
     * The first terminate condition ratis recorded in this JVM since the process exit was
     * disarmed by {@link #start()} - e.g. a transport start failure routed through
     * {@code ExitUtils.terminate}, or the record of a ratis background thread's uncaught
     * exception - or null. A MetadataServer never exits its JVM: poll this when a member
     * goes silent to find the recorded reason. The record is JVM-global (any ratis component
     * in the process may have set it) and sticky.
     */
    public ExitUtils.ExitException getTerminateException() {
        return ExitUtils.getFirstExitException();
    }

    /** This member's raft term; a read failure is loud. */
    public long currentTerm() {
        final RaftServer server = this.raftServer;
        if (server == null) {
            return 0L;
        }
        try {
            final RaftServer.Division division = server.getDivision(groupId);
            return division == null ? 0L : division.getInfo().getCurrentTerm();
        } catch (IOException | RuntimeException e) {
            throw new IllegalStateException(
                    "Failed to read the current raft term of group " + groupId, e);
        }
    }

    /** Closes client, scheduler and raft server in reverse start order; idempotent. */
    @Override
    public synchronized void close() throws IOException {
        if (client != null) {
            client.close();
            client = null;
        }
        if (leaseScheduler != null) {
            leaseScheduler.close();
            leaseScheduler = null;
        }
        if (raftServer != null) {
            raftServer.close();
            raftServer = null;
        }
    }

    /** Closes what start() opened, keeping the close's own failure as a suppressed exception. */
    private <T extends Throwable> T closeAndSuppress(T failure) {
        try {
            close();
        } catch (IOException closeFailure) {
            failure.addSuppressed(closeFailure);
        }
        return failure;
    }

    /**
     * What start() throws for a raft-start failure. An IOException is returned unchanged -
     * netty's bind failure as-is, and a division-start failure unwrapped from the proxy's
     * async CompletionException wrapper, so the original message and cause chain stay intact.
     * Every other flavor - grpc's disarmed ExitException, a CompletionException of some other
     * cause, any other bug - becomes a descriptive IOException carrying the original as its
     * direct cause.
     */
    private IOException toStartFailure(Exception e) {
        if (e instanceof IOException) {
            return (IOException) e;
        }
        if (e instanceof CompletionException && e.getCause() instanceof IOException) {
            return (IOException) e.getCause();
        }
        return new IOException(
                "Starting the raft server of group "
                        + groupId
                        + " (self="
                        + selfId
                        + ", advertised "
                        + peerAddress(selfEndpoint)
                        + ") failed in the "
                        + rpcType
                        + " transport; a port or address conflict is the usual cause.",
                e);
    }

    // ------------------------------------------------------------------
    // assembly
    // ------------------------------------------------------------------

    /** The ratis properties the raft server is built from; visible for the wiring test. */
    @VisibleForTesting
    RaftProperties raftProperties() {
        final RaftProperties properties = new RaftProperties();
        RaftConfigKeys.Rpc.setType(properties, rpcType);
        RaftClientConfigKeys.Rpc.setRequestTimeout(
                properties,
                TimeDuration.valueOf(requestTimeout.toMillis(), TimeUnit.MILLISECONDS));
        RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(raftDir));
        setSnapshotPolicy(properties);
        // Pinned to ratis' default synchronous segment flush. The async-flush path of this
        // build never coalesces queued appends into one force, and the metadata benchmark
        // measures it ten times slower (three members, 256 in flight: ~0.3k ops/s against
        // ~3.3k for the synchronous flush); the synchronous path batches the in-flight
        // entries into one flush cycle and still fsyncs before the flush index advances,
        // so a commit keeps meaning majority-fsync.
        RaftServerConfigKeys.Log.setAsyncFlushEnabled(properties, false);
        // Ratis otherwise appends a METADATA entry per commit advance: two log entries and
        // two flushes for every one-command commit on this metadata-sized workload. Those
        // entries only seed the commit index at restart, which the new leader re-advertises
        // anyway; replaying the tail is idempotent (the state machine's revision marker).
        RaftServerConfigKeys.Log.setLogMetadataEnabled(properties, false);
        // Reads must never trail the committed state a client's write returned: default to
        // the read-index (linearizable) path instead of a leader's local state. Set before
        // the customizer below so an embedder keeps the last word and may trade the
        // guarantee for local-state reads.
        RaftServerConfigKeys.Read.setOption(
                properties, RaftServerConfigKeys.Read.Option.LINEARIZABLE);
        // A five-minute memory fast path; retries past it fall back to the durable result
        // store the state machine commits with every command. A builder knob exists for
        // test harnesses, which keep the burden of choosing a defensible expiry themselves.
        RaftServerConfigKeys.RetryCache.setExpiryTime(properties, retryCacheExpiry);
        // A port-0 self entry resolved to a concrete endpoint that the group and the local
        // metadata client both advertise; pin it as the transport's listen port so a listener
        // actually sits behind that address. Raw key names - the transport artifacts are
        // deliberately not on this module's compile classpath; these are their public, stable
        // *ConfigKeys.Server.PORT_KEY values. Pinned before the customizer: the last word
        // stays with an embedder that owns the network setup.
        final String listenPortKey = transportListenPortKey(rpcType);
        if (selfListenPort > 0 && listenPortKey != null) {
            properties.setInt(listenPortKey, selfListenPort);
        }
        // Transport-specific keys (e.g. the grpc listen port) belong to the transport artifact,
        // which this module deliberately does not depend on; the customizer supplies them.
        if (propertiesCustomizer != null) {
            propertiesCustomizer.accept(properties);
        }
        if (selfListenPort > 0
                && listenPortKey != null
                && properties.getInt(listenPortKey, 0) != selfListenPort) {
            LOG.warn(
                    "The properties customizer moved the transport listen port off the"
                            + " advertised {}; the group and the metadata client still"
                            + " advertise it.",
                    peerAddress(selfEndpoint));
        }
        return properties;
    }

    /** Wires the snapshot lifecycle (auto-trigger, retention, snapshot-on-stop). */
    private void setSnapshotPolicy(RaftProperties properties) {
        RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(properties, snapshotAutoTrigger);
        RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(
                properties, snapshotAutoTriggerThreshold);
        RaftServerConfigKeys.Snapshot.setRetentionFileNum(properties, snapshotRetentionFileNum);
        // A snapshot per clean stop bounds restart replay; retention bounds the checkpoints.
        RaftServerConfigKeys.Snapshot.setTriggerWhenStopEnabled(properties, true);
    }

    /** Whether the directory holds ratis-formatted storage: a raft-meta file below it. */
    static boolean holdsRaftStorage(File raftDir) {
        if (!raftDir.isDirectory()) {
            return false;
        }
        // Marker location: <raftDir>/<group>/current/raft-meta, or <raftDir>/current/raft-meta.
        try (Stream<Path> files = Files.walk(raftDir.toPath(), 3)) {
            return files.anyMatch(
                    file ->
                            Files.isRegularFile(file)
                                    && file.getFileName().toString().equals("raft-meta"));
        } catch (IOException e) {
            // Unreadable cannot be proven to hold the log - treat as gone; ratis fails loudly.
            return false;
        }
    }

    /** Builds a new {@link MetadataServer} builder; required: selfId, quorum, dataDir. */
    public static Builder newBuilder() {
        return new Builder();
    }

    /** The builder of {@link MetadataServer}; see {@link MetadataServer#newBuilder()}. */
    public static final class Builder {

        private String selfId;
        private List<String> quorum;
        private File dataDir;
        private Clock clock = SystemClock.getInstance();
        private SupportedRpcType rpcType = SupportedRpcType.GRPC;
        private java.time.Duration requestTimeout = java.time.Duration.ofSeconds(3);
        private java.time.Duration resultRetention = DEFAULT_RESULT_RETENTION;
        private TimeDuration retryCacheExpiry = DEFAULT_RETRY_CACHE_EXPIRY;
        private java.time.Duration leaseScanInterval = java.time.Duration.ofSeconds(1);
        /** Null means the per-lease default: one renewal interval (ttl / 3). */
        private java.time.Duration leaseClockSkewTolerance;
        private boolean snapshotAutoTrigger = true;
        private long snapshotAutoTriggerThreshold = 400_000L;
        private int snapshotRetentionFileNum = 2;
        private UUID groupId;
        private MetadataStateMachine stateMachine;
        private java.util.function.Consumer<RaftProperties> propertiesCustomizer;
        /**
         * The concrete port this member binds when its quorum entry carries port 0; probed by
         * the caller that owns the network setup, or null for the in-server probe.
         */
        private Integer selfPort;

        private Builder() {}

        /** Sets this member's peer id; it must appear in the quorum list. */
        public Builder setSelfId(String selfId) {
            this.selfId = checkNotNull(selfId, "selfId");
            return this;
        }

        /**
         * Sets the static membership as {@code "id@host:port"} entries. A port 0 is legal only
         * on this member's own entry (an ephemeral port is probed then); remote members need a
         * concrete port.
         */
        public Builder setQuorum(List<String> quorum) {
            this.quorum = new ArrayList<>(checkNotNull(quorum, "quorum"));
            return this;
        }

        /** Sets the quorum as a comma-separated {@code "id@host:port"} string. */
        public Builder setQuorum(String quorum) {
            checkNotNull(quorum, "quorum");
            final List<String> entries = new ArrayList<>();
            for (String entry : quorum.split(",")) {
                if (!entry.trim().isEmpty()) {
                    entries.add(entry.trim());
                }
            }
            return setQuorum(entries);
        }

        /** Sets the directory holding the {@code raft/} and {@code rocksdb/} subtrees. */
        public Builder setDataDir(File dataDir) {
            this.dataDir = checkNotNull(dataDir, "dataDir");
            return this;
        }

        /** Sets the time source shared by the state machine timestamps and lease expiries. */
        public Builder setClock(Clock clock) {
            this.clock = checkNotNull(clock, "clock");
            return this;
        }

        /**
         * Sets the rpc type (default grpc); the transport artifact, e.g. ratis-grpc, must be on
         * the runtime classpath of both this server and its clients.
         */
        public Builder setRpcType(SupportedRpcType rpcType) {
            this.rpcType = checkNotNull(rpcType, "rpcType");
            return this;
        }

        /** Sets the transport-level timeout of one request attempt; ratis retries within it. */
        public Builder setRequestTimeout(java.time.Duration requestTimeout) {
            checkNotNull(requestTimeout, "requestTimeout");
            checkArgument(
                    requestTimeout.toMillis() > 0,
                    "requestTimeout must be positive, got %s",
                    requestTimeout);
            this.requestTimeout = requestTimeout;
            return this;
        }

        /** Sets the result-retention window each leader proposes; must be positive. */
        public Builder setResultRetention(java.time.Duration resultRetention) {
            checkNotNull(resultRetention, "resultRetention");
            checkArgument(
                    resultRetention.toMillis() > 0,
                    "resultRetention must be positive, got %s",
                    resultRetention);
            this.resultRetention = resultRetention;
            return this;
        }

        /**
         * Sets the memory retry-cache expiry (default 5 minutes). The in-memory cache is a fast
         * path; the durable, replicated result store answers retries after it expires. Test
         * harnesses may shorten it to drive the durable invocation results below the in-memory
         * cache.
         */
        public Builder setRetryCacheExpiry(java.time.Duration retryCacheExpiry) {
            checkNotNull(retryCacheExpiry, "retryCacheExpiry");
            checkArgument(
                    retryCacheExpiry.toMillis() > 0,
                    "retryCacheExpiry must be positive, got %s",
                    retryCacheExpiry);
            this.retryCacheExpiry =
                    TimeDuration.valueOf(retryCacheExpiry.toMillis(), TimeUnit.MILLISECONDS);
            return this;
        }

        /** Sets how often the leader scans for due lease expiries; must be positive. */
        public Builder setLeaseScanInterval(java.time.Duration leaseScanInterval) {
            checkNotNull(leaseScanInterval, "leaseScanInterval");
            checkArgument(
                    leaseScanInterval.toMillis() > 0,
                    "leaseScanInterval must be positive, got %s",
                    leaseScanInterval);
            this.leaseScanInterval = leaseScanInterval;
            return this;
        }

        /**
         * Sets how much cross-leader wall-clock skew the expiry scan tolerates before judging a
         * lease due, on top of the ttl / 3 renewal grace. The leader that sequenced a lease's
         * last renewal stamped its expiry with that leader's wall clock; whoever leads later
         * judges it with its own - a successor clock running this far ahead of its
         * predecessor's expires otherwise healthy leases at takeover. One renewal interval
         * (ttl / 3) per lease by default; zero opts out (NTP-disciplined fleets), larger values
         * trade dead-holder expiry latency for skew headroom.
         */
        public Builder setLeaseClockSkewTolerance(java.time.Duration clockSkewTolerance) {
            checkNotNull(clockSkewTolerance, "clockSkewTolerance");
            checkArgument(
                    clockSkewTolerance.toMillis() >= 0,
                    "clockSkewTolerance must not be negative, got %s",
                    clockSkewTolerance);
            this.leaseClockSkewTolerance = clockSkewTolerance;
            return this;
        }

        /** Enables or disables the snapshot auto-trigger and sets its log-distance threshold. */
        public Builder setSnapshotAutoTrigger(boolean enabled, long threshold) {
            checkArgument(threshold > 0, "snapshot threshold must be positive, got %s", threshold);
            this.snapshotAutoTrigger = enabled;
            this.snapshotAutoTriggerThreshold = threshold;
            return this;
        }

        /** Sets how many completed snapshots retention keeps per member. */
        public Builder setSnapshotRetentionFileNum(int retentionFileNum) {
            checkArgument(
                    retentionFileNum > 0,
                    "snapshot retention must be positive, got %s",
                    retentionFileNum);
            this.snapshotRetentionFileNum = retentionFileNum;
            return this;
        }

        /**
         * Overrides the group id; by default it derives deterministically from the sorted
         * membership, so every member and every restart computes the same uuid.
         */
        public Builder setGroupId(UUID groupId) {
            this.groupId = checkNotNull(groupId, "groupId");
            return this;
        }

        /**
         * Injects the state machine instead of building one on the internal {@code rocksdb}
         * directory - for tests that tune the machine (retention) or query it directly. The
         * raft server owns its lifecycle from {@link MetadataServer#start()} on.
         */
        public Builder setStateMachine(MetadataStateMachine stateMachine) {
            this.stateMachine = checkNotNull(stateMachine, "stateMachine");
            return this;
        }

        /**
         * Sets the last word on the ratis properties - the hook for transport-specific keys,
         * e.g. {@code GrpcConfigKeys.Server.setPort(properties, port)} with grpc. Applied after
         * every key this server wires itself, so it can also override them.
         */
        public Builder setPropertiesCustomizer(
                java.util.function.Consumer<RaftProperties> propertiesCustomizer) {
            this.propertiesCustomizer =
                    checkNotNull(propertiesCustomizer, "propertiesCustomizer");
            return this;
        }

        /**
         * Supplies the concrete port to bind when this member's own quorum entry carries port
         * 0 - for embedders that own the network setup; otherwise an ephemeral port is probed.
         * Either way the resolved port is pinned as the transport listen port before the
         * properties customizer runs, so the customizer keeps the last word on it.
         */
        public Builder setSelfPort(int selfPort) {
            checkArgument(selfPort > 0, "selfPort must be positive, got %s", selfPort);
            this.selfPort = selfPort;
            return this;
        }

        /** Builds the server (not yet started; call {@link MetadataServer#start()}). */
        public MetadataServer build() {
            checkNotNull(selfId, "selfId");
            checkNotNull(quorum, "quorum");
            checkArgument(!quorum.isEmpty(), "At least one quorum entry is required");
            checkNotNull(dataDir, "dataDir");
            return new MetadataServer(this);
        }
    }

    // ------------------------------------------------------------------
    // quorum parsing
    // ------------------------------------------------------------------

    /**
     * Parses quorum entries ({@code "id@host:port"}) into the peer list. Port 0 on this
     * member's own entry binds the supplied concrete port instead (or a probed ephemeral one);
     * port 0 on a remote entry is rejected - its address would be unusable to the others.
     */
    @VisibleForTesting
    static List<RaftPeer> parseQuorumPeers(
            List<String> quorum, RaftPeerId selfId, int selfPort) {
        final List<RaftPeer> peers = new ArrayList<>();
        boolean containsSelf = false;
        for (String rawEntry : quorum) {
            final String entry = rawEntry.trim();
            final int at = entry.lastIndexOf('@');
            checkArgument(
                    at > 0 && at < entry.length() - 1,
                    "Invalid quorum entry '%s' - expected 'id@host:port'",
                    entry);
            final String id = entry.substring(0, at);
            InetSocketAddress endpoint = peerEndpoint(entry.substring(at + 1));
            if (endpoint.getPort() == 0) {
                final boolean isSelf = RaftPeerId.valueOf(id).equals(selfId);
                final int port = isSelf ? (selfPort > 0 ? selfPort : probeFreePort()) : -1;
                if (port < 0) {
                    throw new IllegalArgumentException(
                            "Only this server's own quorum entry may use port 0 (entry '"
                                    + entry
                                    + "'); remote members need a concrete port.");
                }
                endpoint = new InetSocketAddress(endpoint.getHostString(), port);
            }
            containsSelf |= RaftPeerId.valueOf(id).equals(selfId);
            peers.add(
                    RaftPeer.newBuilder().setId(id).setAddress(peerAddress(endpoint)).build());
        }
        checkArgument(
                containsSelf,
                "self id '%s' does not appear in the quorum %s",
                selfId,
                quorum);
        return peers;
    }

    /** Parses a host:port quorum address, bracketing a bare IPv6 host, left unresolved. */
    @VisibleForTesting
    static InetSocketAddress peerEndpoint(String hostPort) {
        final int colon = hostPort.lastIndexOf(':');
        checkArgument(
                colon > 0 && colon < hostPort.length() - 1,
                "Invalid address '%s' in a quorum entry - expected 'host:port'",
                hostPort);
        final int port;
        try {
            port = Integer.parseInt(hostPort.substring(colon + 1));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Invalid port in the quorum address '" + hostPort + "'");
        }
        final String host = hostPort.substring(0, colon);
        return InetSocketAddress.createUnresolved(
                host.contains(":") && !host.startsWith("[") ? "[" + host + "]" : host, port);
    }

    /** Formats a quorum endpoint as host:port with a bracketed IPv6 host. */
    @VisibleForTesting
    static String peerAddress(InetSocketAddress endpoint) {
        return endpoint.getHostString() + ":" + endpoint.getPort();
    }

    private static InetSocketAddress selfEndpointOf(List<RaftPeer> peers, RaftPeerId selfId) {
        for (RaftPeer peer : peers) {
            if (peer.getId().equals(selfId)) {
                return peerEndpoint(peer.getAddress());
            }
        }
        throw new IllegalArgumentException("Self id " + selfId + " missing from the peer list");
    }

    /** Whether this member's own quorum entry carries port 0 (its endpoint is substituted). */
    private static boolean selfEntryCarriesPortZero(List<String> quorum, RaftPeerId selfId) {
        for (String rawEntry : quorum) {
            final String entry = rawEntry.trim();
            final int at = entry.lastIndexOf('@');
            if (at > 0 && RaftPeerId.valueOf(entry.substring(0, at)).equals(selfId)) {
                return peerEndpoint(entry.substring(at + 1)).getPort() == 0;
            }
        }
        return false;
    }

    /**
     * The transport listen-port property key of the rpc type, or null when the transport is
     * none of the ones known here (grpc, netty) - its listen wiring then stays wholly with
     * the customizer. Raw names: the transport artifacts are off this module's classpath.
     */
    private static String transportListenPortKey(SupportedRpcType rpcType) {
        if (rpcType == SupportedRpcType.GRPC) {
            return "raft.grpc.server.port";
        }
        if (rpcType == SupportedRpcType.NETTY) {
            return "raft.netty.server.port";
        }
        return null;
    }

    /**
     * A free port probed by bind-and-release; a test/embedding convenience with a small race
     * window - production members carry concrete ports in their quorum entries.
     */
    private static int probeFreePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("Probing a free quorum port failed", e);
        }
    }

    /**
     * Refuses a start on a port this member cannot bind: the transport would otherwise
     * surface the conflict only at its own start (and grpc terminates the embedding JVM on
     * it). Binds the wildcard address - where the grpc server binds by default - with
     * SO_REUSEADDR explicitly on, i.e. exactly the listener semantics the transport itself
     * inherits (ratis sets no reuse option on the listening channel), so the probe is never
     * stricter than the transport: a live listener on the port is refused, while a
     * same-port restart over draining TIME_WAIT sockets passes as it does today. The
     * bind-and-release race and any customizer-repinned transport port are closed by the
     * guard around the raft start.
     */
    private static void checkPortAvailable(InetSocketAddress endpoint) throws IOException {
        if (endpoint.getPort() <= 0) {
            return;
        }
        try (ServerSocket socket = new ServerSocket()) {
            socket.setReuseAddress(true);
            socket.bind(new InetSocketAddress(endpoint.getPort()), 1);
        } catch (IOException e) {
            throw new IOException(
                    "The port of "
                            + peerAddress(endpoint)
                            + " is not bindable in this process (a live listener holds it);"
                            + " refusing to start the metadata server on it.",
                    e);
        }
    }
}
