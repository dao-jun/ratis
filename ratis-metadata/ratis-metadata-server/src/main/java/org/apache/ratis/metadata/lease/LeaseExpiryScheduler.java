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
package org.apache.ratis.metadata.lease;

import org.apache.ratis.metadata.protocol.RaftCommand;
import org.apache.ratis.metadata.protocol.RaftCommandCodec;
import org.apache.ratis.metadata.protocol.RaftResult;
import org.apache.ratis.metadata.statemachine.MetadataStateMachine;
import org.apache.ratis.metadata.util.Clock;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.ratis.metadata.util.Preconditions.checkNotNull;

/** The leader's scheduler proposing one expiry command per due lease through the raft group. */
public final class LeaseExpiryScheduler implements AutoCloseable, MetadataStateMachine.RoleListener {

    private static final Logger LOG = LoggerFactory.getLogger(LeaseExpiryScheduler.class);

    private static final long CLOSE_JOIN_TIMEOUT_MS = 10_000L;

    /**
     * Grace past {@code expireAtMs} before proposing expiry: ttl / 3 (one renewal interval) to
     * bridge one in-flight renewal. The full due-ness slack adds the cross-leader clock-skew
     * tolerance on top (see {@link #SKEW_TOLERANCE_TTL_DIVISOR}).
     */
    private static final long EXPIRY_GRACE_TTL_DIVISOR = 3;

    /**
     * Default cross-leader clock-skew tolerance, in ttl fractions: the leader that sequenced a
     * lease's last renewal stamped {@code expireAtMs} with its wall clock (the envelope's leader
     * sample), while the judging leader reads {@code now} from its own - a successor clock
     * running ahead of its predecessor's reads every stamp that much early. One renewal interval
     * (ttl / 3) unless an absolute override was configured.
     */
    private static final long SKEW_TOLERANCE_TTL_DIVISOR = 3;

    private final RaftServer raftServer;
    private final MetadataStateMachine stateMachine;
    private final RaftGroupId groupId;
    private final RaftPeerId selfId;
    private final Clock clock;
    private final long scanIntervalMs;
    /** Absolute skew tolerance in ms, or null for the per-lease ttl / 3 default. */
    private final Long clockSkewToleranceOverrideMs;

    /** Identifies this scheduler in the server's retry cache; call ids are always fresh. */
    private final ClientId clientId = ClientId.randomId();

    private final AtomicLong callIdSequence = new AtomicLong();

    /** Lease ids with an expiry proposal in flight (one outstanding per lease). */
    private final Set<Long> pendingProposals = ConcurrentHashMap.newKeySet();

    /** Guards {@link #activeLoop} and {@link #closed}; also the wait target of the scan loop. */
    private final Object lock = new Object();

    private volatile boolean closed;
    private ScanLoop activeLoop;

    private final AtomicLong scansCompleted = new AtomicLong();
    private final AtomicLong proposalsSubmitted = new AtomicLong();

    /** Creates a scheduler for one raft group member. */
    public LeaseExpiryScheduler(RaftServer raftServer, MetadataStateMachine stateMachine, RaftGroupId groupId,
            Clock clock, Duration scanInterval, Duration clockSkewTolerance) {
        this.raftServer = checkNotNull(raftServer, "raftServer");
        this.stateMachine = checkNotNull(stateMachine, "stateMachine");
        this.groupId = checkNotNull(groupId, "groupId");
        this.clock = checkNotNull(clock, "clock");
        checkNotNull(scanInterval, "scanInterval");
        // The peer id is stable and is what the proposal's leader field needs.
        this.selfId = raftServer.getId();
        this.scanIntervalMs = scanInterval.toMillis();
        if (this.scanIntervalMs <= 0) {
            throw new IllegalArgumentException(
                    "Lease scan interval must be positive, got " + scanInterval);
        }
        if (clockSkewTolerance != null && clockSkewTolerance.toMillis() < 0) {
            throw new IllegalArgumentException(
                    "Lease clock-skew tolerance must not be negative, got " + clockSkewTolerance);
        }
        this.clockSkewToleranceOverrideMs =
                clockSkewTolerance == null ? null : clockSkewTolerance.toMillis();
    }

    /** Registers for role notifications; starts serving if this member already leads. */
    public void start() {
        synchronized (lock) {
            if (closed) {
                throw new IllegalStateException("LeaseExpiryScheduler is already closed");
            }
            // Register under the lock: a close() racing the start either lands before the
            // check (start throws, nothing registered) or after it (the removal below sees
            // this registration) - never in between, which would leave a closed scheduler's
            // listener attached to the state machine forever.
            stateMachine.addRoleListener(this);
        }
        if (isReadyLeader()) {
            onLeaderReady();
        }
    }

    @Override
    public void onLeaderReady() {
        synchronized (lock) {
            if (closed) {
                return;
            }
            // Replace rather than trust the observed loop: an interleaved deposed-event handler
            // may already be stopping it on another thread, and trusting a doomed loop leaves
            // this member leading with no loop at all. notifyLeaderReady fires once per
            // leadership acquisition, so replacement keeps one loop per member - the stale one
            // drains out on its stop flag.
            final ScanLoop stale = activeLoop;
            if (stale != null) {
                stale.stop();
            }
            activeLoop = new ScanLoop();
            activeLoop.start();
        }
    }

    @Override
    public void onNotLeader() {
        stopActiveLoop("notified not leader");
    }

    @Override
    public void onLeaderChanged(RaftPeerId newLeaderId, boolean selfIsLeader) {
        if (!selfIsLeader) {
            // Belt and braces over onNotLeader: also stop when another member is elected.
            stopActiveLoop("leader changed to " + newLeaderId);
        }
    }

    private void stopActiveLoop(String reason) {
        synchronized (lock) {
            final ScanLoop loop = activeLoop;
            if (loop != null && loop.isRunning()) {
                if (isReadyLeader()) {
                    // Role events interleaved: this member leads the newer term, so the stop
                    // request is stale - a deposed-term handler must not kill the newer loop.
                    return;
                }
                LOG.debug("{}: stopping lease expiry scan loop ({})", selfId, reason);
                loop.stop();
            }
        }
    }

    /**
     * Whether this member is currently the ready leader of the group - probed fresh because a
     * role notification may have been overtaken by a newer term's events. A failed probe means
     * "not the leader now"; later transitions arrive via notifications.
     */
    private boolean isReadyLeader() {
        try {
            final RaftServer.Division division = raftServer.getDivision(groupId);
            if (division == null) {
                return false;
            }
            final DivisionInfo info = division.getInfo();
            return info.isLeader() && info.isLeaderReady();
        } catch (IOException | RuntimeException e) {
            LOG.debug("{}: leadership probe failed", selfId, e);
            return false;
        }
    }

    /** Stops the scan loop, joins it, and detaches from the state machine; idempotent. */
    @Override
    public void close() {
        final ScanLoop loop;
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            loop = activeLoop;
        }
        // Detach first so a ratis notification racing the close finds no listener to wake.
        stateMachine.removeRoleListener(this);
        if (loop != null) {
            loop.stop();
            loop.joinQuietly();
        }
    }

    // --- The scan ---

    private void scanOnce() {
        final long now = clock.milliseconds();
        // Page through the whole table: a capped listing would strand leases behind one page.
        long startAfterLeaseId = 0L;
        while (true) {
            final RaftResult page = listLeases(startAfterLeaseId);
            if (page == null) {
                return;
            }
            for (RaftResult.Lease lease : page.getLeases()) {
                if (now >= lease.getExpireAtMs() + expiryGraceMs(lease) + clockSkewToleranceMs(lease)) {
                    proposeExpiry(lease);
                }
            }
            // An empty page cannot advance the cursor; the guard keeps a protocol bug from
            // spinning.
            if (!page.hasMore() || page.getLeases().isEmpty()) {
                return;
            }
            startAfterLeaseId = page.getLeases().get(page.getLeases().size() - 1).getLeaseId();
        }
    }

    private static long expiryGraceMs(RaftResult.Lease lease) {
        return lease.getTtlMs() / EXPIRY_GRACE_TTL_DIVISOR;
    }

    /**
     * The due-ness slack covering cross-leader wall-clock skew: the stamping and judging leaders
     * read different clocks, so a successor ahead of its predecessor by less than this must not
     * judge a healthy lease due.
     */
    private long clockSkewToleranceMs(RaftResult.Lease lease) {
        return clockSkewToleranceOverrideMs != null ? clockSkewToleranceOverrideMs
                : lease.getTtlMs() / SKEW_TOLERANCE_TTL_DIVISOR;
    }

    /** Reads one lease page via the query path; {@code null} means "skip this scan". */
    private RaftResult listLeases(long startAfterLeaseId) {
        final byte[] reply;
        try {
            final CompletableFuture<Message> future = stateMachine.query(Message.valueOf(ByteBuffer.wrap(
                    RaftCommandCodec.encodeCommand(RaftCommand.leaseList(startAfterLeaseId, 0L)))));
            // The state machine completes queries synchronously (also failures), so this join
            // cannot hang.
            reply = future.join().getContent().toByteArray();
        } catch (RuntimeException e) {
            LOG.debug("{}: lease listing failed, skipping this scan", selfId, e);
            return null;
        }
        final RaftResult result = RaftCommandCodec.decodeResult(reply);
        if (result == null || result.getStatus() != RaftResult.Status.OK) {
            LOG.warn("{}: unexpected lease listing reply {}", selfId, result);
            return null;
        }
        return result;
    }

    private void proposeExpiry(RaftResult.Lease lease) {
        final long callId = callIdSequence.incrementAndGet();
        if (!pendingProposals.add(lease.getLeaseId())) {
            return;
        }
        final RaftCommand command = RaftCommand.leaseExpire(lease.getLeaseId(), lease.getLastRenewIndex());
        final RaftClientRequest request = RaftClientRequest.newBuilder()
                        .setClientId(clientId)
                        .setLeaderId(selfId)
                        .setGroupId(groupId)
                        .setCallId(callId)
                        .setMessage(Message.valueOf(ByteBuffer.wrap(RaftCommandCodec.encodeCommand(command))))
                        .setType(RaftClientRequest.writeRequestType())
                        .build();
        proposalsSubmitted.incrementAndGet();
        LOG.debug("{}: proposing expiry of lease {} (expireAt {}, lastRenewIndex {})", selfId, lease.getLeaseId(),
                lease.getExpireAtMs(), lease.getLastRenewIndex());
        try {
            raftServer.submitClientRequestAsync(request)
                    .whenComplete((reply, error) -> onProposalReply(lease.getLeaseId(), reply, error));
        } catch (IOException | RuntimeException e) {
            // e.g. the server is closing and rejects the submission; the proposal is simply gone.
            pendingProposals.remove(lease.getLeaseId());
            LOG.debug("{}: expiry proposal for lease {} not submitted", selfId, lease, e);
        }
    }

    private void onProposalReply(long leaseId, RaftClientReply reply, Throwable error) {
        pendingProposals.remove(leaseId);
        if (error == null && reply != null && reply.isSuccess()) {
            return;
        }
        // Expected during leadership change or a lost renew race; the new leader's scan re-decides.
        LOG.debug("{}: expiry proposal for lease {} did not apply ({})", selfId, leaseId,
                error != null ? error : reply.getException());
    }

    // --- Observability ---

    /** How many expiry proposals this scheduler submitted (leader or not). */
    @VisibleForTesting
    public long proposalsSubmitted() {
        return proposalsSubmitted.get();
    }

    /** How many scan cycles this scheduler completed. */
    @VisibleForTesting
    public long scansCompleted() {
        return scansCompleted.get();
    }

    /** Whether a scan loop is currently running on this member. */
    @VisibleForTesting
    public boolean isScanLoopRunning() {
        synchronized (lock) {
            return activeLoop != null && activeLoop.isRunning();
        }
    }

    // --- The scan loop ---

    /** One loop per leadership term; per-loop stop flag prevents resurrecting a draining loop. */
    private final class ScanLoop {

        private final Thread thread;
        private volatile boolean stopRequested;

        ScanLoop() {
            thread = new Thread(this::run, "lease-expiry-scheduler-" + selfId);
            thread.setDaemon(true);
        }

        void start() {
            thread.start();
        }

        void stop() {
            stopRequested = true;
            synchronized (lock) {
                lock.notifyAll();
            }
        }

        boolean isRunning() {
            return thread.isAlive() && !stopRequested;
        }

        void joinQuietly() {
            try {
                thread.join(CLOSE_JOIN_TIMEOUT_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (thread.isAlive()) {
                // The loop blocks for at most one scan and one interval wait, so this indicates a
                // bug.
                LOG.warn("{}: lease expiry scan loop did not stop within {}ms", selfId, CLOSE_JOIN_TIMEOUT_MS);
            }
        }

        private void run() {
            while (!stopRequested && !closed) {
                try {
                    scanOnce();
                    scansCompleted.incrementAndGet();
                } catch (Throwable t) {
                    // The scan contains its own expected failures; whatever still escapes
                    // is a bug. One bug must not end expiry for the rest of the term:
                    // surface it and keep looping - the interval wait below paces the
                    // retries.
                    LOG.error("{}: lease expiry scan failed unexpectedly", selfId, t);
                }
                synchronized (lock) {
                    if (stopRequested || closed) {
                        return;
                    }
                    try {
                        lock.wait(scanIntervalMs);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }
    }
}
