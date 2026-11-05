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
package org.apache.ratis.metadata.client;

import org.apache.ratis.metadata.statemachine.ChangeEvent;
import org.apache.ratis.metadata.statemachine.RaftCommand;
import org.apache.ratis.metadata.statemachine.RaftCommandCodec;
import org.apache.ratis.metadata.statemachine.RaftResult;
import org.apache.ratis.metadata.util.AsyncDriver;
import org.apache.ratis.metadata.util.Clock;
import org.apache.ratis.metadata.util.ExceptionUtils;
import org.apache.ratis.metadata.util.NamedThreadFactory;
import org.apache.ratis.metadata.util.SystemClock;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.client.retry.RequestTypeDependentRetryPolicy;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto.TypeCase;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.protocol.exceptions.RaftRetryFailureException;
import org.apache.ratis.retry.ExponentialBackoffRetry;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.ratis.thirdparty.com.google.common.base.Throwables;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongFunction;
import java.util.function.Supplier;

import static org.apache.ratis.metadata.util.Preconditions.checkArgument;
import static org.apache.ratis.metadata.util.Preconditions.checkNotNull;
import static org.apache.ratis.metadata.util.Preconditions.checkState;

/**
 * Typed client of the raft metadata store: one method per {@link RaftCommand}, each with a {@code
 * *Async} twin. The twins never throw: every failure, including invalid arguments and use after
 * {@link #close()}, settles the returned future.
 *
 * <p>Threads: replies settle on a small client-owned executor, and handlers attached to the
 * futures returned by the {@code *Async} twins run on those same threads - never block in such a
 * handler: two blocked ones starve every settle of this client, lease renewals included. Watcher
 * callbacks that set no executor run on the client's separate delivery pool instead.
 */
public final class RaftMetadataClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RaftMetadataClient.class);

    /** Default of the per-attempt request timeout; same as ratis' own client default. */
    private static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(3);

    static final Duration DEFAULT_RETRY_TIMEOUT = Duration.ofHours(24);

    /** The first backoff between failed attempts, doubled with jitter on every retry. */
    private static final Duration RETRY_BACKOFF_BASE = Duration.ofMillis(200);

    /** The backoff cap, so a long outage spaces its retries at a sane cadence. */
    private static final Duration RETRY_BACKOFF_MAX = Duration.ofSeconds(5);

    /**
     * Floors of the renewal pacing, so a sub-second sizing ttl degrades to a sane rpc cadence
     * instead of a busy loop; see {@link #renewalAttemptTimeoutMs(long, long)}.
     */
    private static final long MIN_RENEWAL_ATTEMPT_MS = 250;

    private static final long MIN_RENEWAL_BACKOFF_MS = 50;

    /** A dispatch budget shorter than this buys nothing: one floored attempt barely fits. */
    private static final long MIN_RENEWAL_BUDGET_MS = 1_000;

    /** Resize only when the live minimum halves or doubles: no whipsaw on cohort changes. */
    private static final int RENEWAL_SIZE_HYSTERESIS = 2;

    /** Threads settling async replies off the transport threads; see {@link #sendAsync}. */
    private static final int ASYNC_COMPLETION_THREADS = 2;

    /**
     * Threads of the client's delivery pool - where this client's default-executor {@link
     * MetadataWatcher} callbacks run. Sized above two so one parked callback cannot even delay
     * another; the pool's threads spawn only as concurrent deliveries demand them.
     */
    private static final int DELIVERY_THREADS =
            Math.max(2, Runtime.getRuntime().availableProcessors() / 2);

    /** Sends beyond this bound never enter the queue: the pipeline is wedged or closing. */
    private static final int ASYNC_SEND_QUEUE_CAPACITY = 256;

    /**
     * Non-lease sends are admitted only below this queue depth, keeping a reservation for the
     * lease lifecycle (register/renew/expire): on a saturated pipeline a renewal waits only for
     * the sends queued ahead of it, so the bound is the worst renewal delay the queue can add
     * (~192 sends; ~2 s at 100 op/s). 192 also admits MetadataBenchmark's deepest window
     * (256 in-flight minus ratis' 100 permits = 156 parked locally) without fast-fails.
     */
    private static final int ASYNC_SEND_QUEUE_NON_LEASE_LIMIT = 192;

    /** Grace before close()'s drain fallback: twice the default per-attempt request timeout. */
    private static final Duration ASYNC_SEND_DRAIN_GRACE = Duration.ofSeconds(6);

    /**
     * The floor of the recovery quiet period: one rebuild of a poisoned async pipeline per
     * window at most, and long enough a grace for a replaced client's in-flight sync attempts
     * to keep their per-attempt lifetime. The builder raises it to twice the request timeout.
     */
    private static final Duration RECOVERY_QUIET_PERIOD_MIN = Duration.ofSeconds(5);

    /**
     * The page size of the whole-prefix enumerations ({@link #list}, {@link #listAsync}): one LIST
     * reply then carries at most this many paths. The worst legal path is 8192 chars of 3-byte
     * UTF-8 (surrogates and U+FFF0 up are prohibited, so 3 bytes/char is the maximum) plus ~13
     * framing bytes ~= 24.6 KB/entry, so a page stays under ~25.2 MB - ~2.5x headroom under the
     * grpc 64 MB default message cap (ratis-netty sets no cap). The prefix as a whole may not
     * fit a reply, so the enumerations chain pages through continuation cursors instead of
     * asking for all of it at once. Its own literal on purpose - NOT an alias of {@link
     * RaftCommandCodec#DEFAULT_MAX_LIST_RESULTS}, whose meaning is the raw surface's default page
     * and is free to move: raise this only after redoing the byte arithmetic above (2048 would
     * leave just ~1.3x headroom at the worst-case path).
     */
    private static final long LIST_PAGE_RESULTS = 1024;

    /**
     * The raft client of the general pipeline, replaceable: ratis' ordered async window keeps
     * the failure of one exhausted retry budget (or of any non-retryable failure) permanently,
     * so the only recovery is a fresh client - see {@link #recoverPipelineIfPoisoned}.
     */
    private final AtomicReference<RaftClient> raftClient;

    /** Builds one raft client exactly as configured; null when an injected client cannot be rebuilt. */
    private final Supplier<RaftClient> raftClientFactory;

    /** Single-flight guard: the first detection rebuilds, concurrent detections return. */
    private final AtomicBoolean recoveryInFlight = new AtomicBoolean();

    /**
     * When the last swap happened (client clock); rebuilds inside the quiet period are skipped.
     * Starts one quiet period in the past, so the first detection is never suppressed.
     */
    private volatile long lastRebuildNanos;

    /**
     * The recovery quiet period, doubling as the rebuild cool-down AND the grace before a
     * replaced client is closed: in-flight sync attempts of that client keep their
     * per-attempt lifetime through it; {@code >= 1 ms}.
     */
    private final long recoveryQuietPeriodNanos;

    /** Lazily created; closes abandoned raft clients off every completion and scheduler thread. */
    private volatile ScheduledExecutorService pipelineCloser;

    private final Clock clock;
    private final ExecutorService completionExecutor;

    /** Issues every async send, FIFO - the only thread allowed to park in ratis' backpressure. */
    private final ThreadPoolExecutor sender;

    /**
     * Sessions opened here; closed with the client (LeaseSession detaches on close); package
     * private for the session's own detach.
     */
    @SuppressWarnings("checkstyle:VisibilityModifier")
    final CopyOnWriteArrayList<LeaseSession> sessions = new CopyOnWriteArrayList<>();

    /** How often the renewal ticks look for due sessions. */
    private static final long SESSION_TICK_MS = 50;

    /** Guards {@link #sessionTick} and the closed-check of session registration. */
    private final Object sessionState = new Object();

    /** The shared-scheduler tick driving due-session renewals; live while any session is here. */
    private ScheduledFuture<?> sessionTick;

    /** Where default-executor watcher callbacks run; created on first use, closed with the client. */
    private ExecutorService deliveryExecutor;

    /**
     * Attaches the session to the renewal ticks and takes it into {@link #sessions}; a session
     * of an already-closed client is marked dead instead - nothing of it will ever renew.
     */
    void registerSession(LeaseSession session) {
        synchronized (sessionState) {
            if (closed) {
                session.onDeadClient();
                return;
            }
            sessions.add(session);
            resizeRenewalClientLocked();
            if (sessionTick == null) {
                sessionTick = AsyncDriver.scheduleAtFixedRate(this::tickSessions, SESSION_TICK_MS);
            }
        }
    }

    /** Detaches the session; the ticks stop once the last one is gone. */
    void unregisterSession(LeaseSession session) {
        synchronized (sessionState) {
            sessions.remove(session);
            resizeRenewalClientLocked();
            if (sessions.isEmpty() && sessionTick != null) {
                sessionTick.cancel(false);
                sessionTick = null;
            }
        }
    }

    /**
     * Keeps the renewal client sized by the minimum ttl of the live cohort, with 2x hysteresis
     * both ways: a newly urgent session tightens the pacing; the pacing relaxes only once the
     * cohort's minimum has doubled, so the sizing tracks the sessions that remain without
     * whipsawing - long-ttl cohorts never ride a give-up-churn budget a departed short-ttl
     * session left behind. A mint failure keeps the previous client (or none) - renewals then
     * surface as transport trouble until a later resize mints one; nothing here throws.
     */
    private void resizeRenewalClientLocked() {
        long min = Long.MAX_VALUE;
        for (LeaseSession session : sessions) {
            min = Math.min(min, session.ttlMs());
        }
        if (min == Long.MAX_VALUE || renewalClientTtlMs == min) {
            return;
        }
        final boolean shrink =
                renewalClient != null && min <= renewalClientTtlMs / RENEWAL_SIZE_HYSTERESIS;
        final boolean grow =
                renewalClient != null && renewalClientTtlMs <= min / RENEWAL_SIZE_HYSTERESIS;
        if (renewalClient == null || shrink || grow) {
            final RaftClient minted = mintRenewalClient(min);
            if (minted != null) {
                final RaftClient spent = renewalClient;
                renewalClient = minted;
                renewalClientTtlMs = min;
                closeRenewalClientAsync(spent);
            }
        }
    }

    /** A minted client, or null when the factory failed - never an exception to the caller. */
    private RaftClient mintRenewalClient(long ttlMs) {
        try {
            return renewalClientFactory.apply(ttlMs);
        } catch (RuntimeException e) {
            LOG.warn("Minting the ttl-sized renewal client for ttl {}ms failed", ttlMs, e);
            return null;
        }
    }

    /**
     * Where this client's default-executor {@link MetadataWatcher} callbacks run - deliberately
     * NOT {@link #completionExecutor}: its two threads settle every async reply of this client,
     * so listener callbacks parked there starve the settles, and a lease renewal whose reply
     * never settles latches {@code LeaseSession#renewing} until the lease expires server-side.
     * One pool per client, shared by its default watchers and closed with the client: a blocked
     * callback delays only other callbacks of the SAME client, never any reply.
     */
    ExecutorService deliveryExecutor() {
        synchronized (sessionState) {
            checkNotClosed();
            if (deliveryExecutor == null) {
                deliveryExecutor =
                        Executors.newFixedThreadPool(
                                DELIVERY_THREADS,
                                new NamedThreadFactory("raft-metadata-delivery"));
            }
            return deliveryExecutor;
        }
    }

    private void tickSessions() {
        // A snapshot iteration: a session detaching mid-tick is at worst one wasted due-check.
        // One session's failure must never cancel this fixed-rate task - an uncaught throwable
        // would suppress every later execution and silently stop every renewal of this client -
        // so each tick is isolated.
        for (LeaseSession session : sessions) {
            try {
                session.tick();
            } catch (Throwable t) {
                LOG.error("Renewal tick of {} failed", session, t);
            }
        }
    }

    private volatile boolean closed;

    /** Mints the lease renewals' own raft client, sized for the given ttl (ms). */
    private final LongFunction<RaftClient> renewalClientFactory;

    /**
     * The renewals' dedicated client and the ttl (ms) it was sized by - the minimum among the
     * sessions registered at the last resize; guarded by {@link #sessionState}. Renewals share
     * no semaphore and no sliding window with user operations: through a leader close the
     * general pipeline may park them far past the lease's ttl + ttl/3 budget. {@link
     * RaftClient#close()} blocks on the transport (channel shutdown waits), so a spent or
     * resized client is only ever null-ed out under this lock and closed on {@link
     * #renewalCloser} - never on the scheduler or completion threads.
     */
    private RaftClient renewalClient;

    private long renewalClientTtlMs;

    /** Serializes the blocking close of replaced renewal clients off every hot thread. */
    private final ExecutorService renewalCloser;

    /**
     * Wraps an already-built raft client whose renewals reuse it - the stub-test seam; the
     * builder's two-client wiring is the production path.
     */
    @VisibleForTesting
    RaftMetadataClient(RaftClient raftClient, Clock clock) {
        this(raftClient, clock, ttlMs -> raftClient);
    }

    /** As above, with the renewals' client factory - the pipeline seam the unit suites pin. */
    @VisibleForTesting
    RaftMetadataClient(
            RaftClient raftClient, Clock clock, LongFunction<RaftClient> renewalClientFactory) {
        this(
                new AtomicReference<>(checkNotNull(raftClient, "raftClient")),
                null, // injected: a poisoned pipeline cannot be rebuilt here
                renewalClientFactory,
                clock,
                RECOVERY_QUIET_PERIOD_MIN.toMillis());
    }

    /**
     * Owns how to (re)build the raft client, so a dead async pipeline is replaceable; the
     * renewals ride whatever the main pipeline currently is - the recoverable stub seam.
     */
    @VisibleForTesting
    RaftMetadataClient(
            Supplier<RaftClient> raftClientFactory, Clock clock, long quietPeriodMs) {
        this(raftClientFactory, null, clock, quietPeriodMs);
    }

    /** The builder's wiring: a rebuildable main pipeline plus the renewals' own ttl-sized clients. */
    RaftMetadataClient(
            Supplier<RaftClient> raftClientFactory,
            LongFunction<RaftClient> renewalClientFactory,
            Clock clock,
            long quietPeriodMs) {
        this(
                new AtomicReference<>(checkNotNull(raftClientFactory, "raftClientFactory").get()),
                raftClientFactory,
                renewalClientFactory,
                clock,
                quietPeriodMs);
    }

    /**
     * The whole wiring: the raft client in its reference (a null factory means injected - never
     * rebuilt; a null renewal factory means the renewals ride the main pipeline, the stub seams).
     */
    private RaftMetadataClient(
            AtomicReference<RaftClient> raftClient,
            Supplier<RaftClient> raftClientFactory,
            LongFunction<RaftClient> renewalClientFactory,
            Clock clock,
            long quietPeriodMs) {
        this.raftClient = raftClient;
        this.raftClientFactory = raftClientFactory;
        this.renewalClientFactory =
                renewalClientFactory != null ? renewalClientFactory : ttlMs -> raftClient.get();
        this.recoveryQuietPeriodNanos = Math.max(1, quietPeriodMs) * 1_000_000L;
        this.lastRebuildNanos = -this.recoveryQuietPeriodNanos;
        this.clock = clock;
        this.completionExecutor =
                Executors.newFixedThreadPool(
                        ASYNC_COMPLETION_THREADS,
                        new NamedThreadFactory("raft-metadata-async"));
        // One FIFO thread issues every async send: entering ratis' ordered pipeline can park in
        // its outstanding-request semaphore (100 by default), and the issuer - the shared
        // scheduler, a completion thread, a user thread - must never be the one to park. Only
        // this thread, which no component depends on, may. FIFO keeps the pipeline's submission
        // order equal to the issue order - the lock recipe's create-before-revoke builds on it.
        this.sender =
                new ThreadPoolExecutor(
                        1,
                        1,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new ArrayBlockingQueue<>(ASYNC_SEND_QUEUE_CAPACITY),
                        new NamedThreadFactory("raft-metadata-sender"));
        this.renewalCloser =
                Executors.newSingleThreadExecutor(
                        new NamedThreadFactory("raft-metadata-renewal-closer"));
    }

    /** Upserts {@code data} at {@code path}, creating it (version 1) or bumping the version. */
    public long put(String path, byte[] data) {
        return put(path, data, 0L);
    }

    /** Upserts {@code data} at {@code path} as ephemeral, bound to {@code session}'s lease. */
    public long put(String path, byte[] data, LeaseSession session) {
        return put(path, data, leaseIdOf(session));
    }

    /** Async form of {@link #put(String, byte[])}. */
    public CompletableFuture<Long> putAsync(String path, byte[] data) {
        return settleEarly(() -> putAsync(path, data, 0L));
    }

    /** Async form of {@link #put(String, byte[], LeaseSession)}. */
    public CompletableFuture<Long> putAsync(String path, byte[] data, LeaseSession session) {
        return settleEarly(() -> putAsync(path, data, leaseIdOf(session)));
    }

    private long put(String path, byte[] data, long leaseId) {
        RaftCommand command = RaftCommand.put(path, data, leaseId, clock.milliseconds());
        return accepted(command, "put " + path).getVersion();
    }

    private CompletableFuture<Long> putAsync(String path, byte[] data, long leaseId) {
        RaftCommand command = RaftCommand.put(path, data, leaseId, clock.milliseconds());
        return acceptedAsync(command, "put " + path).thenApply(RaftResult::getVersion);
    }

    /** Upserts only if the record's version equals {@code expectedVersion} (0 = absent). */
    public long compareAndPut(String path, byte[] data, long expectedVersion) {
        RaftCommand command =
                RaftCommand.cas(path, data, expectedVersion, 0L, clock.milliseconds());
        return accepted(command, "compare-and-put " + path).getVersion();
    }

    /** Async form of {@link #compareAndPut(String, byte[], long)}. */
    public CompletableFuture<Long> compareAndPutAsync(
            String path, byte[] data, long expectedVersion) {
        return settleEarly(
                () -> {
                    RaftCommand command =
                            RaftCommand.cas(path, data, expectedVersion, 0L, clock.milliseconds());
                    return acceptedAsync(command, "compare-and-put " + path)
                            .thenApply(RaftResult::getVersion);
                });
    }

    /** Creates {@code path}; the parent must exist (the root always does). */
    public String create(String path, byte[] data) {
        return create(path, data, 0L, false);
    }

    /** Creates {@code path} and missing ancestors atomically; only an existing leaf fails. */
    public String create(String path, byte[] data, boolean createParents) {
        return create(path, data, 0L, createParents);
    }

    /** Creates {@code path} as ephemeral bound to {@code session}'s lease; parent must exist. */
    public String create(String path, byte[] data, LeaseSession session) {
        return create(path, data, leaseIdOf(session), false);
    }

    /** Async form of {@link #create(String, byte[])}. */
    public CompletableFuture<String> createAsync(String path, byte[] data) {
        return settleEarly(() -> createAsync(path, data, 0L, false));
    }

    /** Async form of {@link #create(String, byte[], boolean)}. */
    public CompletableFuture<String> createAsync(String path, byte[] data, boolean createParents) {
        return settleEarly(() -> createAsync(path, data, 0L, createParents));
    }

    /** Async form of {@link #create(String, byte[], LeaseSession)}. */
    public CompletableFuture<String> createAsync(String path, byte[] data, LeaseSession session) {
        return settleEarly(() -> createAsync(path, data, leaseIdOf(session), false));
    }

    private String create(String path, byte[] data, long leaseId, boolean createParents) {
        RaftCommand command =
                RaftCommand.create(path, data, false, leaseId, clock.milliseconds(), createParents);
        return accepted(command, "create " + path).getResultPath();
    }

    private CompletableFuture<String> createAsync(
            String path, byte[] data, long leaseId, boolean createParents) {
        RaftCommand command =
                RaftCommand.create(path, data, false, leaseId, clock.milliseconds(), createParents);
        return acceptedAsync(command, "create " + path).thenApply(RaftResult::getResultPath);
    }

    /** Creates below {@code path} with a monotonic zero-padded 10-digit sequence number. */
    public String createSequential(String path, byte[] data) {
        return createSequential(path, data, 0L);
    }

    /** Async form of {@link #createSequential(String, byte[])}. */
    public CompletableFuture<String> createSequentialAsync(String path, byte[] data) {
        return settleEarly(() -> createSequentialAsync(path, data, 0L));
    }

    private String createSequential(String path, byte[] data, long leaseId) {
        RaftCommand command = RaftCommand.create(path, data, true, leaseId, clock.milliseconds());
        return accepted(command, "create-sequential " + path).getResultPath();
    }

    private CompletableFuture<String> createSequentialAsync(
            String path, byte[] data, long leaseId) {
        RaftCommand command = RaftCommand.create(path, data, true, leaseId, clock.milliseconds());
        return acceptedAsync(command, "create-sequential " + path)
                .thenApply(RaftResult::getResultPath);
    }

    /** Deletes {@code path}, refusing when it has children. */
    public long delete(String path) {
        RaftCommand command = RaftCommand.delete(path, false);
        return accepted(command, "delete " + path).getDeletedCount();
    }

    /** Async form of {@link #delete(String)}. */
    public CompletableFuture<Long> deleteAsync(String path) {
        return settleEarly(
                () -> {
                    RaftCommand command = RaftCommand.delete(path, false);
                    return acceptedAsync(command, "delete " + path)
                            .thenApply(RaftResult::getDeletedCount);
                });
    }

    /** Deletes {@code path} and its whole subtree. */
    public long deleteRecursive(String path) {
        RaftCommand command = RaftCommand.delete(path, true);
        return accepted(command, "delete-recursive " + path).getDeletedCount();
    }

    /** Async form of {@link #deleteRecursive(String)}. */
    public CompletableFuture<Long> deleteRecursiveAsync(String path) {
        return settleEarly(
                () -> {
                    RaftCommand command = RaftCommand.delete(path, true);
                    return acceptedAsync(command, "delete-recursive " + path)
                            .thenApply(RaftResult::getDeletedCount);
                });
    }

    /** Applies one batch atomically; the first failure discards the whole batch. */
    public List<RaftResult> multi(List<RaftCommand> ops) {
        return multi(ops, 0L);
    }

    /** Async form of {@link #multi(List)}. */
    public CompletableFuture<List<RaftResult>> multiAsync(List<RaftCommand> ops) {
        return settleEarly(() -> multiAsync(ops, 0L));
    }

    /** Applies the batch under the writer's term; a higher term answers {@code STALE_TERM}. */
    public List<RaftResult> multi(List<RaftCommand> ops, long writerTerm) {
        RaftCommand command = RaftCommand.multi(ops, writerTerm);
        return accepted(command, "multi of " + ops.size() + " ops").getSubResults();
    }

    /** Async form of {@link #multi(List, long)}. */
    public CompletableFuture<List<RaftResult>> multiAsync(List<RaftCommand> ops, long writerTerm) {
        return settleEarly(
                () -> {
                    RaftCommand command = RaftCommand.multi(ops, writerTerm);
                    return acceptedAsync(command, "multi of " + ops.size() + " ops")
                            .thenApply(RaftResult::getSubResults);
                });
    }

    /** Adds {@code delta} (which may be negative) to the named counter. */
    public long increment(String counter, long delta) {
        RaftCommand command = RaftCommand.increment(counter, delta);
        return accepted(command, "increment " + counter).getValue();
    }

    /**
     * Adds {@code delta} to the named counter, guarded by {@code session}'s lease owning the
     * record at {@code counter} when the command applies; rejects with {@code UNKNOWN_LEASE} or
     * {@code STALE_LEASE} (as {@link RaftMetadataException}) and moves no counter otherwise —
     * the fencing-token bump of a lock acquire.
     */
    public long increment(String counter, long delta, LeaseSession session) {
        RaftCommand command = RaftCommand.increment(counter, delta, leaseIdOf(session));
        return accepted(command, "increment " + counter).getValue();
    }

    /** Async form of {@link #increment(String, long)}. */
    public CompletableFuture<Long> incrementAsync(String counter, long delta) {
        return settleEarly(
                () -> {
                    RaftCommand command = RaftCommand.increment(counter, delta);
                    return acceptedAsync(command, "increment " + counter)
                            .thenApply(RaftResult::getValue);
                });
    }

    /** Async form of {@link #increment(String, long, LeaseSession)}. */
    public CompletableFuture<Long> incrementAsync(
            String counter, long delta, LeaseSession session) {
        return settleEarly(
                () -> {
                    RaftCommand command =
                            RaftCommand.increment(counter, delta, leaseIdOf(session));
                    return acceptedAsync(command, "increment " + counter)
                            .thenApply(RaftResult::getValue);
                });
    }

    /** Reads one record with its metadata. */
    public MetadataRecord get(String path) {
        RaftCommand command = RaftCommand.get(path);
        RaftResult result = queried(command, "get " + path);
        return new MetadataRecord(
                result.getData(),
                result.getVersion(),
                result.getLeaseId(),
                result.getCreateTime(),
                result.getModifyTime());
    }

    /** Async form of {@link #get(String)}. */
    public CompletableFuture<MetadataRecord> getAsync(String path) {
        return settleEarly(
                () -> {
                    RaftCommand command = RaftCommand.get(path);
                    return queriedAsync(command, "get " + path)
                            .thenApply(
                                    result ->
                                            new MetadataRecord(
                                                    result.getData(),
                                                    result.getVersion(),
                                                    result.getLeaseId(),
                                                    result.getCreateTime(),
                                                    result.getModifyTime()));
                });
    }

    /** Whether {@code path} exists. */
    public boolean exists(String path) {
        RaftCommand command = RaftCommand.exists(path);
        return queried(command, "exists " + path).isFound();
    }

    /** Async form of {@link #exists(String)}. */
    public CompletableFuture<Boolean> existsAsync(String path) {
        return settleEarly(
                () -> {
                    RaftCommand command = RaftCommand.exists(path);
                    return queriedAsync(command, "exists " + path).thenApply(RaftResult::isFound);
                });
    }

    /**
     * Lists the whole {@code prefix} subtree in lexicographic order. The enumeration is uncapped,
     * but every request is one bounded {@link #listPage} page of {@link #LIST_PAGE_RESULTS} - a
     * prefix beyond one page is chained through continuation cursors, so no single reply grows
     * past the transport's message cap however large the prefix gets.
     *
     * <p>The enumeration reads one snapshot per page: a record written concurrently is returned
     * only if it sorts after the page cursor at the time its page is read - callers needing one
     * point in time must quiesce writes or follow {@link #revision()}/{@link #poll}. Each page is
     * one request carrying its own full retry budget (see {@link Builder#setRetryTimeout}), like
     * any caller-driven {@code listPage} loop.
     */
    public List<RaftResult.Entry> list(String prefix) {
        final List<RaftResult.Entry> entries = new ArrayList<>();
        String startAfter = "";
        while (true) {
            final ListPage page = listPage(prefix, startAfter, LIST_PAGE_RESULTS);
            entries.addAll(page.getEntries());
            // A bounded page reports more while matching records remain past it; an empty page
            // cannot advance the cursor, so it ends the enumeration instead of spinning.
            if (!page.hasMore() || page.getEntries().isEmpty()) {
                return entries;
            }
            startAfter = page.getStartAfter();
        }
    }

    /**
     * Async form of {@link #list(String)} (its per-page snapshot and retry-budget semantics
     * included): the pages of one enumeration chain into one future. Every follow-on page is
     * sent from the shared scheduler - never from a completion thread, where ratis'
     * outstanding-requests backpressure could park one of the two completion threads and stall
     * every async settlement of this client (the watcher's {@code pollNow} rule).
     */
    public CompletableFuture<List<RaftResult.Entry>> listAsync(String prefix) {
        return settleEarly(() -> listNextPageAsync(prefix, "", new ArrayList<>()));
    }

    private CompletableFuture<List<RaftResult.Entry>> listNextPageAsync(
            String prefix, String startAfter, List<RaftResult.Entry> entries) {
        return listPageAsync(prefix, startAfter, LIST_PAGE_RESULTS)
                .thenCompose(
                        page -> {
                            entries.addAll(page.getEntries());
                            // Same termination as the sync loop: more pages follow only while the
                            // cursor advances over non-empty pages.
                            if (!page.hasMore() || page.getEntries().isEmpty()) {
                                return CompletableFuture.completedFuture(entries);
                            }
                            // Keep the next page off this completion thread - the sender rule the
                            // javadoc above states. A client closed mid-chain fails the next send
                            // and settles this chain; a refusing scheduler fails the thenCompose -
                            // the future always settles.
                            final CompletableFuture<List<RaftResult.Entry>> next =
                                    new CompletableFuture<>();
                            AsyncDriver.schedule(
                                    () ->
                                            listNextPageAsync(
                                                            prefix,
                                                            page.getStartAfter(),
                                                            entries)
                                                    .whenComplete(
                                                            (all, failure) -> {
                                                                if (failure != null) {
                                                                    next.completeExceptionally(
                                                                            failure);
                                                                } else {
                                                                    next.complete(all);
                                                                }
                                                            }),
                                    0L);
                            return next;
                        });
    }

    /** Lists at most {@code maxResults} under {@code prefix}, after {@code startAfter}. */
    public ListPage listPage(String prefix, String startAfter, long maxResults) {
        RaftCommand command = RaftCommand.list(prefix, startAfter, maxResults);
        RaftResult result = queried(command, "list " + prefix);
        return new ListPage(result.getEntries(), result.hasMore());
    }

    /** Async form of {@link #listPage(String, String, long)}. */
    public CompletableFuture<ListPage> listPageAsync(
            String prefix, String startAfter, long maxResults) {
        return settleEarly(
                () -> {
                    RaftCommand command = RaftCommand.list(prefix, startAfter, maxResults);
                    return queriedAsync(command, "list " + prefix)
                            .thenApply(
                                    result -> new ListPage(result.getEntries(), result.hasMore()));
                });
    }

    /** The revision (raft log index) the leader has applied - the watermark of {@link #poll}. */
    public long revision() {
        RaftCommand command = RaftCommand.revision();
        return queried(command, "revision").getRevision();
    }

    /** Async form of {@link #revision()}. */
    public CompletableFuture<Long> revisionAsync() {
        return settleEarly(
                () -> {
                    RaftCommand command = RaftCommand.revision();
                    return queriedAsync(command, "revision").thenApply(RaftResult::getRevision);
                });
    }

    /** Change records after {@code sinceRevision} (exclusive), bounded by {@code maxBytes}. */
    public PollResult poll(long sinceRevision, long maxBytes) {
        RaftCommand command = RaftCommand.pollChanges(sinceRevision, maxBytes);
        RaftResult result = queried(command, "poll from " + sinceRevision);
        return new PollResult(
                result.getRevision(), result.getOldestRetainedRevision(), result.getRecords());
    }

    /** Async form of {@link #poll(long, long)}. */
    public CompletableFuture<PollResult> pollAsync(long sinceRevision, long maxBytes) {
        return settleEarly(
                () -> {
                    RaftCommand command = RaftCommand.pollChanges(sinceRevision, maxBytes);
                    return queriedAsync(command, "poll from " + sinceRevision)
                            .thenApply(
                                    result ->
                                            new PollResult(
                                                    result.getRevision(),
                                                    result.getOldestRetainedRevision(),
                                                    result.getRecords()));
                });
    }

    /** Registers a lease and renews it in the background every third of the ttl. */
    public LeaseSession openLease(Duration ttl) {
        long leaseId = newLeaseId();
        long ttlMs = checkedTtlMs(ttl);
        RaftCommand command =
                RaftCommand.leaseRegister(leaseId, ttlMs, clock.milliseconds() + ttlMs);
        accepted(command, "register lease " + leaseId);
        return startSession(leaseId, ttlMs);
    }

    /** Async form of {@link #openLease(Duration)}; the session starts once the lease applies. */
    public CompletableFuture<LeaseSession> openLeaseAsync(Duration ttl) {
        return settleEarly(
                () -> {
                    long leaseId = newLeaseId();
                    long ttlMs = checkedTtlMs(ttl);
                    RaftCommand command =
                            RaftCommand.leaseRegister(leaseId, ttlMs, clock.milliseconds() + ttlMs);
                    return acceptedAsync(command, "register lease " + leaseId)
                            .thenApply(ignored -> startSession(leaseId, ttlMs));
                });
    }

    private static long checkedTtlMs(Duration ttl) {
        checkNotNull(ttl, "ttl");
        long ttlMs = ttl.toMillis();
        checkArgument(ttlMs > 0, "Lease ttl must be positive, got %s", ttl);
        return ttlMs;
    }

    private LeaseSession startSession(long leaseId, long ttlMs) {
        LeaseSession session = new LeaseSession(this, leaseId, ttlMs, clock);
        session.start();
        return session;
    }

    /**
     * Renews a lease with this ttl; the leader sets the replicated expiry from its own time.
     * Manual renewals ride the general ordered pipeline with the client-wide pacing - for
     * ttl-critical liveness use a session from {@link #openLease(Duration)}, whose renewals
     * ride the dedicated ttl-sized renewal pipeline.
     */
    public long renewLease(long leaseId, Duration ttl) {
        long ttlMs = checkedTtlMs(ttl);
        RaftCommand command =
                RaftCommand.leaseRenew(leaseId, ttlMs, clock.milliseconds() + ttlMs);
        return accepted(command, "renew lease " + leaseId).getRevision();
    }

    /** Async form of {@link #renewLease(long, Duration)}. */
    public CompletableFuture<Long> renewLeaseAsync(long leaseId, Duration ttl) {
        return settleEarly(
                () -> {
                    long ttlMs = checkedTtlMs(ttl);
                    RaftCommand command =
                            RaftCommand.leaseRenew(leaseId, ttlMs, clock.milliseconds() + ttlMs);
                    return acceptedAsync(command, "renew lease " + leaseId)
                            .thenApply(RaftResult::getRevision);
                });
    }

    /**
     * One session renewal on the renewals' own ttl-sized pipeline: per-attempt timeout and
     * backoff derived from the cohort's most urgent live ttl, a dispatch budget of that ttl
     * plus the ttl/3 expiry grace, and no queueing behind user operations - so a leader
     * failover resolving inside the ttl is transparent, as the {@link LeaseSession} contract
     * promises. A renewal client that exhausted its budget (or whose window the exhaustion
     * poisoned) is dropped here and reopened by a later tick: a session must die only of
     * {@code UNKNOWN_LEASE}, never of its own retry machinery - except when the dead pipeline
     * can never be reopened (a renewal riding the main pipeline of a client that cannot
     * rebuild it): there is no way back, so the poison surfaces and the session stops
     * terminally instead of retrying a pipeline whose every mint is the same dead client.
     * Never throws: synchronous failures settle the returned future, exactly like every
     * public async twin.
     */
    CompletableFuture<Long> renewLeaseForSessionAsync(long leaseId, long ttlMs) {
        return settleEarly(
                () -> {
                    // A close race surfaces as IllegalStateException: the session stops, as before.
                    checkNotClosed();
                    final RaftClient target = renewalClientFor();
                    if (target == null) {
                        throw new MetadataTransportException(
                                "No renewal pipeline available for lease " + leaseId);
                    }
                    RaftCommand command =
                            RaftCommand.leaseRenew(leaseId, ttlMs, clock.milliseconds() + ttlMs);
                    return sendAsync(target, command, false, "renew lease " + leaseId)
                            .thenApply(RaftResult::getRevision)
                            .handle(
                                    (revision, failure) -> {
                                        if (failure == null) {
                                            return revision;
                                        }
                                        final Throwable cause =
                                                ExceptionUtils.stripCompletionException(failure);
                                        if (!renewalPipelineSpent(cause)) {
                                            // UNKNOWN_LEASE, real client closes and business
                                            // failures pass through with the twins' usual shape.
                                            if (cause instanceof RuntimeException) {
                                                throw (RuntimeException) cause;
                                            }
                                            throw new MetadataTransportException(
                                                    "Metadata LEASE_RENEW failed: " + cause, cause);
                                        }
                                        final AlreadyClosedException poison =
                                                poisonedWindowOf(cause);
                                        if (poison != null
                                                && target == raftClient.get()
                                                && !canRebuildRaftClient()) {
                                            // The no-way-back case of the javadoc above: surface
                                            // the poison so the session stops terminally.
                                            throw new MetadataTransportException(
                                                    "Metadata LEASE_RENEW failed: the pipeline"
                                                            + " of lease "
                                                            + leaseId
                                                            + " is permanently dead and cannot"
                                                            + " be rebuilt",
                                                    poison);
                                        }
                                        discardRenewalClient(target);
                                        Throwable root = Throwables.getRootCause(cause);
                                        if (root instanceof AlreadyClosedException) {
                                            root = null;
                                        }
                                        // Never hand the session an AlreadyClosedException as
                                        // the direct cause: onRenewalFailure would read it as
                                        // "client closed" and stop the session for good.
                                        throw new MetadataTransportException(
                                                "The ttl-sized renewal pipeline for lease "
                                                        + leaseId
                                                        + " gave up; reopening it on a later tick",
                                                root);
                                    });
                });
    }

    /** Whether the failure says the renewal client itself is spent while this client stays open. */
    private boolean renewalPipelineSpent(Throwable failure) {
        if (closed) {
            return false; // a real close of THIS client, not a poisoned renewal window
        }
        // The ttl-sized budget ran out, or its sliding window's permanent fail-fast poison.
        return ExceptionUtils.causedBy(failure, RaftRetryFailureException.class)
                || ExceptionUtils.causedBy(failure, AlreadyClosedException.class);
    }

    /**
     * The sliding-window poison in this failure's cause chain, or null: the sticky failure of
     * one exhausted budget answers every later send with an {@link AlreadyClosedException} wrap.
     */
    private static AlreadyClosedException poisonedWindowOf(Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (t instanceof AlreadyClosedException) {
                return (AlreadyClosedException) t;
            }
        }
        return null;
    }

    /** The serving renewal client, minted lazily if no resize has; never throws. */
    private RaftClient renewalClientFor() {
        synchronized (sessionState) {
            if (closed) {
                return null;
            }
            if (renewalClient == null) {
                resizeRenewalClientLocked();
            }
            return renewalClient;
        }
    }

    /** Drops a spent renewal client; the close itself is detached - see {@link #renewalCloser}. */
    private void discardRenewalClient(RaftClient spent) {
        synchronized (sessionState) {
            if (renewalClient != spent) {
                return; // already resized, discarded or swept underneath
            }
            renewalClient = null;
            renewalClientTtlMs = 0;
        }
        closeRenewalClientAsync(spent);
    }

    /** {@link RaftClient#close()} blocks on channel shutdown: keep it off every hot thread. */
    private void closeRenewalClientAsync(RaftClient client) {
        if (client == null || client == raftClient.get()) {
            return; // the stub seam reuses the injected client for renewals
        }
        try {
            renewalCloser.execute(
                    () -> {
                        try {
                            client.close();
                        } catch (IOException e) {
                            LOG.debug("Closing a spent lease-renewal client failed", e);
                        }
                    });
        } catch (RejectedExecutionException closing) {
            // Only during this client's own close(): the user thread already blocks on
            // raftClient.close() there, so an inline close of the straggler is acceptable.
            try {
                client.close();
            } catch (IOException e) {
                LOG.debug("Closing a spent lease-renewal client failed", e);
            }
        }
    }

    /**
     * Revokes the lease now, unconditionally: the leader deletes the lease and every record bound
     * to it - and nothing else, which is what makes revocation ownership-safe where a path delete
     * is not. Idempotent: revoking an unknown or already-expired lease is a no-op success
     * settling with 0 deletions. Runs on the ordered async pipeline like every other send, so a
     * revocation issued after a create of the same lease is guaranteed to apply after it - except
     * across an async-pipeline recovery (see {@link #sendAsync}): the recovery abandons the
     * replaced pipeline's in-flight calls (their callers see {@link MetadataTransportException}),
     * and a delivery of such a call already on the wire is unordered against the replacement
     * pipeline, so it may commit after the revocation. Callers revoking around a long outage
     * should confirm the create's outcome first.
     *
     * @return how many bound records the expiry cascade removed
     */
    public CompletableFuture<Long> revokeLeaseAsync(long leaseId) {
        return settleEarly(
                () -> {
                    RaftCommand command = RaftCommand.leaseExpire(leaseId, -1L);
                    return acceptedAsync(command, "revoke lease " + leaseId)
                            .thenApply(RaftResult::getDeletedCount);
                });
    }

    /** A fresh, non-zero lease id; the state machine requires non-zero ids. */
    private static long newLeaseId() {
        return ThreadLocalRandom.current().nextLong(1L, Long.MAX_VALUE);
    }

    private static long leaseIdOf(LeaseSession session) {
        checkNotNull(session, "session");
        return session.getLeaseId();
    }

    /**
     * Closes the lease sessions, then the raft client; close watchers and in-flight ops first.
     * The closed flag flips under {@link #sessionState} - a session registering concurrently
     * either joins before the sweep or is turned away dead, never left attached to a closed
     * client with the tick already cancelled.
     */
    @Override
    public void close() {
        List<LeaseSession> toClose;
        ExecutorService delivery;
        synchronized (sessionState) {
            if (closed) {
                return;
            }
            closed = true;
            toClose = new ArrayList<>(sessions);
            sessions.clear();
            if (sessionTick != null) {
                sessionTick.cancel(false);
                sessionTick = null;
            }
            delivery = deliveryExecutor;
        }
        for (LeaseSession session : toClose) {
            session.close();
        }
        // Whatever the reference currently holds - a recovery may have replaced the client this
        // client was built with; the user thread accepted a blocking close here, as ever.
        closeQuietly(raftClient.get());
        // The renewals' own client goes too; its close blocks on the transport, but the user
        // thread already accepted blocking on raftClient.close() just above.
        final RaftClient renewals;
        synchronized (sessionState) {
            renewals = renewalClient;
            renewalClient = null;
        }
        if (renewals != null && renewals != raftClient) {
            closeQuietly(renewals);
        }
        renewalCloser.shutdown();
        final ScheduledExecutorService closer = pipelineCloser;
        if (closer != null) {
            // Queued delayed closes of replaced clients still drain on the daemon thread after
            // shutdown(); only new submissions would be refused, and close() is final.
            closer.shutdown();
        }
        // Closing the raft client fails the pending replies; let their completions drain.
        completionExecutor.shutdown();
        if (delivery != null) {
            // Queued units may still run; daemon threads never block JVM exit.
            delivery.shutdown();
        }
        // A transport that fails in-flight calls on close lets the queued sends drain through
        // ratis and settle fast. One that does not would leave the worker parked in ratis'
        // backpressure and the queue unsettled forever - so after a grace period, interrupt the
        // worker (the acquire is interruptible) and settle every still-queued future directly:
        // use after close must always settle the future, and close() itself never blocks.
        sender.shutdown();
        AsyncDriver.schedule(this::settleStrandedSends, ASYNC_SEND_DRAIN_GRACE.toMillis());
    }

    /** Whether {@link #close()} has run; components with own retry loops stop on it. */
    public boolean isClosed() {
        return closed;
    }

    /**
     * Whether a dead async pipeline can be replaced by a fresh raft client: the builder's
     * clients can (and are), an injected one cannot. The session gate reads this to tell a
     * poisoned window with a way back from one without.
     */
    boolean canRebuildRaftClient() {
        return raftClientFactory != null;
    }

    private void checkNotClosed() {
        checkState(!closed, "RaftMetadataClient is already closed");
    }

    private RaftResult accepted(RaftCommand command, String what) {
        return map(send(command, false), command, what);
    }

    private RaftResult queried(RaftCommand command, String what) {
        return map(send(command, true), command, what);
    }

    private CompletableFuture<RaftResult> acceptedAsync(RaftCommand command, String what) {
        return sendAsync(raftClient.get(), command, false, what);
    }

    private CompletableFuture<RaftResult> queriedAsync(RaftCommand command, String what) {
        return sendAsync(raftClient.get(), command, true, what);
    }

    /**
     * Runs an async twin's synchronous part. Twins never throw: invalid arguments, a closed client,
     * anything failing before the send settles the returned future instead.
     */
    private static <T> CompletableFuture<T> settleEarly(Supplier<CompletableFuture<T>> call) {
        try {
            return call.get();
        } catch (RuntimeException e) {
            return JavaUtils.completeExceptionally(e);
        }
    }

    private RaftResult send(RaftCommand command, boolean readOnly) {
        checkNotClosed();
        final Message message =
                Message.valueOf(ByteBuffer.wrap(RaftCommandCodec.encodeCommand(command)));
        // The local capture keeps an in-flight sync attempt on the client it started with, so a
        // concurrent pipeline recovery cannot cut its retry budget short: the io() surface never
        // touches the poisoned window, and the replaced client stays open through the grace.
        final RaftClient client = raftClient.get();
        final RaftClientReply reply;
        try {
            reply =
                    readOnly
                            ? client.io().sendReadOnly(message)
                            : client.io().send(message);
        } catch (IOException e) {
            throw translate(e, command, !readOnly);
        }
        return toResult(reply, command);
    }

    /**
     * Sends on ratis' ordered async pipeline - without ever blocking the caller. The send is
     * enqueued on the client's single {@link #sender} thread: entering the pipeline can park in
     * the outstanding-request semaphore (ratis' backpressure), and the issuer may be the
     * process-wide scheduler or a completion thread, which one parked send must never freeze.
     * This is the only code in the module allowed to touch a raft client - every async twin
     * funnels here with the general client, session renewals with the ttl-sized {@link
     * #renewalClient}; a new direct {@code raftClient.async()} call site would reintroduce the
     * frozen-scheduler defect, see {@link AsyncDriver}'s contract.
     *
     * <p>A call that returns has enqueued the send, not yet issued it. A full queue (a wedged
     * pipeline, or a backlog past the non-lease admission bound) settles the future fast with a
     * {@link MetadataTransportException} - the documented "the request never went through"
     * family, safe to retry. Lease-lifecycle commands (register/renew/expire) keep a reserved
     * share of the queue, so saturation delays them by at most the sends already ahead - but a
     * saturated pipeline still defers renewals by queue depth; keep backlogs shallow on short
     * ttls. Replies settle on the client's completion executor, never on the transport thread.
     * One exhausted retry budget (or any non-retryable failure) permanently poisons ratis' async
     * sliding window - it fails every later send fast with {@link AlreadyClosedException}. While
     * this client is open, the settlement of a failing send detects that signature and replaces
     * the raft client, so later async calls - lease registrations, watcher polls, user traffic -
     * flow again, at most one replacement per quiet period; the call that hit the dead window
     * still fails to its caller. Ordering holds within one pipeline: a call issued before a
     * replacement is not ordered against one issued after it (see {@link #revokeLeaseAsync}).
     * The sync surface never touches the window; an attempt in flight on a replaced client
     * keeps its per-attempt lifetime through the quiet period.
     */
    private CompletableFuture<RaftResult> sendAsync(
            RaftClient target, RaftCommand command, boolean readOnly, String what) {
        checkNotClosed();
        final Message message =
                Message.valueOf(ByteBuffer.wrap(RaftCommandCodec.encodeCommand(command)));
        final CompletableFuture<RaftResult> done = new CompletableFuture<>();
        if (!isLeaseCritical(command.getOp())
                && sender.getQueue().size() >= ASYNC_SEND_QUEUE_NON_LEASE_LIMIT) {
            return JavaUtils.completeExceptionally(
                    new MetadataTransportException(
                            "Metadata "
                                    + command.getOp()
                                    + " failed: the send queue is full (a saturated or wedged"
                                    + " pipeline); retry once it drains"));
        }
        try {
            sender.execute(new SendTask(target, message, readOnly, done, command, what));
        } catch (RejectedExecutionException overload) {
            return JavaUtils.completeExceptionally(
                    new MetadataTransportException(
                            "Metadata "
                                    + command.getOp()
                                    + " failed: the send queue is full or the client is closing",
                            overload));
        }
        return done;
    }

    /** The lease lifecycle must keep running through a saturated queue: it holds the sessions. */
    private static boolean isLeaseCritical(RaftCommand.Op op) {
        return op == RaftCommand.Op.LEASE_REGISTER
                || op == RaftCommand.Op.LEASE_RENEW
                || op == RaftCommand.Op.LEASE_EXPIRE;
    }

    /** One queued send; typed so close()'s drain can settle futures the queue still holds. */
    private final class SendTask implements Runnable {
        private final RaftClient target;
        private final Message message;
        private final boolean readOnly;
        private final CompletableFuture<RaftResult> done;
        private final RaftCommand command;
        private final String what;

        SendTask(
                RaftClient target,
                Message message,
                boolean readOnly,
                CompletableFuture<RaftResult> done,
                RaftCommand command,
                String what) {
            this.target = target;
            this.message = message;
            this.readOnly = readOnly;
            this.done = done;
            this.command = command;
            this.what = what;
        }

        @Override
        public void run() {
            issue(this);
        }

        /** The close-time drain's settlement: the client closed while this send was stranded. */
        void settleStranded() {
            done.completeExceptionally(
                    new MetadataTransportException(
                            "Metadata "
                                    + command.getOp()
                                    + " failed: the client closed while its pipeline was wedged",
                            new AlreadyClosedException("settled by close()'s drain")));
        }
    }

    /**
     * Issues one send on the sender thread: {@code target.async()} may park here in ratis'
     * outstanding-request backpressure for as long as that pipeline stays wedged, which is
     * acceptable exactly because no other component runs on this thread. A send queued before
     * close() settles the way the raft client answers it - a {@link MetadataTransportException}
     * once the transport reports the close (ratis fails the window with {@link
     * AlreadyClosedException}), or a real reply if the transport still answers; the closed flag
     * is deliberately NOT checked here, so the semantics match an in-flight send racing close().
     * Any failure settles {@code done}; the future must settle even on Errors - and the sender
     * thread must survive.
     */
    private void issue(SendTask task) {
        final CompletableFuture<RaftClientReply> reply;
        try {
            reply =
                    task.readOnly
                            ? task.target.async().sendReadOnly(task.message)
                            : task.target.async().send(task.message);
        } catch (Throwable rejected) {
            task.done.completeExceptionally(translate(rejected, task.command, !task.readOnly));
            return;
        }
        reply.whenComplete(
                (received, failure) -> {
                    final Runnable settle =
                            () -> {
                                if (failure != null) {
                                    // Repair before the failure becomes observable: by the time
                                    // the caller's whenComplete chains run, the swap - if the
                                    // failure warranted one - already happened, so the next
                                    // send lands on the healed pipeline.
                                    recoverPipelineIfPoisoned(failure, task.target);
                                    task.done.completeExceptionally(
                                            translate(failure, task.command, !task.readOnly));
                                    return;
                                }
                                try {
                                    task.done.complete(
                                            map(toResult(received, task.command), task.command, task.what));
                                } catch (Throwable t) {
                                    // The future must settle even on Errors; settle must not throw.
                                    task.done.completeExceptionally(t);
                                }
                            };
                    // A reply failing after close() may find the executor down; settling inline
                    // still beats never settling (only failures can arrive that late).
                    try {
                        completionExecutor.execute(settle);
                    } catch (RejectedExecutionException closing) {
                        settle.run();
                    }
                });
    }

    /**
     * Close()'s bounded drain, scheduled on {@link AsyncDriver} after the grace period: if the
     * sender has not drained by then, its interrupt un-parks the worker from ratis' (interruptible)
     * backpressure acquire and every still-queued send settles as the transport family - no
     * stranded future, and no task silently discarded.
     */
    private void settleStrandedSends() {
        if (sender.isTerminated()) {
            return;
        }
        for (Runnable task : sender.shutdownNow()) {
            ((SendTask) task).settleStranded();
        }
    }

    /**
     * The async pipeline's sticky death: ratis' sliding window keeps the failure of one
     * exhausted retry budget (or of any non-retryable failure) and answers every later send
     * with {@link AlreadyClosedException} - while this client is open, which is what tells the
     * poison apart from a close ({@link #close()} flips {@link #closed} before any close-induced
     * failure can arrive). The window never heals; only a fresh raft client (a fresh client id
     * and window pair on both sides) recovers the async surface.
     */
    private boolean isPoisonedPipeline(Throwable failure) {
        return !closed
                && raftClientFactory != null
                && ExceptionUtils.stripCompletionException(failure)
                        instanceof AlreadyClosedException;
    }

    /**
     * Replaces the raft client whose async window died, so later sends flow again; the
     * triggering call still fails exactly as before. The identity fast-path first: a straggler
     * of an already-replaced client - a late settlement of the poison burst, or a send whose
     * client the grace-period close finally closed - builds nothing. The single-flight guard
     * keeps a burst of concurrent detections to one builder; the losers return instead of
     * parking a completion thread. The cool-down stamps {@link #lastRebuildNanos} from the
     * client's {@link Clock} and caps a persistently re-poisoning root cause at one rebuild per
     * quiet period - sends inside the window keep failing caller-visibly instead of churning
     * clients. And no raft client is ever closed on this (a completion) thread or the shared
     * scheduler: {@link RaftClient#close()} parks in the transport's graceful shutdown, and
     * these threads settle every reply and drive every periodic step - abandoned clients go to
     * {@link #closeOffThread}.
     */
    private void recoverPipelineIfPoisoned(Throwable failure, RaftClient sentTo) {
        if (!isPoisonedPipeline(failure) || raftClient.get() != sentTo) {
            return; // not the poison, or this client was already replaced - nothing to rebuild
        }
        if (!recoveryInFlight.compareAndSet(false, true)) {
            return; // one detector suffices; its swap covers this client - no thread parked
        }
        try {
            if (raftClient.get() != sentTo // lost the race while acquiring the guard
                    || clock.nanoseconds() - lastRebuildNanos < recoveryQuietPeriodNanos) {
                return; // bounded churn: one rebuild per quiet period covers every transient
                // poison; a persistently re-poisoning cause fails sends fast instead
            }
            final RaftClient fresh;
            try {
                fresh = raftClientFactory.get();
            } catch (Throwable buildFailure) {
                LOG.warn(
                        "Rebuilding the raft client failed; the next poisoned send retries",
                        buildFailure);
                return;
            }
            if (!raftClient.compareAndSet(sentTo, fresh)) {
                closeOffThread(fresh, 0L); // a concurrent swap won; the candidate never served
                return;
            }
            lastRebuildNanos = clock.nanoseconds();
            synchronized (sessionState) {
                // close() flips closed under this monitor BEFORE closing the reference: either
                // we see closed and close fresh ourselves, or close()'s subsequent get()
                // observes the swap - no interleaving leaks the new client's channel.
                if (closed) {
                    closeOffThread(fresh, 0L);
                    return;
                }
            }
            LOG.warn(
                    "The async pipeline failed permanently ({}); rebuilt the raft client"
                            + " (no further rebuild for {} ms)",
                    ExceptionUtils.stripCompletionException(failure),
                    recoveryQuietPeriodNanos / 1_000_000L);
            // The replaced client served traffic: its close parks in the transport's graceful
            // shutdown and would cut in-flight sync attempts mid-retry - keep it open for the
            // grace period, then close it off every hot thread.
            closeOffThread(sentTo, recoveryQuietPeriodNanos / 1_000_000L);
        } finally {
            recoveryInFlight.set(false);
        }
    }

    /** Closes an abandoned raft client off every completion and scheduler thread. */
    private void closeOffThread(RaftClient client, long delayMs) {
        ScheduledExecutorService closer = pipelineCloser;
        if (closer == null) {
            synchronized (sessionState) {
                closer = pipelineCloser;
                if (closer == null) {
                    closer =
                            Executors.newSingleThreadScheduledExecutor(
                                    new NamedThreadFactory("raft-metadata-pipeline-close"));
                    pipelineCloser = closer;
                }
            }
        }
        try {
            closer.schedule(() -> closeQuietly(client), delayMs, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException shuttingDown) {
            // Only this client's own close() refuses, and the queued delayed closes still
            // drain on the daemon thread after shutdown(); a queue refusing even that means
            // the JVM is on its way out anyway.
        }
    }

    /** Closes a raft client, quietly: idempotent (ratis closes on a CAS), safe to lose races. */
    private static void closeQuietly(RaftClient client) {
        try {
            client.close();
        } catch (IOException e) {
            LOG.debug("Closing a raft client failed", e);
        }
    }

    /** Decodes the reply; the op echo must match, else the leader speaks a newer protocol. */
    private static RaftResult toResult(RaftClientReply reply, RaftCommand command) {
        final RaftResult result =
                RaftCommandCodec.decodeResult(reply.getMessage().getContent().toByteArray());
        if (result == null || result.getOp() != command.getOp()) {
            throw new UndecodableReplyException(command.getOp());
        }
        return result;
    }

    /**
     * Surfaces a transport-level failure of both {@link #send} and {@link #sendAsync}: a mutation
     * abandoned with its outcome unknown - its own budget exhausted, or the exhaustion cascade
     * through the ordered pipeline - answers {@link PossiblyCommittedException}; everything else
     * stays the transport failure it was.
     */
    private static RuntimeException translate(
            Throwable failure, RaftCommand command, boolean mutation) {
        final Throwable cause = ExceptionUtils.stripCompletionException(failure);
        if (mutation && outcomeUnknown(cause)) {
            return new PossiblyCommittedException(command.getOp(), cause);
        }
        if (cause instanceof IOException) {
            return new MetadataTransportException(
                    "Metadata " + command.getOp() + " failed after retries: " + cause.getMessage(),
                    cause);
        }
        if (cause instanceof RuntimeException) {
            return (RuntimeException) cause;
        }
        return new MetadataTransportException(
                "Metadata " + command.getOp() + " failed: " + cause, cause);
    }

    /**
     * Whether ratis declared this failure an abandoned-with-unknown-outcome mutation: the
     * request's own budget ran out, or the ordered pipeline failed it with another request's
     * budget failure wrapped in an {@link AlreadyClosedException} (ratis fails the exhausted
     * head with the raw {@link RaftRetryFailureException} and its outstanding siblings with
     * that failure wrapped - sent or merely queued alike, hence the safe-side match). A
     * close-time teardown carries {@code AlreadyClosedException} without the budget failure
     * underneath and stays plain.
     */
    private static boolean outcomeUnknown(Throwable cause) {
        return cause instanceof RaftRetryFailureException
                || ExceptionUtils.stripException(cause, AlreadyClosedException.class)
                        instanceof RaftRetryFailureException;
    }

    /** Maps a non-OK status to the exception family; OK passes through. */
    private static RaftResult map(RaftResult result, RaftCommand command, String what) {
        switch (result.getStatus()) {
            case OK:
                return result;
            case NEED_SNAPSHOT:
                throw new NeedSnapshotException(result.getOldestRetainedRevision());
            case MALFORMED:
                throw new MetadataTransportException(
                        "The metadata state machine rejected " + what + " as malformed");
            default:
                String message =
                        what
                                + " failed with "
                                + result.getStatus()
                                + (result.getFailStatus() != null
                                        ? " (sub-command "
                                                + result.getFailIndex()
                                                + ": "
                                                + result.getFailStatus()
                                                + ")"
                                        : "");
                throw new RaftMetadataException(
                        message, result.getStatus(), result.getFailIndex(), result.getFailStatus());
        }
    }

    /**
     * A third of the sizing ttl per attempt, floored and capped by the generic request timeout:
     * three attempts fit every dispatch budget the renewal law derives.
     */
    @VisibleForTesting
    static long renewalAttemptTimeoutMs(long ttlMs, long capMs) {
        return Math.min(Math.max(ttlMs / 3, MIN_RENEWAL_ATTEMPT_MS), capMs);
    }

    /** The renewal backoff start: a sixth of the sizing ttl, floored at the pacing minimum. */
    @VisibleForTesting
    static long renewalBackoffBaseMs(long ttlMs) {
        return Math.max(MIN_RENEWAL_BACKOFF_MS, Math.min(ttlMs / 6, RETRY_BACKOFF_BASE.toMillis()));
    }

    /** The renewal backoff cap: a third of the sizing ttl, never below its own base. */
    @VisibleForTesting
    static long renewalBackoffMaxMs(long ttlMs) {
        return Math.max(renewalBackoffBaseMs(ttlMs), Math.min(ttlMs / 3, RETRY_BACKOFF_MAX.toMillis()));
    }

    /**
     * The whole renewal dispatch must fit the sizing ttl plus the ttl/3 expiry grace - never
     * the generic 24h: a budget past the grace can only come back after the lease died.
     */
    @VisibleForTesting
    static long renewalRetryBudgetMs(long ttlMs) {
        return Math.max(MIN_RENEWAL_BUDGET_MS, ttlMs + ttlMs / 3);
    }

    /** Renewals only: WRITE pacing derived from the sizing ttl, budget = ttl plus the grace. */
    private static RetryPolicy renewalRetryPolicy(long ttlMs) {
        return RequestTypeDependentRetryPolicy.newBuilder()
                .setRetryPolicy(
                        TypeCase.WRITE,
                        ExponentialBackoffRetry.newBuilder()
                                .setBaseSleepTime(
                                        TimeDuration.valueOf(
                                                renewalBackoffBaseMs(ttlMs), TimeUnit.MILLISECONDS))
                                .setMaxSleepTime(
                                        TimeDuration.valueOf(
                                                renewalBackoffMaxMs(ttlMs), TimeUnit.MILLISECONDS))
                                .build())
                .setTimeout(
                        TypeCase.WRITE,
                        TimeDuration.valueOf(renewalRetryBudgetMs(ttlMs), TimeUnit.MILLISECONDS))
                .build();
    }

    /** Builds a {@link RaftMetadataClient} from membership, group id, timeouts and clock. */
    public static Builder newBuilder() {
        return new Builder();
    }

    /** The builder of {@link RaftMetadataClient}; see {@link RaftMetadataClient#newBuilder()}. */
    public static final class Builder {

        private final Map<String, InetSocketAddress> members = new LinkedHashMap<>();
        private UUID groupId;
        private Duration requestTimeout = DEFAULT_REQUEST_TIMEOUT;
        /** Total retry budget; the metadata protocol requires a finite value. */
        private Duration retryTimeout = DEFAULT_RETRY_TIMEOUT;

        private Clock clock = SystemClock.getInstance();

        /** The rpc type the client speaks; the matching transport jar runs on the classpath. */
        private SupportedRpcType rpcType = SupportedRpcType.GRPC;

        private Builder() {}

        /** Adds one quorum member: {@code peerId} at its QUORUM listener address. */
        public Builder addQuorumMember(String peerId, InetSocketAddress address) {
            checkNotNull(peerId, "peerId");
            checkNotNull(address, "address");
            members.put(peerId, address);
            return this;
        }

        /** Sets the group id all members replicate. */
        public Builder setGroupId(UUID groupId) {
            this.groupId = groupId;
            return this;
        }

        /**
         * Sets the rpc type the client speaks (default grpc); the transport artifact, e.g.
         * ratis-grpc, must be on the runtime classpath.
         */
        public Builder setRpcType(SupportedRpcType rpcType) {
            this.rpcType = checkNotNull(rpcType, "rpcType");
            return this;
        }

        /** Sets the transport-level timeout of one request attempt; ratis retries within it. */
        public Builder setRequestTimeout(Duration requestTimeout) {
            this.requestTimeout = checkNotNull(requestTimeout, "requestTimeout");
            return this;
        }

        /**
         * Total time one operation may retry before failing back to the caller. Must exceed
         * {@link #setRequestTimeout(Duration)}: a budget below one attempt gives up on the very
         * first try, which ratis reports as the raw transport failure - no exhaustion signal, so
         * an ambiguous mutation cannot be told from a definitely-undelivered one.
         */
        public Builder setRetryTimeout(Duration retryTimeout) {
            checkNotNull(retryTimeout, "retryTimeout");
            checkArgument(
                    retryTimeout.toMillis() > 0,
                    "retryTimeout must be positive, got %s",
                    retryTimeout);
            this.retryTimeout = retryTimeout;
            return this;
        }

        /** Rejected: the metadata protocol requires a bounded retry budget. */
        @Deprecated
        public Builder setRetryForever() {
            throw new IllegalStateException(
                    "Retry timeout must be finite; setRetryTimeout(Duration) is required");
        }

        /** Sets the time source for record timestamps and lease expiries. */
        public Builder setClock(Clock clock) {
            this.clock = checkNotNull(clock, "clock");
            return this;
        }

        /** Builds the client; requires a group id and at least one quorum member. */
        public RaftMetadataClient build() {
            checkNotNull(groupId, "groupId");
            checkArgument(!members.isEmpty(), "At least one quorum member is required");
            checkArgument(
                    retryTimeout.compareTo(requestTimeout) > 0,
                    "retryTimeout (%s) must exceed requestTimeout (%s): a budget below one"
                            + " attempt gives up on the first try, which loses the"
                            + " budget-exhaustion signal",
                    retryTimeout,
                    requestTimeout);

            final List<RaftPeer> peers = new ArrayList<>(members.size());
            for (Map.Entry<String, InetSocketAddress> member : members.entrySet()) {
                peers.add(
                        RaftPeer.newBuilder()
                                .setId(member.getKey())
                                .setAddress(
                                        member.getValue().getHostString()
                                                + ":"
                                                + member.getValue().getPort())
                                .build());
            }
            final RaftGroup group = RaftGroup.valueOf(RaftGroupId.valueOf(groupId), peers);

            // The main pipeline is rebuildable: the factory runs once here and once per
            // recovery, each invocation building a fresh client - a poisoned async window is
            // replaced, never lingered in.
            final Supplier<RaftClient> mainFactory =
                    () -> buildRaftClient(group, requestTimeout, generalRetryPolicy());
            // The renewals' own client: sized by the ttl it is asked for, so a renewal's pacing
            // follows its lease's urgency instead of the general pipeline's user-write profile.
            final LongFunction<RaftClient> renewalFactory =
                    ttlMs ->
                            buildRaftClient(
                                    group,
                                    Duration.ofMillis(
                                            renewalAttemptTimeoutMs(
                                                    ttlMs, requestTimeout.toMillis())),
                                    renewalRetryPolicy(ttlMs));
            final long quietPeriodMs =
                    Math.max(
                            RECOVERY_QUIET_PERIOD_MIN.toMillis(),
                            Math.max(1, requestTimeout.multipliedBy(2).toMillis()));
            return new RaftMetadataClient(mainFactory, renewalFactory, clock, quietPeriodMs);
        }

        /**
         * One raft client of this builder's transport wiring; the general client and every
         * renewal client differ only in timeout and policy. One fresh client per invocation:
         * each build gets its own properties and retry-policy instances, so no configuration
         * mutation (or future policy state) leaks across rebuilt clients - nothing built here
         * is shared with another build.
         */
        private RaftClient buildRaftClient(
                RaftGroup group, Duration attemptTimeout, RetryPolicy policy) {
            final RaftProperties properties = new RaftProperties();
            RaftConfigKeys.Rpc.setType(properties, rpcType);
            RaftClientConfigKeys.Rpc.setRequestTimeout(
                    properties,
                    TimeDuration.valueOf(attemptTimeout.toMillis(), TimeUnit.MILLISECONDS));
            // The async pipeline would otherwise open with a dummy watch request that no retry
            // policy budgets: an unset type falls back to retryForeverNoSleep, so against an
            // unreachable quorum it spins without sleep and gates every real request behind it.
            // The metadata protocol has no watches - polls replace them - so nothing needs it.
            RaftClientConfigKeys.Async.Experimental.setSendDummyRequest(properties, false);
            return RaftClient.newBuilder()
                    .setRaftGroup(group)
                    .setProperties(properties)
                    .setRetryPolicy(policy)
                    .build();
        }

        /**
         * The general pipeline's policy: backoff pacing under the caller's whole-operation
         * budget - the factored form of the client's original inline construction.
         */
        private RequestTypeDependentRetryPolicy generalRetryPolicy() {
            // No policy means ratis' retryForeverNoSleep; pace with backoff, cap with the budget.
            final RequestTypeDependentRetryPolicy.Builder retryPolicy =
                    RequestTypeDependentRetryPolicy.newBuilder()
                            .setRetryPolicy(TypeCase.WRITE, backoffRetry())
                            .setRetryPolicy(TypeCase.READ, backoffRetry());
            final TimeDuration budget =
                    TimeDuration.valueOf(retryTimeout.toMillis(), TimeUnit.MILLISECONDS);
            retryPolicy.setTimeout(TypeCase.WRITE, budget);
            retryPolicy.setTimeout(TypeCase.READ, budget);
            return retryPolicy.build();
        }

        /** Retry pacing: exponential backoff with jitter, capped; attempts unlimited. */
        private static RetryPolicy backoffRetry() {
            return ExponentialBackoffRetry.newBuilder()
                    .setBaseSleepTime(
                            TimeDuration.valueOf(
                                    RETRY_BACKOFF_BASE.toMillis(), TimeUnit.MILLISECONDS))
                    .setMaxSleepTime(
                            TimeDuration.valueOf(
                                    RETRY_BACKOFF_MAX.toMillis(), TimeUnit.MILLISECONDS))
                    .build();
        }
    }

    /** One stored document with its metadata, the reply of {@link #get(String)}. */
    public static final class MetadataRecord {

        private final byte[] data;
        private final long version;
        private final long leaseId;
        private final long createTime;
        private final long modifyTime;

        MetadataRecord(byte[] data, long version, long leaseId, long createTime, long modifyTime) {
            this.data = data;
            this.version = version;
            this.leaseId = leaseId;
            this.createTime = createTime;
            this.modifyTime = modifyTime;
        }

        /** The stored payload. */
        public byte[] getData() {
            return data;
        }

        /** The record version; 1 for a fresh record, bumped by every update. */
        public long getVersion() {
            return version;
        }

        /** The lease this record is bound to (0 = persistent). */
        public long getLeaseId() {
            return leaseId;
        }

        /** The proposer clock of the creating command. */
        public long getCreateTime() {
            return createTime;
        }

        /** The proposer clock of the last updating command. */
        public long getModifyTime() {
            return modifyTime;
        }
    }

    /** One {@link #listPage(String, String, long)} reply: a bounded page plus continuation. */
    public static final class ListPage {

        private final List<RaftResult.Entry> entries;
        private final boolean hasMore;

        ListPage(List<RaftResult.Entry> entries, boolean hasMore) {
            this.entries = entries;
            this.hasMore = hasMore;
        }

        /** This page's entries, in lexicographic order. */
        public List<RaftResult.Entry> getEntries() {
            return entries;
        }

        /** Whether more records remain under the prefix past this page. */
        public boolean hasMore() {
            return hasMore;
        }

        /** Cursor for the next {@code listPage}: last listed path, empty if none. */
        public String getStartAfter() {
            return entries.isEmpty() ? "" : entries.get(entries.size() - 1).getPath();
        }
    }

    /** One {@link #poll(long, long)} reply: a covered prefix and its change records. */
    public static final class PollResult {

        private final long watermark;
        private final long oldestRetainedRevision;
        private final List<ChangeEvent> records;

        PollResult(long watermark, long oldestRetainedRevision, List<ChangeEvent> records) {
            this.watermark = watermark;
            this.oldestRetainedRevision = oldestRetainedRevision;
            this.records = records;
        }

        /** Revision prefix covered; absent revisions carried no mutation. */
        public long getWatermark() {
            return watermark;
        }

        /** The smallest revision whose change records are guaranteed retained. */
        public long getOldestRetainedRevision() {
            return oldestRetainedRevision;
        }

        /** The change records after the polled revision, ordered by revision. */
        public List<ChangeEvent> getRecords() {
            return records;
        }
    }
}
