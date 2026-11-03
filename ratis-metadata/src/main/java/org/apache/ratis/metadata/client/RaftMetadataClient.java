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
import org.apache.ratis.metadata.util.FutureUtils;
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
import org.apache.ratis.retry.ExponentialBackoffRetry;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.thirdparty.com.google.common.annotations.VisibleForTesting;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.apache.ratis.metadata.util.Preconditions.checkArgument;
import static org.apache.ratis.metadata.util.Preconditions.checkNotNull;
import static org.apache.ratis.metadata.util.Preconditions.checkState;

/**
 * Typed client of the raft metadata store: one method per {@link RaftCommand}, each with a {@code
 * *Async} twin. The twins never throw: every failure, including invalid arguments and use after
 * {@link #close()}, settles the returned future.
 */
public final class RaftMetadataClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RaftMetadataClient.class);

    /** Default of the per-attempt request timeout; same as ratis' own client default. */
    private static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(3);

    private static final Duration DEFAULT_RETRY_TIMEOUT = Duration.ofSeconds(60);

    /** The first backoff between failed attempts, doubled with jitter on every retry. */
    private static final Duration RETRY_BACKOFF_BASE = Duration.ofMillis(200);

    /** The backoff cap, so a long outage spaces its retries at a sane cadence. */
    private static final Duration RETRY_BACKOFF_MAX = Duration.ofSeconds(5);

    /** Threads settling async replies off the transport threads; see {@link #sendAsync}. */
    private static final int ASYNC_COMPLETION_THREADS = 2;

    private final RaftClient raftClient;
    private final Clock clock;
    private final ExecutorService completionExecutor;

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
            if (sessionTick == null) {
                sessionTick = AsyncDriver.scheduleAtFixedRate(this::tickSessions, SESSION_TICK_MS);
            }
        }
    }

    /** Detaches the session; the ticks stop once the last one is gone. */
    void unregisterSession(LeaseSession session) {
        synchronized (sessionState) {
            sessions.remove(session);
            if (sessions.isEmpty() && sessionTick != null) {
                sessionTick.cancel(false);
                sessionTick = null;
            }
        }
    }

    private void tickSessions() {
        // A snapshot iteration: a session detaching mid-tick is at worst one wasted due-check.
        for (LeaseSession session : sessions) {
            session.tick();
        }
    }

    private volatile boolean closed;

    /** Wraps an already-built raft client; the builder is the production path. */
    @VisibleForTesting
    RaftMetadataClient(RaftClient raftClient, Clock clock) {
        this.raftClient = raftClient;
        this.clock = clock;
        this.completionExecutor =
                Executors.newFixedThreadPool(
                        ASYNC_COMPLETION_THREADS,
                        new NamedThreadFactory("raft-metadata-async", true));
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

    /** Async form of {@link #increment(String, long)}. */
    public CompletableFuture<Long> incrementAsync(String counter, long delta) {
        return settleEarly(
                () -> {
                    RaftCommand command = RaftCommand.increment(counter, delta);
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

    /** Lists the whole {@code prefix} subtree in lexicographic order, uncapped. */
    public List<RaftResult.Entry> list(String prefix) {
        final List<RaftResult.Entry> entries = new ArrayList<>();
        String startAfter = "";
        while (true) {
            final ListPage page = listPage(prefix, startAfter, Long.MAX_VALUE);
            entries.addAll(page.getEntries());
            // Spin guard only - the uncapped page never reports more; empty cannot advance.
            if (!page.hasMore() || page.getEntries().isEmpty()) {
                return entries;
            }
            startAfter = page.getStartAfter();
        }
    }

    /** Async form of {@link #list(String)}: the pages of one enumeration chain into one future. */
    public CompletableFuture<List<RaftResult.Entry>> listAsync(String prefix) {
        return settleEarly(() -> listNextPageAsync(prefix, "", new ArrayList<>()));
    }

    private CompletableFuture<List<RaftResult.Entry>> listNextPageAsync(
            String prefix, String startAfter, List<RaftResult.Entry> entries) {
        return listPageAsync(prefix, startAfter, Long.MAX_VALUE)
                .thenCompose(
                        page -> {
                            entries.addAll(page.getEntries());
                            // Spin guard only - the uncapped page never reports more.
                            if (!page.hasMore() || page.getEntries().isEmpty()) {
                                return CompletableFuture.completedFuture(entries);
                            }
                            return listNextPageAsync(prefix, page.getStartAfter(), entries);
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

    /** Renews a lease, setting its expiry to {@code ttl} from the client's clock. */
    public long renewLease(long leaseId, Duration ttl) {
        checkNotNull(ttl, "ttl");
        long ttlMs = ttl.toMillis();
        checkArgument(ttlMs > 0, "Lease ttl must be positive, got %s", ttl);
        RaftCommand command = RaftCommand.leaseRenew(leaseId, clock.milliseconds() + ttlMs);
        return accepted(command, "renew lease " + leaseId).getRevision();
    }

    /** Async form of {@link #renewLease(long, Duration)}. */
    public CompletableFuture<Long> renewLeaseAsync(long leaseId, Duration ttl) {
        return settleEarly(
                () -> {
                    checkNotNull(ttl, "ttl");
                    long ttlMs = ttl.toMillis();
                    checkArgument(ttlMs > 0, "Lease ttl must be positive, got %s", ttl);
                    RaftCommand command =
                            RaftCommand.leaseRenew(leaseId, clock.milliseconds() + ttlMs);
                    return acceptedAsync(command, "renew lease " + leaseId)
                            .thenApply(RaftResult::getRevision);
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
        }
        for (LeaseSession session : toClose) {
            session.close();
        }
        try {
            raftClient.close();
        } catch (IOException e) {
            LOG.debug("Closing the raft client failed", e);
        }
        // Closing the raft client fails the pending replies; let their completions drain.
        completionExecutor.shutdown();
    }

    /** Whether {@link #close()} has run; components with own retry loops stop on it. */
    public boolean isClosed() {
        return closed;
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
        return sendAsync(command, false, what);
    }

    private CompletableFuture<RaftResult> queriedAsync(RaftCommand command, String what) {
        return sendAsync(command, true, what);
    }

    /**
     * Runs an async twin's synchronous part. Twins never throw: invalid arguments, a closed client,
     * anything failing before the send settles the returned future instead.
     */
    private static <T> CompletableFuture<T> settleEarly(Supplier<CompletableFuture<T>> call) {
        try {
            return call.get();
        } catch (RuntimeException e) {
            return FutureUtils.completedExceptionally(e);
        }
    }

    private RaftResult send(RaftCommand command, boolean readOnly) {
        checkNotClosed();
        final Message message =
                Message.valueOf(ByteBuffer.wrap(RaftCommandCodec.encodeCommand(command)));
        final RaftClientReply reply;
        try {
            reply =
                    readOnly
                            ? raftClient.io().sendReadOnly(message)
                            : raftClient.io().send(message);
        } catch (IOException e) {
            throw new MetadataTransportException(
                    "Metadata " + command.getOp() + " failed after retries: " + e.getMessage(), e);
        }
        return toResult(reply, command);
    }

    /**
     * Sends on ratis' ordered async pipeline. Replies settle on a client-owned executor, never on
     * the transport thread: ratis completes them inline while holding the sliding-window monitor,
     * so decoding or a chained next send there could park that thread in the pipeline's semaphore
     * and wedge every reply. Calls beyond that semaphore (100 outstanding by default) block their
     * issuing thread until one completes - ratis' backpressure. One exhausted retry budget
     * permanently fails the async window: later async calls fail fast until the client is
     * recreated, while the sync surface keeps working.
     */
    private CompletableFuture<RaftResult> sendAsync(
            RaftCommand command, boolean readOnly, String what) {
        checkNotClosed();
        final Message message =
                Message.valueOf(ByteBuffer.wrap(RaftCommandCodec.encodeCommand(command)));
        final CompletableFuture<RaftClientReply> reply =
                readOnly
                        ? raftClient.async().sendReadOnly(message)
                        : raftClient.async().send(message);
        final CompletableFuture<RaftResult> done = new CompletableFuture<>();
        reply.whenComplete(
                (received, failure) -> {
                    final Runnable settle =
                            () -> {
                                if (failure != null) {
                                    done.completeExceptionally(translate(failure, command));
                                    return;
                                }
                                try {
                                    done.complete(map(toResult(received, command), command, what));
                                } catch (Throwable t) {
                                    // The future must settle even on Errors; settle must not throw.
                                    done.completeExceptionally(t);
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
        return done;
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

    /** Surfaces an async failure the way {@link #send} surfaces transport failures. */
    private static RuntimeException translate(Throwable failure, RaftCommand command) {
        final Throwable cause = ExceptionUtils.stripCompletionException(failure);
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

    /** Builds a {@link RaftMetadataClient} from membership, group id, timeouts and clock. */
    public static Builder newBuilder() {
        return new Builder();
    }

    /** The builder of {@link RaftMetadataClient}; see {@link RaftMetadataClient#newBuilder()}. */
    public static final class Builder {

        private final Map<String, InetSocketAddress> members = new LinkedHashMap<>();
        private UUID groupId;
        private Duration requestTimeout = DEFAULT_REQUEST_TIMEOUT;
        /** Null only after {@link #setRetryForever()}: ratis then retries without a total bound. */
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

        /** Total time one operation may retry before failing back to the caller. */
        public Builder setRetryTimeout(Duration retryTimeout) {
            checkNotNull(retryTimeout, "retryTimeout");
            checkArgument(
                    retryTimeout.toMillis() > 0,
                    "retryTimeout must be positive, got %s",
                    retryTimeout);
            this.retryTimeout = retryTimeout;
            return this;
        }

        /** Opts out of the retry budget: operations retry forever, which is ratis' own default. */
        public Builder setRetryForever() {
            this.retryTimeout = null;
            return this;
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

            final RaftProperties properties = new RaftProperties();
            RaftConfigKeys.Rpc.setType(properties, rpcType);
            RaftClientConfigKeys.Rpc.setRequestTimeout(
                    properties,
                    TimeDuration.valueOf(requestTimeout.toMillis(), TimeUnit.MILLISECONDS));
            // The async pipeline would otherwise open with a dummy watch request that no retry
            // policy budgets: an unset type falls back to retryForeverNoSleep, so against an
            // unreachable quorum it spins without sleep and gates every real request behind it.
            // The metadata protocol has no watches - polls replace them - so nothing needs it.
            RaftClientConfigKeys.Async.Experimental.setSendDummyRequest(properties, false);

            // No policy means ratis' retryForeverNoSleep; pace with backoff, cap with the budget.
            final RequestTypeDependentRetryPolicy.Builder retryPolicy =
                    RequestTypeDependentRetryPolicy.newBuilder()
                            .setRetryPolicy(TypeCase.WRITE, backoffRetry())
                            .setRetryPolicy(TypeCase.READ, backoffRetry());
            if (retryTimeout != null) {
                final TimeDuration budget =
                        TimeDuration.valueOf(retryTimeout.toMillis(), TimeUnit.MILLISECONDS);
                retryPolicy.setTimeout(TypeCase.WRITE, budget);
                retryPolicy.setTimeout(TypeCase.READ, budget);
            }

            final RaftClient raftClient =
                    RaftClient.newBuilder()
                            .setRaftGroup(group)
                            .setProperties(properties)
                            .setRetryPolicy(retryPolicy.build())
                            .build();
            return new RaftMetadataClient(raftClient, clock);
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
