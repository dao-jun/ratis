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
package org.apache.ratis.metadata.lock;

import org.apache.ratis.metadata.client.LeaseSession;
import org.apache.ratis.metadata.client.MetadataWatcher;
import org.apache.ratis.metadata.client.RaftMetadataClient;
import org.apache.ratis.metadata.client.RaftMetadataException;
import org.apache.ratis.metadata.protocol.ChangeEvent;
import org.apache.ratis.metadata.protocol.RaftResult;
import org.apache.ratis.metadata.util.AsyncDriver;
import org.apache.ratis.metadata.util.ExceptionUtils;
import org.apache.ratis.util.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeoutException;

import static org.apache.ratis.metadata.util.Preconditions.checkArgument;
import static org.apache.ratis.metadata.util.Preconditions.checkNotNull;
import static org.apache.ratis.metadata.util.Preconditions.checkState;

/**
 * A mutual-exclusion lock over the metadata store: one ephemeral record guards one path, and the
 * lock lives exactly as long as its holder's lease. A contender that finds the record held waits
 * on the change stream and retries when the holder's release - explicit or by lease expiry -
 * deletes the record.
 *
 * <p>Acquire, in order: the parent chain is materialized once; the current revision is taken as
 * the watch position (so no release can slip between the failed attempt and the watch); then the
 * contender loops - open a lease, create the lock record bound to it, and on {@code NODE_EXISTS}
 * wait for the record's {@code DELETE} change, or a {@code DELETE_SUBTREE} change whose deleted
 * root covers the path - an external recursive delete removes the record without any
 * per-victim event (a watchdog tick re-attempts regardless, so a missed event can never wedge
 * a waiter). A contender whose own lease expired underneath
 * ({@code UNKNOWN_LEASE} from the create) reopens one and keeps waiting. Releasing - and every
 * acquire-failure unwind - stops the contender's renewals and revokes its private lease: the
 * server's lease-checked expiry cascade deletes the lock record for a prompt handover, and only
 * ever records bound to that lease, so a late release by an expired holder can never delete a
 * new holder's record at the same path; a crashed holder's record is deleted by the leader's
 * expiry scheduler within roughly one ttl. A caller's {@code cancel()} of the returned future
 * is one more settle: it runs the same unwind - the settle's ownership never hinges on the
 * future itself completing, which a cancel makes impossible.
 *
 * <p>{@link Hold#getToken()} is a fencing token: after each successful create the lock bumps a
 * counter named after the lock path, so tokens are strictly increasing across holders. The bump
 * itself is lease-guarded in the replicated apply: a contender whose lease dies between its
 * create and its bump is refused ({@code UNKNOWN_LEASE} / {@code STALE_LEASE}) and re-enters
 * the race, so a completed acquire's token can never overtake a later holder's. External
 * resources guarded by the lock should reject work carrying a token lower than the last seen one
 * - during a lease expiry the old holder may briefly believe it still holds.
 *
 * <p>The lock is neither reentrant nor fair: all waiters wake on every release and one create
 * wins. One acquire per {@link MetadataLock} instance; the record at the path and the counter of
 * the same name belong to the lock exclusively. {@link #acquireAsync()} never blocks, and never
 * throws for the first acquire on an open lock - the acquire is an event-driven state machine on
 * the client's asynchronous calls, and
 * every failure lands in the future as {@link RaftMetadataException} (misconfiguration), a
 * {@code MetadataTransportException} (the request never went through; a mutation whose retry
 * budget ran out answers its subclass {@code PossiblyCommittedException} - reconcile by
 * reading state before re-acquiring; the acquire's unwind below is already ownership-safe
 * either way), or a {@link TimeoutException}; a closed lock refuses through the future the same
 * way. A path that cannot ride the UTF-8 wire (an
 * unpaired surrogate) is a programmer error and settles the future with {@code
 * IllegalArgumentException}, like the non-positive-timeout checkArgument - while a second
 * acquire on the same instance throws IllegalStateException synchronously, another programmer
 * error on the caller's side of the future.
 */
public final class MetadataLock implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataLock.class);

    /** The longest gap between two watchdog wake-ups, whatever the ttl. */
    private static final long MIN_WATCHDOG_MS = 1000;

    /**
     * Runs the one handoff of the acquire future to its caller. Listeners on that future are
     * the caller's code and may block; neither the lock's state lock nor the module's shared
     * scheduler thread (every client's lease renewals and watcher polls) may pay for them.
     */
    private static final Executor COMPLETIONS = ForkJoinPool.commonPool();

    private static final byte[] EMPTY = new byte[0];

    private final RaftMetadataClient client;
    private final String path;
    private final long ttlMs;

    /** The acquire state machine; every field below runs under this lock. */
    private final Object state = new Object();
    private CompletableFuture<Hold> pending;
    private boolean acquireAttempted;
    private boolean stopped;
    /** Latched by {@link #fail} even before a first acquire, so a close is never missed. */
    private boolean closed;
    /** True while parked on the change stream; a wake or the watchdog un-parks an attempt. */
    private boolean waiting;
    private LeaseSession session;
    private MetadataWatcher watcher;
    private ScheduledFuture<?> watchdog;
    private ScheduledFuture<?> timeoutTask;

    private MetadataLock(RaftMetadataClient client, String path, long ttlMs) {
        this.client = client;
        this.path = path;
        this.ttlMs = ttlMs;
    }

    /** A lock builder; required: the client, the path, and the ttl. */
    public static Builder newBuilder() {
        return new Builder();
    }

    /** The builder of {@link MetadataLock}; see {@link #newBuilder()}. */
    public static final class Builder {

        private RaftMetadataClient client;
        private String path;
        private Duration ttl;

        private Builder() {}

        /** The client all lock traffic goes through. */
        public Builder setClient(RaftMetadataClient client) {
            this.client = checkNotNull(client, "client");
            return this;
        }

        /** The guarded path; its parent chain is created if missing. */
        public Builder setPath(String path) {
            this.path = checkNotNull(path, "path");
            return this;
        }

        /** The lease ttl: the crash-release window; the contender renews every third of it. */
        public Builder setTtl(Duration ttl) {
            this.ttl = checkNotNull(ttl, "ttl");
            return this;
        }

        /** Builds the lock; one acquire per instance. */
        public MetadataLock build() {
            checkState(client != null, "client is required");
            checkState(path != null, "path is required");
            checkState(ttl != null, "ttl is required");
            long ms = ttl.toMillis();
            checkArgument(ms > 0, "ttl must be positive, got %s", ttl);
            return new MetadataLock(client, path, ms);
        }
    }

    // ------------------------------------------------------------------
    // acquire
    // ------------------------------------------------------------------

    /**
     * Acquires the lock, indefinitely; completes with the {@link Hold}, or exceptionally with a
     * {@link RaftMetadataException}, a {@link MetadataTransportException} (an exhausted create
     * answers its subclass {@code PossiblyCommittedException}), or an {@link
     * IllegalStateException} after {@link #close()}. A path that cannot ride the UTF-8 wire (an
     * unpaired surrogate) is a programmer error and settles the future with {@link
     * IllegalArgumentException}.
     */
    public CompletableFuture<Hold> acquireAsync() {
        return acquireAsync(null);
    }

    /**
     * Acquires the lock with a deadline; on expiry the future completes with {@link
     * TimeoutException} and the contender unwinds by revoking its private lease - a create that
     * committed anyway is swept by the lease's expiry cascade, so a timed-out contender never
     * leaks the lock and never touches another holder's record. A path that cannot ride the
     * UTF-8 wire (an unpaired surrogate) is a programmer error and settles the future with
     * {@link IllegalArgumentException}, like the non-positive timeout below.
     */
    public CompletableFuture<Hold> acquireAsync(Duration timeout) {
        if (timeout != null) {
            long ms = timeout.toMillis();
            checkArgument(ms > 0, "timeout must be positive, got %s", timeout);
        }
        final CompletableFuture<Hold> future;
        synchronized (state) {
            if (acquireAttempted) {
                throw new IllegalStateException("acquire was already attempted on this lock");
            }
            if (closed) {
                // Refused through the future, like every other acquire failure.
                return JavaUtils.completeExceptionally(
                        new IllegalStateException("acquire was attempted on a closed lock"));
            }
            acquireAttempted = true;
            pending = future = new CompletableFuture<>();
            // The settle paths hand the future over on COMPLETIONS, but a caller's cancel()
            // lands inline right here: it must unwind the contender exactly like a close.
            future.whenComplete((ignored, failure) -> {
                if (failure instanceof CancellationException) {
                    fail((CancellationException) failure);
                }
            });
            if (timeout != null) {
                timeoutTask = AsyncDriver.schedule(() -> fail(new TimeoutException("Acquiring " + path + " timed out")),
                        timeout.toMillis());
            }
        }
        AsyncDriver.schedule(this::begin, 0);
        return future;
    }

    /** Sync form of {@link #acquireAsync()}; blocks until held or failed. */
    public Hold acquire() throws InterruptedException {
        final CompletableFuture<Hold> future = acquireAsync();
        try {
            return future.get();
        } catch (ExecutionException e) {
            throw rethrowable(e.getCause());
        }
    }

    /** Sync form of {@link #acquireAsync(Duration)}. */
    public Hold acquire(Duration timeout) throws InterruptedException, TimeoutException {
        final CompletableFuture<Hold> future = acquireAsync(timeout);
        try {
            return future.get();
        } catch (ExecutionException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof TimeoutException) {
                throw (TimeoutException) cause;
            }
            throw rethrowable(cause);
        }
    }

    private static RuntimeException rethrowable(Throwable cause) {
        return cause instanceof RuntimeException ? (RuntimeException) cause
                : new IllegalStateException("The acquire failed unexpectedly", cause);
    }

    /**
     * Cancels a pending acquire (its future completes exceptionally); a no-op once settled - the
     * {@link Hold} owns its lease. Cancelling the returned future directly is the equivalent
     * settle. Idempotent.
     */
    @Override
    public void close() {
        fail(new IllegalStateException("The lock was closed while acquiring"));
    }

    // ------------------------------------------------------------------
    // the acquire state machine - event driven, on no thread of its own
    // ------------------------------------------------------------------

    /** Whether the acquire already settled - success, failure or cancellation. */
    private boolean settled() {
        synchronized (state) {
            return stopped || pending.isDone();
        }
    }

    /**
     * The client's async twins wrap their failures; the statuses and exception types the state
     * machine decides on live on the cause underneath.
     */
    private static Throwable unwrap(Throwable failure) {
        return ExceptionUtils.stripCompletionException(failure);
    }

    /**
     * Whether the lock path lies in the subtree a recursive delete removed - the deleted root
     * itself or any descendant. A recursive delete reports one {@code DELETE_SUBTREE} change at
     * its root and enumerates no victims, so a waiter inside the subtree matches on prefix.
     */
    private static boolean inSubtree(String path, String deletedRoot) {
        return path.equals(deletedRoot)
                || path.startsWith(deletedRoot.endsWith("/") ? deletedRoot : deletedRoot + "/");
    }

    /**
     * Materializes the parent chain, then runs {@code next}; {@code NODE_EXISTS} just means
     * the parent is already there. The initial acquire fixes the watch position here; a
     * re-attempt after an external delete removed the chain mid-race re-enters the race.
     */
    private void withParents(Runnable next) {
        final int slash = path.lastIndexOf('/');
        final String parent = slash <= 0 ? "/" : path.substring(0, slash);
        if (parent.equals("/")) {
            next.run();
            return;
        }
        client.createAsync(parent, EMPTY, true)
                .whenComplete((ignored, failure) -> {
                    if (settled()) {
                        return;
                    }
                    // The parent being there already and being created are one state.
                    if (failure == null
                            || isRejectionOf(
                            unwrap(failure), RaftResult.Status.NODE_EXISTS)) {
                        next.run();
                        return;
                    }
                    fail(unwrap(failure));
                });
    }

    private void begin() {
        if (settled()) {
            return;
        }
        withParents(this::takeWatchPosition);
    }

    /**
     * The watch position is taken before the first create attempt: any delete at or after that
     * revision is delivered by the watcher, so no release can slip between attempt and watch.
     */
    private void takeWatchPosition() {
        if (settled()) {
            return;
        }
        client.revisionAsync().whenComplete((head, failure) -> {
            if (settled()) {
                return;
            }
            if (failure != null) {
                fail(unwrap(failure));
                return;
            }
            startWatcher(head);
            attempt();
        });
    }

    private void startWatcher(long head) {
        final MetadataWatcher started = MetadataWatcher.newBuilder(client,
                        new MetadataWatcher.Listener() {
                            @Override
                            public void onRecord(ChangeEvent event) {
                                for (ChangeEvent.Change change : event.getChanges()) {
                                    if ((change.getOp() == ChangeEvent.ChangeOp.DELETE
                                            && path.equals(change.getPath()))
                                            || (change.getOp() == ChangeEvent.ChangeOp.DELETE_SUBTREE
                                            && inSubtree(path, change.getPath()))) {
                                        onWake();
                                        return;
                                    }
                                }
                            }

                            @Override
                            public void onNeedSnapshot(long oldestRetainedRevision) {
                                // The wait position fell below the retention line and the
                                // watcher stopped; resume from the line - the next create
                                // attempt covers the gap.
                                final MetadataWatcher current;
                                synchronized (state) {
                                    current = watcher;
                                }
                                if (current != null) {
                                    current.restartFrom(oldestRetainedRevision);
                                }
                                onWake();
                            }
                        })
                .setSinceRevision(head)
                .build();
        synchronized (state) {
            if (settled()) {
                // The settle won the window above and could close no watcher - one assigned
                // only now would run unowned; never start it.
                return;
            }
            watcher = started;
        }
        started.start();
    }

    /** One acquisition round: a session when lacking one, then one create attempt. */
    private void attempt() {
        final LeaseSession current;
        synchronized (state) {
            if (settled()) {
                return;
            }
            current = session;
        }
        if (current != null) {
            tryCreate(current);
            return;
        }
        client.openLeaseAsync(Duration.ofMillis(ttlMs)).whenComplete((opened, failure) -> {
            if (failure != null) {
                fail(unwrap(failure));
                return;
            }
            final boolean settleWon;
            synchronized (state) {
                settleWon = settled();
                if (!settleWon) {
                    session = opened;
                }
            }
            if (settleWon) {
                // The winner of the settle owns the cleanup; this lease just
                // opened and belongs to no one - retire it here, outside the
                // state lock like every other cleanup.
                retireLeaseOf(opened);
                return;
            }
            tryCreate(opened);
        });
    }

    private void tryCreate(final LeaseSession current) {
        synchronized (state) {
            if (settled()) {
                return;
            }
        }
        client.createAsync(path, EMPTY, current)
                .whenComplete((ignored, failure) -> onCreateOutcome(current, unwrap(failure)));
    }

    private void onCreateOutcome(LeaseSession current, Throwable failure) {
        if (failure == null) {
            synchronized (state) {
                if (settled()) {
                    // The settle path saw the create in flight and already retired this lease;
                    // the client's ordered pipeline runs that revocation after this create.
                    return;
                }
            }
            takeToken(current);
            return;
        }
        if (isRejectionOf(failure, RaftResult.Status.NODE_EXISTS)) {
            synchronized (state) {
                if (settled()) {
                    return;
                }
                waiting = true;
                watchdog = AsyncDriver.schedule(this::onWatchdog, Math.max(MIN_WATCHDOG_MS, ttlMs / 3));
            }
            return;
        }
        if (isRejectionOf(failure, RaftResult.Status.UNKNOWN_LEASE)) {
            // The contender's own lease died (a partition outlasted the ttl); a new one keeps it
            // in the race. This create was rejected, not committed - but the dead lease may still
            // own records from an earlier attempt, so retire it rather than just stop renewing.
            reopenWithFreshLease(current);
            return;
        }
        if (isRejectionOf(failure, RaftResult.Status.NO_PARENT)) {
            // An external delete removed a strict ancestor after the one-time parent setup;
            // the chain is gone but the race goes on. Rebuild it (idempotent) and re-enter.
            withParents(this::attempt);
            return;
        }
        // A definitive rejection (this contender's create never committed) or transport trouble
        // (its outcome is unknown, and a create may have committed anyway): either way the
        // revocation of this contender's own lease is the complete, ownership-safe unwind.
        fail(failure);
    }

    /** The record exists; the guarded counter bump of the same name yields the fencing token. */
    private void takeToken(final LeaseSession current) {
        client.incrementAsync(path, 1, current)
                .whenComplete((token, failure) -> {
                    if (failure != null) {
                        onTokenOutcome(current, unwrap(failure));
                        return;
                    }
                    final MetadataWatcher toClose;
                    final ScheduledFuture<?> toCancel;
                    final Hold hold = new HoldImpl(current, token);
                    synchronized (state) {
                        if (stopped) {
                            // Cancelled or timed out mid-token; that settle's unwind already ran.
                            return;
                        }
                        stopped = true;
                        toClose = watcher;
                        watcher = null;
                        toCancel = timeoutTask;
                        timeoutTask = null;
                    }
                    COMPLETIONS.execute(() -> pending.complete(hold));
                    if (toClose != null) {
                        toClose.close();
                    }
                    if (toCancel != null) {
                        toCancel.cancel(false);
                    }
                });
    }

    /**
     * The token bump was refused: this contender's lease died or the record at the path stopped
     * speaking for it between its committed create and its token — it was superseded, and
     * completing with that token could overtake the new holder. The refusal left no effect, so
     * the ownership-safe unwind (retire this lease; its cascade deletes exactly this contender's
     * record) and a fresh round keep the contender in the race.
     */
    private void onTokenOutcome(final LeaseSession current, Throwable failure) {
        if (isRejectionOf(failure, RaftResult.Status.UNKNOWN_LEASE)
                || isRejectionOf(failure, RaftResult.Status.STALE_LEASE)) {
            reopenWithFreshLease(current);
            return;
        }
        fail(failure);
    }

    /**
     * The lease died under this contender: drop the session under the state lock, retire the
     * lease outside it (its cascade deletes exactly this contender's records), and re-enter
     * the race with a fresh lease.
     */
    private void reopenWithFreshLease(LeaseSession current) {
        synchronized (state) {
            if (settled()) {
                return;
            }
            session = null;
        }
        retireLeaseOf(current);
        AsyncDriver.schedule(this::attempt, 0);
    }

    /** A delete of the lock record arrived (or the retention line tripped): try again now. */
    private void onWake() {
        synchronized (state) {
            if (stopped || !waiting) {
                return;
            }
            waiting = false;
            if (watchdog != null) {
                watchdog.cancel(false);
                watchdog = null;
            }
        }
        AsyncDriver.schedule(this::attempt, 0);
    }

    /** The wait's insurance tick: re-attempt whatever the change stream delivered. */
    private void onWatchdog() {
        synchronized (state) {
            if (stopped || !waiting) {
                return;
            }
            waiting = false;
            watchdog = null;
        }
        attempt();
    }

    /**
     * Settles the acquire with {@code failure} and unwinds it, unless it already settled - the
     * first settler under the state lock owns the unwind. The settle's ownership must not hinge
     * on completing the future: a caller's cancel() completes it without running any unwind,
     * so the internal {@code stopped} flag alone decides. The unwind retires this contender's
     * own private lease whether or not its create committed: the lease-checked expiry cascade
     * removes exactly the records bound to that lease, so a waiting contender (whose lease owns
     * no record) disturbs nothing, while a contender whose create may have committed frees it -
     * and no one else's record at the same path can ever be touched.
     */
    private void fail(Throwable failure) {
        final LeaseSession toRetire;
        final MetadataWatcher watcherToClose;
        final CompletableFuture<Hold> toSettle;
        synchronized (state) {
            closed = true;
            if (stopped) {
                return;
            }
            stopped = true;
            waiting = false;
            if (watchdog != null) {
                watchdog.cancel(false);
                watchdog = null;
            }
            if (timeoutTask != null) {
                timeoutTask.cancel(false);
                timeoutTask = null;
            }
            toRetire = session;
            session = null;
            watcherToClose = watcher;
            watcher = null;
            toSettle = pending;
        }
        if (toSettle != null) {
            COMPLETIONS.execute(() -> toSettle.completeExceptionally(failure));
        }
        if (watcherToClose != null) {
            watcherToClose.close();
        }
        retireLeaseOf(toRetire);
    }

    /**
     * The one cleanup this recipe knows: stop the session's renewals, then revoke its lease
     * through the ordered pipeline - never a delete of the lock path, which cannot tell whose
     * record sits at it. A create of this lease issued earlier in the same pipeline is
     * guaranteed to apply before the revocation, so the cascade covers a create that committed
     * anyway; a create of any OTHER holder is bound to a different lease and survives.
     */
    private void retireLeaseOf(LeaseSession toRetire) {
        if (toRetire == null) {
            return;
        }
        toRetire.close();
        final long leaseId = toRetire.getLeaseId();
        client.revokeLeaseAsync(leaseId).whenComplete((ignored, failure) -> {
            if (failure == null) {
                return;
            }
            // The renewals are already stopped, so the leader's scheduler frees
            // whatever the revocation could not within one ttl. A path delete is
            // never the fallback - it would be ownership-unsafe.
            LOG.warn(
                    "Revoking lease {} of {} failed; the stopped renewals free"
                            + " its records within one ttl",
                    leaseId,
                    path,
                    unwrap(failure));
        });
    }

    private static boolean isRejectionOf(Throwable failure, RaftResult.Status status) {
        return failure instanceof RaftMetadataException && ((RaftMetadataException) failure).getStatus() == status;
    }

    // ------------------------------------------------------------------
    // the hold
    // ------------------------------------------------------------------

    /** One successful acquire of a {@link MetadataLock}; {@link #close()} releases it. */
    public interface Hold extends AutoCloseable {

        /** The fencing token; strictly increasing across holders of the same path. */
        long getToken();

        /**
         * Whether this hold still speaks for a live lease; false after {@link #release()} or once
         * the underlying lease expired (a renewal returned {@code UNKNOWN_LEASE}).
         */
        boolean isHeld();

        /**
         * Releases the lock: stops the renewals and revokes the holder's private lease, whose
         * expiry cascade deletes the lock record for a prompt handover - idempotent, never
         * throws. A revocation that cannot reach the cluster falls back to the crash-release
         * path (the record dies within one ttl); it never falls back to a path delete, which
         * could remove a later holder's record at the same path.
         */
        void release();

        /** {@link #release()}. */
        @Override
        void close();
    }

    private final class HoldImpl implements Hold {

        private final LeaseSession session;
        private final long token;
        private boolean released;

        HoldImpl(LeaseSession session, long token) {
            this.session = session;
            this.token = token;
        }

        @Override
        public long getToken() {
            return token;
        }

        @Override
        public boolean isHeld() {
            return !released && session.isAlive();
        }

        @Override
        public void release() {
            synchronized (this) {
                if (released) {
                    return;
                }
                released = true;
            }
            // The revocation is scoped to this holder's lease, so it deletes exactly this
            // holder's record - even if the release races a new holder at the same path, or a
            // partition outlasted the ttl and the lease already died underneath.
            retireLeaseOf(session);
        }

        @Override
        public void close() {
            release();
        }
    }
}
