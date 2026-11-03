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
import org.apache.ratis.metadata.statemachine.ChangeEvent;
import org.apache.ratis.metadata.statemachine.RaftResult;
import org.apache.ratis.metadata.util.AsyncDriver;
import org.apache.ratis.metadata.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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
 * wait for the record's {@code DELETE} change (a watchdog tick re-attempts regardless, so a
 * missed event can never wedge a waiter). A contender whose own lease expired underneath
 * ({@code UNKNOWN_LEASE} from the create) reopens one and keeps waiting. Releasing deletes the
 * record for a prompt handover; a crashed holder's record is deleted by the leader's expiry
 * scheduler within roughly one ttl.
 *
 * <p>{@link Hold#getToken()} is a fencing token: after each successful create the lock bumps a
 * counter named after the lock path, so tokens are strictly increasing across holders. External
 * resources guarded by the lock should reject work carrying a token lower than the last seen one
 * - during a lease expiry the old holder may briefly believe it still holds.
 *
 * <p>The lock is neither reentrant nor fair: all waiters wake on every release and one create
 * wins. One acquire per {@link MetadataLock} instance; the record at the path and the counter of
 * the same name belong to the lock exclusively. {@link #acquireAsync()} never throws and never
 * blocks - the acquire is an event-driven state machine on the client's asynchronous calls, and
 * every failure lands in the future as {@link RaftMetadataException} (misconfiguration), a
 * {@code MetadataTransportException} (the request never went through), or a {@link
 * TimeoutException}.
 */
public final class MetadataLock implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataLock.class);

    /** The longest gap between two watchdog wake-ups, whatever the ttl. */
    private static final long MIN_WATCHDOG_MS = 1000;

    private static final byte[] EMPTY = new byte[0];

    private final RaftMetadataClient client;
    private final String path;
    private final long ttlMs;

    /** The acquire state machine; every field below runs under this lock. */
    private final Object state = new Object();
    private CompletableFuture<Hold> pending;
    private boolean acquireAttempted;
    private boolean stopped;
    /** True while parked on the change stream; a wake or the watchdog un-parks an attempt. */
    private boolean waiting;
    /** True once this contender's create committed; only then may the undo delete the record. */
    private boolean held;
    /** True while a create may have committed without its outcome being known. */
    private boolean ambiguous;
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
     * {@link RaftMetadataException}, a {@link MetadataTransportException}, or an {@link
     * IllegalStateException} after {@link #close()}.
     */
    public CompletableFuture<Hold> acquireAsync() {
        return acquireAsync(null);
    }

    /**
     * Acquires the lock with a deadline; on expiry the future completes with {@link
     * TimeoutException} and the contender unwinds - a create that committed anyway is undone, so
     * a timed-out contender never leaks the lock.
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
            acquireAttempted = true;
            pending = future = new CompletableFuture<>();
            if (timeout != null) {
                timeoutTask =
                        AsyncDriver.schedule(
                                () -> fail(new TimeoutException("Acquiring " + path + " timed out")),
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
        return cause instanceof RuntimeException
                ? (RuntimeException) cause
                : new IllegalStateException("The acquire failed unexpectedly", cause);
    }

    /**
     * Cancels a pending acquire (its future completes exceptionally); a no-op once settled - the
     * {@link Hold} owns its lease. Idempotent.
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
     * Materializes the parent chain once, then fixes the watch position; {@code NODE_EXISTS}
     * just means the parent is already there.
     */
    private void begin() {
        if (settled()) {
            return;
        }
        final int slash = path.lastIndexOf('/');
        final String parent = slash <= 0 ? "/" : path.substring(0, slash);
        if (parent.equals("/")) {
            takeWatchPosition();
            return;
        }
        client.createAsync(parent, EMPTY, true)
                .whenComplete(
                        (ignored, failure) -> {
                            if (settled()) {
                                return;
                            }
                            // The parent being there already and being created are one state.
                            if (failure == null
                                    || isRejectionOf(
                                            unwrap(failure), RaftResult.Status.NODE_EXISTS)) {
                                takeWatchPosition();
                                return;
                            }
                            fail(unwrap(failure));
                        });
    }

    /**
     * The watch position is taken before the first create attempt: any delete at or after that
     * revision is delivered by the watcher, so no release can slip between attempt and watch.
     */
    private void takeWatchPosition() {
        if (settled()) {
            return;
        }
        client.revisionAsync()
                .whenComplete(
                        (head, failure) -> {
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
        final MetadataWatcher started =
                MetadataWatcher.newBuilder(
                                client,
                                new MetadataWatcher.Listener() {
                                    @Override
                                    public void onRecord(ChangeEvent event) {
                                        for (ChangeEvent.Change change : event.getChanges()) {
                                            if (change.getOp() == ChangeEvent.ChangeOp.DELETE
                                                    && path.equals(change.getPath())) {
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
        client.openLeaseAsync(Duration.ofMillis(ttlMs))
                .whenComplete(
                        (opened, failure) -> {
                            if (failure != null) {
                                fail(unwrap(failure));
                                return;
                            }
                            synchronized (state) {
                                if (settled()) {
                                    // The winner of the settle owns the cleanup; this lease just
                                    // opened and belongs to no one - close it here.
                                    opened.close();
                                    return;
                                }
                                session = opened;
                            }
                            tryCreate(opened);
                        });
    }

    private void tryCreate(final LeaseSession current) {
        synchronized (state) {
            if (settled()) {
                return;
            }
            ambiguous = true;
        }
        client.createAsync(path, EMPTY, current)
                .whenComplete(
                        (ignored, failure) -> onCreateOutcome(current, unwrap(failure)));
    }

    private void onCreateOutcome(LeaseSession current, Throwable failure) {
        if (failure == null) {
            synchronized (state) {
                ambiguous = false;
                held = true;
                if (settled()) {
                    // The settle path saw the create in flight and issued its undo; the client's
                    // ordered pipeline runs that delete after this committed create.
                    return;
                }
            }
            takeToken(current);
            return;
        }
        if (isRejectionOf(failure, RaftResult.Status.NODE_EXISTS)) {
            synchronized (state) {
                ambiguous = false;
                if (settled()) {
                    return;
                }
                waiting = true;
                watchdog =
                        AsyncDriver.schedule(
                                this::onWatchdog, Math.max(MIN_WATCHDOG_MS, ttlMs / 3));
            }
            return;
        }
        if (isRejectionOf(failure, RaftResult.Status.UNKNOWN_LEASE)) {
            // The contender's own lease died (a partition outlasted the ttl); a new one keeps it
            // in the race. This create was rejected, not committed: nothing to undo.
            synchronized (state) {
                ambiguous = false;
                if (settled()) {
                    return;
                }
                session = null;
            }
            current.close();
            AsyncDriver.schedule(this::attempt, 0);
            return;
        }
        if (!(failure instanceof RaftMetadataException)) {
            // Transport trouble or a closed client: the create's outcome is unknown - leave
            // {@code ambiguous} set so the undo covers a create that committed anyway.
            fail(failure);
            return;
        }
        synchronized (state) {
            // A definitive rejection: this contender's create never committed, nothing to undo.
            ambiguous = false;
        }
        fail(failure);
    }

    /** The record exists; the counter bump of the same name yields the fencing token. */
    private void takeToken(final LeaseSession current) {
        client.incrementAsync(path, 1)
                .whenComplete(
                        (token, failure) -> {
                            if (failure != null) {
                                fail(unwrap(failure));
                                return;
                            }
                            final MetadataWatcher toClose;
                            final ScheduledFuture<?> toCancel;
                            synchronized (state) {
                                if (!pending.complete(new HoldImpl(current, token))) {
                                    // A timeout undid this contender mid-token; its cleanup ran.
                                    return;
                                }
                                toClose = watcher;
                                watcher = null;
                                toCancel = timeoutTask;
                                timeoutTask = null;
                            }
                            if (toClose != null) {
                                toClose.close();
                            }
                            if (toCancel != null) {
                                toCancel.cancel(false);
                            }
                        });
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
     * first settler owns the unwind. The undo delete runs only when this contender created the
     * record or a create's outcome is unknown: a waiting contender must leave the holder's
     * record alone.
     */
    private void fail(Throwable failure) {
        final boolean undo;
        final LeaseSession toClose;
        final MetadataWatcher watcherToClose;
        synchronized (state) {
            if (pending == null || !pending.completeExceptionally(failure)) {
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
            undo = held || ambiguous;
            toClose = session;
            session = null;
            watcherToClose = watcher;
            watcher = null;
        }
        if (undo) {
            undoRecord();
        }
        if (toClose != null) {
            toClose.close();
        }
        if (watcherToClose != null) {
            watcherToClose.close();
        }
    }

    /** The best-effort record undo of a failed or overtaken acquire; failures only log. */
    private void undoRecord() {
        client.deleteAsync(path)
                .whenComplete(
                        (ignored, failure) -> {
                            if (failure == null) {
                                return;
                            }
                            if (isRejectionOf(unwrap(failure), RaftResult.Status.NOT_FOUND)) {
                                return;
                            }
                            LOG.warn(
                                    "Undoing the lock record {} failed; the closed lease frees"
                                            + " it within one ttl",
                                    path,
                                    unwrap(failure));
                        });
    }

    private static boolean isRejectionOf(Throwable failure, RaftResult.Status status) {
        return failure instanceof RaftMetadataException
                && ((RaftMetadataException) failure).getStatus() == status;
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
         * Releases the lock: deletes the record for a prompt handover and stops the renewals -
         * idempotent, never throws. A delete that cannot reach the cluster falls back to the
         * crash-release path (the record dies within one ttl).
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
            client.deleteAsync(path)
                    .whenComplete(
                            (ignored, failure) -> {
                                if (failure == null) {
                                    return;
                                }
                                if (isRejectionOf(unwrap(failure), RaftResult.Status.NOT_FOUND)) {
                                    return;
                                }
                                LOG.warn(
                                        "Releasing {} did not reach the cluster; the lease frees it"
                                                + " within one ttl",
                                        path,
                                        unwrap(failure));
                            });
            session.close();
        }

        @Override
        public void close() {
            release();
        }
    }
}
