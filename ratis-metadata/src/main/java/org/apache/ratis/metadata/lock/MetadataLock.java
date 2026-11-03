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
import org.apache.ratis.metadata.client.MetadataTransportException;
import org.apache.ratis.metadata.client.MetadataWatcher;
import org.apache.ratis.metadata.client.RaftMetadataClient;
import org.apache.ratis.metadata.client.RaftMetadataException;
import org.apache.ratis.metadata.statemachine.ChangeEvent;
import org.apache.ratis.metadata.statemachine.RaftResult;
import org.apache.ratis.metadata.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
 * the same name belong to the lock exclusively. {@link #acquireAsync()} never throws - every
 * failure lands in the future as {@link RaftMetadataException} (misconfiguration),
 * {@link MetadataTransportException} (the request never went through), or {@link TimeoutException}.
 */
public final class MetadataLock implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataLock.class);

    /** The longest gap between two watchdog wake-ups, whatever the ttl. */
    private static final long MIN_WATCHDOG_MS = 1000;

    /** One shared daemon scheduler for acquire timeouts; Java 8 futures have none built in. */
    private static final ScheduledExecutorService TIMEOUT_SCHEDULER =
            Executors.newSingleThreadScheduledExecutor(
                    new NamedThreadFactory("metadata-lock-timeout", true));

    private static final byte[] EMPTY = new byte[0];

    private final RaftMetadataClient client;
    private final String path;
    private final long ttlMs;

    /** Guards the lifecycle flags; the waiter sleeps on it until a matching change or a stop. */
    private final Object signal = new Object();
    private boolean wakeSignalled;
    private boolean stopped;
    private boolean acquireAttempted;
    private CompletableFuture<Hold> pending;

    /** Set by the waiter before the watcher starts; the listener callbacks read it after. */
    private MetadataWatcher watcher;

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
     * TimeoutException} and the waiter unwinds - a create that committed anyway is undone, so a
     * timed-out contender never leaks the lock.
     */
    public CompletableFuture<Hold> acquireAsync(Duration timeout) {
        final CompletableFuture<Hold> future;
        synchronized (signal) {
            if (acquireAttempted) {
                throw new IllegalStateException("acquire was already attempted on this lock");
            }
            acquireAttempted = true;
            pending = future = new CompletableFuture<>();
        }
        if (timeout != null) {
            long ms = timeout.toMillis();
            checkArgument(ms > 0, "timeout must be positive, got %s", timeout);
            TIMEOUT_SCHEDULER.schedule(
                    () -> stop(future, new TimeoutException("Acquiring " + path + " timed out")),
                    ms,
                    TimeUnit.MILLISECONDS);
        }
        final Thread waiter = new Thread(() -> runAcquire(future), "metadata-lock:" + path);
        waiter.setDaemon(true);
        waiter.start();
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
        stop(pendingFuture(), new IllegalStateException("The lock was closed while acquiring"));
    }

    private CompletableFuture<Hold> pendingFuture() {
        synchronized (signal) {
            return pending;
        }
    }

    /**
     * Settles the future with {@code failure} and stops the waiter, unless the future is already
     * settled - a waiter that nonetheless ends up holding the record undoes itself.
     */
    private void stop(CompletableFuture<Hold> future, Throwable failure) {
        if (future == null || !future.completeExceptionally(failure)) {
            return;
        }
        synchronized (signal) {
            stopped = true;
            signal.notifyAll();
        }
    }

    // ------------------------------------------------------------------
    // the waiter state machine - one daemon thread per acquiring lock
    // ------------------------------------------------------------------

    private void runAcquire(CompletableFuture<Hold> future) {
        LeaseSession session = null;
        boolean held = false;
        // True while a create may have committed without its outcome being known: only then may
        // the undo delete the record - a contender that never created must never delete the
        // holder's record.
        boolean ambiguous = false;
        // True once the future took the session; the hold owns the lease from there on.
        boolean handed = false;
        try {
            ensureParent();
            startWatcher();
            while (!isStopped()) {
                if (session == null) {
                    session = client.openLease(Duration.ofMillis(ttlMs));
                }
                try {
                    ambiguous = true;
                    client.create(path, EMPTY, session);
                    ambiguous = false;
                    held = true;
                    break;
                } catch (RaftMetadataException e) {
                    ambiguous = false;
                    if (e.getStatus() == RaftResult.Status.NODE_EXISTS) {
                        awaitWake();
                    } else if (e.getStatus() == RaftResult.Status.UNKNOWN_LEASE) {
                        // The contender's own lease died (a partition outlasted the ttl); a new
                        // one keeps it in the race.
                        session.close();
                        session = null;
                    } else {
                        throw e;
                    }
                }
            }
            if (held) {
                final long token = client.increment(path, 1);
                handed = future.complete(new HoldImpl(session, token));
            }
        } catch (RuntimeException failure) {
            future.completeExceptionally(failure);
        } finally {
            if (!handed) {
                // The record may exist only when this waiter created it or a create's outcome is
                // unknown; a waiting contender must leave the holder's record alone.
                if (held || ambiguous) {
                    undoRecord();
                }
                if (session != null) {
                    session.close();
                }
            }
            if (watcher != null) {
                watcher.close();
            }
        }
    }

    private boolean isStopped() {
        synchronized (signal) {
            return stopped;
        }
    }

    /**
     * The watch position is taken before the first create attempt: any delete at or after that
     * revision is delivered by the poll loop, so no release can slip between attempt and watch.
     */
    private void startWatcher() {
        final long head = client.revision();
        watcher =
                MetadataWatcher.newBuilder(
                                client,
                                new MetadataWatcher.Listener() {
                                    @Override
                                    public void onRecord(ChangeEvent event) {
                                        for (ChangeEvent.Change change : event.getChanges()) {
                                            if (change.getOp() == ChangeEvent.ChangeOp.DELETE
                                                    && path.equals(change.getPath())) {
                                                wake();
                                                return;
                                            }
                                        }
                                    }

                                    @Override
                                    public void onNeedSnapshot(long oldestRetainedRevision) {
                                        // The wait position fell below the retention line and the
                                        // loop stopped; resume from the line - the waiter's next
                                        // create attempt covers the gap.
                                        if (watcher != null) {
                                            watcher.restartFrom(oldestRetainedRevision);
                                        }
                                        wake();
                                    }
                                })
                        .setSinceRevision(head)
                        .build();
        watcher.start();
    }

    private void wake() {
        synchronized (signal) {
            wakeSignalled = true;
            signal.notifyAll();
        }
    }

    /**
     * Sleeps until the record's delete arrives, the watchdog tick fires (a re-attempt whatever
     * the events), or the acquire is stopped.
     */
    private void awaitWake() {
        final long deadline = System.currentTimeMillis() + Math.max(MIN_WATCHDOG_MS, ttlMs / 3);
        synchronized (signal) {
            while (!wakeSignalled && !stopped) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    return;
                }
                try {
                    signal.wait(remaining);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            wakeSignalled = false;
        }
    }

    /** Materializes the parent chain once; {@code NODE_EXISTS} just means it is already there. */
    private void ensureParent() {
        int slash = path.lastIndexOf('/');
        final String parent = slash <= 0 ? "/" : path.substring(0, slash);
        if (parent.equals("/")) {
            return;
        }
        try {
            client.create(parent, EMPTY, true);
        } catch (RaftMetadataException alreadyThere) {
            if (alreadyThere.getStatus() != RaftResult.Status.NODE_EXISTS) {
                throw alreadyThere;
            }
        }
    }

    /** The best-effort record undo of a failed or overtaken acquire; never throws. */
    private void undoRecord() {
        try {
            client.delete(path);
        } catch (RaftMetadataException e) {
            if (e.getStatus() != RaftResult.Status.NOT_FOUND) {
                LOG.warn("Undoing the lock record {} failed with {}", path, e.getStatus());
            }
        } catch (MetadataTransportException | IllegalStateException e) {
            LOG.warn(
                    "Undoing the lock record {} failed; the closed lease frees it within one ttl",
                    path,
                    e);
        }
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
            try {
                client.delete(path);
            } catch (RaftMetadataException e) {
                if (e.getStatus() != RaftResult.Status.NOT_FOUND) {
                    LOG.warn("Releasing {} failed with {}", path, e.getStatus());
                }
            } catch (MetadataTransportException | IllegalStateException e) {
                LOG.warn(
                        "Releasing {} did not reach the cluster; the lease frees it within one ttl",
                        path,
                        e);
            }
            session.close();
        }

        @Override
        public void close() {
            release();
        }
    }
}
