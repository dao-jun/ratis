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

import org.apache.ratis.metadata.protocol.ChangeEvent;
import org.apache.ratis.metadata.util.AsyncDriver;
import org.apache.ratis.metadata.util.ExceptionUtils;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.ratis.metadata.util.Preconditions.checkArgument;
import static org.apache.ratis.metadata.util.Preconditions.checkNotNull;
import static org.apache.ratis.metadata.util.Preconditions.checkState;

/**
 * Continuously polls changes, delivering an ordered, gap-explained stream to {@link Listener}.
 *
 * <p>The watcher owns no thread: one poll at a time runs on the client's asynchronous machinery,
 * and each completion schedules the next step on the module's shared scheduler. Delivery is
 * single-flight - one outstanding poll plus one outstanding delivery unit: a non-empty poll
 * creates one delivery unit, and the next poll is scheduled only from that unit's completion,
 * never at queue time. {@link #lastDeliveredRevision()} therefore names a batch the listener
 * actually returned from, not one merely queued.
 *
 * <p>Callbacks run on the client's delivery pool (or on the {@link Builder#setExecutor executor}
 * supplied instead) - never on the client's completion executor, whose two threads settle every
 * async reply: listeners parked there would starve the settles, and a lease renewal whose reply
 * never settles stops its session renewing - the lease dies while its holder blocks on something
 * else entirely. Keep callbacks short anyway, and never let one block waiting on another
 * watcher's delivery (a lock acquire included): default-executor watchers of one client share
 * its delivery pool, and such re-entrant waits can starve each other. Callbacks of one watcher
 * never overlap - not even on a multithreaded executor, because no second poll exists while a
 * delivery is outstanding. A listener Error on the client pool is logged and the worker
 * survives; on a supplied executor it surfaces wherever that executor puts it.
 *
 * <p>Admission: each callback unit (record batch or NEED_SNAPSHOT report) admits itself under a
 * short internal lock immediately before invoking the listener - a unit admitted only after
 * {@link #close()} never runs the listener, while one already admitted may run to completion
 * (its delivery still counts) but schedules nothing further.
 */
public final class MetadataWatcher implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataWatcher.class);

    /** Queue waits this long mean the delivery threads are parked, not slow. */
    private static final long QUEUED_DELIVERY_WARN_MS = 10_000L;

    /** Receives the watcher's stream; see {@link MetadataWatcher}'s javadoc for the contract. */
    public interface Listener {

        /** One change record, in revision order (per the executor caveat in the class javadoc). */
        void onRecord(ChangeEvent event);

        /** Retention line hit, loop stopped; decide: {@link #restartFrom(long)} or rebuild. */
        void onNeedSnapshot(long oldestRetainedRevision);
    }

    private final RaftMetadataClient client;
    private final Listener listener;

    /** Where delivery units run: the builder's executor, or the client's delivery pool. */
    private final Executor executor;

    /** True when the builder set no executor - the client's pool carries listener Errors for us. */
    private final boolean defaultDelivery;

    private final long pollIntervalMs;
    private final long maxPollBytes;

    /**
     * Linearizes admission, close and restart bookkeeping - never held across listener code or
     * a client call. Guards every mutable field below.
     */
    private final Object gate = new Object();

    /** The poll position; advances only past a batch the listener actually returned from. */
    private long checkpoint;

    private long lastDeliveredRevision;
    private boolean running;
    private boolean closed;
    private Throwable failure;

    /** The scheduler step that will fire the next poll. */
    private ScheduledFuture<?> nextStep;

    /** True from the NEED_SNAPSHOT stop until its report completed (or was dropped/rejected). */
    private boolean needSnapshotReportOutstanding;

    /** Last-wins restart checkpoint recorded while {@link #needSnapshotReportOutstanding}. */
    private Long pendingRestartRevision;

    private MetadataWatcher(RaftMetadataClient client, Listener listener, Executor executor, long pollIntervalMs,
            long maxPollBytes, long sinceRevision) {
        this.client = client;
        this.listener = listener;
        this.defaultDelivery = executor == null;
        this.executor = defaultDelivery ? client.deliveryExecutor() : executor;
        this.pollIntervalMs = pollIntervalMs;
        this.maxPollBytes = maxPollBytes;
        this.checkpoint = sinceRevision;
        this.lastDeliveredRevision = sinceRevision;
    }

    /** A watcher builder; required: the client and the listener. */
    public static Builder newBuilder(RaftMetadataClient client, Listener listener) {
        return new Builder(client, listener);
    }

    /** The builder of {@link MetadataWatcher}; see {@link #newBuilder}. */
    public static final class Builder {

        private final RaftMetadataClient client;
        private final Listener listener;

        private long sinceRevision;
        private long pollIntervalMs = 200;
        private long maxPollBytes;
        private Executor executor;

        private Builder(RaftMetadataClient client, Listener listener) {
            this.client = checkNotNull(client, "client");
            this.listener = checkNotNull(listener, "listener");
        }

        /** The revision to resume from (exclusive); 0 starts at the oldest retained record. */
        public Builder setSinceRevision(long sinceRevision) {
            checkArgument(sinceRevision >= 0, "sinceRevision must be >= 0, got %s", sinceRevision);
            this.sinceRevision = sinceRevision;
            return this;
        }

        /** How long an idle fetch loop waits before re-polling; must be positive. */
        public Builder setPollInterval(Duration pollInterval) {
            checkNotNull(pollInterval, "pollInterval");
            long ms = pollInterval.toMillis();
            checkArgument(ms > 0, "pollInterval must be positive, got %s", pollInterval);
            this.pollIntervalMs = ms;
            return this;
        }

        /** The byte bound for {@link RaftMetadataClient#poll}; 0 selects the codec's bound. */
        public Builder setMaxPollBytes(long maxPollBytes) {
            checkArgument(maxPollBytes >= 0, "maxPollBytes must be >= 0, got %s", maxPollBytes);
            this.maxPollBytes = maxPollBytes;
            return this;
        }

        /**
         * Where {@link Listener} callbacks run, one delivery unit at a time; default is the
         * client's delivery pool, isolated from the client's completion executor - give a
         * blocking or re-entrant listener its own executor. The poll chain waits for each
         * callback to return before polling again.
         */
        public Builder setExecutor(Executor executor) {
            this.executor = checkNotNull(executor, "executor");
            return this;
        }

        /** Builds the watcher (not yet started; call {@link MetadataWatcher#start()}). */
        public MetadataWatcher build() {
            return new MetadataWatcher(client, listener, executor, pollIntervalMs, maxPollBytes, sinceRevision);
        }
    }

    /** Starts the poll chain; at most one chain runs per watcher. */
    public void start() {
        synchronized (gate) {
            checkState(!closed, "MetadataWatcher is closed");
            checkState(!running, "MetadataWatcher is already running");
            startChainLocked(checkpoint);
        }
    }

    /**
     * Resumes the poll chain from {@code sinceRevision} (exclusive) after it stopped - from a
     * failure, or from the NEED_SNAPSHOT stop once its report completed. While the report is
     * still outstanding (queued or running) the request is recorded instead - last wins, no
     * exception - and exactly one new chain leaves from the last accepted revision once the
     * report completed; with no recorded request the chain stays stopped. A chain that is
     * polling rejects the request, as before.
     *
     * <p>One failure is not resumable here: a chain stopped because the client's async
     * pipeline failed permanently with no way back (see the transport failure in the README's
     * failure contract) stays bound to that dead client, so a restart stops the same way -
     * build a new watcher on a rebuilt client from {@link #lastDeliveredRevision()} instead.
     */
    public void restartFrom(long sinceRevision) {
        checkArgument(sinceRevision >= 0, "sinceRevision must be >= 0, got %s", sinceRevision);
        synchronized (gate) {
            checkState(!closed, "MetadataWatcher is closed");
            checkState(!running, "MetadataWatcher is still running");
            if (needSnapshotReportOutstanding) {
                pendingRestartRevision = sinceRevision;
                return;
            }
            startChainLocked(sinceRevision);
        }
    }

    /** Whether a poll chain is running (false after a NEED_SNAPSHOT stop, a failure or a close). */
    public boolean isRunning() {
        synchronized (gate) {
            return running;
        }
    }

    /** Failure that stopped the chain, or null; checkpoint in {@link #lastDeliveredRevision()}. */
    public Throwable getFailure() {
        synchronized (gate) {
            return failure;
        }
    }

    /** Revision of the last delivered record, or the start revision when none yet. */
    public long lastDeliveredRevision() {
        synchronized (gate) {
            return lastDeliveredRevision;
        }
    }

    /**
     * Cancels the next poll and stops the deliveries; idempotent, never blocks. A poll already
     * in flight completes and its records are dropped; a callback unit not yet admitted never
     * runs, while one already admitted may finish - its delivery still counts - but nothing
     * further is scheduled. A pending NEED_SNAPSHOT restart is cleared.
     */
    @Override
    public void close() {
        synchronized (gate) {
            if (closed) {
                return;
            }
            closed = true;
            running = false;
            pendingRestartRevision = null;
            if (nextStep != null) {
                nextStep.cancel(false);
                nextStep = null;
            }
        }
    }

    /** Starts a new chain from {@code sinceRevision}; the caller holds {@code gate}. */
    private void startChainLocked(long sinceRevision) {
        checkpoint = sinceRevision;
        lastDeliveredRevision = sinceRevision;
        running = true;
        failure = null;
        scheduleNextLocked(0);
    }

    /**
     * Fires one poll on the shared scheduler's thread - never on a poll's own completion thread:
     * the send itself is enqueued on the client's sender thread, the only thread allowed to
     * park in ratis' outstanding-request backpressure, so a wedged pipeline delays this
     * watcher's poll alone - never the scheduler every other component's ticks run on, and
     * never the completion threads that settle the very replies which free the permits.
     */
    private void pollNow() {
        final long from;
        synchronized (gate) {
            if (closed || !running) {
                return;
            }
            from = checkpoint;
        }
        client.pollAsync(from, maxPollBytes).whenComplete(this::onPoll);
    }

    private void onPoll(RaftMetadataClient.PollResult polled, Throwable pollFailure) {
        synchronized (gate) {
            if (closed) {
                if (pollFailure == null && !polled.getRecords().isEmpty()) {
                    LOG.warn("Dropping the {} change record(s) of a poll that returned after the"
                                    + " watcher closed; no further records will be delivered",
                            polled.getRecords().size());
                }
                return;
            }
            if (pollFailure == null && polled.getRecords().isEmpty()) {
                // Advance past record-less revisions; max() - a leader change cannot regress it.
                checkpoint = Math.max(checkpoint, polled.getWatermark());
                scheduleNextLocked(pollIntervalMs);
                return;
            }
        }
        if (pollFailure != null) {
            // The client's async twins wrap their failures; classify the cause underneath.
            onFailure(ExceptionUtils.stripCompletionException(pollFailure));
        } else {
            deliver(polled);
        }
    }

    private void onFailure(Throwable cause) {
        if (cause instanceof NeedSnapshotException) {
            stopAfterNeedSnapshot(((NeedSnapshotException) cause).getOldestRetainedRevision());
        } else if (cause instanceof UndecodableReplyException) {
            stopAfterFailure(cause, "poll replies are undecodable by this build, no further records will be"
                    + " delivered until the process speaks the leader's protocol");
        } else if (cause instanceof RaftMetadataException) {
            // No protocol failure mode besides NEED_SNAPSHOT; log and idle rather than die.
            LOG.warn("Polling for changes failed with {}; retrying",
                    ((RaftMetadataException) cause).getStatus(), cause);
            scheduleRetry(pollIntervalMs);
        } else if (cause instanceof MetadataTransportException || cause instanceof IllegalStateException) {
            // Client closed; idling would loop forever - close watchers before their client.
            if (cause instanceof IllegalStateException && client.isClosed()) {
                stopAfterFailure(cause, "the metadata client closed underneath the watcher; close watchers before"
                        + " their client");
                return;
            }
            if (ExceptionUtils.causedBy(cause, AlreadyClosedException.class)
                    && (client.isClosed() || !client.canRebuildRaftClient())) {
                // The client's ordered async pipeline is dead for good, with no way back. Ratis
                // fails an async request terminally only together with failAllAsyncRequests
                // (OrderedAsync.handleException: closed client, non-retryable, or budget
                // exhausted), which sticks the failure into the sliding window so every later
                // async call of this client fails fast with AlreadyClosedException. An
                // AlreadyClosed from the transport itself (thrown to trigger reconnection) only
                // ever reaches here as the last attempt inside that same terminal call - until
                // then it is an ordinary retryable IOException. So this cause cannot mean an
                // outage that waiting heals; on a closed or non-rebuildable client, polling
                // again can never succeed. This watcher stays bound to this client, so
                // restartFrom cannot revive it - the resume is a new watcher on a rebuilt
                // client, started from lastDeliveredRevision().
                stopAfterFailure(cause,
                        "the client's async pipeline failed permanently (a request exhausted its"
                                + " finite retry budget, or the client closed); resume by"
                                + " building a new watcher on a rebuilt client from lastDeliveredRevision()");
                return;
            }
            // Longer outage, replies persistently rejected as malformed, or the poisoned window
            // of an open client that rebuilds its pipeline: ratis already retried in-call, an
            // arriving reply keeps the pipeline healthy, and a rebuilt one carries the next
            // poll - so a later poll may yet succeed. Keep polling after the interval (the
            // README failure contract documents this: isRunning() stays true, no failure is
            // set).
            LOG.debug("Polling for changes failed; retrying", cause);
            scheduleRetry(pollIntervalMs);
        } else {
            // Nothing else is expected off the client's future; stopping beats a hot loop.
            stopAfterFailure(cause, "the poll failed unexpectedly");
        }
    }

    /**
     * One delivery unit: the record batch of one non-empty poll. The unit admits itself under
     * {@code gate} strictly before invoking the listener and completes under {@code gate} again
     * - only there do the watermark and the next poll move, which is what keeps the chain
     * single-flight even on a multithreaded executor. The completion carries the highest
     * revision the listener completed: a record whose {@code onRecord} threw an Error never
     * counts, so the chain resumes from it instead of claiming the undelivered tail - a
     * per-record Exception keeps the log-and-continue policy and still counts.
     */
    private void deliver(final RaftMetadataClient.PollResult polled) {
        final List<ChangeEvent> records = polled.getRecords();
        final long lastRecordRevision = records.get(records.size() - 1).getRevision();
        final long resumeFrom;
        synchronized (gate) {
            resumeFrom = checkpoint;
        }
        final long[] completedThrough = {resumeFrom};
        runUnit(() -> {
            for (ChangeEvent event : records) {
                try {
                    listener.onRecord(event);
                } catch (Exception e) {
                    LOG.warn(
                            "Change listener failed on revision {}; continuing",
                            event.getRevision(),
                            e);
                }
                // Reached on a normal return and on a logged-and-continued Exception;
                // an Error skips it, freezing the count at the last completed record.
                completedThrough[0] = event.getRevision();
            }
        }, () -> completeDelivery(completedThrough[0]), "delivery of revision " + lastRecordRevision);
    }

    /** The delivery unit completed: the watermark moves to what was actually delivered. */
    private void completeDelivery(final long completedThrough) {
        synchronized (gate) {
            // Single-flight pins lastDeliveredRevision at the batch's resume point here, so an
            // unchanged value means nothing flowed - the listener died on the first record.
            final boolean advanced = completedThrough != lastDeliveredRevision;
            lastDeliveredRevision = completedThrough;
            checkpoint = completedThrough;
            // Records flowed: the next poll leaves immediately, keeping up with a busy stream.
            // Nothing flowed: re-polling the identical batch immediately would hammer the raft
            // group and the failing listener round-trip fast - pace at the configured interval.
            scheduleNextLocked(advanced ? 0 : pollIntervalMs);
        }
    }

    /**
     * The retention line: the chain stops and the report goes through the same admission gate
     * as a record batch. Restart requests arriving while the report is outstanding are recorded
     * (last wins); the completed report starts the one new chain from the last accepted
     * checkpoint, or leaves the chain stopped when none was requested.
     */
    private void stopAfterNeedSnapshot(final long oldestRetainedRevision) {
        final long stoppedAt;
        synchronized (gate) {
            if (closed) {
                LOG.warn("Dropping the NEED_SNAPSHOT report of revision {} of a poll that returned"
                        + " after the watcher closed", oldestRetainedRevision);
                return;
            }
            running = false;
            needSnapshotReportOutstanding = true;
            stoppedAt = checkpoint;
        }
        runUnit(() -> {
                    try {
                        listener.onNeedSnapshot(oldestRetainedRevision);
                    } catch (Exception e) {
                        LOG.warn("Change listener failed on NEED_SNAPSHOT", e);
                    }
                }, this::completeNeedSnapshotReport, "NEED_SNAPSHOT report of revision " + oldestRetainedRevision);
        LOG.info("Metadata watcher stopped at checkpoint {}: records below revision {} are gone",
                stoppedAt, oldestRetainedRevision);
    }

    /** The report completed: start the last accepted restart, or stay stopped. */
    private void completeNeedSnapshotReport() {
        synchronized (gate) {
            needSnapshotReportOutstanding = false;
            if (closed) {
                pendingRestartRevision = null;
                return;
            }
            final Long restart = pendingRestartRevision;
            pendingRestartRevision = null;
            // A chain started manually while the report ran outranks the recorded request.
            if (restart != null && !running) {
                LOG.info("Metadata watcher resuming from revision {} after NEED_SNAPSHOT", restart);
                startChainLocked(restart);
            }
        }
    }

    private void stopAfterFailure(final Throwable cause, final String why) {
        synchronized (gate) {
            // Failure first: a caller observing the stopped chain must already see the cause.
            failure = cause;
            running = false;
            LOG.warn("Metadata watcher stopped at checkpoint {}: {}", checkpoint, why, cause);
        }
    }

    /**
     * Admits one callback unit under {@code gate} (never while closed), runs the listener work
     * strictly outside it, then runs the completion - which takes {@code gate} itself - from a
     * {@code finally}, so every exit of the listener releases the gate and reschedules. A unit
     * not admitted before a close never invokes the listener; an admitted one may still
     * complete after the close.
     */
    private void runUnit(final Runnable listenerWork, final Runnable completion, final String what) {
        final long queuedAtNanos = System.nanoTime();
        final Runnable unit = () -> {
            warnIfDeliveryWaited(queuedAtNanos, what);
            synchronized (gate) {
                if (closed) {
                    // Close won the race: this unit was never admitted - no listener
                    // call, no state advance, nothing scheduled.
                    return;
                }
            }
            try {
                runListenerWork(listenerWork, what);
            } finally {
                completion.run();
            }
        };
        try {
            executor.execute(unit);
        } catch (RejectedExecutionException e) {
            onExecutorRejection(e, what);
        }
    }

    /**
     * The client pool's worker survives a dying listener - the Error is logged, matching the
     * quiet containment the old inline default landed in. An executor the embedder supplied
     * keeps the contract: the Error surfaces wherever that executor puts it.
     */
    private void runListenerWork(final Runnable listenerWork, final String what) {
        if (!defaultDelivery) {
            listenerWork.run();
            return;
        }
        try {
            listenerWork.run();
        } catch (Throwable t) {
            LOG.warn("The change listener died with an Error during {}; the worker survives", what, t);
        }
    }

    /**
     * Warns when this unit sat queued beyond the wait bound: the delivery threads are parked by
     * blocked listener callbacks, so other callbacks of the same client are being delayed.
     */
    private static void warnIfDeliveryWaited(long queuedAtNanos, String what) {
        final long waitedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - queuedAtNanos);
        if (waitedMs > QUEUED_DELIVERY_WARN_MS) {
            LOG.warn("The {} waited {} ms for a delivery thread; a blocked listener callback is"
                            + " delaying other callbacks - hand blocking watchers an explicit executor",
                    what, waitedMs);
        }
    }

    /** The executor refused the unit: the chain stops with the rejection as its failure. */
    private void onExecutorRejection(final RejectedExecutionException cause, final String what) {
        synchronized (gate) {
            failure = cause;
            running = false;
            needSnapshotReportOutstanding = false;
            pendingRestartRevision = null;
            LOG.warn("Metadata watcher stopped at checkpoint {}: the executor rejected the {}",
                    checkpoint, what, cause);
        }
    }

    /** Schedules the next poll from the shared scheduler; the caller holds {@code gate}. */
    private void scheduleNextLocked(long delayMs) {
        if (closed || !running) {
            return;
        }
        nextStep = AsyncDriver.schedule(this::pollNow, delayMs);
    }

    /** Schedules a retry poll under the gate; a no-op once stopped or closed. */
    private void scheduleRetry(long delayMs) {
        synchronized (gate) {
            scheduleNextLocked(delayMs);
        }
    }
}
