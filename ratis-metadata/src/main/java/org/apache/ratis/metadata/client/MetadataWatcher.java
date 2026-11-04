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
import org.apache.ratis.metadata.util.AsyncDriver;
import org.apache.ratis.metadata.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;

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
 * <p>Callbacks run inline on the client's completion executor (or on the {@link Builder#setExecutor
 * executor} supplied instead) - keep them short: a blocking callback parks its thread until it
 * returns, and on the client's completion executor that also delays the client's other
 * asynchronous calls. Callbacks of one watcher never overlap - not even on a multithreaded
 * executor, because no second poll exists while a delivery is outstanding.
 *
 * <p>Admission: each callback unit (record batch or NEED_SNAPSHOT report) admits itself under a
 * short internal lock immediately before invoking the listener - a unit admitted only after
 * {@link #close()} never runs the listener, while one already admitted may run to completion
 * (its delivery still counts) but schedules nothing further.
 */
public final class MetadataWatcher implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataWatcher.class);

    /** Receives the watcher's stream; see {@link MetadataWatcher}'s javadoc for the contract. */
    public interface Listener {

        /** One change record, in revision order (per the executor caveat in the class javadoc). */
        void onRecord(ChangeEvent event);

        /** Retention line hit, loop stopped; decide: {@link #restartFrom(long)} or rebuild. */
        void onNeedSnapshot(long oldestRetainedRevision);
    }

    private final RaftMetadataClient client;
    private final Listener listener;
    private final Executor executor;
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

    private MetadataWatcher(
            RaftMetadataClient client,
            Listener listener,
            Executor executor,
            long pollIntervalMs,
            long maxPollBytes,
            long sinceRevision) {
        this.client = client;
        this.listener = listener;
        this.executor = executor;
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
         * Where {@link Listener} callbacks run, one delivery unit at a time; default is inline
         * on the client's completion executor - the poll chain waits for each callback to
         * return before polling again.
         */
        public Builder setExecutor(Executor executor) {
            this.executor = checkNotNull(executor, "executor");
            return this;
        }

        /** Builds the watcher (not yet started; call {@link MetadataWatcher#start()}). */
        public MetadataWatcher build() {
            return new MetadataWatcher(
                    client, listener, executor, pollIntervalMs, maxPollBytes, sinceRevision);
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
     * under ratis' async backpressure (the outstanding-requests semaphore) the send can block its
     * issuer, and blocking a completion thread risks wedging the very replies that free the
     * permits. The scheduler stalling instead only delays ticks, never deadlocks.
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
                    LOG.warn(
                            "Dropping the {} change record(s) of a poll that returned after the"
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
            stopAfterFailure(
                    cause,
                    "poll replies are undecodable by this build, no further records will be"
                            + " delivered until the process speaks the leader's protocol");
        } else if (cause instanceof RaftMetadataException) {
            // No protocol failure mode besides NEED_SNAPSHOT; log and idle rather than die.
            LOG.warn(
                    "Polling for changes failed with {}; retrying",
                    ((RaftMetadataException) cause).getStatus(),
                    cause);
            scheduleRetry(pollIntervalMs);
        } else if (cause instanceof MetadataTransportException
                || cause instanceof IllegalStateException) {
            // Client closed; idling would loop forever - close watchers before their client.
            if (cause instanceof IllegalStateException && client.isClosed()) {
                stopAfterFailure(
                        cause,
                        "the metadata client closed underneath the watcher; close watchers before"
                                + " their client");
                return;
            }
            // Longer outage: ratis already retried in-call; keep polling after the interval.
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
        runUnit(
                () -> {
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
                },
                () -> completeDelivery(completedThrough[0]),
                "delivery of revision " + lastRecordRevision);
    }

    /** The delivery unit completed: the watermark moves to what was actually delivered. */
    private void completeDelivery(final long completedThrough) {
        synchronized (gate) {
            lastDeliveredRevision = completedThrough;
            checkpoint = completedThrough;
            // Records flowed: the next poll leaves immediately, keeping up with a busy stream.
            scheduleNextLocked(0);
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
                LOG.warn(
                        "Dropping the NEED_SNAPSHOT report of revision {} of a poll that returned"
                                + " after the watcher closed",
                        oldestRetainedRevision);
                return;
            }
            running = false;
            needSnapshotReportOutstanding = true;
            stoppedAt = checkpoint;
        }
        runUnit(
                () -> {
                    try {
                        listener.onNeedSnapshot(oldestRetainedRevision);
                    } catch (Exception e) {
                        LOG.warn("Change listener failed on NEED_SNAPSHOT", e);
                    }
                },
                this::completeNeedSnapshotReport,
                "NEED_SNAPSHOT report of revision " + oldestRetainedRevision);
        LOG.info(
                "Metadata watcher stopped at checkpoint {}: records below revision {} are gone",
                stoppedAt,
                oldestRetainedRevision);
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
        final Runnable unit =
                () -> {
                    synchronized (gate) {
                        if (closed) {
                            // Close won the race: this unit was never admitted - no listener
                            // call, no state advance, nothing scheduled.
                            return;
                        }
                    }
                    try {
                        listenerWork.run();
                    } finally {
                        completion.run();
                    }
                };
        if (executor == null) {
            unit.run();
        } else {
            try {
                executor.execute(unit);
            } catch (RejectedExecutionException e) {
                onExecutorRejection(e, what);
            }
        }
    }

    /** The executor refused the unit: the chain stops with the rejection as its failure. */
    private void onExecutorRejection(final RejectedExecutionException cause, final String what) {
        synchronized (gate) {
            failure = cause;
            running = false;
            needSnapshotReportOutstanding = false;
            pendingRestartRevision = null;
            LOG.warn(
                    "Metadata watcher stopped at checkpoint {}: the executor rejected the {}",
                    checkpoint,
                    what,
                    cause);
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
