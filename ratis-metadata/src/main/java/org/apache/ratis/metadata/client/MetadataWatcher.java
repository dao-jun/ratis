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
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;

import static org.apache.ratis.metadata.util.Preconditions.checkArgument;
import static org.apache.ratis.metadata.util.Preconditions.checkNotNull;
import static org.apache.ratis.metadata.util.Preconditions.checkState;

/**
 * Continuously polls changes, delivering an ordered, gap-explained stream to {@link Listener}.
 *
 * <p>The watcher owns no thread: one poll at a time runs on the client's asynchronous machinery,
 * and each completion schedules the next step on the module's shared scheduler. At most one poll
 * is outstanding, so the records of consecutive polls arrive in revision order. While records
 * flow the next poll leaves immediately; an empty poll waits the interval first.
 *
 * <p>Callbacks run inline on the client's completion executor (or on the {@link Builder#setExecutor
 * executor} supplied instead) - keep them short: a blocking callback parks a thread that settles
 * the client's other asynchronous calls. Callbacks of one watcher never overlap.
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

    /** The poll position; owned by the poll chain alone - only it runs while {@code running}. */
    private long checkpoint;

    private volatile long lastDeliveredRevision;
    private volatile boolean running;
    private volatile boolean closed;
    private volatile Throwable failure;

    /** The scheduler step that will fire the next poll; guarded by {@code this}. */
    private ScheduledFuture<?> nextStep;

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
         * Where {@link Listener} callbacks run; default is inline on the client's completion
         * executor - the poll chain waits for each callback to return.
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
    public synchronized void start() {
        checkState(!closed, "MetadataWatcher is closed");
        checkState(!running, "MetadataWatcher is already running");
        running = true;
        failure = null;
        scheduleNext(0);
    }

    /** Resumes the poll chain from {@code sinceRevision} (exclusive) after it stopped. */
    public synchronized void restartFrom(long sinceRevision) {
        checkArgument(sinceRevision >= 0, "sinceRevision must be >= 0, got %s", sinceRevision);
        checkState(!closed, "MetadataWatcher is closed");
        checkState(!running, "MetadataWatcher is still running");
        checkpoint = sinceRevision;
        lastDeliveredRevision = sinceRevision;
        start();
    }

    /** Whether a poll chain is running (false after a NEED_SNAPSHOT stop, a failure or a close). */
    public synchronized boolean isRunning() {
        return running;
    }

    /** Failure that stopped the chain, or null; checkpoint in {@link #lastDeliveredRevision()}. */
    public synchronized Throwable getFailure() {
        return failure;
    }

    /** Revision of the last delivered record, or the start revision when none yet. */
    public synchronized long lastDeliveredRevision() {
        return lastDeliveredRevision;
    }

    /**
     * Cancels the next poll and stops the deliveries; idempotent, never blocks. A poll already
     * in flight completes and its records are dropped - the listener sees nothing more.
     */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        running = false;
        if (nextStep != null) {
            nextStep.cancel(false);
            nextStep = null;
        }
    }

    /**
     * Fires one poll on the shared scheduler's thread - never on a poll's own completion thread:
     * under ratis' async backpressure (the outstanding-requests semaphore) the send can block its
     * issuer, and blocking a completion thread risks wedging the very replies that free the
     * permits. The scheduler stalling instead only delays ticks, never deadlocks.
     */
    private void pollNow() {
        if (closed || !running) {
            return;
        }
        client.pollAsync(checkpoint, maxPollBytes).whenComplete(this::onPoll);
    }

    private void onPoll(RaftMetadataClient.PollResult polled, Throwable pollFailure) {
        if (closed) {
            if (pollFailure == null && !polled.getRecords().isEmpty()) {
                LOG.warn(
                        "Dropping the {} change record(s) of a poll that returned after the watcher"
                                + " closed; no further records will be delivered",
                        polled.getRecords().size());
            }
            return;
        }
        if (pollFailure != null) {
            // The client's async twins wrap their failures; classify the cause underneath.
            onFailure(ExceptionUtils.stripCompletionException(pollFailure));
            return;
        }
        if (polled.getRecords().isEmpty()) {
            // Advance past record-less revisions; max() - a leader change cannot regress it.
            checkpoint = Math.max(checkpoint, polled.getWatermark());
            scheduleNext(pollIntervalMs);
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
            scheduleNext(pollIntervalMs);
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
            scheduleNext(pollIntervalMs);
        } else {
            // Nothing else is expected off the client's future; stopping beats a hot loop.
            stopAfterFailure(cause, "the poll failed unexpectedly");
        }
    }

    private void deliver(final RaftMetadataClient.PollResult polled) {
        final long lastRecordRevision =
                polled.getRecords().get(polled.getRecords().size() - 1).getRevision();
        final Runnable deliverBatch =
                () -> {
                    for (ChangeEvent event : polled.getRecords()) {
                        try {
                            listener.onRecord(event);
                        } catch (Exception e) {
                            LOG.warn(
                                    "Change listener failed on revision {}; continuing",
                                    event.getRevision(),
                                    e);
                        }
                    }
                };
        if (executor == null) {
            deliverBatch.run();
        } else {
            executor.execute(deliverBatch);
        }
        lastDeliveredRevision = lastRecordRevision;
        checkpoint = lastRecordRevision;
        // Records flowed: the next poll leaves immediately, keeping up with a busy stream.
        scheduleNext(0);
    }

    private void stopAfterNeedSnapshot(final long oldestRetainedRevision) {
        running = false;
        if (closed) {
            LOG.warn(
                    "Dropping the NEED_SNAPSHOT report of revision {} of a poll that returned after"
                            + " the watcher closed",
                    oldestRetainedRevision);
            return;
        }
        final Runnable report =
                () -> {
                    try {
                        listener.onNeedSnapshot(oldestRetainedRevision);
                    } catch (Exception e) {
                        LOG.warn("Change listener failed on NEED_SNAPSHOT", e);
                    }
                };
        if (executor == null) {
            report.run();
        } else {
            executor.execute(report);
        }
        LOG.info(
                "Metadata watcher stopped at checkpoint {}: records below revision {} are gone",
                checkpoint,
                oldestRetainedRevision);
    }

    private void stopAfterFailure(final Throwable cause, final String why) {
        // Failure first: a caller observing the stopped chain must already see the cause.
        failure = cause;
        running = false;
        LOG.warn("Metadata watcher stopped at checkpoint {}: {}", checkpoint, why, cause);
    }

    /** Schedules the next poll from the shared scheduler; a no-op once stopped or closed. */
    private synchronized void scheduleNext(long delayMs) {
        if (closed || !running) {
            return;
        }
        nextStep = AsyncDriver.schedule(this::pollNow, delayMs);
    }
}
