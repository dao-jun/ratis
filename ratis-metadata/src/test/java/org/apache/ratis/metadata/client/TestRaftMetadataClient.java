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

import org.apache.ratis.metadata.MetadataCluster;
import org.apache.ratis.metadata.lock.MetadataLock;
import org.apache.ratis.metadata.statemachine.ChangeEvent;
import org.apache.ratis.metadata.statemachine.MetadataStateMachine;
import org.apache.ratis.metadata.statemachine.RaftCommand;
import org.apache.ratis.metadata.statemachine.RaftCommandCodec;
import org.apache.ratis.metadata.statemachine.RaftResult;
import org.apache.ratis.metadata.util.Clock;
import org.apache.ratis.metadata.util.ExceptionUtils;
import org.apache.ratis.metadata.util.FutureUtils;
import org.apache.ratis.metadata.util.ManualClock;
import org.apache.ratis.metadata.util.SystemClock;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.client.RaftClientRpc;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.apache.ratis.metadata.util.TestWait.waitUntil;

/**
 * Unit tests of {@link RaftMetadataClient}, {@link LeaseSession} and {@link MetadataWatcher} on
 * hand-written rpc stubs: async-settlement semantics, failure translation and watcher lifecycle
 * without a live quorum - plus the cluster-dependent end-to-end suites that run the same surface
 * against a live in-process quorum via {@link MetadataCluster}.
 */
public class TestRaftMetadataClient {

    private static final String GROUP_UUID = "7e4c1d2a-9b3f-4a58-8c6e-2f1d5a7b9c34";
    private static final Duration OPERATION_TIMEOUT = Duration.ofSeconds(30);
    /**
     * The observation window of the negative watcher assertions: the defect each guards against
     * trips its latch within microseconds, never near this bound - the bound only ends the
     * wait once the absence is proven for a margin no scheduling can miss.
     */
    private static final Duration NEGATIVE_WINDOW = Duration.ofMillis(1500);
    private static final long POLL_BUDGET_BYTES = 4 * 1024 * 1024;
    private static final String SEQUENTIAL_PATH_PATTERN = "/\\w+/task-\\d{10}";

    private static final int FAILOVER_WRITES = 60;

    @TempDir File tempDir;

    @Test
    void testRetryDefaultsAreFiniteAndRetryForeverIsRejected() {
        assertEquals(Duration.ofHours(24), RaftMetadataClient.DEFAULT_RETRY_TIMEOUT);
        org.junit.jupiter.api.Assertions.assertThrows(
                IllegalStateException.class, () -> RaftMetadataClient.newBuilder().setRetryForever());

        final RaftMetadataClient.Builder builder = RaftMetadataClient.newBuilder();
        assertEquals(builder, builder.setRetryTimeout(Duration.ofMillis(1)));
        org.junit.jupiter.api.Assertions.assertThrows(
                IllegalArgumentException.class, () -> builder.setRetryTimeout(Duration.ZERO));
        org.junit.jupiter.api.Assertions.assertThrows(
                IllegalArgumentException.class, () -> builder.setRetryTimeout(Duration.ofMillis(-1)));
    }

    @Test
    void testAsyncCallEndsWhenTheClientClosesUnderneath() throws Exception {
        final InetSocketAddress deadMember = new InetSocketAddress("localhost", refusedPort());
        final RaftMetadataClient unbounded =
                RaftMetadataClient.newBuilder()
                        .setGroupId(UUID.fromString(GROUP_UUID))
                        .addQuorumMember("n0", deadMember)
                        .setRetryTimeout(Duration.ofHours(1))
                        .build();
        final CompletableFuture<Long> pending = unbounded.putAsync("/unreachable", bytes("x"));
        unbounded.close();
        final Throwable failure =
                awaitFailureOf(pending, "Closing the client never ended the async call");
        assertInstanceOf(MetadataTransportException.class, failure);
    }

    @Test
    void testAsyncCallOnClosedClientFailsTheFuture() throws Exception {
        final RaftMetadataClient client = newStubClient(new FailingTransportRpc(), null);
        client.close();
        final Throwable failure =
                awaitFailureOf(
                        client.putAsync("/closed", bytes("x")),
                        "A closed client never settled the async call");
        assertInstanceOf(IllegalStateException.class, failure);
    }

    @Test
    void testAsyncArgumentErrorsSettleTheFuture() throws Exception {
        final RaftMetadataClient client = newStubClient(new FailingTransportRpc(), null);
        try {
            assertInstanceOf(
                    NullPointerException.class,
                    awaitFailureOf(
                            client.openLeaseAsync(null),
                            "A null lease ttl never settled the future"));
            assertInstanceOf(
                    IllegalArgumentException.class,
                    awaitFailureOf(
                            client.renewLeaseAsync(1L, Duration.ZERO),
                            "A zero lease ttl never settled the future"));
            assertInstanceOf(
                    NullPointerException.class,
                    awaitFailureOf(
                            client.putAsync(null, bytes("x")),
                            "A null path never settled the future"));
            assertInstanceOf(
                    NullPointerException.class,
                    awaitFailureOf(
                            client.multiAsync(null), "A null op list never settled the future"));
        } finally {
            client.close();
        }
    }

    @Test
    void testAsyncUndecodableReplyFailsTheFuture() throws Exception {
        final RaftMetadataClient poisoned = newStubClient(new UndecodableReplyRpc(), null);
        try {
            final Throwable failure =
                    awaitFailureOf(
                            poisoned.getAsync("/any"), "The async get never decoded its reply");
            assertInstanceOf(UndecodableReplyException.class, failure);
        } finally {
            poisoned.close();
        }
    }

    @Test
    void testAsyncCompletionsRunOnTheClientExecutor() throws Exception {
        final ControllableReplyRpc rpc = new ControllableReplyRpc();
        final RaftMetadataClient client = newStubClient(rpc, null);
        try {
            final CompletableFuture<String> completingThread =
                    client.getAsync("/any").thenApply(record -> Thread.currentThread().getName());
            // The call stays pending until the test supplies the reply, so the chain provably
            // runs on whatever thread settles it - the client's executor, not the caller's.
            rpc.completePendingGet();
            assertTrue(
                    completingThread.join().startsWith("raft-metadata-async"),
                    "completions must run on the client's own executor");
        } finally {
            client.close();
        }
    }

    @Test
    void testWatcherStopsLoudlyOnUndecodableReply() throws Exception {
        final RaftMetadataClient poisoned = newStubClient(new UndecodableReplyRpc(), null);
        try {
            final Recorder recorder = new Recorder();
            final MetadataWatcher watcher =
                    MetadataWatcher.newBuilder(poisoned, recorder)
                            .setSinceRevision(7)
                            .setPollInterval(Duration.ofMillis(50))
                            .build();
            watcher.start();
            waitUntil(
                    () -> watcher.getFailure() != null && !watcher.isRunning(),
                    OPERATION_TIMEOUT,
                    "The watcher did not stop on the undecodable reply");
            assertInstanceOf(UndecodableReplyException.class, watcher.getFailure());
            assertEquals(
                    7L,
                    watcher.lastDeliveredRevision(),
                    "nothing decoded means nothing skipped: the checkpoint stays put");
            assertTrue(recorder.deliveredEvents().isEmpty());
            assertNull(
                    recorder.needSnapshotLine(), "an undecodable reply is not a retention report");

            // Resuming stays the caller's decision; the new loop clears the failure state.
            watcher.restartFrom(7);
            assertTrue(watcher.isRunning());
            assertNull(watcher.getFailure());
            waitUntil(
                    () -> watcher.getFailure() != null && !watcher.isRunning(),
                    OPERATION_TIMEOUT,
                    "The restarted watcher did not stop on the undecodable reply");
            assertEquals(7L, watcher.lastDeliveredRevision());
            watcher.close();
        } finally {
            poisoned.close();
        }
    }

    /**
     * Admission vs close on the report path: a restart requested while the NEED_SNAPSHOT report
     * is merely QUEUED is recorded (last-wins) rather than starting a chain on the spot, and a
     * report admitted only after {@code close()} never runs the listener at all.
     */
    @Test
    void testWatcherSubmitsNoListenerCallbackAfterClose() throws Exception {
        final GatedNeedSnapshotRpc rpc = new GatedNeedSnapshotRpc();
        final RaftMetadataClient client = newStubClient(rpc, null);
        try {
            final QueueingExecutor executor = new QueueingExecutor();
            final Recorder recorder = new Recorder();
            final MetadataWatcher watcher =
                    MetadataWatcher.newBuilder(client, recorder)
                            .setExecutor(executor)
                            .setPollInterval(Duration.ofMillis(50))
                            .build();
            watcher.start();
            waitUntil(
                    rpc::firstPollInFlight,
                    OPERATION_TIMEOUT,
                    "The watcher never issued its first poll");
            rpc.answerFirstPoll();
            waitUntil(
                    () -> executor.submitted.size() == 1,
                    OPERATION_TIMEOUT,
                    "The watcher never queued its first NEED_SNAPSHOT report");

            // The restart lands while the report is queued, not running: it is recorded, and
            // the chain stays down until the report completed - a poll now would race the
            // report's own restart decision.
            watcher.restartFrom(1);
            assertFalse(watcher.isRunning(), "the restart stays pending while the report runs");

            watcher.close();
            executor.submitted.peek().run(); // the executor drains only after the close

            assertNull(
                    recorder.needSnapshotLine(),
                    "a report admitted only after close must not run the listener");
            assertEquals(1, executor.submitted.size(), "the closed watcher must not report");
            assertFalse(watcher.isRunning());
            assertThrows(
                    IllegalStateException.class,
                    () -> watcher.restartFrom(1),
                    "a closed watcher rejects further restarts");
        } finally {
            client.close();
        }
    }

    @Test
    void testWatcherStopsWhenTheClientClosesUnderneath() throws Exception {
        final FailingTransportRpc rpc = new FailingTransportRpc();
        // Two fast attempts per poll: each poll fails within ms, so the loop re-polls, not pins.
        final RaftMetadataClient client =
                newStubClient(
                        rpc,
                        RetryPolicies.retryUpToMaximumCountWithFixedSleep(
                                2, TimeDuration.valueOf(50, TimeUnit.MILLISECONDS)));
        try {
            final MetadataWatcher watcher =
                    MetadataWatcher.newBuilder(client, new Recorder())
                            .setPollInterval(Duration.ofMillis(50))
                            .build();
            watcher.start();
            waitUntil(
                    () -> rpc.attempts.get() >= 2,
                    OPERATION_TIMEOUT,
                    "The watcher never entered its retry loop");

            client.close();

            waitUntil(
                    () -> !watcher.isRunning() && watcher.getFailure() != null,
                    OPERATION_TIMEOUT,
                    "The watcher kept idling over the closed client instead of stopping");
            assertInstanceOf(IllegalStateException.class, watcher.getFailure());
            assertTrue(
                    watcher.getFailure().getMessage().contains("already closed"),
                    "the stop must carry the closed-client cause");
            watcher.close();
        } finally {
            client.close();
        }
    }

    // ------------------------------------------------------------------
    // Watcher delivery is single-flight and admitted under the close gate
    // ------------------------------------------------------------------

    /**
     * Single-flight: while one delivery unit's callback is parked on a multithreaded executor,
     * no second poll leaves and the watermark stays put - both move only after the listener
     * actually returned, so callbacks of one watcher can never overlap.
     */
    @Test
    void testWatcherPollsAgainOnlyAfterTheListenerReturned() throws Exception {
        final GatedPollRpc rpc = new GatedPollRpc();
        final RaftMetadataClient client = newStubClient(rpc, null);
        final ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            final BlockingListener listener = new BlockingListener();
            final MetadataWatcher watcher =
                    MetadataWatcher.newBuilder(client, listener)
                            .setExecutor(pool)
                            .setSinceRevision(0)
                            .setPollInterval(Duration.ofMillis(50))
                            .build();
            watcher.start();
            waitUntil(
                    rpc::hasInFlightPoll,
                    OPERATION_TIMEOUT,
                    "The watcher never issued its first poll");
            rpc.answerRecords(1L, 1L);
            assertTrue(
                    listener.firstEntered.await(OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS),
                    "The first listener callback never started");
            assertNeverTripped(
                    rpc.anotherPollArrived,
                    NEGATIVE_WINDOW,
                    "The chain polled again while the delivery was still in flight");
            assertEquals(
                    0L,
                    watcher.lastDeliveredRevision(),
                    "the watermark moves only after the listener returned");
            assertFalse(listener.overlapped(), "listener callbacks of one watcher never overlap");

            listener.release();
            assertTrue(
                    listener.firstReturned.await(OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS),
                    "The first listener callback never returned");
            waitUntil(
                    () -> rpc.pollCount() == 2 && rpc.hasInFlightPoll(),
                    OPERATION_TIMEOUT,
                    "The chain never resumed polling after the delivery completed");
            assertEquals(
                    1L,
                    rpc.sinceRevisionOf(2),
                    "the resumed poll continues after the batch that was actually delivered");

            rpc.answerRecords(3L, 2L, 3L);
            waitUntil(
                    () -> listener.events().size() == 3,
                    OPERATION_TIMEOUT,
                    "The second batch never reached the listener");
            assertEquals(3L, watcher.lastDeliveredRevision());
            assertFalse(listener.overlapped(), "listener callbacks of one watcher never overlap");
            watcher.close();
        } finally {
            pool.shutdownNow();
            client.close();
        }
    }

    /**
     * Admission vs close on the record path: a delivery unit queued on an executor that is
     * admitted only after {@code close()} must never run the listener nor move any state.
     */
    @Test
    void testWatcherDropsRecordCallbacksQueuedBeforeClose() throws Exception {
        final GatedPollRpc rpc = new GatedPollRpc();
        final RaftMetadataClient client = newStubClient(rpc, null);
        try {
            final QueueingExecutor executor = new QueueingExecutor();
            final Recorder recorder = new Recorder();
            final MetadataWatcher watcher =
                    MetadataWatcher.newBuilder(client, recorder)
                            .setExecutor(executor)
                            .setSinceRevision(0)
                            .setPollInterval(Duration.ofMillis(50))
                            .build();
            watcher.start();
            waitUntil(
                    rpc::hasInFlightPoll,
                    OPERATION_TIMEOUT,
                    "The watcher never issued its first poll");
            rpc.answerRecords(1L, 1L);
            waitUntil(
                    () -> executor.submitted.size() == 1,
                    OPERATION_TIMEOUT,
                    "The watcher never queued its first delivery");

            watcher.close();
            executor.submitted.poll().run(); // the executor drains only after the close

            assertTrue(
                    recorder.deliveredEvents().isEmpty(),
                    "a unit admitted only after close must not run the listener");
            assertEquals(
                    0L,
                    watcher.lastDeliveredRevision(),
                    "a dropped unit must not advance the watermark");
            assertEquals(1, rpc.pollCount(), "the closed watcher polls no further");
        } finally {
            client.close();
        }
    }

    /**
     * The other side of the admission race: a callback already admitted (running) when
     * {@code close()} lands finishes and its delivery still counts - but nothing further is
     * scheduled and the chain is down.
     */
    @Test
    void testWatcherLetsAnAdmittedCallbackFinishAfterClose() throws Exception {
        final GatedPollRpc rpc = new GatedPollRpc();
        final RaftMetadataClient client = newStubClient(rpc, null);
        final ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            final BlockingListener listener = new BlockingListener();
            final MetadataWatcher watcher =
                    MetadataWatcher.newBuilder(client, listener)
                            .setExecutor(pool)
                            .setSinceRevision(0)
                            .setPollInterval(Duration.ofMillis(50))
                            .build();
            watcher.start();
            waitUntil(
                    rpc::hasInFlightPoll,
                    OPERATION_TIMEOUT,
                    "The watcher never issued its first poll");
            rpc.answerRecords(1L, 1L);
            assertTrue(
                    listener.firstEntered.await(OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS),
                    "The listener callback never started");

            watcher.close(); // mid-callback: the admitted unit may finish
            listener.release();
            assertTrue(
                    listener.firstReturned.await(OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS),
                    "An already admitted callback must finish after close");
            assertEquals(1, listener.events().size(), "the admitted callback completed its batch");
            assertEquals(
                    1L,
                    watcher.lastDeliveredRevision(),
                    "the finished delivery still records its watermark");
            assertFalse(watcher.isRunning());
            assertNeverTripped(
                    rpc.anotherPollArrived,
                    NEGATIVE_WINDOW,
                    "no poll may leave a closed watcher");
        } finally {
            pool.shutdownNow();
            client.close();
        }
    }

    /**
     * NEED_SNAPSHOT restart arbitration: restart requests arriving while the report is merely
     * queued (or running) are recorded last-wins without exception; the completed report then
     * starts exactly one new chain from the LAST accepted checkpoint. A restart against a
     * polling chain keeps its rejection.
     */
    @Test
    void testNeedSnapshotRestartsAreLastWinsUntilTheReportCompleted() throws Exception {
        final GatedPollRpc rpc = new GatedPollRpc();
        final RaftMetadataClient client = newStubClient(rpc, null);
        try {
            final QueueingExecutor executor = new QueueingExecutor();
            final Recorder recorder = new Recorder();
            final MetadataWatcher watcher =
                    MetadataWatcher.newBuilder(client, recorder)
                            .setExecutor(executor)
                            .setSinceRevision(0)
                            .setPollInterval(Duration.ofMillis(50))
                            .build();
            watcher.start();
            waitUntil(
                    rpc::hasInFlightPoll,
                    OPERATION_TIMEOUT,
                    "The watcher never issued its first poll");
            rpc.answerNeedSnapshot(5L);
            waitUntil(
                    () -> executor.submitted.size() == 1,
                    OPERATION_TIMEOUT,
                    "The watcher never queued its NEED_SNAPSHOT report");
            assertFalse(
                    watcher.isRunning(), "the chain is stopped while the report is outstanding");

            watcher.restartFrom(7L); // queued, not running: recorded, no chain yet
            watcher.restartFrom(9L); // last wins
            assertFalse(
                    watcher.isRunning(),
                    "the restart stays pending until the report completed");
            assertNeverTripped(
                    rpc.anotherPollArrived,
                    NEGATIVE_WINDOW,
                    "the pending restart must not poll before the report completed");

            executor.submitted.poll().run(); // the report finally runs
            waitUntil(
                    () -> rpc.pollCount() == 2 && rpc.hasInFlightPoll(),
                    OPERATION_TIMEOUT,
                    "The completed report never started the pending chain");
            assertEquals(
                    9L,
                    rpc.sinceRevisionOf(2),
                    "exactly one chain leaves, from the LAST accepted checkpoint");

            assertThrows(
                    IllegalStateException.class,
                    () -> watcher.restartFrom(11L),
                    "a restart against a polling chain keeps its rejection");

            rpc.answerRecords(10L, 10L);
            // The queued executor holds the delivery until the test drains it - as it held
            // the report before.
            waitUntil(
                    () -> executor.submitted.size() == 1,
                    OPERATION_TIMEOUT,
                    "The restarted chain never queued its delivery");
            executor.submitted.peek().run();
            waitUntil(
                    () -> !recorder.deliveredEvents().isEmpty(),
                    OPERATION_TIMEOUT,
                    "The restarted chain never delivered");
            assertEquals(10L, watcher.lastDeliveredRevision());
            watcher.close();
        } finally {
            client.close();
        }
    }

    /** A report that completes with no restart request leaves the chain stopped. */
    @Test
    void testNeedSnapshotReportWithoutRestartLeavesTheChainStopped() throws Exception {
        final GatedPollRpc rpc = new GatedPollRpc();
        final RaftMetadataClient client = newStubClient(rpc, null);
        try {
            final Recorder recorder = new Recorder();
            final MetadataWatcher watcher =
                    MetadataWatcher.newBuilder(client, recorder)
                            .setSinceRevision(0)
                            .setPollInterval(Duration.ofMillis(50))
                            .build();
            watcher.start();
            waitUntil(
                    rpc::hasInFlightPoll,
                    OPERATION_TIMEOUT,
                    "The watcher never issued its first poll");
            rpc.answerNeedSnapshot(5L); // the report runs inline here
            waitUntil(
                    () -> recorder.needSnapshotLine() != null && !watcher.isRunning(),
                    OPERATION_TIMEOUT,
                    "The watcher did not stop with the NEED_SNAPSHOT report");
            assertEquals(Long.valueOf(5L), recorder.needSnapshotLine());
            assertNeverTripped(
                    rpc.anotherPollArrived,
                    NEGATIVE_WINDOW,
                    "without a pending restart the chain must stay stopped");
            assertFalse(watcher.isRunning());
            watcher.close();
        } finally {
            client.close();
        }
    }

    /**
     * An executor that refuses the unit ends the chain loudly: the rejection is the watcher's
     * recorded failure - for record batches and for NEED_SNAPSHOT reports alike - and leaves
     * no pending restart behind, so a later restart takes the ordinary direct path.
     */
    @Test
    void testWatcherRecordsTheFailureWhenTheExecutorRejects() throws Exception {
        final GatedPollRpc rpc = new GatedPollRpc();
        final RaftMetadataClient client = newStubClient(rpc, null);
        try {
            final Recorder recorder = new Recorder();
            final MetadataWatcher watcher =
                    MetadataWatcher.newBuilder(client, recorder)
                            .setExecutor(new RejectingExecutor())
                            .setSinceRevision(0)
                            .setPollInterval(Duration.ofMillis(50))
                            .build();
            watcher.start();
            waitUntil(
                    rpc::hasInFlightPoll,
                    OPERATION_TIMEOUT,
                    "The watcher never issued its first poll");
            rpc.answerRecords(1L, 1L);
            waitUntil(
                    () -> watcher.getFailure() != null && !watcher.isRunning(),
                    OPERATION_TIMEOUT,
                    "The executor rejection never stopped the chain");
            assertInstanceOf(RejectedExecutionException.class, watcher.getFailure());
            assertTrue(
                    recorder.deliveredEvents().isEmpty(),
                    "a rejected unit never ran the listener");
            assertEquals(1, rpc.pollCount(), "a stopped chain polls no further");

            watcher.restartFrom(2L);
            assertTrue(watcher.isRunning(), "after the failure a restart takes the direct path");
            assertNull(watcher.getFailure());
            waitUntil(
                    () -> rpc.pollCount() == 2 && rpc.hasInFlightPoll(),
                    OPERATION_TIMEOUT,
                    "The restarted chain never polled");

            rpc.answerNeedSnapshot(20L); // the report's submission is rejected too
            waitUntil(
                    () -> watcher.getFailure() != null && !watcher.isRunning(),
                    OPERATION_TIMEOUT,
                    "The rejected NEED_SNAPSHOT report never stopped the chain");
            assertInstanceOf(RejectedExecutionException.class, watcher.getFailure());
            assertNull(recorder.needSnapshotLine(), "the rejected report never ran");

            watcher.restartFrom(21L);
            assertTrue(
                    watcher.isRunning(), "a rejected report must leave no pending restart");
            watcher.close();
        } finally {
            client.close();
        }
    }

    /**
     * A listener that dies with an Error mid-batch must not claim the undelivered tail: the
     * watermark advances only to the last record the listener completed, and the chain
     * reschedules from there, re-delivering what never arrived. The Error itself surfaces
     * wherever its executor thread puts it - here, synchronously out of the drained unit.
     */
    @Test
    void testWatcherResumesFromTheLastCompletedRecordWhenTheListenerDies() throws Exception {
        final GatedPollRpc rpc = new GatedPollRpc();
        final RaftMetadataClient client = newStubClient(rpc, null);
        try {
            final QueueingExecutor executor = new QueueingExecutor();
            final AtomicBoolean firstRecordFatal = new AtomicBoolean(true);
            final List<ChangeEvent> delivered = new CopyOnWriteArrayList<>();
            final MetadataWatcher watcher =
                    MetadataWatcher.newBuilder(
                                    client,
                                    new MetadataWatcher.Listener() {
                                        @Override
                                        public void onRecord(ChangeEvent event) {
                                            if (firstRecordFatal.compareAndSet(true, false)) {
                                                throw new UnknownError("listener dies mid-batch");
                                            }
                                            delivered.add(event);
                                        }

                                        @Override
                                        public void onNeedSnapshot(
                                                long oldestRetainedRevision) {}
                                    })
                            .setExecutor(executor)
                            .setSinceRevision(0)
                            .setPollInterval(Duration.ofMillis(50))
                            .build();
            watcher.start();
            waitUntil(
                    rpc::hasInFlightPoll,
                    OPERATION_TIMEOUT,
                    "The watcher never issued its first poll");
            rpc.answerRecords(2L, 1L, 2L); // one batch of two records
            waitUntil(
                    () -> executor.submitted.size() == 1,
                    OPERATION_TIMEOUT,
                    "The watcher never queued its first delivery");

            // The unit runs on the test's own thread, so the Error surfaces right here.
            try {
                executor.submitted.poll().run();
                fail("The listener's Error must surface through the drained unit");
            } catch (UnknownError expected) {
                // where the executor's thread puts it - here, the test's own thread
            }

            assertTrue(delivered.isEmpty(), "the dying listener kept no record");
            assertEquals(
                    0L,
                    watcher.lastDeliveredRevision(),
                    "the watermark claims only records the listener completed");
            assertTrue(watcher.isRunning(), "the chain survives the dying listener");
            assertNull(watcher.getFailure(), "a dying listener is not a chain failure");

            // The chain rescheduled from the pre-batch position: the whole batch is re-polled.
            waitUntil(
                    () -> rpc.pollCount() == 2 && rpc.hasInFlightPoll(),
                    OPERATION_TIMEOUT,
                    "The chain never rescheduled after the dying listener");
            assertEquals(
                    0L,
                    rpc.sinceRevisionOf(2),
                    "the redelivery resumes at the pre-batch position");
            rpc.answerRecords(2L, 1L, 2L);
            waitUntil(
                    () -> executor.submitted.size() == 1,
                    OPERATION_TIMEOUT,
                    "The redelivered batch never queued");
            executor.submitted.poll().run();
            assertEquals(
                    Arrays.asList(1L, 2L),
                    revisionsOf(delivered),
                    "the redelivery hands over both records exactly once");
            assertEquals(2L, watcher.lastDeliveredRevision());
            watcher.close();
        } finally {
            client.close();
        }
    }

    /**
     * A record reply landing after {@code close()} is dropped whole: no delivery unit is
     * submitted, the listener never runs, no failure is recorded and nothing schedules.
     */
    @Test
    void testWatcherDropsARecordReplyLandingAfterClose() throws Exception {
        final GatedPollRpc rpc = new GatedPollRpc();
        final RaftMetadataClient client = newStubClient(rpc, null);
        try {
            final QueueingExecutor executor = new QueueingExecutor();
            final Recorder recorder = new Recorder();
            final MetadataWatcher watcher =
                    MetadataWatcher.newBuilder(client, recorder)
                            .setExecutor(executor)
                            .setSinceRevision(0)
                            .setPollInterval(Duration.ofMillis(50))
                            .build();
            watcher.start();
            waitUntil(
                    rpc::hasInFlightPoll,
                    OPERATION_TIMEOUT,
                    "The watcher never issued its first poll");

            watcher.close(); // the poll is still in flight
            rpc.answerRecords(1L, 1L); // its reply lands after the close

            assertTrue(
                    recorder.deliveredEvents().isEmpty(), "no listener callback after close");
            assertEquals(0, executor.submitted.size(), "no delivery unit is submitted");
            assertEquals(0L, watcher.lastDeliveredRevision(), "the watermark never moved");
            assertNull(watcher.getFailure(), "a dropped reply is not a failure");
            assertFalse(watcher.isRunning());
            assertNeverTripped(
                    rpc.anotherPollArrived,
                    NEGATIVE_WINDOW,
                    "nothing schedules after a post-close reply");
        } finally {
            client.close();
        }
    }

    @Test
    void testLeaseSessionStopsRenewingWhenTheClientCloses() throws Exception {
        final RaftMetadataClient client = newStubClient(new FailingTransportRpc(), null);
        client.close();
        // Constructed directly: openLease() needs a live client; here the attach faces a closed
        // client.
        final LeaseSession session = new LeaseSession(client, 77L, 30_000L, new ManualClock(0L));
        session.start();
        // The registration on a closed client turns the session away dead: no renewal is ever
        // due on the shared scheduler, and the holder's checkpoint reports it.
        assertFalse(session.isAlive(), "A session of a closed client must report dead");
    }

    /**
     * The {@code UNKNOWN_LEASE} renewal is a terminal path: state clears under the session lock
     * and the session then leaves the client's registry - outside that lock - so the final tick
     * dies with it. A session that stays registered after its lease died leaks a dead entry (and
     * keeps the scheduler tick alive) for the life of the client.
     */
    @Test
    void testUnknownLeaseRenewalDetachesTheSessionAndCancelsTheFinalTick() throws Exception {
        final ScriptedReplyRpc rpc = new ScriptedReplyRpc();
        rpc.errorStatuses.put(RaftCommand.Op.LEASE_RENEW, RaftResult.Status.UNKNOWN_LEASE);
        final RaftMetadataClient client = newStubClient(rpc, null);
        try {
            final ManualClock clock = new ManualClock(0L);
            final LeaseSession session = new LeaseSession(client, 4242L, 3_000L, clock);
            session.start();
            assertTrue(client.sessions.contains(session), "a started session is registered");
            assertNotNull(sessionTickOf(client), "the session tick is live while one is registered");

            // A third of the ttl passes; the due renewal comes back UNKNOWN_LEASE - terminal.
            clock.advanceTime(1_100L, TimeUnit.MILLISECONDS);
            session.tick();
            waitUntil(
                    () -> !session.isAlive(),
                    Duration.ofSeconds(5),
                    "The session did not surface its lease's death");

            waitUntil(
                    () -> !client.sessions.contains(session),
                    Duration.ofSeconds(5),
                    "The UNKNOWN_LEASE terminal path left the session registered");
            assertNull(
                    sessionTickOf(client),
                    "the final tick was not cancelled once the last session detached");

            // No resurrection and no further renewals: a stopped session dispatches nothing.
            final int renewals = rpc.countOf(RaftCommand.Op.LEASE_RENEW);
            session.tick();
            assertEquals(
                    renewals,
                    rpc.countOf(RaftCommand.Op.LEASE_RENEW),
                    "a stopped session must not dispatch another renewal");
            session.close(); // idempotent on the terminal path
            assertFalse(session.isAlive());
        } finally {
            client.close();
        }
    }

    /**
     * The client-close terminal path: the sweep stops and detaches every session, and a renewal
     * completion landing AFTER the stop (its reply was in flight) must not resurrect anything -
     * the session stays dead, unregistered, tick-less, and dispatches no further renewal.
     */
    @Test
    void testClientCloseDetachesSessionsAndDelayedCompletionsDoNotResurrect() throws Exception {
        final ScriptedReplyRpc rpc = new ScriptedReplyRpc();
        rpc.holdRenewals = true;
        final RaftMetadataClient client = newStubClient(rpc, null);
        final ManualClock clock = new ManualClock(0L);
        final LeaseSession session = new LeaseSession(client, 555L, 3_000L, clock);
        session.start();
        clock.advanceTime(1_100L, TimeUnit.MILLISECONDS);
        session.tick(); // the renewal is in flight; its reply is withheld
        waitUntil(
                () -> rpc.countOf(RaftCommand.Op.LEASE_RENEW) >= 1,
                Duration.ofSeconds(5),
                "The renewal never went out");

        client.close();
        assertFalse(session.isAlive(), "the sweep stopped the session");
        assertFalse(client.sessions.contains(session), "the sweep detached the session");
        assertNull(sessionTickOf(client), "the sweep cancelled the tick");

        // The delayed successful completion lands after the stop.
        rpc.completeHeld();
        Thread.sleep(200);
        assertFalse(session.isAlive(), "a completion after stop resurrected the session");
        assertFalse(client.sessions.contains(session), "a completion after stop re-registered");
        assertNull(sessionTickOf(client), "a completion after stop resurrected the tick");
        final int renewals = rpc.countOf(RaftCommand.Op.LEASE_RENEW);
        session.tick();
        assertEquals(
                renewals, rpc.countOf(RaftCommand.Op.LEASE_RENEW), "no renewal after stop");
    }

    // ------------------------------------------------------------------
    // The lock speaks the typed revocation, never a path delete
    // ------------------------------------------------------------------

    /**
     * A successful release stops the session's renewals and then revokes the holder's private
     * lease through the ordered pipeline - the one command whose server-side cascade is scoped to
     * that lease. It must never send a {@code DELETE} of the lock path: a path delete is
     * ownership-unsafe (it cannot tell whose record sits at the path).
     */
    @Test
    void testLockReleaseRevokesTheLeaseNeverDeletesThePath() throws Exception {
        final ScriptedReplyRpc rpc = new ScriptedReplyRpc();
        final RaftMetadataClient client = newStubClient(rpc, null);
        try {
            final MetadataLock lock = newLockOn(client, "/stub/lock");
            final MetadataLock.Hold hold =
                    lock.acquireAsync().get(OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            assertTrue(hold.isHeld());
            final long registeredLease = rpc.singleLeaseIdOf();
            assertTrue(registeredLease != 0L, "the acquire registered exactly one lease");

            hold.release();
            waitUntil(
                    () ->
                            rpc.countOf(RaftCommand.Op.LEASE_EXPIRE) > 0
                                    || rpc.countOf(RaftCommand.Op.DELETE) > 0,
                    Duration.ofSeconds(5),
                    "The release never sent its cleanup command");
            assertEquals(
                    0,
                    rpc.countOf(RaftCommand.Op.DELETE),
                    "release must revoke the lease, never delete the lock path");
            assertEquals(
                    1,
                    rpc.countOf(RaftCommand.Op.LEASE_EXPIRE),
                    "release sends exactly one revocation");
            assertEquals(
                    registeredLease,
                    rpc.sentLeaseIdOf(RaftCommand.Op.LEASE_EXPIRE),
                    "the revoked lease is the holder's own private lease");
            assertFalse(hold.isHeld());
        } finally {
            client.close();
        }
    }

    /**
     * The ambiguous create: its command went out, the reply is still in flight (its outcome is
     * unknown to the contender), and the acquire is cancelled. The unwind must still speak only
     * the lease-scoped revocation - issued AFTER the create in the ordered pipeline, so the
     * server's leaseId-checked cascade covers a create that committed anyway - and never a path
     * delete, which could remove a later holder's record at the same path.
     */
    @Test
    void testAmbiguousCommittedCreateIsRetiredByRevocationNeverByAPathDelete() throws Exception {
        final ScriptedReplyRpc rpc = new ScriptedReplyRpc();
        rpc.holdCreatePath = "/stub/lock";
        final RaftMetadataClient client = newStubClient(rpc, null);
        try {
            final MetadataLock lock = newLockOn(client, "/stub/lock");
            final CompletableFuture<MetadataLock.Hold> pending = lock.acquireAsync();
            waitUntil(
                    () -> rpc.countOf(RaftCommand.Op.CREATE, "/stub/lock") == 1,
                    Duration.ofSeconds(5),
                    "The contender's create never went out");
            final long contendersLease = rpc.sentLeaseIdOf(RaftCommand.Op.CREATE, "/stub/lock");

            // Cancel while the create's outcome is unknown.
            lock.close();
            final ExecutionException failure =
                    assertThrows(
                            ExecutionException.class,
                            () -> pending.get(OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
            assertTrue(failure.getCause() instanceof IllegalStateException, "close cancels");

            waitUntil(
                    () ->
                            rpc.countOf(RaftCommand.Op.LEASE_EXPIRE) > 0
                                    || rpc.countOf(RaftCommand.Op.DELETE) > 0,
                    Duration.ofSeconds(5),
                    "The unwind never sent its cleanup command");
            assertEquals(
                    0,
                    rpc.countOf(RaftCommand.Op.DELETE),
                    "the ambiguous-undo must revoke, never delete the path");
            assertEquals(
                    contendersLease,
                    rpc.sentLeaseIdOf(RaftCommand.Op.LEASE_EXPIRE),
                    "the revoked lease is the one the ambiguous create bound its record to");
            assertTrue(
                    rpc.firstIndexOf(RaftCommand.Op.CREATE, "/stub/lock")
                            < rpc.firstIndexOf(RaftCommand.Op.LEASE_EXPIRE, null),
                    "the revocation follows the create in the ordered pipeline, so the cascade"
                            + " covers a create that committed anyway");

            // The delayed reply lands: the create had committed after all. Nothing may resurrect.
            rpc.completeHeld();
            Thread.sleep(200);
            assertEquals(
                    0, rpc.countOf(RaftCommand.Op.DELETE), "the late commit changed nothing");
            assertTrue(pending.isDone() && pending.isCompletedExceptionally());
        } finally {
            client.close();
        }
    }

    /**
     * A revocation that cannot reach the cluster (transport dead) is logged and the release falls
     * back to scheduler expiry within one ttl - the stopped renewals already doom the lease. It
     * must never fall back to a path delete.
     */
    @Test
    void testFailedRevocationFallsBackToExpiryNeverToAPathDelete() throws Exception {
        final ScriptedReplyRpc rpc = new ScriptedReplyRpc();
        final RaftMetadataClient client =
                newStubClient(
                        rpc,
                        RetryPolicies.retryUpToMaximumCountWithFixedSleep(
                                2, TimeDuration.valueOf(50, TimeUnit.MILLISECONDS)));
        try {
            final MetadataLock lock = newLockOn(client, "/stub/lock");
            final MetadataLock.Hold hold =
                    lock.acquireAsync().get(OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            rpc.failAll = true; // the revocation cannot get through anymore
            hold.release(); // must not throw
            waitUntil(
                    () -> rpc.countOf(RaftCommand.Op.LEASE_EXPIRE) > 0,
                    Duration.ofSeconds(5),
                    "The revocation never went out despite the failing transport");
            assertEquals(
                    0,
                    rpc.countOf(RaftCommand.Op.DELETE),
                    "a failed revocation must fall back to scheduler expiry, never to a path"
                            + " delete");
            assertFalse(hold.isHeld());
        } finally {
            client.close();
        }
    }

    // ------------------------------------------------------------------
    // Cluster tests
    // ------------------------------------------------------------------

    @Test
    void testWriteReadRoundtripAndConflictMapping() throws Exception {
        try (MetadataCluster cluster = newCluster(false)) {
            cluster.start();
            final RaftMetadataClient client = cluster.newMetadataClient();
            try {
                // A creating put requires the parent record (as ZooKeeper's setData fails NoNode).
                assertEquals("/a", client.create("/a", bytes("dir")));

                assertEquals(1L, client.put("/a/b", bytes("v1")));
                assertEquals(2L, client.put("/a/b", bytes("v2")));
                final RaftMetadataClient.MetadataRecord record = client.get("/a/b");
                assertArrayEquals(bytes("v2"), record.getData());
                assertEquals(2L, record.getVersion());
                assertEquals(
                        0L, record.getLeaseId(), "an unbound record carries no lease");

                assertEquals("/a/c", client.create("/a/c", bytes("c")));
                assertEquals(
                        RaftResult.Status.NODE_EXISTS,
                        expectMetadataFailure(() -> client.create("/a/c", bytes("x"))));
                assertEquals(
                        RaftResult.Status.NO_PARENT,
                        expectMetadataFailure(() -> client.create("/no/parent", bytes("x"))));

                // compare-and-swap: the matching version wins, a stale one loses, 0 means create.
                assertEquals(3L, client.compareAndPut("/a/b", bytes("v3"), 2L));
                assertArrayEquals(bytes("v3"), client.get("/a/b").getData());
                assertEquals(
                        RaftResult.Status.VERSION_MISMATCH,
                        expectMetadataFailure(
                                () -> client.compareAndPut("/a/b", bytes("v4"), 2L)));
                assertEquals(1L, client.compareAndPut("/a/fresh", bytes("f"), 0L));

                // Zero-padded numbers: lexicographic equals numeric at fixed width, never
                // colliding.
                client.create("/seq", bytes("dir"));
                final String first = client.createSequential("/seq/task-", bytes("1"));
                final String second = client.createSequential("/seq/task-", bytes("2"));
                final String third = client.createSequential("/seq/task-", bytes("3"));
                assertTrue(first.matches(SEQUENTIAL_PATH_PATTERN));
                assertTrue(
                        first.compareTo(second) < 0,
                        "sequential numbers grow monotonically");
                assertTrue(second.compareTo(third) < 0);

                // listing is a prefix scan in lexicographic order (the prefix record included).
                assertEquals(
                        Arrays.asList("/a", "/a/b", "/a/c", "/a/fresh"),
                        pathsOf(client.list("/a")));
                assertEquals(
                        Arrays.asList(first, second, third), pathsOf(client.list("/seq/task-")));

                assertTrue(client.exists("/a/b"));
                assertFalse(client.exists("/missing"));
                assertEquals(
                        RaftResult.Status.NOT_FOUND,
                        expectMetadataFailure(() -> client.get("/missing")));

                assertEquals(
                        RaftResult.Status.NOT_FOUND,
                        expectMetadataFailure(() -> client.delete("/never")));
                assertEquals(
                        RaftResult.Status.HAS_CHILDREN,
                        expectMetadataFailure(() -> client.delete("/a")));
                assertEquals(
                        4L,
                        client.deleteRecursive("/a"),
                        "the recursive delete removes the subtree and its root record");
                assertFalse(client.exists("/a"));
                assertFalse(client.exists("/a/b"));

                client.create("/m", bytes("dir"));
                final List<RaftResult> applied =
                        client.multi(
                                Arrays.asList(
                                        RaftCommand.put("/m/x", bytes("x"), 0L, 0L),
                                        RaftCommand.put("/m/y", bytes("y"), 0L, 0L)));
                assertEquals(2, applied.size());
                assertEquals(RaftResult.Status.OK, applied.get(0).getStatus());
                assertEquals(RaftResult.Status.OK, applied.get(1).getStatus());
                try {
                    client.multi(
                            Arrays.asList(
                                    RaftCommand.put("/m/z", bytes("z"), 0L, 0L),
                                    RaftCommand.cas("/m/x", bytes("clash"), 99L, 0L, 0L)));
                    throw new AssertionError("The failing MULTI was expected to throw");
                } catch (RaftMetadataException e) {
                    assertEquals(RaftResult.Status.MULTI_FAILED, e.getStatus());
                    assertEquals(1, e.getFailIndex());
                    assertEquals(
                            RaftResult.Status.VERSION_MISMATCH,
                            e.getFailStatus(),
                            "/m/x holds version 1, so the stale expected version loses");
                }
                assertFalse(
                        client.exists("/m/z"),
                        "a failed MULTI applies none of its sub-commands");
                assertTrue(client.exists("/m/x"));

                assertEquals(5L, client.increment("rounds", 5));
                assertEquals(3L, client.increment("rounds", -2));

                final RaftMetadataClient.PollResult everything = client.poll(0, POLL_BUDGET_BYTES);
                assertTrue(
                        everything.getWatermark() >= client.revision() - 1L,
                        "the watermark covers every write this test made");
                assertStrictlyIncreasing(revisionsOf(everything.getRecords()));
                assertEquals(
                        1L,
                        everything.getOldestRetainedRevision(),
                        "no snapshot was taken, so nothing was collected");
            } finally {
                client.close();
            }
        }
    }

    @Test
    void testAsyncRoundtripAndConcurrentWrites() throws Exception {
        try (MetadataCluster cluster = newCluster(false)) {
            cluster.start();
            final RaftMetadataClient client = cluster.newMetadataClient();
            try {
                assertEquals("/async", client.createAsync("/async", bytes("dir")).join());
                assertEquals(1L, client.putAsync("/async/b", bytes("v1")).join());
                assertEquals(2L, client.putAsync("/async/b", bytes("v2")).join());
                final RaftMetadataClient.MetadataRecord record =
                        client.getAsync("/async/b").join();
                assertArrayEquals(bytes("v2"), record.getData());
                assertEquals(2L, record.getVersion());

                // Concurrent async writes share one ordered pipeline but apply as separate
                // entries.
                client.createAsync("/async/p", bytes("dir")).join();
                final List<CompletableFuture<Long>> writes = new ArrayList<>();
                for (int i = 0; i < 10; i++) {
                    writes.add(client.putAsync("/async/p/" + i, bytes("v" + i)));
                }
                CompletableFuture.allOf(writes.toArray(new CompletableFuture[0])).join();
                for (CompletableFuture<Long> write : writes) {
                    assertEquals(1L, write.join());
                }

                final List<String> expected = new ArrayList<>();
                expected.add("/async");
                expected.add("/async/b");
                expected.add("/async/p");
                for (int i = 0; i < 10; i++) {
                    expected.add("/async/p/" + i);
                    assertTrue(client.existsAsync("/async/p/" + i).join());
                }
                assertEquals(expected, pathsOf(client.listAsync("/async").join()));

                // The status mapping of the sync surface holds in the async one.
                final RaftMetadataException conflict =
                        (RaftMetadataException)
                                awaitFailureOf(
                                        client.compareAndPutAsync("/async/b", bytes("v3"), 99L),
                                        "The stale compare-and-put never completed");
                assertEquals(RaftResult.Status.VERSION_MISMATCH, conflict.getStatus());

                assertEquals(5L, client.incrementAsync("async-rounds", 5).join());
                assertEquals(3L, client.incrementAsync("async-rounds", -2).join());

                // The async-opened session is a live session: renewable, and closed with the
                // client.
                final LeaseSession asyncLease = client.openLeaseAsync(Duration.ofSeconds(30)).join();
                assertTrue(
                        client.renewLeaseAsync(asyncLease.getLeaseId(), Duration.ofSeconds(30))
                                .join()
                                > 0);

                final long revisionBefore = client.revisionAsync().join();
                client.putAsync("/async/watermark", bytes("w")).join();
                final RaftMetadataClient.PollResult polled =
                        client.pollAsync(0L, POLL_BUDGET_BYTES).join();
                assertTrue(
                        polled.getWatermark() > revisionBefore,
                        "the async write is committed and applied");
                assertStrictlyIncreasing(revisionsOf(polled.getRecords()));
            } finally {
                client.close();
            }
        }
    }

    @Test
    void testConcurrentSequentialCreatesAreUnique() throws Exception {
        try (MetadataCluster cluster = newCluster(false)) {
            cluster.start();
            final RaftMetadataClient client = cluster.newMetadataClient();
            try {
                client.create("/jobs", bytes("dir"));

                final int threads = 4;
                final int createsPerThread = 25;
                final ExecutorService pool = Executors.newFixedThreadPool(threads);
                try {
                    final List<Future<List<String>>> futures = new ArrayList<>();
                    for (int t = 0; t < threads; t++) {
                        futures.add(
                                pool.submit(
                                        () -> {
                                            final List<String> created = new ArrayList<>();
                                            for (int i = 0; i < createsPerThread; i++) {
                                                created.add(
                                                        client.createSequential(
                                                                "/jobs/task-",
                                                                bytes("payload-" + i)));
                                            }
                                            return created;
                                        }));
                    }
                    final Set<String> unique = new HashSet<>();
                    for (Future<List<String>> future : futures) {
                        unique.addAll(
                                future.get(OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
                    }
                    assertEquals(
                            threads * createsPerThread,
                            unique.size(),
                            "every concurrent create got its own sequence number");
                    for (String path : unique) {
                        assertTrue(path.matches(SEQUENTIAL_PATH_PATTERN));
                    }
                    assertEquals(
                            new ArrayList<>(new TreeSet<>(unique)),
                            pathsOf(client.list("/jobs/task-")),
                            "the store lists exactly the acknowledged creates, in order");
                } finally {
                    pool.shutdownNow();
                }
            } finally {
                client.close();
            }
        }
    }

    @Test
    void testListPagesBeyondOneResultCap() throws Exception {
        try (MetadataCluster cluster = newCluster(false)) {
            cluster.start();
            final RaftMetadataClient client = cluster.newMetadataClient();
            try {
                client.create("/pages", bytes("dir"));

                // one MULTI is capped at RaftCommand.MAX_MULTI_OPS, so seed in batches
                final List<String> expected = new ArrayList<>();
                for (int batch = 0; batch < 2; batch++) {
                    final List<RaftCommand> puts = new ArrayList<>();
                    for (int i = 0; i < 600; i++) {
                        final String path =
                                String.format(Locale.ROOT, "/pages/%04d", batch * 600 + i);
                        expected.add(path);
                        puts.add(RaftCommand.put(path, new byte[0], 0L, 1L));
                    }
                    client.multi(puts);
                }

                final RaftMetadataClient.ListPage firstPage =
                        client.listPage("/pages/", "", 128);
                assertEquals(expected.subList(0, 128), pathsOf(firstPage.getEntries()));
                assertTrue(firstPage.hasMore());
                assertEquals(
                        expected,
                        pathsOf(client.list("/pages/")),
                        "the paging client enumerates the whole prefix");
            } finally {
                client.close();
            }
        }
    }

    @Test
    void testListEnumeratesOnePointInTimeCommand() throws Exception {
        try (MetadataCluster cluster = newCluster(false)) {
            cluster.start();
            final RaftMetadataClient client = cluster.newMetadataClient();
            try {
                client.create("/pit", bytes("dir"));

                final List<String> expected = new ArrayList<>();
                for (int batch = 0; batch < 2; batch++) {
                    final List<RaftCommand> puts = new ArrayList<>();
                    for (int i = 0; i < 600; i++) {
                        final String path =
                                String.format(Locale.ROOT, "/pit/%04d", batch * 600 + i);
                        expected.add(path);
                        puts.add(RaftCommand.put(path, new byte[0], 0L, 1L));
                    }
                    client.multi(puts);
                }
                // Sorts inside what a first 1024-cap page consumes; a resumed later page would
                // skip it.
                final String late = "/pit/0500x";
                final List<String> expectedWithLate = new ArrayList<>(expected);
                expectedWithLate.add(late);
                Collections.sort(expectedWithLate);

                // The prototype observed the single LIST request through a forwarding client
                // rpc; the stock transports admit no such seam, so the same write now races the
                // enumeration from a second thread and the outcome is asserted directly:
                // whatever the interleaving, the result is exactly one consistent view.
                final AtomicReference<Throwable> lateFailure = new AtomicReference<>();
                final Thread lateWriter =
                        new Thread(
                                () -> {
                                    try {
                                        client.put(late, bytes("late"));
                                    } catch (Throwable t) {
                                        lateFailure.set(t);
                                    }
                                },
                                "late-metadata-writer");
                lateWriter.start();
                final List<String> listed = pathsOf(client.list("/pit/"));
                lateWriter.join(OPERATION_TIMEOUT.toMillis());
                assertFalse(lateWriter.isAlive());
                assertNull(lateFailure.get());
                assertTrue(
                        expectedWithLate.containsAll(listed),
                        "the one-command enumeration is exactly one consistent view");
                assertTrue(
                        listed.containsAll(expected),
                        "the enumeration never misses a pre-existing record");
                assertArrayEquals(bytes("late"), client.get(late).getData());
            } finally {
                client.close();
            }
        }
    }

    @Test
    void testTransparentLeaderFailoverDuringWrites() throws Exception {
        try (MetadataCluster cluster = newCluster(false)) {
            cluster.start();
            final RaftMetadataClient client = cluster.newMetadataClient();
            try {
                // Cached once up front: leaderId() may return null while an election runs.
                final RaftPeerId leaderId = cluster.awaitLeader();
                final int leaderIndex = cluster.indexOf(leaderId);
                client.create("/fo", bytes("dir"));

                final ConcurrentLinkedQueue<String> created = new ConcurrentLinkedQueue<>();
                final AtomicReference<Throwable> writerFailure = new AtomicReference<>();
                final Thread writer =
                        new Thread(
                                () -> {
                                    try {
                                        for (int i = 0; i < FAILOVER_WRITES; i++) {
                                            created.add(
                                                    client.createSequential(
                                                            "/fo/item-", bytes("item-" + i)));
                                        }
                                    } catch (Throwable t) {
                                        writerFailure.set(t);
                                    }
                                },
                                "failover-writer");
                writer.start();

                waitUntil(
                        () -> created.size() >= 5,
                        OPERATION_TIMEOUT,
                        "The writer did not start producing through the first leader");
                final List<RaftPeerId> survivors = cluster.peerIds();
                survivors.remove(leaderId);
                cluster.killServer(leaderIndex);
                cluster.awaitLeader(survivors);

                writer.join(OPERATION_TIMEOUT.toMillis());
                assertFalse(writer.isAlive(), "the writer completed after the failover");
                assertNull(
                        writerFailure.get(),
                        "no exception may leak: the failover is transparent");
                assertEquals(FAILOVER_WRITES, created.size());
                assertEquals(
                        FAILOVER_WRITES,
                        new HashSet<>(created).size(),
                        "no sequential number was handed out twice");

                for (String path : created) {
                    assertTrue(client.exists(path));
                }
                assertEquals(
                        new ArrayList<>(new TreeSet<>(created)),
                        pathsOf(client.list("/fo/item-")));

                client.put("/fo/after-failover", bytes("ok"));
                assertArrayEquals(bytes("ok"), client.get("/fo/after-failover").getData());

                cluster.restartServer(leaderIndex);
                final MetadataStateMachine rejoined = cluster.stateMachine(leaderIndex);
                final long targetRevision = client.revision();
                waitUntil(
                        () ->
                                MetadataCluster.directQuery(rejoined, RaftCommand.revision())
                                                .getRevision()
                                        >= targetRevision,
                        OPERATION_TIMEOUT,
                        "The rejoined node did not catch up to revision " + targetRevision);
                assertArrayEquals(
                        bytes("ok"),
                        MetadataCluster.directQuery(rejoined, RaftCommand.get("/fo/after-failover"))
                                .getData());
            } finally {
                client.close();
            }
        }
    }

    @Test
    void testLeaseSessionLifecycleFromTheClientView() throws Exception {
        try (MetadataCluster cluster = newCluster(false)) {
            cluster.start();
            final RaftMetadataClient client = cluster.newMetadataClient();
            try {
                final long ttlMs = TimeUnit.SECONDS.toMillis(30);

                final LeaseSession session = client.openLease(Duration.ofSeconds(30));
                final long leaseId = session.getLeaseId();
                final long originalExpireAt = cluster.clock.milliseconds() + ttlMs;

                client.create("/eph", bytes("dir"));
                assertEquals("/eph/a", client.create("/eph/a", bytes("a"), session));
                client.put("/eph/b", bytes("b"), session);
                client.create("/eph/keep", bytes("keep"));
                assertEquals(leaseId, client.get("/eph/a").getLeaseId());

                for (int round = 1; round <= 4; round++) {
                    cluster.clock.advanceTime(10, TimeUnit.SECONDS);
                    final long expectedExpireAt = cluster.clock.milliseconds() + ttlMs;
                    MetadataCluster.waitThroughTransientFailure(
                            () -> {
                                final RaftResult.Lease lease =
                                        leaseOf(
                                                adminSendReadOnly(cluster, RaftCommand.leaseList()),
                                                leaseId);
                                return lease != null
                                        && lease.getExpireAtMs() == expectedExpireAt;
                            },
                            OPERATION_TIMEOUT,
                            "round "
                                    + round
                                    + ": the session did not renew to "
                                    + expectedExpireAt);
                }
                assertTrue(
                        cluster.clock.milliseconds() > originalExpireAt,
                        "the session outlived the lease's original expiry");
                assertTrue(client.exists("/eph/a"));
                assertTrue(client.exists("/eph/b"));

                // Past the last expiry plus the clock-domain grace (ttl/3), the scheduler takes
                // over.
                session.close();
                cluster.clock.advanceTime(45, TimeUnit.SECONDS);
                waitUntil(
                        () ->
                                !client.exists("/eph/a")
                                        && !client.exists("/eph/b")
                                        && client.exists("/eph/keep"),
                        OPERATION_TIMEOUT,
                        "The closed session's lease and ephemeral records were not expired");
                final RaftPeerId expiringLeader = cluster.awaitLeader(cluster.peerIds());
                // The harness exposes no per-server scheduler proposal counter; the lease
                // vanishing from the new leader's listing is the same expiry, observed from the
                // client side instead of the leader's scheduler metrics.
                MetadataCluster.waitThroughTransientFailure(
                        () ->
                                leaseOf(adminSendReadOnly(cluster, RaftCommand.leaseList()), leaseId)
                                        == null,
                        OPERATION_TIMEOUT,
                        "The leader "
                                + expiringLeader
                                + " did not process the expiry of the closed session");
            } finally {
                client.close();
            }
        }
    }

    @Test
    void testLeaseSessionLivenessSurfacesTheLeasesDeath() throws Exception {
        try (MetadataCluster cluster = newCluster(false)) {
            cluster.start();
            final RaftMetadataClient client = cluster.newMetadataClient();
            try {
                final long ttlMs = TimeUnit.SECONDS.toMillis(30);
                final long now = cluster.clock.milliseconds();
                // A lease with a ttl long enough that no clock advance of this test can make it
                // due.
                final long quietLeaseId = 801L;
                final long deadLeaseId = 802L;
                assertEquals(
                        RaftResult.Status.OK,
                        adminSend(
                                        cluster,
                                        RaftCommand.leaseRegister(
                                                quietLeaseId, 10 * ttlMs, now + 10 * ttlMs))
                                .getStatus());
                assertEquals(
                        RaftResult.Status.OK,
                        adminSend(
                                        cluster,
                                        RaftCommand.leaseRegister(deadLeaseId, ttlMs, now + ttlMs))
                                .getStatus());

                final LeaseSession quiet =
                        new LeaseSession(client, quietLeaseId, 10 * ttlMs, cluster.clock);
                quiet.start();
                assertTrue(quiet.isAlive(), "a started session on a live lease is alive");
                quiet.close();
                assertFalse(quiet.isAlive(), "closing the session ends its liveness");

                final LeaseSession paused =
                        new LeaseSession(client, deadLeaseId, ttlMs, cluster.clock);
                assertFalse(
                        paused.isAlive(), "a session that was never started is not alive");
                cluster.clock.advanceTime(45, TimeUnit.SECONDS);
                MetadataCluster.waitThroughTransientFailure(
                        () ->
                                leaseOf(adminSendReadOnly(cluster, RaftCommand.leaseList()), deadLeaseId)
                                        == null,
                        OPERATION_TIMEOUT,
                        "The paused holder's lease was not expired underneath it");
                paused.start();
                assertTrue(
                        paused.isAlive(),
                        "the session is alive until its renewal is rejected");
                waitUntil(
                        () -> !paused.isAlive(),
                        OPERATION_TIMEOUT,
                        "The session did not surface its lease's death through isAlive()");
                paused.close();
            } finally {
                client.close();
            }
        }
    }

    /**
     * End-to-end through the real renewal machinery: once a session's lease dies underneath it,
     * the {@code UNKNOWN_LEASE} renewal detaches the session from the client's registry and the
     * final tick dies with it - and a session opened afterwards ticks again, proving the registry
     * was truly emptied and refilled. The lease's death is forced through the admin client (the
     * exact command the leader's scheduler proposes after a ttl lapse) so the death is
     * deterministic; racing the scheduler's scan cadence against the session's own renewal would
     * let the renewal re-extend the lease instead.
     */
    @Test
    void testExpiredSessionDetachesAndTickingResumesForTheNextSession() throws Exception {
        try (MetadataCluster cluster = newCluster(false)) {
            cluster.start();
            final RaftMetadataClient client = cluster.newMetadataClient();
            try {
                final LeaseSession session = client.openLease(Duration.ofSeconds(2));
                final long leaseId = session.getLeaseId();
                client.create("/det", bytes("dir"));
                assertEquals("/det/a", client.create("/det/a", bytes("a"), session));
                assertTrue(client.sessions.contains(session), "the session is registered");
                assertNotNull(sessionTickOf(client), "the tick is live");

                // The lease dies underneath the session; its bound record cascades away.
                assertEquals(
                        RaftResult.Status.OK,
                        adminSend(cluster, RaftCommand.leaseExpire(leaseId, -1L)).getStatus());
                waitUntil(
                        () -> !client.exists("/det/a"),
                        OPERATION_TIMEOUT,
                        "The dead lease's bound record was not cascaded away");

                // One renewal interval past due: the session's next renewal returns
                // UNKNOWN_LEASE - the terminal path.
                cluster.clock.advanceTime(3, TimeUnit.SECONDS);
                waitUntil(
                        () -> !session.isAlive(),
                        OPERATION_TIMEOUT,
                        "The session did not surface its lease's death");
                assertFalse(
                        client.sessions.contains(session),
                        "the expired session must leave the client's registry");
                assertNull(
                        sessionTickOf(client),
                        "the final tick must be cancelled once the last session is gone");

                // The reopened session re-attaches and ticks again.
                final LeaseSession reopened = client.openLease(Duration.ofSeconds(30));
                assertTrue(client.sessions.contains(reopened));
                waitUntil(
                        () -> sessionTickOf(client) != null,
                        OPERATION_TIMEOUT,
                        "The tick did not restart for the reopened session");
                final long renewIndexAtOpen = lastRenewIndexOf(cluster, reopened.getLeaseId());
                cluster.clock.advanceTime(11, TimeUnit.SECONDS);
                waitUntil(
                        () -> lastRenewIndexOf(cluster, reopened.getLeaseId()) > renewIndexAtOpen,
                        OPERATION_TIMEOUT,
                        "The reopened session did not renew through the recreated tick");
                reopened.close();
            } finally {
                client.close();
            }
        }
    }

    /**
     * Renewal due-ness is monotonic: a client whose wall milliseconds stand still (skewed away
     * from the servers') still renews once its {@link Clock#nanoseconds()} advance - wall-clock
     * gating would starve the lease while the holder is perfectly alive. The leader's expiry
     * decision is unaffected: it uses the replicated leader sample (its own clock never moves
     * here), so the lease never lapses server-side during the test.
     */
    @Test
    void testSkewedWallClockClientStillRenewsByMonotonicTime() throws Exception {
        try (MetadataCluster cluster = newCluster(false)) {
            cluster.start();
            final Clock skewed =
                    new Clock() {
                        @Override
                        public long milliseconds() {
                            return 987_654L; // frozen, and far from the servers' clock
                        }

                        @Override
                        public long nanoseconds() {
                            return System.nanoTime(); // advancing
                        }
                    };
            final RaftMetadataClient client = cluster.newMetadataClient(skewed);
            try {
                final LeaseSession session =
                        client.openLeaseAsync(Duration.ofSeconds(3)).join();
                final long leaseId = session.getLeaseId();
                final long renewIndexAtOpen = lastRenewIndexOf(cluster, leaseId);
                assertTrue(renewIndexAtOpen > 0L, "the registration stamped a renew index");

                waitUntil(
                        () -> lastRenewIndexOf(cluster, leaseId) > renewIndexAtOpen,
                        Duration.ofSeconds(15),
                        "The session never renewed: due-ness starved on the frozen wall clock");
                assertNotNull(
                        leaseOf(adminSendReadOnly(cluster, RaftCommand.leaseList()), leaseId),
                        "the lease stayed alive server-side throughout");
                session.close();
            } finally {
                client.close();
            }
        }
    }

    /**
     * The typed revocation through the client: it removes exactly the revoked lease's records,
     * leaves every other lease alone, and is idempotent - revoking an unknown or already-expired
     * lease succeeds as a no-op rather than failing.
     */
    @Test
    void testRevokeThroughTheClientIsIdempotentAndScopedToItsOwnLease() throws Exception {
        try (MetadataCluster cluster = newCluster(false)) {
            cluster.start();
            final RaftMetadataClient client = cluster.newMetadataClient();
            try {
                final LeaseSession sessionA =
                        client.openLeaseAsync(Duration.ofSeconds(30)).join();
                final LeaseSession sessionB =
                        client.openLeaseAsync(Duration.ofSeconds(30)).join();
                client.create("/rk", bytes("dir"));
                assertEquals("/rk/of-a", client.create("/rk/of-a", bytes("a"), sessionA));
                assertEquals("/rk/of-b", client.create("/rk/of-b", bytes("b"), sessionB));

                assertEquals(
                        1L,
                        client.revokeLeaseAsync(sessionA.getLeaseId()).join(),
                        "the revocation cascaded exactly session A's record");
                assertFalse(client.exists("/rk/of-a"));
                assertTrue(client.exists("/rk/of-b"), "the sibling lease's record is untouched");

                assertEquals(
                        0L,
                        client.revokeLeaseAsync(sessionA.getLeaseId()).join(),
                        "a repeated revoke of the now-unknown lease is a no-op success");
                assertTrue(client.exists("/rk/of-b"), "the repeat touched nothing");

                assertEquals(
                        1L,
                        client.revokeLeaseAsync(sessionB.getLeaseId()).join(),
                        "the other lease revokes on its own");
                assertFalse(client.exists("/rk/of-b"));
                assertTrue(client.exists("/rk"), "the persistent parent record stays");
            } finally {
                client.close();
            }
        }
    }

    @Test
    void testWatcherCheckpointResumeAndNeedSnapshot() throws Exception {
        try (MetadataCluster cluster = newCluster(true)) {
            cluster.start();
            final RaftMetadataClient client = cluster.newMetadataClient();
            try {
                client.put("/wa", bytes("a"));
                client.put("/wb", bytes("b"));
                client.put("/wc", bytes("c"));

                final Recorder liveRecorder = new Recorder();
                final MetadataWatcher live =
                        MetadataWatcher.newBuilder(client, liveRecorder)
                                .setSinceRevision(0)
                                .setPollInterval(Duration.ofMillis(100))
                                .build();
                live.start();
                waitUntil(
                        () ->
                                createsUnder(liveRecorder.deliveredEvents())
                                        .equals(Arrays.asList("/wa", "/wb", "/wc")),
                        OPERATION_TIMEOUT,
                        "The live watcher did not receive the first three writes in order");
                assertTrue(live.lastDeliveredRevision() > 0L);

                client.put("/wd", bytes("d"));
                client.put("/we", bytes("e"));
                waitUntil(
                        () ->
                                createsUnder(liveRecorder.deliveredEvents())
                                        .equals(
                                                Arrays.asList(
                                                        "/wa", "/wb", "/wc", "/wd", "/we")),
                        OPERATION_TIMEOUT,
                        "The live watcher did not receive the pre-snapshot writes in order");

                // Snapshot + purge: the retention line becomes snapshotIndex + 1, seen via
                // admin first. The probe is addressed to the member that took the snapshot:
                // the retention line is per-member GC state, and under the LINEARIZABLE
                // default any member - including a follower with a stale line - legitimately
                // serves reads, so an untargeted probe can observe (and pin the client's
                // leader cache to) the wrong member.
                final RaftPeerId snapshotTaker = cluster.leaderId();
                final long snapshotIndex = cluster.takeSnapshotOnLeader();
                final long retentionLine = snapshotIndex + 1;
                MetadataCluster.waitThroughTransientFailure(
                        () -> {
                            final RaftResult probe =
                                    adminSendReadOnly(
                                            cluster,
                                            RaftCommand.pollChanges(0, POLL_BUDGET_BYTES),
                                            snapshotTaker);
                            return probe.getStatus() == RaftResult.Status.NEED_SNAPSHOT
                                    && probe.getOldestRetainedRevision() == retentionLine;
                        },
                        OPERATION_TIMEOUT,
                        "The retention line did not advance to " + retentionLine);

                final Recorder resyncRecorder = new Recorder();
                final MetadataWatcher resync =
                        MetadataWatcher.newBuilder(client, resyncRecorder)
                                .setSinceRevision(0)
                                .setPollInterval(Duration.ofMillis(100))
                                .build();
                resync.start();
                waitUntil(
                        () ->
                                resyncRecorder.needSnapshotLine() != null
                                        && !resync.isRunning(),
                        OPERATION_TIMEOUT,
                        "The watcher below the retention line did not stop with NEED_SNAPSHOT");
                assertEquals(
                        Long.valueOf(retentionLine), resyncRecorder.needSnapshotLine());
                assertTrue(
                        resyncRecorder.deliveredEvents().isEmpty(),
                        "a watcher below the line delivers nothing before stopping");

                resync.restartFrom(retentionLine - 1);
                // The restart may land while the report is still completing: the handoff is
                // last-wins, so the new chain leaves within moments - not synchronously.
                waitUntil(
                        resync::isRunning,
                        OPERATION_TIMEOUT,
                        "The watcher did not resume from the retention line");
                client.put("/wf", bytes("f"));
                waitUntil(
                        () -> createsUnder(resyncRecorder.deliveredEvents()).contains("/wf"),
                        OPERATION_TIMEOUT,
                        "The resumed watcher did not receive the post-resume write");
                assertTrue(resync.lastDeliveredRevision() > snapshotIndex);
                assertEquals(
                        Long.valueOf(retentionLine),
                        resyncRecorder.needSnapshotLine(),
                        "resuming at the line does not re-trigger NEED_SNAPSHOT");
                assertStrictlyIncreasing(revisionsOf(resyncRecorder.deliveredEvents()));

                waitUntil(
                        () ->
                                !live.isRunning()
                                        || createsUnder(liveRecorder.deliveredEvents())
                                                .contains("/wf"),
                        OPERATION_TIMEOUT,
                        "The live watcher neither stopped at the line nor kept streaming");
                if (live.isRunning()) {
                    assertTrue(createsUnder(liveRecorder.deliveredEvents()).contains("/wf"));
                } else {
                    assertEquals(
                            Long.valueOf(retentionLine), liveRecorder.needSnapshotLine());
                }
                live.close();
                resync.close();
            } finally {
                client.close();
            }
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /** A lock on a stub client: a ttl far from due, so no renewal muddies the recorded ops. */
    private static MetadataLock newLockOn(RaftMetadataClient client, String path) {
        return MetadataLock.newBuilder()
                .setClient(client)
                .setPath(path)
                .setTtl(Duration.ofSeconds(60))
                .build();
    }

    /**
     * The client's session-tick handle, read reflectively: the harness asserts the final tick is
     * cancelled and later recreated without widening the production surface. Reading without the
     * client's session lock is a benign race - the callers poll until the expected value appears.
     */
    private static Object sessionTickOf(RaftMetadataClient client) {
        try {
            final Field field = RaftMetadataClient.class.getDeclaredField("sessionTick");
            field.setAccessible(true);
            return field.get(client);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Cannot read the client's session tick", e);
        }
    }

    /** The lease's replicated last-renew index, read through the admin listing. */
    private static long lastRenewIndexOf(MetadataCluster cluster, long leaseId) throws IOException {
        final RaftResult.Lease lease =
                leaseOf(adminSendReadOnly(cluster, RaftCommand.leaseList()), leaseId);
        return lease != null ? lease.getLastRenewIndex() : -1L;
    }

    private static RaftMetadataClient newStubClient(RaftClientRpc rpc, RetryPolicy retryPolicy) {
        // Mirrors the Builder's async wiring, so stub clients behave like built ones.
        final RaftProperties properties = new RaftProperties();
        RaftClientConfigKeys.Async.Experimental.setSendDummyRequest(properties, false);
        final RaftClient.Builder builder =
                RaftClient.newBuilder()
                        .setRaftGroup(
                                RaftGroup.valueOf(
                                        RaftGroupId.valueOf(UUID.fromString(GROUP_UUID)),
                                        RaftPeer.newBuilder()
                                                .setId("n0")
                                                .setAddress("localhost:1")
                                                .build()))
                        .setProperties(properties)
                        .setClientRpc(rpc);
        if (retryPolicy != null) {
            builder.setRetryPolicy(retryPolicy);
        }
        return new RaftMetadataClient(builder.build(), SystemClock.getInstance());
    }

    /**
     * The failure a metadata future completed with, unwrapped the way a consumer's exception
     * handling would see it; never completing fails the test instead.
     */
    private static <T> Throwable awaitFailureOf(CompletableFuture<T> future, String what) {
        waitUntil(future::isDone, OPERATION_TIMEOUT, what);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        future.whenComplete(
                (reply, e) -> {
                    if (e != null) {
                        failure.set(ExceptionUtils.stripCompletionException(e));
                    }
                });
        assertNotNull(failure.get(), what + ": the future completed normally instead");
        return failure.get();
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /** A bound-then-released port: connections are refused within ms, no connect-timeout wait. */
    private static int refusedPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    // ------------------------------------------------------------------
    // Cluster-suite helpers
    // ------------------------------------------------------------------

    /** The 3-member in-process cluster on the stock transport, under the suite's group uuid. */
    private MetadataCluster newCluster(boolean purgeForSnapshot) {
        return MetadataCluster.newBuilder(tempDir)
                .setNumServers(3)
                .setPurgeForSnapshot(purgeForSnapshot)
                .setGroupUuid(UUID.fromString(GROUP_UUID))
                .build();
    }

    private static RaftResult adminSend(MetadataCluster cluster, RaftCommand command)
            throws IOException {
        return decodeReply(cluster.adminClient().io().send(toMessage(command)), command);
    }

    private static RaftResult adminSendReadOnly(MetadataCluster cluster, RaftCommand command)
            throws IOException {
        return decodeReply(cluster.adminClient().io().sendReadOnly(toMessage(command)), command);
    }

    /** Targets one member explicitly - for per-member state (e.g. the retention line) where an
     * untargeted read may legally land on a follower under the LINEARIZABLE default. */
    private static RaftResult adminSendReadOnly(
            MetadataCluster cluster, RaftCommand command, RaftPeerId server) throws IOException {
        return decodeReply(
                cluster.adminClient().io().sendReadOnly(toMessage(command), server), command);
    }

    private static RaftResult.Status expectMetadataFailure(Runnable action) {
        try {
            action.run();
        } catch (RaftMetadataException e) {
            return e.getStatus();
        }
        throw new AssertionError("The action was expected to fail with RaftMetadataException");
    }

    /** The ordered CREATE paths a watcher delivered (all events, in delivery order). */
    private static List<String> createsUnder(List<ChangeEvent> events) {
        final List<String> creates = new ArrayList<>();
        for (ChangeEvent event : events) {
            for (ChangeEvent.Change change : event.getChanges()) {
                if (change.getOp() == ChangeEvent.ChangeOp.CREATE && !change.getPath().isEmpty()) {
                    creates.add(change.getPath());
                }
            }
        }
        return creates;
    }

    private static List<String> pathsOf(List<RaftResult.Entry> entries) {
        final List<String> paths = new ArrayList<>();
        for (RaftResult.Entry entry : entries) {
            paths.add(entry.getPath());
        }
        return paths;
    }

    private static List<Long> revisionsOf(List<ChangeEvent> records) {
        final List<Long> revisions = new ArrayList<>();
        for (ChangeEvent record : records) {
            revisions.add(record.getRevision());
        }
        return revisions;
    }

    /** Adjacent revisions must strictly grow: sorted, with no duplicate checkpoint. */
    private static void assertStrictlyIncreasing(List<Long> values) {
        for (int i = 1; i < values.size(); i++) {
            assertTrue(
                    values.get(i - 1) < values.get(i),
                    "revisions must be strictly increasing: " + values);
        }
    }

    /** The lease entry of the given id, or null when the listing does not contain it. */
    private static RaftResult.Lease leaseOf(RaftResult listing, long leaseId) {
        assertEquals(RaftResult.Status.OK, listing.getStatus());
        for (RaftResult.Lease lease : listing.getLeases()) {
            if (lease.getLeaseId() == leaseId) {
                return lease;
            }
        }
        return null;
    }

    private static RaftResult decodeReply(RaftClientReply reply, RaftCommand command) {
        final RaftResult result =
                RaftCommandCodec.decodeResult(reply.getMessage().getContent().toByteArray());
        assertNotNull(
                result, "reply of " + command.getOp() + " decodes (op echo must match)");
        assertEquals(command.getOp(), result.getOp());
        return result;
    }

    private static Message toMessage(RaftCommand command) {
        return Message.valueOf(ByteBuffer.wrap(RaftCommandCodec.encodeCommand(command)));
    }

    /** Collects a watcher's stream: every record, plus the retention line if hit. */
    private static final class Recorder implements MetadataWatcher.Listener {

        private final List<ChangeEvent> events = new ArrayList<>();
        private Long needSnapshotLine;

        @Override
        public synchronized void onRecord(ChangeEvent event) {
            events.add(event);
        }

        @Override
        public synchronized void onNeedSnapshot(long oldestRetainedRevision) {
            needSnapshotLine = oldestRetainedRevision;
        }

        synchronized List<ChangeEvent> deliveredEvents() {
            return new ArrayList<>(events);
        }

        synchronized Long needSnapshotLine() {
            return needSnapshotLine;
        }
    }

    /** Serves one async poll at a time; the test decides when each reply arrives. */
    private static final class GatedNeedSnapshotRpc implements RaftClientRpc {

        private final byte[] reply =
                RaftCommandCodec.encodeResult(
                        RaftResult.error(
                                RaftCommand.Op.POLL_CHANGES, RaftResult.Status.NEED_SNAPSHOT));
        private RaftClientRequest firstRequest;
        private CompletableFuture<RaftClientReply> firstReply;

        @Override
        public synchronized CompletableFuture<RaftClientReply> sendRequestAsync(
                RaftClientRequest request) {
            final CompletableFuture<RaftClientReply> reply = new CompletableFuture<>();
            if (firstReply == null) {
                firstRequest = request;
                firstReply = reply;
            } else {
                // No second poll is expected while the report never completed; failing the
                // request loudly beats letting an unexpected chain poll on silently.
                reply.completeExceptionally(
                        new AssertionError("A second poll reached the stub: " + request));
            }
            return reply;
        }

        synchronized boolean firstPollInFlight() {
            return firstReply != null;
        }

        synchronized void answerFirstPoll() {
            firstReply.complete(replyOf(firstRequest));
        }

        private RaftClientReply replyOf(RaftClientRequest request) {
            return RaftClientReply.newBuilder()
                    .setRequest(request)
                    .setSuccess()
                    .setMessage(Message.valueOf(ByteBuffer.wrap(reply)))
                    .build();
        }

        @Override
        public RaftClientReply sendRequest(RaftClientRequest request) {
            throw new UnsupportedOperationException("This stub speaks async only");
        }

        @Override
        public void addRaftPeers(Collection<RaftPeer> peers) {}

        @Override
        public void close() {}
    }

    private static final class FailingTransportRpc implements RaftClientRpc {

        final AtomicInteger attempts = new AtomicInteger();

        @Override
        public RaftClientReply sendRequest(RaftClientRequest request) throws IOException {
            attempts.incrementAndGet();
            throw new IOException("stub transport failure");
        }

        @Override
        public CompletableFuture<RaftClientReply> sendRequestAsync(RaftClientRequest request) {
            attempts.incrementAndGet();
            return FutureUtils.completedExceptionally(
                    new IOException("stub transport failure"));
        }

        @Override
        public void addRaftPeers(Collection<RaftPeer> peers) {}

        @Override
        public void close() {}
    }

    /** An executor this test owns: it queues submissions without ever running them. */
    private static final class QueueingExecutor implements Executor {

        final Queue<Runnable> submitted = new ConcurrentLinkedQueue<>();

        @Override
        public void execute(Runnable command) {
            submitted.add(command);
        }
    }

    /** An executor that refuses every unit: the watcher must record the rejection and stop. */
    private static final class RejectingExecutor implements Executor {

        @Override
        public void execute(Runnable command) {
            throw new RejectedExecutionException("scripted executor rejection");
        }
    }

    /** A listener whose first record callback parks until the test releases it. */
    private static final class BlockingListener implements MetadataWatcher.Listener {

        private final List<ChangeEvent> events = new CopyOnWriteArrayList<>();
        private final AtomicInteger inListener = new AtomicInteger();
        private final AtomicBoolean overlap = new AtomicBoolean();
        private final AtomicBoolean firstPending = new AtomicBoolean(true);
        private final CountDownLatch firstEntered = new CountDownLatch(1);
        private final CountDownLatch releaseFirst = new CountDownLatch(1);
        private final CountDownLatch firstReturned = new CountDownLatch(1);
        private volatile Long needSnapshotLine;

        @Override
        public void onRecord(ChangeEvent event) {
            enter();
            try {
                events.add(event);
                if (firstPending.compareAndSet(true, false)) {
                    firstEntered.countDown();
                    awaitReleased();
                }
            } finally {
                firstReturned.countDown();
                inListener.decrementAndGet();
            }
        }

        @Override
        public void onNeedSnapshot(long oldestRetainedRevision) {
            enter();
            try {
                needSnapshotLine = oldestRetainedRevision;
            } finally {
                inListener.decrementAndGet();
            }
        }

        /** Un-parks the first callback. */
        void release() {
            releaseFirst.countDown();
        }

        List<ChangeEvent> events() {
            return events;
        }

        boolean overlapped() {
            return overlap.get();
        }

        private void enter() {
            if (inListener.incrementAndGet() != 1) {
                overlap.set(true);
            }
        }

        private void awaitReleased() {
            try {
                releaseFirst.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while parked in the listener", e);
            }
        }
    }

    /**
     * Serves the watcher's polls; the test decides when each reply arrives and what it carries
     * (records, a watermark-only empty poll, or NEED_SNAPSHOT). Every poll's
     * {@code sinceRevision} is recorded, so a chain that polled again mid-delivery is caught.
     */
    private static final class GatedPollRpc implements RaftClientRpc {

        /** Trips the moment a poll beyond the first arrives - the single-flight probe. */
        final CountDownLatch anotherPollArrived = new CountDownLatch(1);

        private final List<Long> sinceRevisions = new CopyOnWriteArrayList<>();
        private final Queue<PendingPoll> inFlight = new ConcurrentLinkedQueue<>();

        private static final class PendingPoll {
            private final RaftClientRequest request;
            private final CompletableFuture<RaftClientReply> reply;

            PendingPoll(RaftClientRequest request, CompletableFuture<RaftClientReply> reply) {
                this.request = request;
                this.reply = reply;
            }
        }

        @Override
        public synchronized CompletableFuture<RaftClientReply> sendRequestAsync(
                RaftClientRequest request) {
            final RaftCommand command =
                    RaftCommandCodec.decodeCommand(
                            request.getMessage().getContent().toByteArray());
            if (command.getOp() != RaftCommand.Op.POLL_CHANGES) {
                final CompletableFuture<RaftClientReply> reply = new CompletableFuture<>();
                reply.completeExceptionally(
                        new AssertionError("The watcher speaks POLL_CHANGES only: " + command));
                return reply;
            }
            sinceRevisions.add(command.getSinceRevision());
            if (sinceRevisions.size() > 1) {
                anotherPollArrived.countDown();
            }
            final CompletableFuture<RaftClientReply> reply = new CompletableFuture<>();
            inFlight.add(new PendingPoll(request, reply));
            return reply;
        }

        synchronized int pollCount() {
            return sinceRevisions.size();
        }

        /** The {@code sinceRevision} of the n-th poll (1-based), in poll order. */
        synchronized long sinceRevisionOf(int n) {
            return sinceRevisions.get(n - 1);
        }

        synchronized boolean hasInFlightPoll() {
            return !inFlight.isEmpty();
        }

        /** Answers the oldest in-flight poll with one CREATE record per given revision. */
        void answerRecords(long watermark, long... revisions) {
            final List<ChangeEvent> records = new ArrayList<>();
            for (long revision : revisions) {
                records.add(
                        new ChangeEvent(
                                revision,
                                Collections.singletonList(
                                        new ChangeEvent.Change(
                                                ChangeEvent.ChangeOp.CREATE,
                                                "/rev-" + revision,
                                                new byte[0]))));
            }
            final RaftResult result = RaftResult.ok(RaftCommand.Op.POLL_CHANGES);
            setRaftResultField(result, "revision", watermark);
            setRaftResultField(result, "records", records);
            answer(result);
        }

        /** Answers the oldest in-flight poll with NEED_SNAPSHOT of the given retention line. */
        void answerNeedSnapshot(long oldestRetainedRevision) {
            final RaftResult result =
                    RaftResult.error(
                            RaftCommand.Op.POLL_CHANGES, RaftResult.Status.NEED_SNAPSHOT);
            setRaftResultField(result, "oldestRetainedRevision", oldestRetainedRevision);
            answer(result);
        }

        private void answer(RaftResult result) {
            final PendingPoll poll = inFlight.poll();
            assertNotNull(poll, "no poll in flight to answer");
            poll.reply.complete(
                    RaftClientReply.newBuilder()
                            .setRequest(poll.request)
                            .setSuccess()
                            .setMessage(
                                    Message.valueOf(
                                            ByteBuffer.wrap(
                                                    RaftCommandCodec.encodeResult(result))))
                            .build());
        }

        @Override
        public RaftClientReply sendRequest(RaftClientRequest request) {
            throw new UnsupportedOperationException("This stub speaks async only");
        }

        @Override
        public void addRaftPeers(Collection<RaftPeer> peers) {}

        @Override
        public void close() {}
    }

    /**
     * Sets one {@link RaftResult} payload field for a scripted reply: the fluent setters are
     * package-private to the state machine while the watcher stubs live here. A field rename
     * breaks this probe loudly, which is the intended failure mode.
     */
    private static void setRaftResultField(RaftResult result, String name, Object value) {
        try {
            final Field field = RaftResult.class.getDeclaredField(name);
            field.setAccessible(true);
            field.set(result, value);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Cannot script RaftResult." + name, e);
        }
    }

    /**
     * Requires the latch NOT to trip within the window. The defect each call guards against
     * trips its latch within microseconds of its cause - never near the bound - so the bound
     * only ends the observation once absence is proven beyond any scheduling noise.
     */
    private static void assertNeverTripped(CountDownLatch latch, Duration window, String what) {
        final boolean tripped;
        try {
            tripped = latch.await(window.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while watching: " + what, e);
        }
        assertFalse(tripped, what);
    }

    /** One command the client sent, decoded: the op plus the path and lease it acted on. */
    private static final class SentCommand {
        final RaftCommand.Op op;
        final String path;
        final long leaseId;

        SentCommand(RaftCommand command) {
            this.op = command.getOp();
            this.path = command.getPath();
            this.leaseId = command.getLeaseId();
        }

        boolean matches(RaftCommand.Op anOp, String aPath) {
            return op == anOp && (aPath == null || aPath.equals(path));
        }

        @Override
        public String toString() {
            return op + "(" + path + ", lease " + leaseId + ")";
        }
    }

    /**
     * The scripted transport of the lock suites: every async request is recorded and answered
     * from a small script - ok by default, an error status per op, all failing, or specific
     * replies withheld until the test releases them.
     */
    private static final class ScriptedReplyRpc implements RaftClientRpc {

        /** Per-op error statuses to answer with (business errors, not transport failures). */
        final Map<RaftCommand.Op, RaftResult.Status> errorStatuses = new ConcurrentHashMap<>();

        /** Replies of the CREATE of exactly this path are withheld until {@link #completeHeld}. */
        volatile String holdCreatePath;

        /** Replies of every LEASE_RENEW are withheld until {@link #completeHeld}. */
        volatile boolean holdRenewals;

        /** Every request fails at the transport level when set. */
        volatile boolean failAll;

        private final List<SentCommand> sent = new CopyOnWriteArrayList<>();
        private final List<RaftClientRequest> heldRequests = new CopyOnWriteArrayList<>();
        private final List<CompletableFuture<RaftClientReply>> heldReplies =
                new CopyOnWriteArrayList<>();

        @Override
        public synchronized CompletableFuture<RaftClientReply> sendRequestAsync(
                RaftClientRequest request) {
            final RaftCommand command =
                    RaftCommandCodec.decodeCommand(
                            request.getMessage().getContent().toByteArray());
            sent.add(new SentCommand(command));
            final CompletableFuture<RaftClientReply> reply = new CompletableFuture<>();
            final boolean hold =
                    (command.getOp() == RaftCommand.Op.CREATE
                                    && command.getPath() != null
                                    && command.getPath().equals(holdCreatePath))
                            || (command.getOp() == RaftCommand.Op.LEASE_RENEW && holdRenewals);
            if (hold) {
                heldRequests.add(request);
                heldReplies.add(reply);
                return reply;
            }
            settle(request, command, reply);
            return reply;
        }

        private void settle(
                RaftClientRequest request, RaftCommand command,
                CompletableFuture<RaftClientReply> reply) {
            if (failAll) {
                reply.completeExceptionally(new IOException("scripted transport failure"));
                return;
            }
            final RaftResult.Status status = errorStatuses.get(command.getOp());
            final RaftResult result =
                    status != null
                            ? RaftResult.error(command.getOp(), status)
                            : RaftResult.ok(command.getOp());
            reply.complete(
                    RaftClientReply.newBuilder()
                            .setRequest(request)
                            .setSuccess()
                            .setMessage(
                                    Message.valueOf(
                                            ByteBuffer.wrap(
                                                    RaftCommandCodec.encodeResult(result))))
                            .build());
        }

        /** Completes every withheld reply successfully - the delayed outcome finally lands. */
        synchronized void completeHeld() {
            for (int i = 0; i < heldReplies.size(); i++) {
                final CompletableFuture<RaftClientReply> reply = heldReplies.get(i);
                final RaftClientRequest request = heldRequests.get(i);
                if (!reply.isDone()) {
                    settle(request, lastCommandOf(request), reply);
                }
            }
        }

        private static RaftCommand lastCommandOf(RaftClientRequest request) {
            return RaftCommandCodec.decodeCommand(
                    request.getMessage().getContent().toByteArray());
        }

        /** How many commands of this op were sent ({@code path} null = any path). */
        int countOf(RaftCommand.Op op) {
            return countOf(op, null);
        }

        int countOf(RaftCommand.Op op, String path) {
            int count = 0;
            for (SentCommand command : sent) {
                if (command.matches(op, path)) {
                    count++;
                }
            }
            return count;
        }

        /** The lease id of the first command of this op; 0 when none was sent. */
        long sentLeaseIdOf(RaftCommand.Op op) {
            for (SentCommand command : sent) {
                if (command.op == op) {
                    return command.leaseId;
                }
            }
            return 0L;
        }

        long sentLeaseIdOf(RaftCommand.Op op, String path) {
            for (SentCommand command : sent) {
                if (command.matches(op, path)) {
                    return command.leaseId;
                }
            }
            return 0L;
        }

        /** The lease id of the single lease the client ever registered. */
        long singleLeaseIdOf() {
            return sentLeaseIdOf(RaftCommand.Op.LEASE_REGISTER);
        }

        /** Index of the first command of this op in send order; -1 when never sent. */
        int firstIndexOf(RaftCommand.Op op, String path) {
            for (int i = 0; i < sent.size(); i++) {
                if (sent.get(i).matches(op, path)) {
                    return i;
                }
            }
            return -1;
        }

        @Override
        public RaftClientReply sendRequest(RaftClientRequest request) {
            throw new UnsupportedOperationException("This stub speaks async only");
        }

        @Override
        public void addRaftPeers(Collection<RaftPeer> peers) {}

        @Override
        public void close() {}
    }

    private static final class UndecodableReplyRpc implements RaftClientRpc {

        @Override
        public RaftClientReply sendRequest(RaftClientRequest request) {
            return RaftClientReply.newBuilder()
                    .setRequest(request)
                    .setSuccess()
                    .setMessage(Message.valueOf(ByteBuffer.wrap(new byte[] {0x7f, 0x00, 0x01})))
                    .build();
        }

        @Override
        public CompletableFuture<RaftClientReply> sendRequestAsync(RaftClientRequest request) {
            return CompletableFuture.completedFuture(sendRequest(request));
        }

        @Override
        public void addRaftPeers(Collection<RaftPeer> peers) {}

        @Override
        public void close() {}
    }

    /** Serves one async request at a time; the test decides when its reply arrives. */
    private static final class ControllableReplyRpc implements RaftClientRpc {
        private RaftClientRequest pendingRequest;
        private CompletableFuture<RaftClientReply> pendingReply;

        @Override
        public RaftClientReply sendRequest(RaftClientRequest request) {
            throw new UnsupportedOperationException("This stub speaks async only");
        }

        @Override
        public synchronized CompletableFuture<RaftClientReply> sendRequestAsync(
                RaftClientRequest request) {
            pendingRequest = request;
            pendingReply = new CompletableFuture<>();
            return pendingReply;
        }

        synchronized void completePendingGet() {
            pendingReply.complete(
                    RaftClientReply.newBuilder()
                            .setRequest(pendingRequest)
                            .setSuccess()
                            .setMessage(
                                    Message.valueOf(
                                            ByteBuffer.wrap(
                                                    RaftCommandCodec.encodeResult(
                                                            RaftResult.ok(RaftCommand.Op.GET)))))
                            .build());
        }

        @Override
        public void addRaftPeers(Collection<RaftPeer> peers) {}

        @Override
        public void close() {}
    }
}
