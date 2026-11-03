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
import org.apache.ratis.metadata.util.ExceptionUtils;
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
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.apache.ratis.metadata.util.TestWait.waitUntil;
import static org.apache.ratis.metadata.util.TestWait.waitValue;

/**
 * Unit tests of {@link RaftMetadataClient}, {@link LeaseSession} and {@link MetadataWatcher} on
 * hand-written rpc stubs: async-settlement semantics, failure translation and watcher lifecycle
 * without a live quorum.
 */
public class TestRaftMetadataClient {

    private static final String GROUP_UUID = "7e4c1d2a-9b3f-4a58-8c6e-2f1d5a7b9c34";
    private static final Duration OPERATION_TIMEOUT = Duration.ofSeconds(30);

    @Test
    void testAsyncCallEndsWhenTheClientClosesUnderneath() throws Exception {
        final InetSocketAddress deadMember = new InetSocketAddress("localhost", refusedPort());
        final RaftMetadataClient unbounded =
                RaftMetadataClient.newBuilder()
                        .setGroupId(UUID.fromString(GROUP_UUID))
                        .addQuorumMember("n0", deadMember)
                        .setRetryForever()
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

    @Test
    void testWatcherSubmitsNoListenerCallbackAfterClose() throws Exception {
        final GatedNeedSnapshotRpc rpc = new GatedNeedSnapshotRpc();
        final RaftMetadataClient client = newStubClient(rpc, null);
        try {
            final QueueingExecutor executor = new QueueingExecutor();
            final MetadataWatcher watcher =
                    MetadataWatcher.newBuilder(client, new Recorder())
                            .setExecutor(executor)
                            .setPollInterval(Duration.ofMillis(50))
                            .build();
            watcher.start();
            waitUntil(
                    () -> executor.submitted.size() == 1,
                    OPERATION_TIMEOUT,
                    "The watcher never queued its first NEED_SNAPSHOT report");

            watcher.restartFrom(1);
            final Thread parkedFetchLoop =
                    waitValue(
                            rpc::parkedPollingThread,
                            OPERATION_TIMEOUT,
                            "The restarted watcher never entered its second poll");
            watcher.close();
            waitUntil(
                    () -> !parkedFetchLoop.isAlive(),
                    OPERATION_TIMEOUT,
                    "The fetch loop did not settle after close");
            rpc.release();

            assertEquals(
                    1,
                    executor.submitted.size(),
                    "the report owed by the closed loop is dropped, not submitted");
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

    @Test
    void testLeaseSessionStopsRenewingWhenTheClientCloses() throws Exception {
        final ManualClock clock = new ManualClock(0L);
        final RaftMetadataClient client = newStubClient(new FailingTransportRpc(), null);
        client.close();
        // Constructed directly: openLease() needs a live client; here the loop faces a closed one.
        final LeaseSession session = new LeaseSession(client, 77L, 30_000L, clock);
        session.start();
        // The due renewal hits the closed-state exception; a transport failure would retry.
        clock.advanceTime(11, TimeUnit.SECONDS);
        waitUntil(
                () -> leaseSessionThread(77L) == null,
                OPERATION_TIMEOUT,
                "The renewal thread kept running against the closed client");
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

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

    /** The live renewal thread of the given lease id, or {@code null} once it has exited. */
    private static Thread leaseSessionThread(long leaseId) {
        final String name = "raft-lease-session-" + leaseId;
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (thread.getName().equals(name)) {
                return thread;
            }
        }
        return null;
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

    private static final class GatedNeedSnapshotRpc implements RaftClientRpc {

        private final byte[] reply =
                RaftCommandCodec.encodeResult(
                        RaftResult.error(
                                RaftCommand.Op.POLL_CHANGES, RaftResult.Status.NEED_SNAPSHOT));
        private final AtomicInteger polls = new AtomicInteger();
        private final CountDownLatch release = new CountDownLatch(1);
        private volatile Thread pollingThread;

        @Override
        public RaftClientReply sendRequest(RaftClientRequest request) throws IOException {
            // Recorded before the count: a reader seeing the count already sees this thread.
            pollingThread = Thread.currentThread();
            if (polls.incrementAndGet() > 1) {
                try {
                    release.await();
                } catch (InterruptedException e) {
                    // Interrupted by close(); answer anyway so the loop reaches code under test.
                    Thread.currentThread().interrupt();
                }
            }
            return RaftClientReply.newBuilder()
                    .setRequest(request)
                    .setSuccess()
                    .setMessage(Message.valueOf(ByteBuffer.wrap(reply)))
                    .build();
        }

        /** The thread of the second and later polls, once one has started. */
        synchronized Optional<Thread> parkedPollingThread() {
            return polls.get() >= 2 && pollingThread != null
                    ? Optional.of(pollingThread)
                    : Optional.empty();
        }

        void release() {
            release.countDown();
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
