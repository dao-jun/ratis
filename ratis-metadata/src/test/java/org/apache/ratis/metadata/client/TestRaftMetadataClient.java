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
import org.apache.ratis.metadata.statemachine.ChangeEvent;
import org.apache.ratis.metadata.statemachine.MetadataStateMachine;
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
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
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
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.apache.ratis.metadata.util.TestWait.waitUntil;
import static org.apache.ratis.metadata.util.TestWait.waitValue;

/**
 * Unit tests of {@link RaftMetadataClient}, {@link LeaseSession} and {@link MetadataWatcher} on
 * hand-written rpc stubs: async-settlement semantics, failure translation and watcher lifecycle
 * without a live quorum - plus the cluster-dependent end-to-end suites that run the same surface
 * against a live in-process quorum via {@link MetadataCluster}.
 */
public class TestRaftMetadataClient {

    private static final String GROUP_UUID = "7e4c1d2a-9b3f-4a58-8c6e-2f1d5a7b9c34";
    private static final Duration OPERATION_TIMEOUT = Duration.ofSeconds(30);
    private static final long POLL_BUDGET_BYTES = 4 * 1024 * 1024;
    private static final String SEQUENTIAL_PATH_PATTERN = "/\\w+/task-\\d{10}";

    private static final int FAILOVER_WRITES = 60;

    @TempDir File tempDir;

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
                // admin first.
                final long snapshotIndex = cluster.takeSnapshotOnLeader();
                final long retentionLine = snapshotIndex + 1;
                MetadataCluster.waitThroughTransientFailure(
                        () -> {
                            final RaftResult probe =
                                    adminSendReadOnly(
                                            cluster,
                                            RaftCommand.pollChanges(0, POLL_BUDGET_BYTES));
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
                assertTrue(resync.isRunning());
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
