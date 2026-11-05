/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.metadata.statemachine;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.metadata.MetadataCluster;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.server.storage.StorageImplUtils;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.TransactionContextImpl;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.SizeInBytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.ratis.metadata.util.TestWait.waitValue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** End-to-end test of the raft-replicated {@link MetadataStateMachine} operation set. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestRaftMetadataE2E {

    private static final int NUM_SERVERS = 3;
    private static final String GROUP_UUID = "3f8a1c2d-9b4e-4d67-a5f1-2c8d6b0e7a91";
    private static final Duration CALL_TIMEOUT = Duration.ofSeconds(60);
    private static final Duration EVENT_TIMEOUT = Duration.ofSeconds(30);

    private static final byte[] NO_DATA = new byte[0];

    // static so it is injected before @BeforeAll even though this class has one shared instance
    @TempDir static File tempDir;

    private MetadataCluster cluster;

    @BeforeAll
    void startCluster() throws Exception {
        cluster =
                MetadataCluster.newBuilder(tempDir)
                        .setNumServers(NUM_SERVERS)
                        .setGroupUuid(UUID.fromString(GROUP_UUID))
                        .build();
        cluster.start();

        awaitLeader(cluster.peerIds());
    }

    @AfterAll
    void stopCluster() throws Exception {
        cluster.close();
    }

    // ------------------------------------------------------------------
    // Tests
    // ------------------------------------------------------------------

    @Test
    void testPutGetRoundTripAndVersionIncrement() throws Exception {
        send(RaftCommand.put("/put", NO_DATA, 0, 1L));
        assertEquals(1L, send(RaftCommand.put("/put/a", bytes("v1"), 0, 1000L)).getVersion());

        final RaftResult first = sendReadOnly(RaftCommand.get("/put/a"));
        assertEquals(RaftResult.Status.OK, first.getStatus());
        assertArrayEquals(bytes("v1"), first.getData());
        assertEquals(1L, first.getVersion());
        assertEquals(0L, first.getLeaseId());
        assertEquals(1000L, first.getCreateTime());
        assertEquals(1000L, first.getModifyTime());

        assertEquals(2L, send(RaftCommand.put("/put/a", bytes("v2"), 0, 2000L)).getVersion());

        final RaftResult second = sendReadOnly(RaftCommand.get("/put/a"));
        assertArrayEquals(bytes("v2"), second.getData());
        assertEquals(2L, second.getVersion());
        assertEquals(1000L, second.getCreateTime());
        assertEquals(2000L, second.getModifyTime());

        assertTrue(sendReadOnly(RaftCommand.exists("/put/a")).isFound());
        assertFalse(sendReadOnly(RaftCommand.exists("/put/missing")).isFound());
        assertEquals(
                RaftResult.Status.NOT_FOUND, sendReadOnly(RaftCommand.get("/put/missing")).getStatus());
    }

    @Test
    void testCreateSequentialUniqueAndMonotonic() throws Exception {
        // CREATE checks the parent chain first (like ZooKeeper); PUT never creates parents
        send(RaftCommand.put("/seq", NO_DATA, 0, 1L));
        assertEquals(1L, send(RaftCommand.put("/seq/parent", NO_DATA, 0, 1L)).getVersion());

        String previous = null;
        for (int i = 1; i <= 3; i++) {
            final RaftResult created =
                    send(RaftCommand.create("/seq/parent/node-", bytes("node-" + i), true, 0, 1L));
            assertEquals(RaftResult.Status.OK, created.getStatus());
            assertEquals("/seq/parent/node-" + String.format("%010d", i), created.getResultPath());
            assertEquals(1L, created.getVersion());
            assertTrue(previous == null || created.getResultPath().compareTo(previous) > 0);
            previous = created.getResultPath();
        }

        final List<String> paths = createConcurrently("/seq/parent/conc-", 4, 25);
        assertEquals(100, paths.size());
        assertEquals(100, new LinkedHashSet<>(paths).size());
        final List<Long> numbers = new ArrayList<>();
        for (String path : paths) {
            numbers.add(Long.parseLong(path.substring(path.lastIndexOf('-') + 1)));
        }
        numbers.sort(null);
        for (int i = 1; i < numbers.size(); i++) {
            assertEquals(1L, numbers.get(i) - numbers.get(i - 1));
        }

        assertEquals(
                RaftResult.Status.NODE_EXISTS,
                send(RaftCommand.create("/seq/parent", NO_DATA, false, 0, 1L)).getStatus());
        assertEquals(
                RaftResult.Status.NO_PARENT,
                send(RaftCommand.create("/seq/no-parent/child", NO_DATA, false, 0, 1L)).getStatus());
    }

    @Test
    void testCreateWithParentsMaterializesAncestorsAtomically() throws Exception {
        final long before = sendReadOnly(RaftCommand.revision()).getRevision();
        final RaftResult created =
                send(RaftCommand.create("/cwp/a/b/leaf", bytes("leaf"), false, 0, 100L, true));
        assertEquals(RaftResult.Status.OK, created.getStatus());
        assertEquals("/cwp/a/b/leaf", created.getResultPath());
        assertEquals(1L, created.getVersion());
        final long after = sendReadOnly(RaftCommand.revision()).getRevision();

        // ancestors are persistent records with empty data, exactly like ZK parent znodes
        for (String ancestor : Arrays.asList("/cwp/a", "/cwp/a/b")) {
            final RaftResult record = sendReadOnly(RaftCommand.get(ancestor));
            assertEquals(
                    RaftResult.Status.OK, record.getStatus(), "ancestor " + ancestor + " was materialized");
            assertEquals(0, record.getData().length);
            assertEquals(1L, record.getVersion());
        }

        ChangeEvent leafEvent = null;
        for (ChangeEvent record : sendReadOnly(RaftCommand.pollChanges(before, 1 << 20)).getRecords()) {
            if (record.getRevision() > after) {
                break;
            }
            for (ChangeEvent.Change change : record.getChanges()) {
                if (change.getPath().equals("/cwp/a/b/leaf")) {
                    leafEvent = record;
                }
            }
        }
        assertNotNull(leafEvent, "the create's revision carries a change record");
        assertEquals(
                Arrays.asList("/cwp", "/cwp/a", "/cwp/a/b", "/cwp/a/b/leaf"),
                leafEvent.getChanges().stream()
                        .map(ChangeEvent.Change::getPath)
                        .collect(Collectors.toList()));
        final List<ChangeEvent.ChangeOp> leafOps =
                leafEvent.getChanges().stream()
                        .map(ChangeEvent.Change::getOp)
                        .collect(Collectors.toList());
        assertTrue(leafOps.contains(ChangeEvent.ChangeOp.CREATE));
        assertEquals(1, new LinkedHashSet<>(leafOps).size());

        assertEquals(
                RaftResult.Status.OK,
                send(RaftCommand.create("/cwp/a/b/sibling", bytes("s"), false, 0, 100L, true))
                        .getStatus());
        assertEquals(
                RaftResult.Status.NODE_EXISTS,
                send(RaftCommand.create("/cwp/a/b/leaf", bytes("again"), false, 0, 100L, true))
                        .getStatus());
        assertEquals(
                RaftResult.Status.NO_PARENT,
                send(RaftCommand.create("/cwp/none/x/y", NO_DATA, false, 0, 1L)).getStatus());

        final ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            final List<Future<RaftResult>> futures = new ArrayList<>();
            for (int i = 0; i < 12; i++) {
                final String path = "/cwp/race/sib-" + i;
                futures.add(
                        pool.submit(
                                (Callable<RaftResult>)
                                        () -> send(RaftCommand.create(path, NO_DATA, false, 0, 100L, true))));
            }
            for (Future<RaftResult> future : futures) {
                assertEquals(RaftResult.Status.OK, future.get(30, TimeUnit.SECONDS).getStatus());
            }
        } finally {
            pool.shutdownNow();
        }
        assertEquals(12, sendReadOnly(RaftCommand.list("/cwp/race/", 0)).getEntries().size());

        final RaftResult batch =
                send(
                        RaftCommand.multi(
                                Arrays.asList(
                                        RaftCommand.create("/cwp/m/p/first", bytes("1"), false, 0, 100L, true),
                                        RaftCommand.create(
                                                "/cwp/m/p/second", bytes("2"), false, 0, 100L, true))));
        assertEquals(RaftResult.Status.OK, batch.getStatus());
        assertEquals(RaftResult.Status.OK, sendReadOnly(RaftCommand.get("/cwp/m/p/first")).getStatus());
        assertEquals(
                RaftResult.Status.OK, sendReadOnly(RaftCommand.get("/cwp/m/p/second")).getStatus());
    }

    @Test
    void testDeleteRecursive() throws Exception {
        send(RaftCommand.put("/del", NO_DATA, 0, 1L));
        send(RaftCommand.put("/del/t", NO_DATA, 0, 1L));
        send(RaftCommand.put("/del/t/x", bytes("x"), 0, 1L));
        send(RaftCommand.put("/del/t/y", NO_DATA, 0, 1L));
        send(RaftCommand.put("/del/t/y/z", bytes("z"), 0, 1L));
        send(RaftCommand.put("/del/s", NO_DATA, 0, 1L));
        send(RaftCommand.put("/del/s/w", bytes("w"), 0, 1L));

        assertEquals(
                RaftResult.Status.HAS_CHILDREN, send(RaftCommand.delete("/del/t", false)).getStatus());

        final RaftResult deleted = send(RaftCommand.delete("/del/t", true));
        assertEquals(RaftResult.Status.OK, deleted.getStatus());
        assertEquals(4L, deleted.getDeletedCount());

        assertEquals(RaftResult.Status.NOT_FOUND, sendReadOnly(RaftCommand.get("/del/t")).getStatus());
        assertEquals(
                RaftResult.Status.NOT_FOUND, sendReadOnly(RaftCommand.get("/del/t/x")).getStatus());
        assertEquals(
                RaftResult.Status.NOT_FOUND, sendReadOnly(RaftCommand.get("/del/t/y/z")).getStatus());
        assertEquals(RaftResult.Status.OK, sendReadOnly(RaftCommand.get("/del/s/w")).getStatus());

        final RaftResult listed = sendReadOnly(RaftCommand.list("/del/", 0));
        assertEquals(
                Arrays.asList("/del/s", "/del/s/w"),
                listed.getEntries().stream()
                        .map(RaftResult.Entry::getPath)
                        .collect(Collectors.toList()));

        assertEquals(1L, send(RaftCommand.delete("/del/s/w", false)).getDeletedCount());
        assertEquals(
                RaftResult.Status.NOT_FOUND, send(RaftCommand.delete("/del/missing", true)).getStatus());
        assertEquals(RaftResult.Status.NOT_ALLOWED, send(RaftCommand.delete("/", true)).getStatus());
    }

    @Test
    void testCompareAndSwap() throws Exception {
        // expected version 0 means "must not exist yet": creates at version 1
        send(RaftCommand.put("/cas", NO_DATA, 0, 1L));
        assertEquals(1L, send(RaftCommand.cas("/cas/x", bytes("a"), 0, 0, 100L)).getVersion());
        assertEquals(2L, send(RaftCommand.cas("/cas/x", bytes("b"), 1, 0, 200L)).getVersion());

        final RaftResult mismatch = send(RaftCommand.cas("/cas/x", bytes("c"), 1, 0, 300L));
        assertEquals(RaftResult.Status.VERSION_MISMATCH, mismatch.getStatus());
        final RaftResult after = sendReadOnly(RaftCommand.get("/cas/x"));
        assertEquals(2L, after.getVersion());
        assertArrayEquals(bytes("b"), after.getData());
        assertEquals(200L, after.getModifyTime());

        assertEquals(
                RaftResult.Status.VERSION_MISMATCH,
                send(RaftCommand.cas("/cas/y", bytes("y"), 5, 0, 100L)).getStatus());
    }

    @Test
    void testMultiAtomicity() throws Exception {
        send(RaftCommand.put("/multi", NO_DATA, 0, 1L));
        send(RaftCommand.put("/multi/a", bytes("a"), 0, 1L));
        final long revisionBefore = sendReadOnly(RaftCommand.revision()).getRevision();

        final RaftResult failed =
                send(
                        RaftCommand.multi(
                                Arrays.asList(
                                        RaftCommand.put("/multi/b", bytes("b"), 0, 1L),
                                        RaftCommand.delete("/multi/missing", true))));
        assertEquals(RaftResult.Status.MULTI_FAILED, failed.getStatus());
        assertEquals(1, failed.getFailIndex());
        assertEquals(RaftResult.Status.NOT_FOUND, failed.getFailStatus());
        assertEquals(
                RaftResult.Status.NOT_FOUND, sendReadOnly(RaftCommand.get("/multi/b")).getStatus());

        final RaftResult gapPoll = sendReadOnly(RaftCommand.pollChanges(revisionBefore, 1 << 20));
        assertTrue(gapPoll.getRevision() >= revisionBefore + 1);
        for (ChangeEvent record : gapPoll.getRecords()) {
            // the failed batch's revision exists in the watermark but carries no record
            assertNotEquals(revisionBefore + 1, record.getRevision());
        }

        final RaftResult ok =
                send(
                        RaftCommand.multi(
                                Arrays.asList(
                                        RaftCommand.put("/multi/b", bytes("b"), 0, 2L),
                                        RaftCommand.delete("/multi/a", true),
                                        RaftCommand.increment("multi-cnt", 3))));
        assertEquals(RaftResult.Status.OK, ok.getStatus());
        assertEquals(3, ok.getSubResults().size());
        assertEquals(1L, ok.getSubResults().get(0).getVersion());
        assertEquals(1L, ok.getSubResults().get(1).getDeletedCount());
        assertEquals(3L, ok.getSubResults().get(2).getValue());
        assertArrayEquals(bytes("b"), sendReadOnly(RaftCommand.get("/multi/b")).getData());
        assertEquals(
                RaftResult.Status.NOT_FOUND, sendReadOnly(RaftCommand.get("/multi/a")).getStatus());
    }

    @Test
    void testMultiDeleteChildrenThenNonRecursiveParent() throws Exception {
        send(RaftCommand.put("/mdel", NO_DATA, 0, 1L));
        send(RaftCommand.put("/mdel/p", NO_DATA, 0, 1L));
        send(RaftCommand.put("/mdel/p/c1", bytes("c1"), 0, 1L));
        send(RaftCommand.put("/mdel/p/c2", bytes("c2"), 0, 1L));

        assertEquals(
                RaftResult.Status.HAS_CHILDREN, send(RaftCommand.delete("/mdel/p", false)).getStatus());

        final RaftResult batch =
                send(
                        RaftCommand.multi(
                                Arrays.asList(
                                        RaftCommand.delete("/mdel/p/c1", false),
                                        RaftCommand.delete("/mdel/p/c2", false),
                                        RaftCommand.delete("/mdel/p", false))));
        assertEquals(RaftResult.Status.OK, batch.getStatus());
        assertEquals(1L, batch.getSubResults().get(2).getDeletedCount());
        assertEquals(
                RaftResult.Status.NOT_FOUND, sendReadOnly(RaftCommand.get("/mdel/p")).getStatus());
        assertTrue(sendReadOnly(RaftCommand.list("/mdel/", 0)).getEntries().isEmpty());

        send(RaftCommand.put("/mdel/q", NO_DATA, 0, 1L));
        send(RaftCommand.put("/mdel/q/old", NO_DATA, 0, 1L));
        final RaftResult mixed =
                send(
                        RaftCommand.multi(
                                Arrays.asList(
                                        RaftCommand.delete("/mdel/q/old", false),
                                        RaftCommand.put("/mdel/q/new", bytes("n"), 0, 1L),
                                        RaftCommand.delete("/mdel/q", false))));
        assertEquals(RaftResult.Status.MULTI_FAILED, mixed.getStatus());
        assertEquals(2, mixed.getFailIndex());
        assertEquals(RaftResult.Status.HAS_CHILDREN, mixed.getFailStatus());
        assertEquals(RaftResult.Status.OK, sendReadOnly(RaftCommand.get("/mdel/q")).getStatus());
        assertEquals(RaftResult.Status.OK, sendReadOnly(RaftCommand.get("/mdel/q/old")).getStatus());

        send(RaftCommand.put("/mdel/r", NO_DATA, 0, 1L));
        send(RaftCommand.put("/mdel/r/c", NO_DATA, 0, 1L));
        send(RaftCommand.put("/mdel/r/g", NO_DATA, 0, 1L));
        final RaftResult recursive =
                send(
                        RaftCommand.multi(
                                Arrays.asList(
                                        RaftCommand.delete("/mdel/r/c", false),
                                        RaftCommand.delete("/mdel/r", true))));
        assertEquals(RaftResult.Status.OK, recursive.getStatus());
        assertEquals(2L, recursive.getSubResults().get(1).getDeletedCount());
        assertEquals(
                Arrays.asList("/mdel/q", "/mdel/q/old"),
                sendReadOnly(RaftCommand.list("/mdel/", 0)).getEntries().stream()
                        .map(RaftResult.Entry::getPath)
                        .collect(Collectors.toList()));
    }

    @Test
    void testQueryConcurrentWithReinitialize() throws Exception {
        try (Standalone standalone = new Standalone("storm")) {
            final MetadataStateMachine sm = standalone.stateMachine;

            sm.notifyTermIndexUpdated(1L, 1L);
            assertEquals(1L, sm.takeSnapshot());

            final AtomicBoolean stop = new AtomicBoolean();
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final AtomicLong queries = new AtomicLong();
            final Runnable queryLoop =
                    () -> {
                        while (!stop.get() && failure.get() == null) {
                            try {
                                final CompletableFuture<Message> reply =
                                        sm.query(
                                                Message.valueOf(
                                                        ByteBuffer.wrap(
                                                                RaftCommandCodec.encodeCommand(
                                                                        RaftCommand.list("/qbusy/", 0)))));
                                final Message message = reply.get(10, TimeUnit.SECONDS);
                                final RaftResult result =
                                        RaftCommandCodec.decodeResult(message.getContent().toByteArray());
                                assertNotNull(result);
                                assertTrue(result.getEntries().isEmpty());
                                queries.incrementAndGet();
                            } catch (Throwable t) {
                                failure.set(t instanceof ExecutionException ? t.getCause() : t);
                            }
                        }
                    };

            final Thread first = new Thread(queryLoop, "qbusy-query-1");
            final Thread second = new Thread(queryLoop, "qbusy-query-2");
            first.setDaemon(true);
            second.setDaemon(true);
            first.start();
            second.start();
            try {
                // each round is ratis' install-snapshot hand-shake: pause, rebuild from the
                // snapshot, resume
                for (int round = 0; round < 15 && failure.get() == null; round++) {
                    sm.pause();
                    sm.reinitialize();
                }
            } finally {
                stop.set(true);
                first.join(TimeUnit.SECONDS.toMillis(30));
                second.join(TimeUnit.SECONDS.toMillis(30));
            }
            assertNull(failure.get());
            assertFalse(first.isAlive() || second.isAlive());
            assertTrue(queries.get() > 0, "the storm must actually exercise the query path");

            assertEquals(LifeCycle.State.RUNNING, sm.getLifeCycleState());
            assertTrue(directQuery(sm, RaftCommand.list("/qbusy/", 0)).getEntries().isEmpty());
        }
    }

    @Test
    void testListPaginationEnumeratesLargePrefixes() throws Exception {
        // more records than one page holds; two batches - one MULTI is capped at MAX_MULTI_OPS
        final List<RaftCommand> firstBatch = new ArrayList<>();
        final List<RaftCommand> secondBatch = new ArrayList<>();
        send(RaftCommand.put("/page", NO_DATA, 0, 1L));
        for (int i = 0; i < 1200; i++) {
            final RaftCommand put =
                    RaftCommand.put(String.format(Locale.ROOT, "/page/%04d", i), NO_DATA, 0, 1L);
            if (i < 600) {
                firstBatch.add(put);
            } else {
                secondBatch.add(put);
            }
        }
        assertEquals(RaftResult.Status.OK, send(RaftCommand.multi(firstBatch)).getStatus());
        assertEquals(RaftResult.Status.OK, send(RaftCommand.multi(secondBatch)).getStatus());

        final List<String> collected = new ArrayList<>();
        String startAfter = "";
        while (true) {
            final RaftResult page = sendReadOnly(RaftCommand.list("/page/", startAfter, 0));
            assertEquals(RaftResult.Status.OK, page.getStatus());
            collected.addAll(pathsOf(page));
            if (!page.hasMore()) {
                break;
            }
            // a cut-off page is always full: hasMore is only set when the cap stopped the scan
            assertEquals((int) RaftCommandCodec.DEFAULT_MAX_LIST_RESULTS, page.getEntries().size());
            startAfter = page.getEntries().get(page.getEntries().size() - 1).getPath();
        }
        assertEquals(1200, collected.size());
        assertEquals(1200, new LinkedHashSet<>(collected).size());
        for (int i = 1; i < collected.size(); i++) {
            assertTrue(collected.get(i - 1).compareTo(collected.get(i)) <= 0, "collected paths are sorted");
        }

        final List<String> paged = new ArrayList<>();
        String cursor = "";
        while (true) {
            final RaftResult page = sendReadOnly(RaftCommand.list("/page/", cursor, 7));
            paged.addAll(pathsOf(page));
            if (!page.hasMore()) {
                break;
            }
            cursor = page.getEntries().get(page.getEntries().size() - 1).getPath();
        }
        assertEquals(collected, paged);

        // the uncapped raw command (still legal on the admin surface; the client's list() now
        // pages): one iterator view, one point in time
        final RaftResult uncapped = sendReadOnly(RaftCommand.list("/page/", "", Long.MAX_VALUE));
        assertFalse(uncapped.hasMore());
        assertEquals(collected, pathsOf(uncapped));

        assertFalse(sendReadOnly(RaftCommand.list("/page/", "/page/9999", 0)).hasMore());
        assertTrue(
                sendReadOnly(RaftCommand.list("/page/", "/page/9999", 0)).getEntries().isEmpty());
        assertEquals(
                Arrays.asList("/page/0000", "/page/0001", "/page/0002"),
                pathsOf(sendReadOnly(RaftCommand.list("/page/", "/0", 3))));
        assertEquals(
                Arrays.asList("/page/0006", "/page/0007", "/page/0008"),
                pathsOf(sendReadOnly(RaftCommand.list("/page/", "/page/0005", 3))));
    }

    @Test
    void testIncrement() throws Exception {
        assertEquals(5L, send(RaftCommand.increment("cnt-a", 5)).getValue());
        assertEquals(3L, send(RaftCommand.increment("cnt-a", -2)).getValue());
        assertEquals(3L, send(RaftCommand.increment("cnt-a", 0)).getValue());
        assertEquals(1L, send(RaftCommand.increment("cnt-b", 1)).getValue());
    }

    @Test
    void testLeaseLifecycle() throws Exception {
        final long now = System.currentTimeMillis();
        final RaftResult register = send(RaftCommand.leaseRegister(42L, 30000, now + 30000));
        assertEquals(RaftResult.Status.OK, register.getStatus());
        assertTrue(register.getRevision() > 0);

        send(RaftCommand.put("/lease", NO_DATA, 0, 1L));
        assertEquals(
                RaftResult.Status.OK, send(RaftCommand.put("/lease/x", bytes("x"), 42L, now)).getStatus());
        assertEquals(
                RaftResult.Status.OK, send(RaftCommand.put("/lease/y", bytes("y"), 0, now)).getStatus());

        final RaftResult expire = send(RaftCommand.leaseExpire(42L, register.getRevision()));
        assertEquals(RaftResult.Status.OK, expire.getStatus());
        assertTrue(expire.isExpired());
        assertEquals(1L, expire.getDeletedCount());
        assertEquals(
                RaftResult.Status.NOT_FOUND, sendReadOnly(RaftCommand.get("/lease/x")).getStatus());
        assertEquals(RaftResult.Status.OK, sendReadOnly(RaftCommand.get("/lease/y")).getStatus());

        assertEquals(
                RaftResult.Status.UNKNOWN_LEASE,
                send(RaftCommand.leaseRenew(42L, 30000L, now + 60000)).getStatus());
        assertFalse(send(RaftCommand.leaseExpire(42L, -1)).isExpired());

        final RaftResult poll =
                sendReadOnly(RaftCommand.pollChanges(register.getRevision(), 1 << 20));
        final List<ChangeEvent.ChangeOp> expiryOps =
                poll.getRecords().stream()
                        .flatMap(record -> record.getChanges().stream())
                        .map(ChangeEvent.Change::getOp)
                        .collect(Collectors.toList());
        assertTrue(expiryOps.contains(ChangeEvent.ChangeOp.DELETE));
        assertTrue(expiryOps.contains(ChangeEvent.ChangeOp.LEASE_EXPIRE));

        // the renew guard: an expiry decision formed before the last renew must not apply
        final RaftResult register43 = send(RaftCommand.leaseRegister(43L, 30000, now + 30000));
        final RaftResult stale = send(RaftCommand.leaseExpire(43L, register43.getRevision() - 1));
        assertFalse(stale.isExpired());
        assertEquals(
                RaftResult.Status.OK,
                send(RaftCommand.leaseRenew(43L, 30000L, now + 60000)).getStatus());
    }

    @Test
    void testPollChangesOrderedWithWatermark() throws Exception {
        send(RaftCommand.put("/poll", NO_DATA, 0, 1L));
        final long base = sendReadOnly(RaftCommand.revision()).getRevision();
        send(RaftCommand.put("/poll/a", bytes("a-data"), 0, 1L));
        send(
                RaftCommand.multi(
                        Arrays.asList(
                                RaftCommand.put("/poll/b", bytes("b-data"), 0, 2L),
                                RaftCommand.increment("poll-cnt", 7))));
        final long head = sendReadOnly(RaftCommand.revision()).getRevision();
        assertTrue(head >= base + 2);

        final RaftResult poll = sendReadOnly(RaftCommand.pollChanges(base, 1 << 20));
        assertEquals(RaftResult.Status.OK, poll.getStatus());
        assertTrue(poll.getRevision() >= head);
        assertEquals(1L, poll.getOldestRetainedRevision());
        assertEquals(2, poll.getRecords().size());

        final ChangeEvent first = poll.getRecords().get(0);
        // gap revisions may sit between the base read and this write, so only bound from below
        assertTrue(first.getRevision() > base);
        assertEquals(1, first.getChanges().size());
        assertEquals(ChangeEvent.ChangeOp.CREATE, first.getChanges().get(0).getOp());
        assertEquals("/poll/a", first.getChanges().get(0).getPath());
        assertArrayEquals(bytes("a-data"), first.getChanges().get(0).getData());

        final ChangeEvent second = poll.getRecords().get(1);
        assertTrue(second.getRevision() > first.getRevision());
        assertEquals(2, second.getChanges().size());
        assertEquals("/poll/b", second.getChanges().get(0).getPath());
        assertArrayEquals(bytes("b-data"), second.getChanges().get(0).getData());
        assertEquals(ChangeEvent.ChangeOp.INCREMENT, second.getChanges().get(1).getOp());
        assertEquals(7L, second.getChanges().get(1).getCounterValue());

        // a byte budget always yields at least one record, then truncates
        assertEquals(1, sendReadOnly(RaftCommand.pollChanges(base, 1)).getRecords().size());
    }

    @Test
    void testReadYourWriteIsLinearizable() throws Exception {
        send(RaftCommand.put("/rw", NO_DATA, 0, 1L));
        send(RaftCommand.put("/rw/x", bytes("now-you-see-me"), 0, 1L));
        assertArrayEquals(
                bytes("now-you-see-me"), sendReadOnly(RaftCommand.get("/rw/x")).getData());
    }

    @Test
    void testFollowerServesReadAfterWriteThroughTheLinearizableDefault() throws Exception {
        final RaftPeerId leader = awaitLeader(cluster.peerIds());
        RaftPeerId follower = null;
        for (RaftPeerId candidate : cluster.peerIds()) {
            if (!candidate.equals(leader)) {
                follower = candidate;
                break;
            }
        }
        assertNotNull(follower, "a three-member group always has a follower");

        assertEquals(RaftResult.Status.OK, send(RaftCommand.put("/linear", NO_DATA, 0, 1L)).getStatus());
        assertEquals(
                RaftResult.Status.OK,
                send(RaftCommand.put("/linear/x", bytes("read-after-write"), 0, 1L)).getStatus());

        // A read addressed to the follower: only the linearizable default (read index)
        // lets a non-leader answer a read after the client's write committed - a
        // local-state read would fail NotLeader instead.
        final RaftClientRequest request =
                RaftClientRequest.newBuilder()
                        .setClientId(cluster.adminClient().getId())
                        .setServerId(follower)
                        .setGroupId(cluster.groupId())
                        .setCallId(9_000_001L)
                        .setMessage(toMessage(RaftCommand.get("/linear/x")))
                        .setType(RaftClientRequest.readRequestType())
                        .build();
        final RaftClientReply reply =
                cluster.adminClient()
                        .getClientRpc()
                        .sendRequestAsyncUnordered(request)
                        .get(30, TimeUnit.SECONDS);
        assertTrue(reply.isSuccess(), () -> "the follower read failed: " + reply);
        final RaftResult read = decodeResult(reply.getMessage().getContent().toByteArray());
        assertEquals(RaftCommand.Op.GET, read.getOp());
        assertArrayEquals(bytes("read-after-write"), read.getData());
    }

    @Test
    void testRootWritesAreRejectedThroughTheReplicatedApply() throws Exception {
        // the cluster path: the leader's envelope-serialized write fails the same way
        assertEquals(
                RaftResult.Status.NOT_ALLOWED, send(RaftCommand.put("/", NO_DATA, 0, 1L)).getStatus());
        assertEquals(
                RaftResult.Status.NOT_ALLOWED,
                send(RaftCommand.cas("/", bytes("root"), 0, 0, 1L)).getStatus());
        final RaftResult batch =
                send(
                        RaftCommand.multi(
                                Arrays.asList(
                                        RaftCommand.put("/root-batch", NO_DATA, 0, 1L),
                                        RaftCommand.put("/", NO_DATA, 0, 1L))));
        assertEquals(RaftResult.Status.MULTI_FAILED, batch.getStatus());
        assertEquals(1, batch.getFailIndex());
        assertEquals(RaftResult.Status.NOT_ALLOWED, batch.getFailStatus());
        assertEquals(
                RaftResult.Status.NOT_FOUND, sendReadOnly(RaftCommand.get("/root-batch")).getStatus());

        // the raw replicated entry: no client-side gate, only the state machine's apply
        try (Standalone standalone = new Standalone("root-apply")) {
            final MetadataStateMachine sm = standalone.stateMachine;
            long index = 0;
            applyOnStandalone(
                    sm, 1L, ++index, RaftCommand.put("/", NO_DATA, 0, 1L),
                    RaftResult.Status.NOT_ALLOWED);
            applyOnStandalone(
                    sm, 1L, ++index, RaftCommand.cas("/", bytes("root"), 0, 0, 1L),
                    RaftResult.Status.NOT_ALLOWED);
            applyOnStandalone(
                    sm,
                    1L,
                    ++index,
                    RaftCommand.multi(
                            Arrays.asList(
                                    RaftCommand.create("/root-raw/child", NO_DATA, false, 0, 1L, true),
                                    RaftCommand.put("/", NO_DATA, 0, 1L))),
                    RaftResult.Status.MULTI_FAILED);
            assertFalse(
                    directQuery(sm, RaftCommand.exists("/root-raw/child")).isFound(),
                    "the aborted raw batch left no prefix behind");
        }
    }

    @Test
    void testPollChangesNeedSnapshotErrorPath() throws Exception {
        try {
            // pretend change-record GC moved past 10000 (real GC is gcChangeRecords' retention
            // window)
            for (MetadataStateMachine stateMachine : stateMachines()) {
                stateMachine.setOldestRetainedRevision(10_000);
            }
            final RaftResult result = sendReadOnly(RaftCommand.pollChanges(5, 1 << 20));
            assertEquals(RaftResult.Status.NEED_SNAPSHOT, result.getStatus());
            assertEquals(10_000L, result.getOldestRetainedRevision());
        } finally {
            for (MetadataStateMachine stateMachine : stateMachines()) {
                stateMachine.setOldestRetainedRevision(1);
            }
        }
    }

    @Test
    void testPollChangesRechecksRetentionAfterIterating() throws Exception {
        try (Standalone standalone = new Standalone("poll-recheck")) {
            final MetadataStateMachine sm = standalone.stateMachine;
            // the old tight coupling, so the seam's one snapshot collects the whole walked range
            sm.setChangeRecordRetention(0, Long.MAX_VALUE);
            standalone.apply(RaftCommand.put("/race-a", bytes("a"), 0, 1L));
            standalone.apply(RaftCommand.put("/race-b", bytes("b"), 0, 1L));
            final long head = directQuery(sm, RaftCommand.revision()).getRevision();

            sm.pollBetweenCheckAndIteration =
                    () -> {
                        try {
                            assertEquals(head, sm.takeSnapshot());
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    };
            final RaftResult raced = directQuery(sm, RaftCommand.pollChanges(0, 1 << 20));
            sm.pollBetweenCheckAndIteration = null;

            assertEquals(RaftResult.Status.NEED_SNAPSHOT, raced.getStatus());
            assertEquals(head + 1, raced.getOldestRetainedRevision());

            assertTrue(
                    directQuery(sm, RaftCommand.pollChanges(head, 1 << 20)).getRecords().isEmpty());
            standalone.apply(RaftCommand.put("/race-after", bytes("c"), 0, 1L));
            final RaftResult tail = directQuery(sm, RaftCommand.pollChanges(head, 1 << 20));
            assertEquals(RaftResult.Status.OK, tail.getStatus());
            assertEquals(Collections.singletonList(head + 1), revisionsOf(tail.getRecords()));
        }
    }

    @Test
    void testPollChangesStopsAtTheFirstRevisionAboveTheSampledWatermark() throws Exception {
        try (Standalone standalone = new Standalone("poll-watermark")) {
            final MetadataStateMachine sm = standalone.stateMachine;
            standalone.apply(RaftCommand.put("/wm-a", bytes("a"), 0, 1L));
            final long watermark = directQuery(sm, RaftCommand.revision()).getRevision();

            // a commit landing between the watermark sample and the iteration: the live
            // iterator would still walk into it
            sm.pollBetweenCheckAndIteration =
                    () -> {
                        try {
                            standalone.apply(RaftCommand.put("/wm-b", bytes("b"), 0, 1L));
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    };
            final RaftResult raced = directQuery(sm, RaftCommand.pollChanges(0, 1 << 20));
            sm.pollBetweenCheckAndIteration = null;

            assertEquals(RaftResult.Status.OK, raced.getStatus());
            assertEquals(watermark, raced.getRevision());
            for (ChangeEvent record : raced.getRecords()) {
                assertTrue(
                        record.getRevision() <= raced.getRevision(),
                        "record revision "
                                + record.getRevision()
                                + " exceeds the sampled watermark "
                                + raced.getRevision());
            }
            assertEquals(
                    Collections.singletonList(watermark),
                    revisionsOf(raced.getRecords()),
                    "the later commit stays parked for the next poll instead of leaking in");

            // ... and the next poll retrieves exactly that parked record
            final RaftResult next = directQuery(sm, RaftCommand.pollChanges(watermark, 1 << 20));
            assertEquals(RaftResult.Status.OK, next.getStatus());
            assertEquals(Collections.singletonList(watermark + 1), revisionsOf(next.getRecords()));
        }
    }

    @Test
    void testChangeRecordGcAdvancesOnApplyCadenceAndSnapshotKeepsWindow() throws Exception {
        try (Standalone standalone = new Standalone("gc-window")) {
            final MetadataStateMachine sm = standalone.stateMachine;
            final int window = 4;
            final int cadence = 2;
            sm.setChangeRecordRetention(window, cadence);
            for (int i = 1; i <= 8; i++) {
                standalone.apply(RaftCommand.put("/gc-k" + i, bytes("v"), 0, 1L));
            }
            // cadence GCs at revisions 2, 4, 6, 8 - the last moves the line to 8 + 1 - 4 = 5
            assertEquals(
                    RaftResult.Status.NEED_SNAPSHOT,
                    directQuery(sm, RaftCommand.pollChanges(3, 1 << 20)).getStatus());
            final RaftResult live = directQuery(sm, RaftCommand.pollChanges(4, 1 << 20));
            assertEquals(RaftResult.Status.OK, live.getStatus());
            assertEquals(5L, live.getOldestRetainedRevision());
            assertEquals(Arrays.asList(5L, 6L, 7L, 8L), revisionsOf(live.getRecords()));

            assertEquals(8L, sm.takeSnapshot());
            final RaftResult afterSnapshot = directQuery(sm, RaftCommand.pollChanges(4, 1 << 20));
            assertEquals(
                    RaftResult.Status.OK,
                    afterSnapshot.getStatus(),
                    "a snapshot must not eat the retention window");
            assertEquals(5L, afterSnapshot.getOldestRetainedRevision());
            assertEquals(Arrays.asList(5L, 6L, 7L, 8L), revisionsOf(afterSnapshot.getRecords()));
        }
    }

    @Test
    void testRecursiveDeleteEmitsOneSubtreeEventAndKeepsCounts() throws Exception {
        try (Standalone standalone = new Standalone("subtree-del")) {
            final MetadataStateMachine sm = standalone.stateMachine;
            standalone.apply(RaftCommand.put("/db", bytes("root"), 0, 1L));
            standalone.apply(RaftCommand.put("/db/t1", bytes("t1"), 0, 1L));
            standalone.apply(RaftCommand.put("/db/t1/c1", bytes("c1"), 0, 1L));
            standalone.apply(RaftCommand.put("/db/t2", bytes("t2"), 0, 1L));

            final RaftResult deleted = standalone.apply(RaftCommand.delete("/db", true));
            assertEquals(4L, deleted.getDeletedCount());
            assertFalse(directQuery(sm, RaftCommand.exists("/db")).isFound());
            assertFalse(directQuery(sm, RaftCommand.exists("/db/t1/c1")).isFound());

            final RaftResult poll = directQuery(sm, RaftCommand.pollChanges(0, 1 << 20));
            final List<ChangeEvent.ChangeOp> deleteOps = new ArrayList<>();
            for (ChangeEvent event : poll.getRecords()) {
                for (ChangeEvent.Change change : event.getChanges()) {
                    if (change.getOp() == ChangeEvent.ChangeOp.DELETE
                            || change.getOp() == ChangeEvent.ChangeOp.DELETE_SUBTREE) {
                        deleteOps.add(change.getOp());
                    }
                }
            }
            assertEquals(
                    Collections.singletonList(ChangeEvent.ChangeOp.DELETE_SUBTREE),
                    deleteOps,
                    "one subtree event instead of a DELETE per victim");
            final ChangeEvent.Change subtreeEvent =
                    poll.getRecords().get(poll.getRecords().size() - 1).getChanges().get(0);
            assertEquals("/db", subtreeEvent.getPath());
        }
    }

    @Test
    void testMultiAroundRecursiveDeleteInOneRevision() throws Exception {
        try (Standalone standalone = new Standalone("subtree-multi")) {
            final MetadataStateMachine sm = standalone.stateMachine;
            standalone.apply(RaftCommand.put("/m2", NO_DATA, 0, 1L));
            final RaftResult swept =
                    standalone.apply(
                            RaftCommand.multi(
                                    Arrays.asList(
                                            RaftCommand.put("/m2/a", bytes("a"), 0, 1L),
                                            RaftCommand.put("/m2/a/x", bytes("x"), 0, 1L),
                                            RaftCommand.delete("/m2/a", true))));
            assertEquals(2L, swept.getSubResults().get(2).getDeletedCount());
            assertFalse(directQuery(sm, RaftCommand.exists("/m2/a")).isFound());
            assertFalse(directQuery(sm, RaftCommand.exists("/m2/a/x")).isFound());

            standalone.apply(RaftCommand.put("/m2/b", bytes("b"), 0, 1L));
            standalone.apply(RaftCommand.put("/m2/b/y", bytes("y"), 0, 1L));
            final RaftResult recreated =
                    standalone.apply(
                            RaftCommand.multi(
                                    Arrays.asList(
                                            RaftCommand.delete("/m2/b", true),
                                            RaftCommand.create("/m2/b/z", bytes("z"), false, 0L, 1L, true))));
            assertEquals(2L, recreated.getSubResults().get(0).getDeletedCount());
            assertFalse(directQuery(sm, RaftCommand.exists("/m2/b/y")).isFound());
            assertArrayEquals(bytes("z"), directQuery(sm, RaftCommand.get("/m2/b/z")).getData());
        }
    }

    @Test
    void testRecursiveDeleteReleasesLeaseBoundVictims() throws Exception {
        try (Standalone standalone = new Standalone("subtree-lease")) {
            final MetadataStateMachine sm = standalone.stateMachine;
            final long now = 1_000_000L;
            final RaftResult register =
                    standalone.apply(RaftCommand.leaseRegister(7L, 30_000, now + 30_000));
            standalone.apply(RaftCommand.put("/ldb", bytes("root"), 0, 1L));
            standalone.apply(RaftCommand.put("/ldb/eph", bytes("e"), 7L, 1L));
            standalone.apply(RaftCommand.put("/ldb/per", bytes("p"), 0, 1L));

            assertEquals(3L, standalone.apply(RaftCommand.delete("/ldb", true)).getDeletedCount());

            standalone.apply(RaftCommand.leaseExpire(7L, -1L));
            final RaftResult tail =
                    directQuery(sm, RaftCommand.pollChanges(register.getRevision(), 1 << 20));
            assertEquals(
                    Collections.singletonList(ChangeEvent.ChangeOp.LEASE_EXPIRE),
                    tail.getRecords().get(tail.getRecords().size() - 1).getChanges().stream()
                            .map(ChangeEvent.Change::getOp)
                            .collect(Collectors.toList()),
                    "the expiry found no reverse entries left to cascade over");
        }
    }

    @Test
    void testInvalidCommandRetryCompletesWithoutLogGrowth() throws Exception {
        final RaftPeerId leader = awaitLeader(cluster.peerIds());
        final RaftClientRequest request =
                RaftClientRequest.newBuilder()
                        .setClientId(cluster.adminClient().getId())
                        .setServerId(leader)
                        .setGroupId(cluster.groupId())
                        .setCallId(Long.MAX_VALUE - 17L)
                        .setMessage(Message.valueOf(ByteBuffer.wrap(new byte[] {1, 2, 3})))
                        .setType(RaftClientRequest.writeRequestType())
                        .build();

        final long before = leaderLogNextIndex(leader);
        final Throwable first = invalidRequestFailure(request, "first");
        final long afterFirst = leaderLogNextIndex(leader);
        final Throwable retry = invalidRequestFailure(request, "retry");
        final long afterRetry = leaderLogNextIndex(leader);

        org.junit.jupiter.api.Assertions.assertAll(
                () -> org.junit.jupiter.api.Assertions.assertNull(first, "first RPC did not reply"),
                () -> org.junit.jupiter.api.Assertions.assertNull(retry, "retry did not reply"),
                () -> assertEquals(before, afterFirst, "the first invalid RPC appended a log entry"),
                () -> assertEquals(before, afterRetry, "the invalid RPC retry appended a log entry"));
    }

    @Test
    void testMalformedPayloadIsRejectedBeforeAppendOnEveryReplica() throws Exception {
        final byte[] malformed = {
            (byte) RaftCommandCodec.CURRENT_FORMAT_VERSION,
            RaftCommand.Op.PUT.code(),
            (byte) 0xFF,
            (byte) 0xFF,
            (byte) 0xFF,
            (byte) 0xFF,
            (byte) 0xFF,
            (byte) 0xFF,
            (byte) 0xFF,
            (byte) 0xFF,
            (byte) 0xFF,
            0x01
        };
        long callId = 0;
        for (MetadataStateMachine stateMachine : stateMachines()) {
            final RaftClientRequest request =
                    RaftClientRequest.newBuilder()
                            .setClientId(ClientId.valueOf(UUID.fromString(GROUP_UUID)))
                            .setServerId(stateMachine.getId())
                            .setGroupId(stateMachine.getGroupId())
                            .setCallId(++callId)
                            .setMessage(Message.valueOf(ByteBuffer.wrap(malformed)))
                            .setType(RaftClientRequest.writeRequestType())
                            .build();
            final TransactionContext context = stateMachine.startTransaction(request);
            assertTrue(
                    context.getException() instanceof IllegalArgumentException,
                    "the malformed command must be rejected before its log append");
            org.junit.jupiter.api.Assertions.assertSame(request, context.getClientRequest());
            assertNull(context.getLogEntry());
        }

        send(RaftCommand.put("/malformed", NO_DATA, 0, 1L));
        assertEquals(
                RaftResult.Status.OK,
                send(RaftCommand.put("/malformed/survivor", bytes("ok"), 0, 1L)).getStatus());
        for (MetadataStateMachine stateMachine : stateMachines()) {
            waitValue(
                    () ->
                            directQuery(stateMachine, RaftCommand.exists("/malformed/survivor")).isFound()
                                    ? Optional.of(true)
                                    : Optional.empty(),
                    EVENT_TIMEOUT,
                    "state machine " + stateMachine.getId() + " survived the malformed entry");
        }
    }

    @Test
    void testTermFencedMultiRejectedWhenTheEntryTermAdvanced() throws Exception {
        try (Standalone standalone = new Standalone("fence")) {
            final MetadataStateMachine sm = standalone.stateMachine;

            long index = 0;
            applyOnStandalone(
                    sm,
                    2L,
                    ++index,
                    RaftCommand.multi(
                            Arrays.asList(
                                    RaftCommand.put("/fence", NO_DATA, 0, 1L),
                                    RaftCommand.put("/fence/anchor", bytes("a"), 0, 1L)),
                            2L),
                    RaftResult.Status.OK);
            applyOnStandalone(
                    sm,
                    2L,
                    ++index,
                    RaftCommand.multi(
                            Collections.singletonList(RaftCommand.put("/fence/stale", bytes("s"), 0, 1L)), 1L),
                    RaftResult.Status.STALE_TERM);
            assertFalse(
                    directQuery(sm, RaftCommand.exists("/fence/stale")).isFound(),
                    "nothing of the fenced batch applied");
            applyOnStandalone(
                    sm,
                    2L,
                    ++index,
                    RaftCommand.multi(
                            Collections.singletonList(RaftCommand.put("/fence/fresh", bytes("f"), 0, 1L)), 2L),
                    RaftResult.Status.OK);
            applyOnStandalone(
                    sm,
                    3L,
                    ++index,
                    RaftCommand.multi(
                            Collections.singletonList(RaftCommand.put("/fence/unfenced", bytes("u"), 0, 1L)),
                            0L),
                    RaftResult.Status.OK);
            assertTrue(directQuery(sm, RaftCommand.exists("/fence/anchor")).isFound());
            assertTrue(directQuery(sm, RaftCommand.exists("/fence/fresh")).isFound());
            assertTrue(directQuery(sm, RaftCommand.exists("/fence/unfenced")).isFound());
        }
    }

    @Test
    void testPostRetentionRetryBoundaryRunsAsNewInvocationAndRestartKeepsGenerations()
            throws Exception {
        // Documented unsupported boundary: a retry arriving after the replicated cutoff collected
        // the first result legitimately re-executes, producing a second business effect. This
        // test pins that sequence with a controlled restart - it is not SIGKILL or power-loss
        // coverage.
        try (MetadataCluster retained =
                        MetadataCluster.newBuilder(new File(tempDir, "post-retention-boundary"))
                                .setGroupUuid(UUID.randomUUID())
                                .setResultRetention(Duration.ofMillis(1_000))
                                .setRetryCacheExpiry(Duration.ofMillis(500))
                                .build()) {
            retained.start();
            RaftPeerId leader = retained.awaitLeader();
            // Warm the leader up: an ordered admin write retries internally until the leader is
            // ready, keeping the raw unordered stream below safe from a LeaderNotReady reply.
            assertEquals(
                    RaftResult.Status.OK,
                    sendOn(retained, RaftCommand.put("/post-retention-warmup", NO_DATA, 0, 1L))
                            .getStatus());
            final ClientId clientId = ClientId.valueOf(UUID.randomUUID());
            final RaftCommand command = RaftCommand.put("/post-retention", bytes("v"), 0, 1L);
            final RaftClientRequest request =
                    rawWriteRequest(retained, leader, clientId, 4242L, command);

            final RaftClientReply first = sendRaw(retained, request);
            final byte[] firstEncoded = first.getMessage().getContent().toByteArray();
            assertEquals(1L, decodeResult(firstEncoded).getVersion());
            final long firstIndex = first.getLogIndex();

            Thread.sleep(1_200L); // outlive the memory retry cache

            // Advance the shared clock - the only time source envelopes sample - and commit an
            // unrelated write: its replicated cutoff passes the first result's retentionTime, so
            // deterministic GC collects the first-generation record on every replica.
            retained.clock.advanceTime(Duration.ofMillis(3_000));
            assertEquals(
                    RaftResult.Status.OK,
                    sendOn(
                                    retained,
                                    RaftCommand.put("/post-retention/advance", NO_DATA, 0, 1L))
                            .getStatus());

            final RaftClientReply second = sendRaw(retained, request);
            final byte[] secondEncoded = second.getMessage().getContent().toByteArray();
            final long secondIndex = second.getLogIndex();
            assertTrue(secondIndex > firstIndex, "the boundary retry appended a new entry");
            assertEquals(
                    2L,
                    decodeResult(secondEncoded).getVersion(),
                    "the collected first generation re-executed: a second business effect");

            // Final cache position: an immediate retry is served from the memory cache at the
            // newer index without appending anything.
            final long nextIndexBeforeCacheHit = logNextIndexOf(retained, leader);
            final RaftClientReply cached = sendRaw(retained, request);
            assertEquals(secondIndex, cached.getLogIndex(), "the cache holds the newer index");
            assertArrayEquals(secondEncoded, cached.getMessage().getContent().toByteArray());
            assertEquals(
                    nextIndexBeforeCacheHit,
                    logNextIndexOf(retained, leader),
                    "the cached retry appended no log entry");

            // Full controlled restart: exactly two business effects survive.
            for (int i = 0; i < retained.numServers(); i++) {
                retained.killServer(i);
            }
            for (int i = 0; i < retained.numServers(); i++) {
                retained.restartServer(i);
            }
            leader = retained.awaitLeader();
            final RaftClientReply warmupAfterRestart =
                    sendOnReply(
                            retained,
                            RaftCommand.put(
                                    "/post-retention-warmup-after-restart", NO_DATA, 0, 1L));
            assertEquals(
                    RaftResult.Status.OK,
                    decodeReply(warmupAfterRestart, RaftCommand.put("/", NO_DATA, 0, 1L))
                            .getStatus(),
                    "warm the restarted leader up before the next raw send");
            assertEquals(
                    2L,
                    sendReadOnlyOn(retained, RaftCommand.get("/post-retention")).getVersion(),
                    "exactly two business effects survive the restart");

            // The restart emptied every memory cache, but the new leader's startup log replay
            // re-seeds its cache with the replayed entries' replies. Waiting out the expiry
            // proves the DURABLE store answers: the same retry re-reads generation two's record.
            Thread.sleep(1_200L);
            final RaftClientReply third =
                    sendRaw(retained, rawWriteRequest(retained, leader, clientId, 4242L, command));
            assertTrue(
                    third.getLogIndex() > secondIndex,
                    "thirdIndex="
                            + third.getLogIndex()
                            + ", secondIndex="
                            + secondIndex
                            + ", warmupAfterRestartIndex="
                            + warmupAfterRestart.getLogIndex());
            assertArrayEquals(
                    secondEncoded,
                    third.getMessage().getContent().toByteArray(),
                    "generation two's original encoded reply");
            assertEquals(
                    2L,
                    sendReadOnlyOn(retained, RaftCommand.get("/post-retention")).getVersion(),
                    "no third business effect");

            // Old- and new-generation replay of the committed entries against the restarted
            // leader's state machine. This is a synthetic re-apply of the committed indices: a
            // clean restart never re-applies entries below its durable marker.
            final MetadataStateMachine leaderMachine =
                    retained.stateMachine(retained.indexOf(leader));
            final RaftResult oldGeneration =
                    decodeResult(
                            applyCommittedIndex(
                                    leaderMachine, clientId, 4242L, command, firstIndex));
            assertEquals(
                    RaftResult.Status.RESULT_EXPIRED,
                    oldGeneration.getStatus(),
                    "the old generation is fenced off by generation two");
            assertArrayEquals(
                    secondEncoded,
                    applyCommittedIndex(leaderMachine, clientId, 4242L, command, secondIndex),
                    "the current generation replays its original encoded reply");
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private RaftResult send(RaftCommand command) throws IOException {
        final RaftClientReply reply = cluster.adminClient().io().send(toMessage(command));
        return decodeReply(reply, command);
    }

    /** Returns any failure instead of throwing so the same invocation can always be sent twice. */
    private Throwable invalidRequestFailure(RaftClientRequest request, String attempt) {
        final CompletableFuture<RaftClientReply> future =
                cluster.adminClient().getClientRpc().sendRequestAsyncUnordered(request);
        try {
            final RaftClientReply reply = future.get(5, TimeUnit.SECONDS);
            assertNotNull(reply, attempt + " RPC returned no reply");
            assertFalse(reply.isSuccess(), attempt + " invalid RPC unexpectedly succeeded");
            assertTrue(
                    reply.getException()
                            instanceof org.apache.ratis.protocol.exceptions.StateMachineException,
                    attempt + " RPC did not return the state machine input failure: " + reply);
            return null;
        } catch (java.util.concurrent.TimeoutException timeout) {
            future.cancel(true);
            return timeout;
        } catch (Exception | AssertionError failure) {
            return failure;
        }
    }

    /** Reads the actual leader log, not the applied state-machine revision. */
    private long leaderLogNextIndex(RaftPeerId leader) throws Exception {
        return logNextIndexOf(cluster, leader);
    }

    /** Reads any live member's raft log next index through the harness server field. */
    private static long logNextIndexOf(MetadataCluster cluster, RaftPeerId member)
            throws Exception {
        final java.lang.reflect.Field field =
                org.apache.ratis.metadata.MetadataServer.class.getDeclaredField("raftServer");
        field.setAccessible(true);
        final RaftServer raftServer =
                (RaftServer) field.get(cluster.server(cluster.indexOf(member)));
        final RaftServer.Division division = raftServer.getDivision(cluster.groupId());
        assertNotNull(division);
        return division.getRaftLog().getNextIndex();
    }

    /** A fixed write request: resending it re-uses one ClientInvocationId across attempts. */
    private static RaftClientRequest rawWriteRequest(
            MetadataCluster cluster,
            RaftPeerId server,
            ClientId clientId,
            long callId,
            RaftCommand command) {
        return RaftClientRequest.newBuilder()
                .setClientId(clientId)
                .setServerId(server)
                .setGroupId(cluster.groupId())
                .setCallId(callId)
                .setMessage(
                        Message.valueOf(
                                ByteBuffer.wrap(RaftCommandCodec.encodeCommand(command))))
                .setType(RaftClientRequest.writeRequestType())
                .build();
    }

    /**
     * Sends a fixed raw write request on a fresh admin client and asserts it succeeded. Callers
     * must warm the addressed leader up with an ordered admin write first: a raw unordered send
     * has no client-side retry, and a LeaderNotReady reply closes its unordered stream.
     */
    private static RaftClientReply sendRaw(MetadataCluster cluster, RaftClientRequest request)
            throws Exception {
        try (RaftClient client = cluster.newAdminClient()) {
            final CompletableFuture<RaftClientReply> future =
                    client.getClientRpc().sendRequestAsyncUnordered(request);
            final RaftClientReply reply = future.get(30, TimeUnit.SECONDS);
            assertNotNull(reply, "the raw write returned no reply");
            assertTrue(reply.isSuccess(), () -> "the raw write failed: " + reply);
            return reply;
        }
    }

    /** Re-applies one committed index against an open state machine; asserts nothing about raft. */
    private static byte[] applyCommittedIndex(
            MetadataStateMachine stateMachine,
            ClientId clientId,
            long callId,
            RaftCommand command,
            long index)
            throws Exception {
        final RaftClientRequest request =
                RaftClientRequest.newBuilder()
                        .setClientId(clientId)
                        .setServerId(stateMachine.getId())
                        .setGroupId(stateMachine.getGroupId())
                        .setCallId(callId)
                        .setMessage(
                                Message.valueOf(
                                        ByteBuffer.wrap(
                                                RaftCommandCodec.encodeCommand(command))))
                        .setType(RaftClientRequest.writeRequestType())
                        .build();
        final TransactionContext context = stateMachine.startTransaction(request);
        ((TransactionContextImpl) context).initLogEntry(1L, index);
        return stateMachine.applyTransaction(context)
                .get(10, TimeUnit.SECONDS)
                .getContent()
                .toByteArray();
    }

    private static RaftResult decodeResult(byte[] encoded) {
        final RaftResult result = RaftCommandCodec.decodeResult(encoded);
        assertNotNull(result, "the raw reply decodes");
        return result;
    }

    private static RaftResult sendOn(MetadataCluster cluster, RaftCommand command)
            throws IOException {
        return decodeReply(cluster.adminClient().io().send(toMessage(command)), command);
    }

    /** The raw admin-client reply of one ordered write, for tests that need its log index. */
    private static RaftClientReply sendOnReply(MetadataCluster cluster, RaftCommand command)
            throws IOException {
        return cluster.adminClient().io().send(toMessage(command));
    }

    private static RaftResult sendReadOnlyOn(MetadataCluster cluster, RaftCommand command)
            throws IOException {
        return decodeReply(cluster.adminClient().io().sendReadOnly(toMessage(command)), command);
    }

    private static final class Standalone implements AutoCloseable {

        final MetadataStateMachine stateMachine;
        private final RaftServer server;
        private long index;

        Standalone(String name) throws Exception {
            final RaftPeerId selfId = RaftPeerId.valueOf("s-" + name);
            final RaftGroup group =
                    RaftGroup.valueOf(
                            RaftGroupId.valueOf(UUID.fromString(GROUP_UUID)),
                            RaftPeer.newBuilder().setId(selfId).setAddress("localhost:0").build());
            final RaftProperties properties = new RaftProperties();
            RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.GRPC);
            // the machine is never started; the rpc binds an ephemeral port at build time
            GrpcConfigKeys.Server.setPort(properties, 0);
            RaftServerConfigKeys.setStorageDir(
                    properties, Collections.singletonList(new File(tempDir, "raft-" + name)));
            stateMachine = new MetadataStateMachine(new File(tempDir, "meta-" + name));
            server =
                    RaftServer.newBuilder()
                            .setServerId(selfId)
                            .setGroup(group)
                            .setStateMachine(stateMachine)
                            .setProperties(properties)
                            .build();
            final RaftStorage storage =
                    StorageImplUtils.newRaftStorage(
                            new File(tempDir, "raft-" + name),
                            SizeInBytes.valueOf("0MB"),
                            RaftStorage.StartupOption.FORMAT,
                            null);
            storage.initialize();
            stateMachine.initialize(server, group.getGroupId(), storage);
        }

        /** Applies one command as the next committed entry and asserts it succeeded. */
        RaftResult apply(RaftCommand command) throws Exception {
            return applyOnStandalone(stateMachine, 1L, ++index, command, RaftResult.Status.OK);
        }

        @Override
        public void close() throws Exception {
            server.close();
            stateMachine.close();
        }
    }

    /**
     * Applies one synthetic committed entry at the given term - the term a stamp compares against.
     */
    private static RaftResult applyOnStandalone(
            MetadataStateMachine sm, long term, long index, RaftCommand command,
            RaftResult.Status expected) throws Exception {
        final RaftClientRequest request =
                RaftClientRequest.newBuilder()
                        .setClientId(ClientId.valueOf(UUID.fromString(GROUP_UUID)))
                        .setServerId(sm.getId())
                        .setGroupId(sm.getGroupId())
                        .setCallId(index)
                        .setMessage(
                                Message.valueOf(
                                        ByteBuffer.wrap(RaftCommandCodec.encodeCommand(command))))
                        .setType(RaftClientRequest.writeRequestType())
                        .build();
        final TransactionContext context = sm.startTransaction(request);
        ((TransactionContextImpl) context).initLogEntry(term, index);
        final Message reply = sm.applyTransaction(context).get(10, TimeUnit.SECONDS);
        final RaftResult result = RaftCommandCodec.decodeResult(reply.getContent().toByteArray());
        assertNotNull(result, "reply of a fenced " + command.getOp() + " decodes");
        assertEquals(
                expected,
                result.getStatus(),
                "a MULTI stamped "
                        + command.getWriterTerm()
                        + " committing under entry term "
                        + term);
        return result;
    }

    private RaftResult sendReadOnly(RaftCommand command) throws IOException {
        final RaftClientReply reply = cluster.adminClient().io().sendReadOnly(toMessage(command));
        return decodeReply(reply, command);
    }

    private static Message toMessage(RaftCommand command) {
        return Message.valueOf(ByteBuffer.wrap(RaftCommandCodec.encodeCommand(command)));
    }

    /**
     * Queries one replica's state machine directly, bypassing raft (not the linearizable state).
     */
    private static RaftResult directQuery(MetadataStateMachine stateMachine, RaftCommand command) {
        try {
            final CompletableFuture<Message> reply = stateMachine.query(toMessage(command));
            final RaftResult result =
                    RaftCommandCodec.decodeResult(
                            reply.get(10, TimeUnit.SECONDS).getContent().toByteArray());
            assertNotNull(result, "direct query " + command.getOp() + " decodes");
            return result;
        } catch (Exception e) {
            throw new AssertionError("Direct query " + command.getOp() + " failed", e);
        }
    }

    private static RaftResult decodeReply(RaftClientReply reply, RaftCommand command) {
        final RaftResult result =
                RaftCommandCodec.decodeResult(reply.getMessage().getContent().toByteArray());
        assertNotNull(result, "reply of " + command.getOp() + " decodes (op echo must match)");
        assertEquals(command.getOp(), result.getOp());
        return result;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static List<String> pathsOf(RaftResult listResult) {
        final List<String> paths = new ArrayList<>(listResult.getEntries().size());
        for (RaftResult.Entry entry : listResult.getEntries()) {
            paths.add(entry.getPath());
        }
        return paths;
    }

    private List<String> createConcurrently(String basePath, int threads, int createsPerThread)
            throws Exception {
        final ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            final List<Future<List<String>>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                futures.add(
                        pool.submit(
                                (Callable<List<String>>)
                                        () -> {
                                            final List<String> paths = new ArrayList<>();
                                            for (int i = 0; i < createsPerThread; i++) {
                                                final RaftClientReply reply =
                                                        cluster
                                                                .adminClient()
                                                                .io()
                                                                .send(
                                                                        toMessage(
                                                                                RaftCommand.create(
                                                                                        basePath,
                                                                                        bytes("conc"),
                                                                                        true,
                                                                                        0,
                                                                                        1L)));
                                                final RaftResult result =
                                                        RaftCommandCodec.decodeResult(
                                                                reply.getMessage().getContent().toByteArray());
                                                assertNotNull(result);
                                                assertEquals(RaftResult.Status.OK, result.getStatus());
                                                paths.add(result.getResultPath());
                                            }
                                            return paths;
                                        }));
            }
            final List<String> paths = new ArrayList<>();
            for (Future<List<String>> future : futures) {
                paths.addAll(future.get(CALL_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
            }
            return paths;
        } finally {
            pool.shutdownNow();
        }
    }

    private static List<Long> revisionsOf(List<ChangeEvent> events) {
        final List<Long> revisions = new ArrayList<>();
        for (ChangeEvent event : events) {
            revisions.add(event.getRevision());
        }
        return revisions;
    }

    private RaftPeerId awaitLeader(List<RaftPeerId> candidates) {
        return cluster.awaitLeader(candidates);
    }

    private List<MetadataStateMachine> stateMachines() {
        final List<MetadataStateMachine> machines = new ArrayList<>(cluster.numServers());
        for (int i = 0; i < cluster.numServers(); i++) {
            machines.add(cluster.stateMachine(i));
        }
        return machines;
    }
}
