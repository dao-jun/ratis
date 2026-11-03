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
package org.apache.ratis.metadata.statemachine;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.Message;
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
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.SizeInBytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Lease-side invariants of {@link MetadataStateMachine} on a standalone, thread-free machine. */
public class TestMetadataStateMachine {

    private static final String GROUP_UUID = "8d1f4b7c-2a5e-4f8b-9c3d-7e6a1b0d5f27";
    private static final long TTL_MS = TimeUnit.SECONDS.toMillis(60);

    @TempDir File tempDir;

    private static final class StandaloneMachine implements AutoCloseable {

        private final RaftServer server;
        private final MetadataStateMachine stateMachine;
        private long index;

        StandaloneMachine(File dir) throws Exception {
            final RaftPeerId selfId = RaftPeerId.valueOf("m0");
            final RaftGroup group =
                    RaftGroup.valueOf(
                            RaftGroupId.valueOf(UUID.fromString(GROUP_UUID)),
                            RaftPeer.newBuilder().setId(selfId).setAddress("localhost:0").build());
            final RaftProperties properties = new RaftProperties();
            RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.GRPC);
            // the machine is never started; the rpc binds an ephemeral port at build time
            GrpcConfigKeys.Server.setPort(properties, 0);
            RaftServerConfigKeys.setStorageDir(
                    properties, Collections.singletonList(new File(dir, "raft")));

            stateMachine = new MetadataStateMachine(new File(dir, "meta"));
            server =
                    RaftServer.newBuilder()
                            .setServerId(selfId)
                            .setGroup(group)
                            .setStateMachine(stateMachine)
                            .setProperties(properties)
                            .build();
            // build() registers no group (impls are created on start()), so initialize by hand
            final RaftStorage storage =
                    StorageImplUtils.newRaftStorage(
                            new File(dir, "raft"),
                            SizeInBytes.valueOf("0MB"),
                            RaftStorage.StartupOption.FORMAT,
                            null);
            storage.initialize();
            stateMachine.initialize(server, group.getGroupId(), storage);
        }

        RaftResult apply(RaftCommand command) throws Exception {
            final LogEntryProto entry =
                    LogEntryProto.newBuilder()
                            .setTerm(1L)
                            .setIndex(++index)
                            .setStateMachineLogEntry(
                                    StateMachineLogEntryProto.newBuilder()
                                            .setLogData(
                                                    ByteString.copyFrom(
                                                            RaftCommandCodec.encodeCommand(
                                                                    command))))
                            .build();
            final Message reply =
                    stateMachine
                            .applyTransaction(
                                    TransactionContext.newBuilder()
                                            .setStateMachine(stateMachine)
                                            .setLogEntry(entry)
                                            .build())
                            .get(10, TimeUnit.SECONDS);
            final RaftResult result =
                    RaftCommandCodec.decodeResult(reply.getContent().toByteArray());
            assertNotNull(result, "reply of " + command.getOp() + " decodes");
            assertEquals(command.getOp(), result.getOp());
            return result;
        }

        RaftResult query(RaftCommand command) {
            final CompletableFuture<Message> future =
                    stateMachine.query(
                            Message.valueOf(
                                    ByteBuffer.wrap(RaftCommandCodec.encodeCommand(command))));
            final RaftResult result =
                    RaftCommandCodec.decodeResult(future.join().getContent().toByteArray());
            assertNotNull(result, "query " + command.getOp() + " decodes");
            return result;
        }

        @Override
        public void close() throws Exception {
            server.close();
            stateMachine.close();
        }
    }

    @Test
    void testWritesRejectBindingToALeaseThatDoesNotExist() throws Exception {
        try (StandaloneMachine machine = new StandaloneMachine(tempDir)) {
            final long leaseId = 7L;
            final long now = 1_000L;
            assertEquals(
                    RaftResult.Status.OK,
                    machine.apply(RaftCommand.put("/ul", bytes(""), 0L, now)).getStatus());
            assertEquals(
                    RaftResult.Status.OK,
                    machine.apply(RaftCommand.leaseRegister(leaseId, TTL_MS, now + TTL_MS))
                            .getStatus());

            assertEquals(
                    RaftResult.Status.OK,
                    machine.apply(RaftCommand.put("/ul/live", bytes("d"), leaseId, now))
                            .getStatus());
            assertEquals(leaseId, machine.query(RaftCommand.get("/ul/live")).getLeaseId());

            assertTrue(machine.apply(RaftCommand.leaseExpire(leaseId, -1L)).isExpired());

            assertEquals(
                    RaftResult.Status.UNKNOWN_LEASE,
                    machine.apply(RaftCommand.put("/ul/p", bytes("d"), leaseId, now))
                            .getStatus(),
                    "PUT onto a dead lease is rejected");
            assertEquals(
                    RaftResult.Status.UNKNOWN_LEASE,
                    machine.apply(RaftCommand.create("/ul/c", bytes("d"), false, leaseId, now))
                            .getStatus(),
                    "CREATE onto a dead lease is rejected");
            assertEquals(
                    RaftResult.Status.UNKNOWN_LEASE,
                    machine.apply(RaftCommand.create("/ul/s-", bytes("d"), true, leaseId, now))
                            .getStatus(),
                    "sequential CREATE onto a dead lease is rejected");
            assertEquals(
                    RaftResult.Status.UNKNOWN_LEASE,
                    machine.apply(RaftCommand.cas("/ul/x", bytes("d"), 0L, leaseId, now))
                            .getStatus(),
                    "CAS onto a dead lease is rejected");
            assertEquals(
                    RaftResult.Status.UNKNOWN_LEASE,
                    machine.apply(RaftCommand.put("/ul/n", bytes("d"), 99L, now)).getStatus(),
                    "a never-registered lease id is rejected the same way");

            for (String path :
                    Arrays.asList("/ul/p", "/ul/c", "/ul/s-0000000001", "/ul/x", "/ul/n")) {
                assertEquals(
                        RaftResult.Status.NOT_FOUND,
                        machine.query(RaftCommand.get(path)).getStatus(),
                        "no record of the rejected writes exists at " + path);
            }

            final RaftResult batch =
                    machine.apply(
                            RaftCommand.multi(
                                    Arrays.asList(
                                            RaftCommand.put("/ul/m", bytes("m"), leaseId, now),
                                            RaftCommand.put("/ul/other", bytes("o"), 0L, now))));
            assertEquals(RaftResult.Status.MULTI_FAILED, batch.getStatus());
            assertEquals(0, batch.getFailIndex());
            assertEquals(RaftResult.Status.UNKNOWN_LEASE, batch.getFailStatus());
            assertEquals(
                    RaftResult.Status.NOT_FOUND,
                    machine.query(RaftCommand.get("/ul/other")).getStatus());

            // the control record was bound to the expired lease, so it died with it
            assertEquals(
                    RaftResult.Status.NOT_FOUND,
                    machine.query(RaftCommand.get("/ul/live")).getStatus());
        }
    }

    @Test
    void testLeaseExpireCascadesToBindingsStagedInTheSameBatch() throws Exception {
        try (StandaloneMachine machine = new StandaloneMachine(tempDir)) {
            final long leaseId = 9L;
            final long now = 1_000L;
            final RaftResult batch =
                    machine.apply(
                            RaftCommand.multi(
                                    Arrays.asList(
                                            RaftCommand.put("/o", bytes(""), 0L, now),
                                            RaftCommand.leaseRegister(leaseId, TTL_MS, now + TTL_MS),
                                            RaftCommand.put("/o/x", bytes("x"), leaseId, now),
                                            RaftCommand.create(
                                                    "/o/y", bytes("y"), false, leaseId, now, true),
                                            RaftCommand.leaseExpire(leaseId, -1L))));
            assertEquals(RaftResult.Status.OK, batch.getStatus());
            assertEquals(RaftResult.Status.OK, batch.getSubResults().get(0).getStatus());
            assertEquals(RaftResult.Status.OK, batch.getSubResults().get(1).getStatus());
            assertEquals(1L, batch.getSubResults().get(2).getVersion());
            assertEquals(RaftResult.Status.OK, batch.getSubResults().get(3).getStatus());
            assertTrue(
                    batch.getSubResults().get(4).isExpired(),
                    "the same-batch expiry took the lease");
            assertEquals(
                    2L,
                    batch.getSubResults().get(4).getDeletedCount(),
                    "the cascade reached both same-batch ephemeral records");

            assertEquals(
                    RaftResult.Status.NOT_FOUND,
                    machine.query(RaftCommand.get("/o/x")).getStatus(),
                    "the same-batch put died with the lease");
            assertEquals(
                    RaftResult.Status.NOT_FOUND,
                    machine.query(RaftCommand.get("/o/y")).getStatus(),
                    "the same-batch create died with the lease");
            assertEquals(
                    RaftResult.Status.OK,
                    machine.query(RaftCommand.get("/o")).getStatus(),
                    "the persistent parent survives the cascade");
            assertTrue(
                    machine.query(RaftCommand.leaseList()).getLeases().isEmpty(),
                    "the lease itself is gone");
            assertEquals(
                    RaftResult.Status.UNKNOWN_LEASE,
                    machine.apply(RaftCommand.leaseRenew(leaseId, now + 2 * TTL_MS)).getStatus());
        }
    }

    @Test
    void testRegisterAndBindInOneBatchWithoutExpiryKeepsTheRecord() throws Exception {
        try (StandaloneMachine machine = new StandaloneMachine(tempDir)) {
            final long leaseId = 5L;
            final long now = 1_000L;
            assertEquals(
                    RaftResult.Status.OK,
                    machine.apply(RaftCommand.put("/b", bytes(""), 0L, now)).getStatus());
            final RaftResult batch =
                    machine.apply(
                            RaftCommand.multi(
                                    Arrays.asList(
                                            RaftCommand.leaseRegister(leaseId, TTL_MS, now + TTL_MS),
                                            RaftCommand.put("/b/x", bytes("x"), leaseId, now))));
            assertEquals(RaftResult.Status.OK, batch.getStatus());
            assertEquals(leaseId, machine.query(RaftCommand.get("/b/x")).getLeaseId());

            final RaftResult expire = machine.apply(RaftCommand.leaseExpire(leaseId, -1L));
            assertTrue(expire.isExpired());
            assertEquals(1L, expire.getDeletedCount());
            assertEquals(
                    RaftResult.Status.NOT_FOUND,
                    machine.query(RaftCommand.get("/b/x")).getStatus());
        }
    }

    @Test
    void testLeaseExpireCascadesToABindingStagedAfterASubtreeTombstone() throws Exception {
        try (StandaloneMachine machine = new StandaloneMachine(tempDir)) {
            final long now = 1_000L;
            final long leaseId = 31L;
            final long liveLeaseId = 32L;
            assertEquals(
                    RaftResult.Status.OK,
                    machine.apply(RaftCommand.put("/db", bytes("db"), 0L, now)).getStatus());
            for (long lease : Arrays.asList(leaseId, liveLeaseId)) {
                assertEquals(
                        RaftResult.Status.OK,
                        machine.apply(RaftCommand.leaseRegister(lease, TTL_MS, now + TTL_MS))
                                .getStatus());
            }

            final RaftResult expired =
                    machine.apply(
                            RaftCommand.multi(
                                    Arrays.asList(
                                            RaftCommand.delete("/db", true),
                                            // re-created with create-parents: the recursive delete
                                            // took the parent chain
                                            RaftCommand.create(
                                                    "/db/x", bytes("x"), false, leaseId, now, true),
                                            RaftCommand.leaseExpire(leaseId, -1L))));
            assertEquals(RaftResult.Status.OK, expired.getStatus());
            assertEquals(
                    1L,
                    expired.getSubResults().get(2).getDeletedCount(),
                    "the expiry reached the binding staged after the subtree tombstone");
            assertEquals(
                    RaftResult.Status.NOT_FOUND,
                    machine.query(RaftCommand.get("/db/x")).getStatus(),
                    "the re-created ephemeral died with its lease");

            assertEquals(
                    RaftResult.Status.OK,
                    machine.apply(RaftCommand.put("/db2", bytes("db"), 0L, now)).getStatus());
            final RaftResult live =
                    machine.apply(
                            RaftCommand.multi(
                                    Arrays.asList(
                                            RaftCommand.delete("/db2", true),
                                            RaftCommand.create(
                                                    "/db2/y",
                                                    bytes("y"),
                                                    false,
                                                    liveLeaseId,
                                                    now,
                                                    true))));
            assertEquals(RaftResult.Status.OK, live.getStatus());
            assertEquals(
                    liveLeaseId,
                    machine.query(RaftCommand.get("/db2/y")).getLeaseId(),
                    "the control's live lease keeps its re-created record");
        }
    }

    @Test
    void testLeaseExpireCountsARecordRefreshedInTheSameBatchOnce() throws Exception {
        try (StandaloneMachine machine = new StandaloneMachine(tempDir)) {
            final long leaseId = 51L;
            final long now = 1_000L;
            assertEquals(
                    RaftResult.Status.OK,
                    machine.apply(RaftCommand.leaseRegister(leaseId, TTL_MS, now + TTL_MS))
                            .getStatus());
            assertEquals(
                    RaftResult.Status.OK,
                    machine.apply(RaftCommand.put("/w", bytes("v1"), leaseId, now)).getStatus());

            final RaftResult batch =
                    machine.apply(
                            RaftCommand.multi(
                                    Arrays.asList(
                                            RaftCommand.put("/w", bytes("v2"), leaseId, now),
                                            RaftCommand.leaseExpire(leaseId, -1L))));
            assertEquals(RaftResult.Status.OK, batch.getStatus());
            assertEquals(
                    1L,
                    batch.getSubResults().get(1).getDeletedCount(),
                    "the refreshed record is one expiry victim");
            assertEquals(
                    RaftResult.Status.NOT_FOUND,
                    machine.query(RaftCommand.get("/w")).getStatus());
        }
    }

    @Test
    void testRebindingARecordReleasesThePreviousLeasesReverseEntry() throws Exception {
        try (StandaloneMachine machine = new StandaloneMachine(tempDir)) {
            final long leaseA = 41L;
            final long leaseB = 42L;
            final long now = 1_000L;
            for (long lease : Arrays.asList(leaseA, leaseB)) {
                assertEquals(
                        RaftResult.Status.OK,
                        machine.apply(RaftCommand.leaseRegister(lease, TTL_MS, now + TTL_MS))
                                .getStatus());
            }
            assertEquals(
                    RaftResult.Status.OK,
                    machine.apply(RaftCommand.put("/r", bytes("v1"), leaseA, now)).getStatus());
            assertTrue(machine.stateMachine.leasePathsContains(leaseA, "/r"));

            assertEquals(
                    RaftResult.Status.OK,
                    machine.apply(RaftCommand.put("/r", bytes("v2"), leaseB, now)).getStatus());
            assertTrue(machine.stateMachine.leasePathsContains(leaseB, "/r"));
            assertFalse(
                    machine.stateMachine.leasePathsContains(leaseA, "/r"),
                    "the rebind to another lease released the old lease's entry");

            // the old lease's expiry spares the record (bound to B now) and deletes nothing
            final RaftResult expireA = machine.apply(RaftCommand.leaseExpire(leaseA, -1L));
            assertTrue(expireA.isExpired());
            assertEquals(0L, expireA.getDeletedCount());
            assertEquals(leaseB, machine.query(RaftCommand.get("/r")).getLeaseId());

            assertEquals(
                    RaftResult.Status.OK,
                    machine.apply(RaftCommand.put("/r", bytes("v3"), 0L, now)).getStatus());
            assertFalse(
                    machine.stateMachine.leasePathsContains(leaseB, "/r"),
                    "the rebind to persistent released the entry too");
            assertEquals(0L, machine.query(RaftCommand.get("/r")).getLeaseId());
        }
    }

    @Test
    void testRecursiveDeleteCountsEachVictimExactlyOnceUnderOverlayShapes() throws Exception {
        try (StandaloneMachine machine = new StandaloneMachine(tempDir)) {
            final long now = 1_000L;
            for (String path :
                    Arrays.asList(
                            "/s1", "/s1/t", "/s2", "/s3", "/s3/t", "/s4", "/s4/m", "/s4/m/t")) {
                assertEquals(
                        RaftResult.Status.OK,
                        machine.apply(RaftCommand.put(path, bytes("v1"), 0L, now)).getStatus());
            }

            final RaftResult batch =
                    machine.apply(
                            RaftCommand.multi(
                                    Arrays.asList(
                                            // shape 1: refresh a committed child
                                            RaftCommand.put("/s1/t", bytes("v2"), 0L, now),
                                            // shape 2: create a child no committed copy has
                                            RaftCommand.put("/s2/n", bytes("n"), 0L, now),
                                            // shape 3: delete a committed child, then re-create it
                                            RaftCommand.delete("/s3/t", false),
                                            RaftCommand.put("/s3/t", bytes("v2"), 0L, now),
                                            // shape 4: re-create under a staged subtree tombstone
                                            // (create-parents)
                                            RaftCommand.delete("/s4/m", true),
                                            RaftCommand.create(
                                                    "/s4/m/t", bytes("v2"), false, 0L, now, true),
                                            RaftCommand.delete("/s1", true),
                                            RaftCommand.delete("/s2", true),
                                            RaftCommand.delete("/s3", true),
                                            RaftCommand.delete("/s4", true))));
            assertEquals(RaftResult.Status.OK, batch.getStatus());

            assertEquals(
                    2L,
                    batch.getSubResults().get(6).getDeletedCount(),
                    "refresh of a committed child: root and child, each counted once");
            assertEquals(
                    2L,
                    batch.getSubResults().get(7).getDeletedCount(),
                    "batch-created child: root and child");
            assertEquals(
                    2L,
                    batch.getSubResults().get(8).getDeletedCount(),
                    "delete-then-recreate of a committed child: root and child, once");
            assertEquals(
                    3L,
                    batch.getSubResults().get(9).getDeletedCount(),
                    "recreate under an inner subtree tombstone: root, the re-materialized "
                            + "inner node and the re-created child");

            for (String path :
                    Arrays.asList(
                            "/s1", "/s1/t", "/s2", "/s2/n", "/s3", "/s3/t", "/s4", "/s4/m",
                            "/s4/m/t")) {
                assertEquals(
                        RaftResult.Status.NOT_FOUND,
                        machine.query(RaftCommand.get(path)).getStatus(),
                        "no victim of the four deletes survives at " + path);
            }
        }
    }

    @Test
    void testLeaseListPaginatesWithCursorAndHasMore() throws Exception {
        try (StandaloneMachine machine = new StandaloneMachine(tempDir)) {
            // insertion order deliberately unlike key order
            for (long leaseId : new long[] {30L, 10L, 20L}) {
                assertEquals(
                        RaftResult.Status.OK,
                        machine.apply(RaftCommand.leaseRegister(leaseId, TTL_MS, 1_000L + TTL_MS))
                                .getStatus());
            }

            assertEquals(
                    Arrays.asList(10L, 20L),
                    leaseIdsOf(machine.query(RaftCommand.leaseList(0L, 2L))),
                    "the first page holds the two lowest keys");
            assertTrue(machine.query(RaftCommand.leaseList(0L, 2L)).hasMore());

            assertEquals(
                    Collections.singletonList(30L),
                    leaseIdsOf(machine.query(RaftCommand.leaseList(20L, 2L))),
                    "resuming after the cursor returns the remainder");
            assertFalse(machine.query(RaftCommand.leaseList(20L, 2L)).hasMore());

            assertTrue(
                    machine.query(RaftCommand.leaseList(30L, 2L)).getLeases().isEmpty(),
                    "a cursor at the end yields an empty final page");
            assertFalse(machine.query(RaftCommand.leaseList(30L, 2L)).hasMore());

            assertEquals(
                    Arrays.asList(20L, 30L),
                    leaseIdsOf(machine.query(RaftCommand.leaseList(15L, 2L))),
                    "a cursor between keys resumes at the next key");

            assertEquals(
                    Arrays.asList(10L, 20L, 30L),
                    leaseIdsOf(machine.query(RaftCommand.leaseList())),
                    "the default page covers the whole small table");
            assertFalse(machine.query(RaftCommand.leaseList()).hasMore());

            assertTrue(
                    machine.query(RaftCommand.leaseList(99L, 2L)).getLeases().isEmpty(),
                    "a cursor beyond the table yields nothing");

            assertFalse(machine.query(RaftCommand.leaseList(0L, 3L)).hasMore());
        }
    }

    @Test
    void testPutThatCreatesRequiresAnExistingParent() throws Exception {
        try (StandaloneMachine machine = new StandaloneMachine(tempDir)) {
            final long now = 1_000L;
            assertEquals(
                    RaftResult.Status.NO_PARENT,
                    machine.apply(RaftCommand.put("/pp/t", bytes("v"), 0L, now)).getStatus(),
                    "creating /pp/t with no /pp is rejected, like create without the flag");
            assertEquals(
                    RaftResult.Status.NOT_FOUND,
                    machine.query(RaftCommand.get("/pp/t")).getStatus());

            // the createTable/dropDatabase interleaving: a put after the delete must fail loudly
            assertEquals(
                    RaftResult.Status.OK,
                    machine.apply(RaftCommand.put("/pp", bytes(""), 0L, now)).getStatus());
            assertEquals(
                    RaftResult.Status.OK,
                    machine.apply(RaftCommand.delete("/pp", true)).getStatus());
            assertEquals(
                    RaftResult.Status.NO_PARENT,
                    machine.apply(RaftCommand.put("/pp/t", bytes("v"), 0L, now)).getStatus(),
                    "a put racing the subtree's deletion fails NO_PARENT, not silently");

            // with the parent in place, put creates (the upsert contract) and then updates
            assertEquals(
                    RaftResult.Status.OK,
                    machine.apply(RaftCommand.put("/pp", bytes(""), 0L, now)).getStatus());
            assertEquals(
                    1L,
                    machine.apply(RaftCommand.put("/pp/t", bytes("v1"), 0L, now)).getVersion());
            assertEquals(
                    2L,
                    machine.apply(RaftCommand.put("/pp/t", bytes("v2"), 0L, now)).getVersion());
        }
    }

    @Test
    void testNoChildrenUnderALeaseBoundParent() throws Exception {
        try (StandaloneMachine machine = new StandaloneMachine(tempDir)) {
            final long leaseId = 11L;
            final long now = 1_000L;
            assertEquals(
                    RaftResult.Status.OK,
                    machine.apply(RaftCommand.leaseRegister(leaseId, TTL_MS, now + TTL_MS))
                            .getStatus());
            assertEquals(
                    RaftResult.Status.OK,
                    machine.apply(RaftCommand.put("/e", bytes(""), 0L, now)).getStatus());
            assertEquals(
                    RaftResult.Status.OK,
                    machine.apply(RaftCommand.put("/e/eph", bytes("d"), leaseId, now)).getStatus());

            assertEquals(
                    RaftResult.Status.NOT_ALLOWED,
                    machine.apply(RaftCommand.create("/e/eph/c", bytes("c"), false, 0L, now))
                            .getStatus(),
                    "a plain create under a lease-bound parent is rejected");
            assertEquals(
                    RaftResult.Status.NOT_ALLOWED,
                    machine.apply(
                                    RaftCommand.create(
                                            "/e/eph/g/c", bytes("c"), false, 0L, now, true))
                            .getStatus(),
                    "create-with-parents cannot graft a chain onto a lease-bound ancestor");
            assertEquals(
                    RaftResult.Status.NOT_ALLOWED,
                    machine.apply(RaftCommand.put("/e/eph/p", bytes("p"), 0L, now)).getStatus(),
                    "a put that would create under a lease-bound parent is rejected");
        }
    }

    @Test
    void testListValidatesItsPrefixLikeAPath() throws Exception {
        try (StandaloneMachine machine = new StandaloneMachine(tempDir)) {
            final long now = 1_000L;
            assertEquals(
                    RaftResult.Status.OK,
                    machine.apply(RaftCommand.put("/lp", bytes(""), 0L, now)).getStatus());
            assertEquals(
                    RaftResult.Status.OK,
                    machine.apply(RaftCommand.put("/lp/c", bytes(""), 0L, now)).getStatus());

            assertEquals(
                    RaftResult.Status.BAD_PATH,
                    machine.query(RaftCommand.list("", 10)).getStatus(),
                    "an empty prefix would dump the whole state family");
            assertEquals(
                    RaftResult.Status.BAD_PATH,
                    machine.query(RaftCommand.list("/lp//", 10)).getStatus(),
                    "a doubled trailing slash is a malformed prefix, not a deeper listing");
            assertEquals(
                    RaftResult.Status.OK,
                    machine.query(RaftCommand.list("/lp/", 10)).getStatus(),
                    "a parent's child prefix is the listing idiom");
            assertEquals(
                    2,
                    machine.query(RaftCommand.list("/", 10)).getEntries().size(),
                    "the root prefix lists every record");
        }
    }

    @Test
    void testLongLegalPathsAreAccepted() throws Exception {
        try (StandaloneMachine machine = new StandaloneMachine(tempDir)) {
            final long now = 1_000L;
            final StringBuilder builder = new StringBuilder("/p");
            while (builder.length() < 1_100) {
                builder.append('x');
            }
            assertEquals(
                    RaftResult.Status.OK,
                    machine.apply(RaftCommand.put(builder.toString(), bytes("v"), 0L, now))
                            .getStatus(),
                    "a 1100-character path is legal metadata (a long partition path)");

            while (builder.length() < 8_300) {
                builder.append('y');
            }
            assertEquals(
                    RaftResult.Status.BAD_PATH,
                    machine.apply(RaftCommand.put(builder.toString(), bytes("v"), 0L, now))
                            .getStatus(),
                    "the bound still exists past 8192");
        }
    }

    @Test
    void testPathCharacterRulesMirrorZooKeeper() throws Exception {
        try (StandaloneMachine machine = new StandaloneMachine(tempDir)) {
            final long now = 1_000L;
            for (String badPath :
                    Arrays.asList(
                            "/a\u0000b", // NUL
                            "/a\u0001b", // C0 control
                            "/a\u007Fb", // DEL
                            "/a\u0085b", // C1 control
                            "/a\uF8FFb", // private use
                            "/a\uFFFEb", // specials
                            "/a/./b", // "." segment
                            "/a/../b", // ".." segment
                            "/a/..", // trailing ".."
                            "a/b", // no leading slash
                            "/a/", // trailing slash
                            "/a//b")) { // empty segment
                assertEquals(
                        RaftResult.Status.BAD_PATH,
                        machine.apply(RaftCommand.put(badPath, bytes("v"), 0L, now)).getStatus(),
                        "path <" + badPath + "> is rejected");
            }
            assertEquals(
                    RaftResult.Status.OK,
                    machine.apply(RaftCommand.put("/a.b", bytes("v"), 0L, now)).getStatus());
            assertEquals(
                    RaftResult.Status.OK,
                    machine.apply(RaftCommand.put("/c..d", bytes("v"), 0L, now)).getStatus());
            // a lone surrogate is normalized to '?' by the UTF-8 wire encoding before checkPath
            assertEquals(
                    RaftResult.Status.OK,
                    machine.apply(RaftCommand.put("/a\uD800b", bytes("v"), 0L, now)).getStatus(),
                    "the surrogate was normalized to '?' by the UTF-8 wire encoding");
            assertEquals(
                    RaftResult.Status.OK,
                    machine.query(RaftCommand.get("/a?b")).getStatus());
        }
    }

    @Test
    void testExistsRootIsAlwaysTrue() throws Exception {
        try (StandaloneMachine machine = new StandaloneMachine(tempDir)) {
            assertTrue(machine.query(RaftCommand.exists("/")).isFound());
            assertEquals(
                    RaftResult.Status.NOT_FOUND,
                    machine.query(RaftCommand.get("/")).getStatus());
        }
    }

    private static List<Long> leaseIdsOf(RaftResult listing) {
        final List<Long> leaseIds = new ArrayList<>();
        for (RaftResult.Lease lease : listing.getLeases()) {
            leaseIds.add(lease.getLeaseId());
        }
        return leaseIds;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
