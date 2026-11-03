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
import org.apache.ratis.metadata.protocol.ChangeEvent;
import org.apache.ratis.metadata.protocol.RaftCommand;
import org.apache.ratis.metadata.protocol.RaftCommandCodec;
import org.apache.ratis.metadata.protocol.RaftResult;
import org.apache.ratis.metadata.util.ManualClock;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
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
import org.apache.ratis.util.SizeInBytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Lease-side invariants of {@link MetadataStateMachine} on a standalone, thread-free machine. */
public class TestMetadataStateMachine {

    private static final String GROUP_UUID = "8d1f4b7c-2a5e-4f8b-9c3d-7e6a1b0d5f27";
    private static final long TTL_MS = TimeUnit.SECONDS.toMillis(60);

    @TempDir File tempDir;

    private static final class AppliedReply {
        private final LogEntryProto entry;
        private final byte[] encoded;
        private final RaftResult result;

        AppliedReply(LogEntryProto entry, byte[] encoded) {
            this.entry = entry;
            this.encoded = encoded;
            this.result = RaftCommandCodec.decodeResult(encoded);
            assertNotNull(result, "applied reply decodes");
        }
    }

    private static final class StandaloneMachine implements AutoCloseable {

        private final File dir;
        private final ManualClock clock = new ManualClock(100_000L);
        private RaftServer server;
        private MetadataStateMachine stateMachine;
        private long index;

        StandaloneMachine(File dir) throws Exception {
            this.dir = dir;
            open();
        }

        private void open() throws Exception {
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

            stateMachine =
                    new MetadataStateMachine(
                            new File(dir, "meta"), clock, Duration.ofMillis(10_000L));
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
                            RaftStorage.StartupOption.RECOVER,
                            null);
            try {
                storage.initialize();
                stateMachine.initialize(server, group.getGroupId(), storage);
            } finally {
                storage.close();
            }
        }

        RaftResult apply(RaftCommand command) throws Exception {
            final long revision = ++index;
            final RaftResult result = applyEntry(1L, revision, revision, command).result;
            assertEquals(command.getOp(), result.getOp());
            return result;
        }

        AppliedReply applyEntry(long term, long revision, long callId, RaftCommand command)
                throws Exception {
            return applyContext(prepareEntry(term, revision, callId, command));
        }

        TransactionContext prepareEntry(
                long term, long revision, long callId, RaftCommand command) throws Exception {
            index = Math.max(index, revision);
            final RaftClientRequest request =
                    RaftClientRequest.newBuilder()
                            .setClientId(ClientId.valueOf(UUID.fromString(GROUP_UUID)))
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
            ((TransactionContextImpl) context).initLogEntry(term, revision);
            return context;
        }

        AppliedReply replay(AppliedReply original) throws Exception {
            return applyEntry(original.entry);
        }

        AppliedReply duplicateAtNewRevision(AppliedReply original, long term, long revision)
                throws Exception {
            index = Math.max(index, revision);
            return applyEntry(
                    LogEntryProto.newBuilder(original.entry)
                            .setTerm(term)
                            .setIndex(revision)
                            .build());
        }

        private AppliedReply applyEntry(LogEntryProto entry) throws Exception {
            return applyContext(
                    TransactionContext.newBuilder()
                            .setStateMachine(stateMachine)
                            .setLogEntry(entry)
                            .build());
        }

        private AppliedReply applyContext(TransactionContext context) throws Exception {
            final Message reply =
                    stateMachine.applyTransaction(context).get(10, TimeUnit.SECONDS);
            return new AppliedReply(
                    context.getLogEntry(), reply.getContent().toByteArray());
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

        void reopen() throws Exception {
            close();
            open();
        }

        @Override
        public void close() throws Exception {
            if (server != null) {
                server.close();
                server = null;
            }
            if (stateMachine != null) {
                stateMachine.close();
                stateMachine = null;
            }
        }
    }

    @Test
    void testSequentialCreateReplayAfterReopenReturnsOriginalEncodedResult() throws Exception {
        try (StandaloneMachine machine = new StandaloneMachine(tempDir)) {
            assertEquals(
                    RaftResult.Status.OK,
                    machine.apply(RaftCommand.put("/replay", bytes(""), 0L, 1L)).getStatus());
            final AppliedReply original =
                    machine.applyEntry(
                            1L,
                            2L,
                            2L,
                            RaftCommand.create("/replay/node-", bytes("v"), true, 0L, 1L));
            assertEquals("/replay/node-0000000001", original.result.getResultPath());

            machine.reopen();
            final AppliedReply replayed = machine.replay(original);

            assertArrayEquals(original.encoded, replayed.encoded);
            assertEquals(1, machine.query(RaftCommand.list("/replay/", 0)).getEntries().size());
        }
    }

    @Test
    void testDuplicateIncrementAtNewRevisionReturnsOriginalWithoutSecondEffect() throws Exception {
        try (StandaloneMachine machine = new StandaloneMachine(tempDir)) {
            final AppliedReply original =
                    machine.applyEntry(1L, 1L, 40L, RaftCommand.increment("/replay-counter", 5L));

            final AppliedReply duplicate = machine.duplicateAtNewRevision(original, 1L, 2L);

            assertArrayEquals(original.encoded, duplicate.encoded);
            assertEquals(
                    5L,
                    machine
                            .applyEntry(1L, 3L, 41L, RaftCommand.increment("/replay-counter", 0L))
                            .result
                            .getValue());
        }
    }

    @Test
    void testUnpairedSurrogateCounterNameNeverReachesTheApply() throws Exception {
        try (StandaloneMachine machine = new StandaloneMachine(tempDir)) {
            // an unpaired-surrogate counter name would arrive at the apply substituted with '?',
            // indistinguishable from a counter a second client literally named "/locks?x",
            // silently aliasing a fencing counter. The codec rejects the encode before any log
            // entry exists.
            assertThrows(
                    IllegalArgumentException.class,
                    () -> machine.applyEntry(1L, 1L, 50L, RaftCommand.increment("/locks\uD800x", 1L)));

            // the literal '?' name starts fresh at 1: the rejected increment never landed on it
            assertEquals(
                    1L,
                    machine.applyEntry(1L, 2L, 51L, RaftCommand.increment("/locks?x", 1L))
                            .result
                            .getValue());

            // no substituted name was ever committed: exactly one change, for the literal name
            final List<ChangeEvent> records =
                    machine.query(RaftCommand.pollChanges(0L, 1 << 20)).getRecords();
            assertEquals(1, records.size());
            assertEquals(1, records.get(0).getChanges().size());
            assertEquals(ChangeEvent.ChangeOp.INCREMENT, records.get(0).getChanges().get(0).getOp());
            assertEquals("/locks?x", records.get(0).getChanges().get(0).getPath());

            // a paired surrogate is one code point and encodes losslessly: an astral counter
            // name rides the wire unchanged (the no-over-rejection boundary of the same scan),
            // and the apply then rejects it with BAD_PATH - the path grammar every write op
            // shares - never by silent substitution; a rejected increment stages no change
            assertEquals(
                    RaftResult.Status.BAD_PATH,
                    machine.applyEntry(1L, 3L, 52L, RaftCommand.increment("/cnt\uD83D\uDE00x", 1L))
                            .result
                            .getStatus());
            assertEquals(1, machine.query(RaftCommand.pollChanges(0L, 1 << 20)).getRecords().size());
        }
    }

    @Test
    void testDuplicateSelectionPrecedesHigherEntryTermFence() throws Exception {
        try (StandaloneMachine machine = new StandaloneMachine(tempDir)) {
            final AppliedReply original =
                    machine.applyEntry(
                            1L,
                            1L,
                            42L,
                            RaftCommand.multi(
                                    Collections.singletonList(
                                            RaftCommand.put(
                                                    "/duplicate-before-fence", bytes("v"), 0L, 1L)),
                                    1L));
            assertEquals(RaftResult.Status.OK, original.result.getStatus());
            final int changesBefore =
                    machine.query(RaftCommand.pollChanges(0L, 1 << 20)).getRecords().size();

            final AppliedReply duplicate = machine.duplicateAtNewRevision(original, 2L, 2L);

            assertArrayEquals(original.encoded, duplicate.encoded);
            assertEquals(RaftResult.Status.OK, duplicate.result.getStatus());
            assertEquals(1L, machine.query(RaftCommand.get("/duplicate-before-fence")).getVersion());
            assertEquals(
                    changesBefore,
                    machine.query(RaftCommand.pollChanges(0L, 1 << 20)).getRecords().size(),
                    "the control-only duplicate emits no business change event");
        }
    }

    @Test
    void testLeaseRenewDuplicatePreservesFirstEmbeddedRevision() throws Exception {
        try (StandaloneMachine machine = new StandaloneMachine(tempDir)) {
            machine.applyEntry(1L, 1L, 45L, RaftCommand.leaseRegister(45L, 1_000L, 1L));
            // LEASE_RENEW applies monotone in expiry - a non-extending renewal is a no-op that
            // bumps no fence - and this fixture's clock is frozen, so the original renewal must
            // move past the register's normalized expiry to apply at all.
            machine.clock.advanceTime(1, TimeUnit.SECONDS);
            final AppliedReply original =
                    machine.applyEntry(1L, 2L, 46L, RaftCommand.leaseRenew(45L, 1_000L, 2L));
            assertEquals(2L, original.result.getRevision());

            final AppliedReply duplicate = machine.duplicateAtNewRevision(original, 1L, 3L);

            assertArrayEquals(original.encoded, duplicate.encoded);
            assertEquals(2L, duplicate.result.getRevision());
            assertEquals(
                    2L,
                    machine.query(RaftCommand.leaseList()).getLeases().get(0).getLastRenewIndex());
        }
    }

    @Test
    void testFailedMultiReplayAfterReopenReturnsOriginalFailureAndNoPrefix() throws Exception {
        try (StandaloneMachine machine = new StandaloneMachine(tempDir)) {
            final AppliedReply original =
                    machine.applyEntry(
                            1L,
                            1L,
                            50L,
                            RaftCommand.multi(
                                    Arrays.asList(
                                            RaftCommand.put(
                                                    "/failed-multi-prefix", bytes("v"), 0L, 1L),
                                            RaftCommand.delete("/failed-multi-missing", true))));
            assertEquals(RaftResult.Status.MULTI_FAILED, original.result.getStatus());

            machine.reopen();
            final AppliedReply replayed = machine.replay(original);

            assertArrayEquals(original.encoded, replayed.encoded);
            assertEquals(
                    RaftResult.Status.NOT_FOUND,
                    machine.query(RaftCommand.get("/failed-multi-prefix")).getStatus());
        }
    }

    @Test
    void testSuccessfulCommandWriteFailureHasNoVisiblePrefixAndFullRedo() throws Exception {
        try (StandaloneMachine machine = new StandaloneMachine(tempDir)) {
            final TransactionContext context =
                    machine.prepareEntry(
                            1L,
                            1L,
                            51L,
                            RaftCommand.put("/write-failure", bytes("v"), 0L, 1L));
            machine.stateMachine.beforeCommandWrite =
                    () -> {
                        throw new RocksDBException("injected before command db.write");
                    };

            assertThrows(
                    CompletionException.class,
                    () -> machine.applyContext(context));
            assertEquals(
                    RaftResult.Status.NOT_FOUND,
                    machine.query(RaftCommand.get("/write-failure")).getStatus());
            assertTrue(machine.query(RaftCommand.pollChanges(0L, 1 << 20)).getRecords().isEmpty());
            assertEquals(-1L, machine.query(RaftCommand.revision()).getRevision());

            machine.stateMachine.beforeCommandWrite = null;
            final AppliedReply redone = machine.applyContext(context);
            assertEquals(RaftResult.Status.OK, redone.result.getStatus());
            assertArrayEquals(
                    bytes("v"), machine.query(RaftCommand.get("/write-failure")).getData());
            assertEquals(1, machine.query(RaftCommand.pollChanges(0L, 1 << 20)).getRecords().size());

            machine.reopen();
            assertArrayEquals(redone.encoded, machine.replay(redone).encoded);
        }
    }

    @Test
    void testFailedMultiWriteFailureHasNoControlPrefixAndFullRedo() throws Exception {
        try (StandaloneMachine machine = new StandaloneMachine(tempDir)) {
            final TransactionContext context =
                    machine.prepareEntry(
                            1L,
                            1L,
                            52L,
                            RaftCommand.multi(
                                    Arrays.asList(
                                            RaftCommand.put(
                                                    "/failed-control-prefix", bytes("v"), 0L, 1L),
                                            RaftCommand.delete("/failed-control-missing", true))));
            machine.stateMachine.beforeCommandWrite =
                    () -> {
                        throw new RocksDBException("injected before control db.write");
                    };

            assertThrows(
                    CompletionException.class,
                    () -> machine.applyContext(context));
            assertEquals(
                    RaftResult.Status.NOT_FOUND,
                    machine.query(RaftCommand.get("/failed-control-prefix")).getStatus());
            assertTrue(machine.query(RaftCommand.pollChanges(0L, 1 << 20)).getRecords().isEmpty());
            assertEquals(-1L, machine.query(RaftCommand.revision()).getRevision());

            machine.stateMachine.beforeCommandWrite = null;
            final AppliedReply redone = machine.applyContext(context);
            assertEquals(RaftResult.Status.MULTI_FAILED, redone.result.getStatus());
            assertEquals(1, redone.result.getFailIndex());
            assertEquals(
                    RaftResult.Status.NOT_FOUND,
                    machine.query(RaftCommand.get("/failed-control-prefix")).getStatus());
            assertTrue(machine.query(RaftCommand.pollChanges(0L, 1 << 20)).getRecords().isEmpty());

            machine.reopen();
            assertArrayEquals(redone.encoded, machine.replay(redone).encoded);
        }
    }

    @Test
    void testStaleTermReplayAfterReopenReturnsOriginalFailure() throws Exception {
        try (StandaloneMachine machine = new StandaloneMachine(tempDir)) {
            final AppliedReply original =
                    machine.applyEntry(
                            2L,
                            1L,
                            60L,
                            RaftCommand.multi(
                                    Collections.singletonList(
                                            RaftCommand.put("/stale-replay", bytes("v"), 0L, 1L)),
                                    1L));
            assertEquals(RaftResult.Status.STALE_TERM, original.result.getStatus());

            machine.reopen();
            final AppliedReply replayed = machine.replay(original);

            assertArrayEquals(original.encoded, replayed.encoded);
            assertEquals(
                    RaftResult.Status.NOT_FOUND,
                    machine.query(RaftCommand.get("/stale-replay")).getStatus());
        }
    }

    @Test
    void testSameInvocationDifferentDigestDoesNotExecute() throws Exception {
        try (StandaloneMachine machine = new StandaloneMachine(tempDir)) {
            final AppliedReply original =
                    machine.applyEntry(
                            1L,
                            1L,
                            70L,
                            RaftCommand.put("/invocation-mismatch", bytes("first"), 0L, 1L));

            final AppliedReply mismatch =
                    machine.applyEntry(
                            1L,
                            2L,
                            70L,
                            RaftCommand.put("/invocation-mismatch", bytes("second"), 0L, 2L));

            assertEquals(RaftResult.Status.INVOCATION_MISMATCH, mismatch.result.getStatus());
            assertEquals(RaftCommand.Op.PUT, mismatch.result.getOp());
            assertEquals(1L, machine.query(RaftCommand.get("/invocation-mismatch")).getVersion());
            assertArrayEquals(
                    bytes("first"), machine.query(RaftCommand.get("/invocation-mismatch")).getData());
            assertEquals(1L, original.result.getVersion());
        }
    }

    @Test
    void testBusinessRejectionsReplayAfterReopenReturnOriginalFailures() throws Exception {
        try (StandaloneMachine machine = new StandaloneMachine(tempDir)) {
            machine.applyEntry(
                    1L, 1L, 80L, RaftCommand.put("/rejected-replay", bytes("v"), 0L, 1L));
            final AppliedReply nodeExists =
                    machine.applyEntry(
                            1L,
                            2L,
                            81L,
                            RaftCommand.create("/rejected-replay", bytes("x"), false, 0L, 1L));
            final AppliedReply versionMismatch =
                    machine.applyEntry(
                            1L,
                            3L,
                            82L,
                            RaftCommand.cas("/rejected-replay", bytes("x"), 99L, 0L, 1L));
            assertEquals(RaftResult.Status.NODE_EXISTS, nodeExists.result.getStatus());
            assertEquals(RaftResult.Status.VERSION_MISMATCH, versionMismatch.result.getStatus());

            machine.reopen();
            final AppliedReply replayedNodeExists = machine.replay(nodeExists);
            final AppliedReply replayedVersionMismatch = machine.replay(versionMismatch);

            assertAll(
                    () -> assertArrayEquals(nodeExists.encoded, replayedNodeExists.encoded),
                    () ->
                            assertArrayEquals(
                                    versionMismatch.encoded, replayedVersionMismatch.encoded));
            assertEquals(1L, machine.query(RaftCommand.get("/rejected-replay")).getVersion());
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
    void testGuardedIncrementIsRejectedWhenTheLeaseNoLongerOwnsThePath() throws Exception {
        try (StandaloneMachine machine = new StandaloneMachine(tempDir)) {
            final long leaseA = 71L;
            final long leaseB = 72L;
            final long leaseC = 73L; // live, owns nothing at the path
            final long now = 1_000L;
            assertEquals(
                    RaftResult.Status.OK,
                    machine.apply(RaftCommand.put("/locks", bytes(""), 0L, now)).getStatus());
            for (long lease : Arrays.asList(leaseA, leaseB, leaseC)) {
                assertEquals(
                        RaftResult.Status.OK,
                        machine.apply(RaftCommand.leaseRegister(lease, TTL_MS, now + TTL_MS))
                                .getStatus());
            }
            assertEquals(
                    RaftResult.Status.OK,
                    machine.apply(
                                    RaftCommand.create("/locks/i", bytes("a"), false, leaseA, now))
                            .getStatus());

            // the normal acquire path: the owner's guarded bump takes the next token
            assertEquals(
                    1L, machine.apply(RaftCommand.increment("/locks/i", 1L, leaseA)).getValue());

            // A stalls past its ttl: the expiry cascade frees lease and record in one revision,
            // B re-creates the path and takes a token of its own
            assertTrue(machine.apply(RaftCommand.leaseExpire(leaseA, -1L)).isExpired());
            assertEquals(
                    RaftResult.Status.NOT_FOUND,
                    machine.query(RaftCommand.get("/locks/i")).getStatus());
            assertEquals(
                    RaftResult.Status.OK,
                    machine.apply(
                                    RaftCommand.create("/locks/i", bytes("b"), false, leaseB, now))
                            .getStatus());
            assertEquals(
                    2L, machine.apply(RaftCommand.increment("/locks/i", 1L, leaseB)).getValue());

            // A's late bump must die on its dead lease, never as a token above B's
            assertEquals(
                    RaftResult.Status.UNKNOWN_LEASE,
                    machine.apply(RaftCommand.increment("/locks/i", 1L, leaseA)).getStatus(),
                    "the dead contender's bump is rejected");
            assertEquals(
                    2L,
                    machine.apply(RaftCommand.increment("/locks/i", 0L, leaseB)).getValue(),
                    "the rejected bump moved no counter");
            // a live lease that does not own the record is stale all the same
            assertEquals(
                    RaftResult.Status.STALE_LEASE,
                    machine.apply(RaftCommand.increment("/locks/i", 1L, leaseC)).getStatus(),
                    "a non-owner's bump is rejected");
            assertEquals(
                    2L,
                    machine.apply(RaftCommand.increment("/locks/i", 0L)).getValue(),
                    "no rejection moved the counter");

            // plain counter arithmetic keeps its unconditional semantics
            assertEquals(
                    3L, machine.apply(RaftCommand.increment("/locks/i", 1L)).getValue());
        }
    }

    @Test
    void testGuardedIncrementSeesARecordStagedInTheSameMultiBatch() throws Exception {
        try (StandaloneMachine machine = new StandaloneMachine(tempDir)) {
            final long leaseId = 81L;
            final long strangerLeaseId = 82L; // registered, owns nothing at the path
            final long now = 1_000L;
            final RaftResult batch =
                    machine.apply(
                            RaftCommand.multi(
                                    Arrays.asList(
                                            RaftCommand.put("/locks", bytes(""), 0L, now),
                                            RaftCommand.leaseRegister(leaseId, TTL_MS, now + TTL_MS),
                                            RaftCommand.create(
                                                    "/locks/m", bytes("m"), false, leaseId, now),
                                            RaftCommand.increment("/locks/m", 1L, leaseId))));
            assertEquals(RaftResult.Status.OK, batch.getStatus());
            assertEquals(
                    1L,
                    batch.getSubResults().get(3).getValue(),
                    "the guard read the record staged earlier in the same batch");

            // a live lease that owns nothing at the path fails its batch with the guard's status
            assertEquals(
                    RaftResult.Status.OK,
                    machine.apply(
                                    RaftCommand.leaseRegister(
                                            strangerLeaseId, TTL_MS, now + TTL_MS))
                            .getStatus());
            final RaftResult stranger =
                    machine.apply(
                            RaftCommand.multi(
                                    Collections.singletonList(
                                            RaftCommand.increment(
                                                    "/locks/m", 1L, strangerLeaseId))));
            assertEquals(RaftResult.Status.MULTI_FAILED, stranger.getStatus());
            assertEquals(0, stranger.getFailIndex());
            assertEquals(RaftResult.Status.STALE_LEASE, stranger.getFailStatus());
            assertEquals(
                    1L,
                    machine.apply(RaftCommand.increment("/locks/m", 0L)).getValue(),
                    "the aborted batch moved no counter");
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
                    machine.apply(RaftCommand.leaseRenew(leaseId, TTL_MS, now + 2 * TTL_MS))
                            .getStatus());
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
            // a lone surrogate cannot ride the UTF-8 wire: the codec rejects it at encode instead
            // of substituting '?' (itself a legal path character), so a surrogate path can never
            // alias a literal one
            assertThrows(
                    IllegalArgumentException.class,
                    () -> machine.apply(RaftCommand.put("/a\uD800b", bytes("v"), 0L, now)));
            assertEquals(
                    RaftResult.Status.OK,
                    machine.apply(RaftCommand.put("/a?b", bytes("v"), 0L, now)).getStatus(),
                    "a literal '?' is a legal path character");
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

    @Test
    void testPutAndCasOnTheRootAreNotAllowed() throws Exception {
        try (StandaloneMachine machine = new StandaloneMachine(tempDir)) {
            final long now = 1_000L;
            assertEquals(
                    RaftResult.Status.NOT_ALLOWED,
                    machine.apply(RaftCommand.put("/", bytes("v"), 0L, now)).getStatus(),
                    "PUT on the root is rejected like CREATE and DELETE");
            assertEquals(
                    RaftResult.Status.NOT_ALLOWED,
                    machine.apply(RaftCommand.cas("/", bytes("v"), 0L, 0L, now)).getStatus(),
                    "CAS on the root is rejected the same way");
            assertEquals(
                    RaftResult.Status.NOT_FOUND,
                    machine.query(RaftCommand.get("/")).getStatus(),
                    "no rejected write left a record at the root");
            assertTrue(
                    machine.query(RaftCommand.pollChanges(0L, 1 << 20)).getRecords().isEmpty(),
                    "the rejected root writes staged no change record");

            // a failing MULTI child aborts the batch: the root rejection has no bypass
            final RaftResult batch =
                    machine.apply(
                            RaftCommand.multi(
                                    Arrays.asList(
                                            RaftCommand.put("/root-multi", bytes("v"), 0L, now),
                                            RaftCommand.put("/", bytes("v"), 0L, now))));
            assertEquals(RaftResult.Status.MULTI_FAILED, batch.getStatus());
            assertEquals(1, batch.getFailIndex());
            assertEquals(RaftResult.Status.NOT_ALLOWED, batch.getFailStatus());
            assertEquals(
                    RaftResult.Status.NOT_FOUND,
                    machine.query(RaftCommand.get("/root-multi")).getStatus(),
                    "the batch's earlier child was discarded with it");
        }
    }

    @Test
    void testSequentialCreateValidatesTheFinalCandidateBeforeAnyEffect() throws Exception {
        try (StandaloneMachine machine = new StandaloneMachine(tempDir)) {
            final long now = 1_000L;
            // an 8183-char base: every candidate base + %010d is 8193 chars, past the 8192
            // bound, so the bounded loop finds no legal candidate and fails terminally
            final StringBuilder over = new StringBuilder("/seq-over");
            while (over.length() < 8_183) {
                over.append('x');
            }
            assertEquals(
                    RaftResult.Status.NODE_EXISTS,
                    machine.apply(RaftCommand.create(over.toString(), bytes("v"), true, 0L, now))
                            .getStatus(),
                    "no legal final candidate exists: the loop's terminal outcome stands");
            assertTrue(
                    machine.query(RaftCommand.pollChanges(0L, 1 << 20)).getRecords().isEmpty(),
                    "no counter write, record or change event was staged for an invalid"
                            + " candidate");

            // the boundary: an 8182-char base yields exactly the 8192-char maximum
            final StringBuilder edge = new StringBuilder("/seq-edge");
            while (edge.length() < 8_182) {
                edge.append('y');
            }
            final RaftResult atEdge =
                    machine.apply(RaftCommand.create(edge.toString(), bytes("v"), true, 0L, now));
            assertEquals(RaftResult.Status.OK, atEdge.getStatus());
            assertEquals(8_192, atEdge.getResultPath().length());
            assertEquals(
                    RaftResult.Status.OK,
                    machine.query(RaftCommand.get(atEdge.getResultPath())).getStatus());
        }
    }

    @Test
    void testLeaseBindingRejectsLiveDescendantsCommittedAndStaged() throws Exception {
        try (StandaloneMachine machine = new StandaloneMachine(tempDir)) {
            final long leaseId = 61L;
            final long now = 1_000L;
            assertEquals(
                    RaftResult.Status.OK,
                    machine.apply(RaftCommand.leaseRegister(leaseId, TTL_MS, now + TTL_MS))
                            .getStatus());
            assertEquals(
                    RaftResult.Status.OK,
                    machine.apply(RaftCommand.put("/lb", bytes(""), 0L, now)).getStatus());
            assertEquals(
                    RaftResult.Status.OK,
                    machine.apply(RaftCommand.put("/lb/child", bytes("c"), 0L, now)).getStatus());

            // committed descendant: binding would make an ephemeral record carry a child
            assertEquals(
                    RaftResult.Status.NOT_ALLOWED,
                    machine.apply(RaftCommand.put("/lb", bytes("v"), leaseId, now)).getStatus(),
                    "a lease cannot bind to a path with a live committed descendant");
            assertEquals(
                    RaftResult.Status.NOT_ALLOWED,
                    machine.apply(RaftCommand.cas("/lb", bytes("v"), 1L, leaseId, now))
                            .getStatus(),
                    "CAS binds a lease the same way");
            assertEquals(0L, machine.query(RaftCommand.get("/lb")).getLeaseId());

            // staged descendant: the check must see the MULTI overlay, not just committed state
            final RaftResult staged =
                    machine.apply(
                            RaftCommand.multi(
                                    Arrays.asList(
                                            RaftCommand.put("/lb/kid", bytes("k"), 0L, now),
                                            RaftCommand.put("/lb", bytes("v"), leaseId, now))));
            assertEquals(RaftResult.Status.MULTI_FAILED, staged.getStatus());
            assertEquals(1, staged.getFailIndex());
            assertEquals(RaftResult.Status.NOT_ALLOWED, staged.getFailStatus());
            assertEquals(
                    RaftResult.Status.NOT_FOUND,
                    machine.query(RaftCommand.get("/lb/kid")).getStatus(),
                    "the aborted batch left no staged child either");

            // permissive direction: the same batch may delete the descendant, then bind
            final RaftResult rebound =
                    machine.apply(
                            RaftCommand.multi(
                                    Arrays.asList(
                                            RaftCommand.delete("/lb/child", false),
                                            RaftCommand.put("/lb", bytes("v"), leaseId, now))));
            assertEquals(RaftResult.Status.OK, rebound.getStatus());
            assertEquals(leaseId, machine.query(RaftCommand.get("/lb")).getLeaseId());
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
