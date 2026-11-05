/*
 * Licensed to the Apache Software Foundation (ASF) under one
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
import org.apache.ratis.metadata.util.Clock;
import org.apache.ratis.metadata.util.ManualClock;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
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
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.SizeInBytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Versioning, leader normalization and replicated-time application of metadata log envelopes. */
public class TestMetadataLogEnvelope {

    private static final String GROUP_UUID = "8d1f4b7c-2a5e-4f8b-9c3d-7e6a1b0d5f27";
    private static final int ENVELOPE_HEADER_LENGTH = 53;
    private static final int COMMAND_LENGTH_OFFSET = 49;
    private static final long RETENTION_MILLIS = Duration.ofDays(7).toMillis();

    @TempDir File tempDir;

    private static final class CountingClock implements Clock {
        private long now;
        private int samples;

        CountingClock(long now) {
            this.now = now;
        }

        @Override
        public long milliseconds() {
            samples++;
            return now += 1_000L;
        }

        @Override
        public long nanoseconds() {
            return TimeUnit.MILLISECONDS.toNanos(now);
        }
    }

    @Test
    void testRawIdenticalRenewalsKeepTheirDigestAcrossLeaderTimes() throws Exception {
        final byte[] raw = RaftCommandCodec.encodeCommand(RaftCommand.leaseRenew(42L, 300L, 7L));
        final byte[] original = raw.clone();

        final MetadataLogEnvelope first =
                MetadataLogEnvelope.create(raw, new ManualClock(1_000L), RETENTION_MILLIS);
        final MetadataLogEnvelope second =
                MetadataLogEnvelope.create(raw, new ManualClock(9_000L), RETENTION_MILLIS);

        assertArrayEquals(original, raw, "normalization must not modify the client's bytes");
        assertArrayEquals(MessageDigest.getInstance("SHA-256").digest(original), first.getRawCommandDigest());
        assertArrayEquals(first.getRawCommandDigest(), second.getRawCommandDigest());
        assertEquals(1_300L, first.getCommand().getExpireAtMs());
        assertEquals(9_300L, second.getCommand().getExpireAtMs());
        assertNotEquals(
                first.getCommand().getExpireAtMs(),
                second.getCommand().getExpireAtMs(),
                "the digest follows the raw invocation, not the normalized expiry");
    }

    @Test
    void testMultiLeaseChildrenShareOneLeaderClockSample() {
        final CountingClock clock = new CountingClock(4_000L);
        final RaftCommand multi =
                RaftCommand.multi(
                        Arrays.asList(
                                RaftCommand.leaseRegister(10L, 100L, 1L),
                                RaftCommand.leaseRenew(10L, 250L, 2L),
                                RaftCommand.put("/m", bytes("v"), 10L, 77L)),
                        9L);

        final MetadataLogEnvelope envelope =
                MetadataLogEnvelope.create(
                        RaftCommandCodec.encodeCommand(multi), clock, RETENTION_MILLIS);

        assertEquals(1, clock.samples, "the top-level envelope owns the only time sample");
        assertEquals(5_000L, envelope.getServerTimeMillis());
        assertEquals(5_100L, envelope.getCommand().getOps().get(0).getExpireAtMs());
        assertEquals(5_250L, envelope.getCommand().getOps().get(1).getExpireAtMs());
        assertEquals(100L, envelope.getCommand().getOps().get(0).getTtlMs());
        assertEquals(250L, envelope.getCommand().getOps().get(1).getTtlMs());
        assertEquals(9L, envelope.getCommand().getWriterTerm());

        final RaftCommand decoded = MetadataLogEnvelope.decode(envelope.encode()).getCommand();
        assertEquals(RaftCommand.Op.MULTI, decoded.getOp());
        assertEquals(3, decoded.getOps().size());
        assertEquals("/m", decoded.getOps().get(2).getPath());
        assertArrayEquals(bytes("v"), decoded.getOps().get(2).getData());
        assertEquals(10L, decoded.getOps().get(2).getLeaseId());
        assertEquals(77L, decoded.getOps().get(2).getTimestamp());
    }

    @Test
    void testEnvelopeRoundTripAndStrictFraming() {
        final MetadataLogEnvelope envelope =
                MetadataLogEnvelope.create(
                        RaftCommandCodec.encodeCommand(RaftCommand.put("/p", bytes("v"), 0L, 8L)),
                        new ManualClock(1_000L),
                        400L);
        final byte[] encoded = envelope.encode();

        assertEquals(1, encoded[0] & 0xFF, "the envelope has its own version, independent of commands");
        assertEquals(
                RaftCommandCodec.CURRENT_FORMAT_VERSION,
                encoded[ENVELOPE_HEADER_LENGTH] & 0xFF);
        assertEquals(
                encoded.length - ENVELOPE_HEADER_LENGTH,
                ByteBuffer.wrap(encoded).getInt(COMMAND_LENGTH_OFFSET));

        final MetadataLogEnvelope decoded = MetadataLogEnvelope.decode(encoded);
        assertNotNull(decoded);
        assertEquals(1_000L, decoded.getServerTimeMillis());
        assertEquals(600L, decoded.getProposedCutoffMillis());
        assertArrayEquals(envelope.getRawCommandDigest(), decoded.getRawCommandDigest());
        assertEquals("/p", decoded.getCommand().getPath());
        assertArrayEquals(bytes("v"), decoded.getCommand().getData());
        assertArrayEquals(encoded, decoded.encode());

        assertNull(MetadataLogEnvelope.decode(Arrays.copyOf(encoded, encoded.length - 1)));
        assertNull(MetadataLogEnvelope.decode(Arrays.copyOf(encoded, encoded.length + 1)));

        final byte[] unsupported = encoded.clone();
        unsupported[0] = 2;
        assertNull(MetadataLogEnvelope.decode(unsupported));

        final byte[] negativeLength = encoded.clone();
        ByteBuffer.wrap(negativeLength).putInt(COMMAND_LENGTH_OFFSET, -1);
        assertNull(MetadataLogEnvelope.decode(negativeLength));
    }

    @Test
    void testEnvelopeRejectsNullMalformedNonWriteAndOldPayloads() {
        assertNull(MetadataLogEnvelope.decode(null));
        assertThrows(
                IllegalArgumentException.class,
                () -> MetadataLogEnvelope.create(null, new ManualClock(0L), RETENTION_MILLIS));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        MetadataLogEnvelope.create(
                                new byte[] {99, 1, 2}, new ManualClock(0L), RETENTION_MILLIS));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        MetadataLogEnvelope.create(
                                RaftCommandCodec.encodeCommand(RaftCommand.get("/query")),
                                new ManualClock(0L),
                                RETENTION_MILLIS));

        final byte[] oldBareLogPayload =
                RaftCommandCodec.encodeCommand(RaftCommand.put("/old", bytes("v"), 0L, 1L));
        assertNull(MetadataLogEnvelope.decode(oldBareLogPayload));
    }

    @Test
    void testCreateRejectsInvalidTtlLegacyRenewAndExpiryOverflow() {
        for (RaftCommand invalid :
                Arrays.asList(
                        RaftCommand.leaseRegister(1L, 0L, 0L),
                        RaftCommand.leaseRegister(1L, -1L, 0L),
                        RaftCommand.leaseRenew(1L, 0L, 0L),
                        RaftCommand.leaseRenew(1L, -1L, 0L),
                        RaftCommand.leaseRenew(1L, 0L, 123L))) {
            assertThrows(
                    IllegalArgumentException.class,
                    () ->
                            MetadataLogEnvelope.create(
                                    RaftCommandCodec.encodeCommand(invalid),
                                    new ManualClock(1_000L),
                                    RETENTION_MILLIS),
                    "invalid lease command " + invalid);
        }

        for (RaftCommand overflow :
                Arrays.asList(
                        RaftCommand.leaseRegister(1L, Long.MAX_VALUE, 0L),
                        RaftCommand.leaseRenew(1L, Long.MAX_VALUE, 0L))) {
            assertThrows(
                    IllegalArgumentException.class,
                    () ->
                            MetadataLogEnvelope.create(
                                    RaftCommandCodec.encodeCommand(overflow),
                                    new ManualClock(1L),
                                    RETENTION_MILLIS));
        }
    }

    @Test
    void testRetentionIsPositiveAndCutoffSaturates() {
        final byte[] raw = RaftCommandCodec.encodeCommand(RaftCommand.delete("/p", false));
        assertThrows(
                IllegalArgumentException.class,
                () -> MetadataLogEnvelope.create(raw, new ManualClock(0L), 0L));
        assertThrows(
                IllegalArgumentException.class,
                () -> MetadataLogEnvelope.create(raw, new ManualClock(0L), -1L));

        // ManualClock stores nanoseconds, so its millisecond constructor would overflow here.
        final Clock nearMinimum =
                new Clock() {
                    @Override
                    public long milliseconds() {
                        return Long.MIN_VALUE + 5L;
                    }

                    @Override
                    public long nanoseconds() {
                        return 0L;
                    }
                };
        final MetadataLogEnvelope envelope = MetadataLogEnvelope.create(raw, nearMinimum, 10L);
        assertEquals(Long.MIN_VALUE, envelope.getProposedCutoffMillis());
    }

    @Test
    void testLeaderContextWrapsEnvelopeWithoutChangingInvocationIdentity() throws Exception {
        final CountingClock clock = new CountingClock(10_000L);
        final MetadataStateMachine stateMachine =
                new MetadataStateMachine(new File(tempDir, "leader"), clock, Duration.ofMillis(500L));
        final ClientId clientId = ClientId.valueOf(UUID.fromString(GROUP_UUID));
        final byte[] raw = RaftCommandCodec.encodeCommand(RaftCommand.leaseRenew(8L, 200L, 1L));
        final RaftClientRequest request = writeRequest(clientId, 73L, raw);

        final TransactionContext context = stateMachine.startTransaction(request);
        ((TransactionContextImpl) context).initLogEntry(2L, 19L);

        assertSame(request, context.getClientRequest());
        assertEquals(clientId, context.getClientRequest().getClientId());
        assertEquals(73L, context.getClientRequest().getCallId());
        final byte[] logData =
                context.getLogEntry().getStateMachineLogEntry().getLogData().toByteArray();
        final MetadataLogEnvelope envelope = MetadataLogEnvelope.decode(logData);
        assertNotNull(envelope, "the actual transaction logData is one envelope");
        assertNull(RaftCommandCodec.decodeCommand(logData), "the envelope is not a bare command");
        assertEquals(11_000L, envelope.getServerTimeMillis());
        assertEquals(10_500L, envelope.getProposedCutoffMillis());
        assertEquals(11_200L, envelope.getCommand().getExpireAtMs());
        assertEquals(1, clock.samples);
    }

    @Test
    void testLeaderRejectsMalformedAndLegacyRawCommandsBeforeAppend() throws Exception {
        final MetadataStateMachine stateMachine =
                new MetadataStateMachine(
                        new File(tempDir, "reject"), new ManualClock(1_000L), Duration.ofDays(7));
        final ClientId clientId = ClientId.valueOf(UUID.fromString(GROUP_UUID));

        for (RaftClientRequest request :
                Arrays.asList(
                        writeRequest(clientId, 1L, new byte[] {1, 2, 3}),
                        writeRequest(
                                clientId,
                                2L,
                                RaftCommandCodec.encodeCommand(RaftCommand.leaseRenew(8L, 0L, 9_000L))))) {
            final TransactionContext context = stateMachine.startTransaction(request);
            org.junit.jupiter.api.Assertions.assertTrue(
                    context.getException() instanceof IllegalArgumentException,
                    "expected client input failure, got " + context.getException());
            assertSame(request, context.getClientRequest());
            assertNull(context.getLogEntry(), "an invalid command has no log entry");
        }
    }

    @Test
    void testApplyStoresRenewTtlAndUsesOnlyReplicatedLeaderTime() throws Exception {
        final CountingClock followerClock = new CountingClock(899_000L);
        try (StandaloneMachine machine = new StandaloneMachine(tempDir, followerClock)) {
            final MetadataLogEnvelope register =
                    MetadataLogEnvelope.create(
                            RaftCommandCodec.encodeCommand(RaftCommand.leaseRegister(5L, 1_000L, 1L)),
                            new ManualClock(1_000L),
                            RETENTION_MILLIS);
            assertEquals(RaftResult.Status.OK, machine.apply(register).getStatus());
            assertLease(machine, 5L, 1_000L, 2_000L);

            final MetadataLogEnvelope renew =
                    MetadataLogEnvelope.create(
                            RaftCommandCodec.encodeCommand(RaftCommand.leaseRenew(5L, 2_000L, 2L)),
                            new ManualClock(5_000L),
                            RETENTION_MILLIS);
            assertEquals(RaftResult.Status.OK, machine.apply(renew).getStatus());
            assertLease(machine, 5L, 2_000L, 7_000L);
            assertEquals(0, followerClock.samples, "apply must not read the replica's local clock");
        }
    }

    @Test
    void testApplyRejectsBareCommandsAndInvalidLeaseArithmetic() throws Exception {
        try (StandaloneMachine machine =
                new StandaloneMachine(tempDir, new ManualClock(900_000L))) {
            final RaftResult bare =
                    machine.applyLogData(
                            RaftCommandCodec.encodeCommand(RaftCommand.put("/bare", bytes("v"), 0L, 1L)));
            assertEquals(RaftResult.Status.MALFORMED, bare.getStatus());
            final LogEntryProto malformedEntry = machine.newEntry(new byte[] {1, 2, 3});
            final byte[] malformed = machine.applyEntry(malformedEntry);
            final byte[] malformedReplay = machine.applyEntry(malformedEntry);
            assertEquals(
                    RaftResult.Status.MALFORMED,
                    RaftCommandCodec.decodeResult(malformed).getStatus());
            assertArrayEquals(
                    malformed,
                    malformedReplay,
                    "an invalid envelope follows the same no-op branch after its marker advances");
            assertEquals(
                    RaftResult.Status.NOT_FOUND,
                    machine.query(RaftCommand.get("/bare")).getStatus());

            assertEquals(
                    RaftResult.Status.OK,
                    machine.applyLogData(
                                    encodeEnvelope(RaftCommand.leaseRegister(6L, 1_000L, 2_000L), 1_000L))
                            .getStatus());
            assertLease(machine, 6L, 1_000L, 2_000L);

            final LogEntryProto invalidLeaseEntry =
                    machine.newEntry(
                            encodeEnvelope(RaftCommand.leaseRenew(6L, 2_000L, 9_999L), 1_000L));
            final byte[] invalidLeaseReply = machine.applyEntry(invalidLeaseEntry);
            assertEquals(
                    RaftResult.Status.MALFORMED,
                    RaftCommandCodec.decodeResult(invalidLeaseReply).getStatus());
            assertArrayEquals(
                    invalidLeaseReply,
                    machine.applyEntry(invalidLeaseEntry),
                    "a decoded command's first MALFORMED result is durable and replayable");
            assertEquals(
                    RaftResult.Status.MALFORMED,
                    machine.applyLogData(
                                    encodeEnvelope(RaftCommand.leaseRenew(6L, 0L, 9_999L), 1_000L))
                            .getStatus());
            assertEquals(
                    RaftResult.Status.MALFORMED,
                    machine.applyLogData(
                                    encodeEnvelope(
                                            RaftCommand.leaseRegister(7L, 1L, Long.MIN_VALUE),
                                            Long.MAX_VALUE))
                            .getStatus());
            assertLease(machine, 6L, 1_000L, 2_000L);
        }
    }

    @Test
    void testMissingClientIdUsesMalformedNoopForInvalidAndValidEnvelopes() throws Exception {
        try (StandaloneMachine machine = new StandaloneMachine(tempDir, new ManualClock(0L))) {
            org.junit.jupiter.api.Assertions.assertAll(
                    () -> assertInvalidClientIdNoop(machine, null, false),
                    () -> assertInvalidClientIdNoop(machine, null, true));
        }
    }

    @Test
    void testNon16ByteClientIdUsesMalformedNoopForInvalidAndValidEnvelopes() throws Exception {
        try (StandaloneMachine machine = new StandaloneMachine(tempDir, new ManualClock(0L))) {
            final ByteString invalidClientId = ByteString.copyFrom(new byte[] {1, 2, 3});
            org.junit.jupiter.api.Assertions.assertAll(
                    () -> assertInvalidClientIdNoop(machine, invalidClientId, false),
                    () -> assertInvalidClientIdNoop(machine, invalidClientId, true));
        }
    }

    @Test
    void testOpenedMachineCannotBeReconfigured() throws Exception {
        try (StandaloneMachine machine = new StandaloneMachine(tempDir, new ManualClock(0L))) {
            assertThrows(
                    IllegalStateException.class,
                    () ->
                            machine.stateMachine.configureBeforeOpen(
                                    new ManualClock(1L), Duration.ofMillis(1L)));
        }
    }

    @Test
    void testFailedConfigurationIsAtomicAndCanBeRetried() throws Exception {
        final MetadataStateMachine stateMachine =
                new MetadataStateMachine(
                        new File(tempDir, "configuration"),
                        new ManualClock(1_000L),
                        Duration.ofMillis(500L));
        final ClientId clientId = ClientId.valueOf(UUID.fromString(GROUP_UUID));

        assertThrows(
                NullPointerException.class,
                () -> stateMachine.configureBeforeOpen(new ManualClock(2_000L), null));
        assertEnvelopeConfiguration(stateMachine, writeRequest(clientId, 1L, validRenew()), 1_000L, 500L);

        assertThrows(
                IllegalArgumentException.class,
                () -> stateMachine.configureBeforeOpen(new ManualClock(3_000L), Duration.ZERO));
        assertEnvelopeConfiguration(stateMachine, writeRequest(clientId, 2L, validRenew()), 1_000L, 500L);

        assertThrows(
                ArithmeticException.class,
                () ->
                        stateMachine.configureBeforeOpen(
                                new ManualClock(4_000L), Duration.ofSeconds(Long.MAX_VALUE)));
        assertEnvelopeConfiguration(stateMachine, writeRequest(clientId, 3L, validRenew()), 1_000L, 500L);

        stateMachine.configureBeforeOpen(new ManualClock(5_000L), Duration.ofMillis(250L));
        assertEnvelopeConfiguration(stateMachine, writeRequest(clientId, 4L, validRenew()), 5_000L, 250L);
    }

    private static void assertInvalidClientIdNoop(
            StandaloneMachine machine, ByteString clientId, boolean validEnvelope) throws Exception {
        final byte[] payload =
                validEnvelope
                        ? MetadataLogEnvelope.create(
                                        RaftCommandCodec.encodeCommand(
                                                RaftCommand.put("/invalid-client", bytes("v"), 0L, 1L)),
                                        new ManualClock(1_000L),
                                        RETENTION_MILLIS)
                                .encode()
                        : new byte[] {1, 2, 3};
        final LogEntryProto entry = machine.newEntry(payload, clientId);
        final long revision = entry.getIndex();
        final byte[] expected =
                RaftCommandCodec.encodeResult(
                        RaftResult.error(RaftCommand.Op.UNKNOWN, RaftResult.Status.MALFORMED));

        assertArrayEquals(expected, machine.applyEntry(entry), "first apply must be a deterministic no-op");
        assertEquals(revision, machine.query(RaftCommand.revision()).getRevision());
        assertArrayEquals(expected, machine.applyEntry(entry), "same-entry replay must return identical bytes");
        assertEquals(
                revision,
                machine.query(RaftCommand.revision()).getRevision(),
                "replaying the gap entry must not advance the marker again");
        assertEquals(
                RaftResult.Status.NOT_FOUND,
                machine.query(RaftCommand.get("/invalid-client")).getStatus());
        org.junit.jupiter.api.Assertions.assertTrue(
                machine.query(RaftCommand.pollChanges(0L, 1 << 20)).getRecords().isEmpty(),
                "an invalid invocation must not emit business changes");
    }

    private static byte[] validRenew() {
        return RaftCommandCodec.encodeCommand(RaftCommand.leaseRenew(8L, 100L, 1L));
    }

    private static void assertEnvelopeConfiguration(
            MetadataStateMachine stateMachine,
            RaftClientRequest request,
            long expectedServerTime,
            long expectedRetention)
            throws Exception {
        final TransactionContext context = stateMachine.startTransaction(request);
        assertNull(context.getException());
        ((TransactionContextImpl) context).initLogEntry(1L, 1L);
        final MetadataLogEnvelope envelope =
                MetadataLogEnvelope.decode(
                        context.getLogEntry().getStateMachineLogEntry().getLogData().toByteArray());
        assertNotNull(envelope);
        assertEquals(expectedServerTime, envelope.getServerTimeMillis());
        assertEquals(expectedServerTime - expectedRetention, envelope.getProposedCutoffMillis());
    }

    private static void assertLease(
            StandaloneMachine machine, long leaseId, long ttlMs, long expireAtMs) {
        for (RaftResult.Lease lease : machine.query(RaftCommand.leaseList()).getLeases()) {
            if (lease.getLeaseId() == leaseId) {
                assertEquals(ttlMs, lease.getTtlMs());
                assertEquals(expireAtMs, lease.getExpireAtMs());
                return;
            }
        }
        throw new AssertionError("Lease " + leaseId + " not found");
    }

    private static RaftClientRequest writeRequest(ClientId clientId, long callId, byte[] raw) {
        return RaftClientRequest.newBuilder()
                .setClientId(clientId)
                .setServerId(RaftPeerId.valueOf("m0"))
                .setGroupId(RaftGroupId.valueOf(UUID.fromString(GROUP_UUID)))
                .setCallId(callId)
                .setMessage(Message.valueOf(ByteBuffer.wrap(raw)))
                .setType(RaftClientRequest.writeRequestType())
                .build();
    }

    /** Test-only envelope encoder for hostile replicated entries that create() would normalize away. */
    private static byte[] encodeEnvelope(RaftCommand command, long serverTimeMillis) {
        final byte[] encodedCommand = RaftCommandCodec.encodeCommand(command);
        return ByteBuffer.allocate(ENVELOPE_HEADER_LENGTH + encodedCommand.length)
                .put((byte) 1)
                .putLong(serverTimeMillis)
                .putLong(Long.MIN_VALUE)
                .put(new byte[32])
                .putInt(encodedCommand.length)
                .put(encodedCommand)
                .array();
    }

    private static final class StandaloneMachine implements AutoCloseable {
        private final RaftServer server;
        private final MetadataStateMachine stateMachine;
        private long index;

        StandaloneMachine(File dir, Clock clock) throws Exception {
            final RaftPeerId selfId = RaftPeerId.valueOf("m0");
            final RaftGroup group =
                    RaftGroup.valueOf(
                            RaftGroupId.valueOf(UUID.fromString(GROUP_UUID)),
                            RaftPeer.newBuilder().setId(selfId).setAddress("localhost:0").build());
            final RaftProperties properties = new RaftProperties();
            RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.GRPC);
            GrpcConfigKeys.Server.setPort(properties, 0);
            RaftServerConfigKeys.setStorageDir(
                    properties, Collections.singletonList(new File(dir, "raft")));

            stateMachine =
                    new MetadataStateMachine(new File(dir, "meta"), clock, Duration.ofDays(7));
            server =
                    RaftServer.newBuilder()
                            .setServerId(selfId)
                            .setGroup(group)
                            .setStateMachine(stateMachine)
                            .setProperties(properties)
                            .build();
            final RaftStorage storage =
                    StorageImplUtils.newRaftStorage(
                            new File(dir, "raft"),
                            SizeInBytes.valueOf("0MB"),
                            RaftStorage.StartupOption.FORMAT,
                            null);
            storage.initialize();
            stateMachine.initialize(server, group.getGroupId(), storage);
        }

        RaftResult apply(MetadataLogEnvelope envelope) throws Exception {
            final RaftResult result = applyLogData(envelope.encode());
            assertEquals(envelope.getCommand().getOp(), result.getOp());
            return result;
        }

        RaftResult applyLogData(byte[] logData) throws Exception {
            final RaftResult result = RaftCommandCodec.decodeResult(applyEntry(newEntry(logData)));
            assertNotNull(result);
            return result;
        }

        LogEntryProto newEntry(byte[] logData) {
            return newEntry(
                    logData, ClientId.valueOf(UUID.fromString(GROUP_UUID)).toByteString());
        }

        LogEntryProto newEntry(byte[] logData, ByteString clientId) {
            final long revision = ++index;
            final StateMachineLogEntryProto.Builder stateEntry =
                    StateMachineLogEntryProto.newBuilder()
                            .setCallId(revision)
                            .setLogData(ByteString.copyFrom(logData));
            if (clientId != null) {
                stateEntry.setClientId(clientId);
            }
            return LogEntryProto.newBuilder()
                    .setTerm(1L)
                    .setIndex(revision)
                    .setStateMachineLogEntry(stateEntry)
                    .build();
        }

        byte[] applyEntry(LogEntryProto entry) throws Exception {
            final Message reply =
                    stateMachine
                            .applyTransaction(
                                    TransactionContext.newBuilder()
                                            .setStateMachine(stateMachine)
                                            .setLogEntry(entry)
                                            .build())
                            .get(10, TimeUnit.SECONDS);
            return reply.getContent().toByteArray();
        }

        RaftResult query(RaftCommand command) {
            final Message reply =
                    stateMachine
                            .query(
                                    Message.valueOf(
                                            ByteBuffer.wrap(RaftCommandCodec.encodeCommand(command))))
                            .join();
            return RaftCommandCodec.decodeResult(reply.getContent().toByteArray());
        }

        @Override
        public void close() throws Exception {
            server.close();
            stateMachine.close();
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
