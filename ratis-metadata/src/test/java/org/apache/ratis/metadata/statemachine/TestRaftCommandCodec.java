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

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Hostile and version-mismatched payloads must decode to null, never throw. */
public class TestRaftCommandCodec {

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /** A PUT whose path-length varint claims -1: ten unsigned-LEB128 bytes with bit 63 set. */
    private static byte[] putWithNegativePathLength() {
        final byte[] payload = new byte[2 + 10];
        payload[0] = (byte) RaftCommandCodec.CURRENT_FORMAT_VERSION;
        payload[1] = RaftCommand.Op.PUT.code();
        Arrays.fill(payload, 2, 11, (byte) 0xFF);
        payload[11] = 0x01;
        return payload;
    }

    /** A POLL_CHANGES OK reply whose record-count header claims 2^31-1 records. */
    private static byte[] pollChangesWithHugeRecordCount() {
        // [POLL_CHANGES][OK][watermark=1][oldestRetained=1][recordCount=2^31-1 as uvarint]
        return new byte[] {
            RaftCommand.Op.POLL_CHANGES.code(),
            RaftResult.Status.OK.code(),
            1,
            1,
            (byte) 0xFF,
            (byte) 0xFF,
            (byte) 0xFF,
            (byte) 0xFF,
            0x07
        };
    }

    @Test
    void testNegativeVarintLengthIsMalformed() {
        assertNull(RaftCommandCodec.decodeCommand(putWithNegativePathLength()));
    }

    @Test
    void testVarintLongerThanTenBytesIsMalformed() {
        final byte[] payload = new byte[2 + 11];
        payload[0] = (byte) RaftCommandCodec.CURRENT_FORMAT_VERSION;
        payload[1] = RaftCommand.Op.PUT.code();
        Arrays.fill(payload, 2, 12, (byte) 0xFF);
        payload[12] = 0x01;
        assertNull(RaftCommandCodec.decodeCommand(payload));
    }

    @Test
    void testNegativeVarlongValuesRoundTrip() {
        // a negative long is a legal field value (REVISION -1); only length prefixes reject it
        final RaftResult revision =
                RaftCommandCodec.decodeResult(
                        RaftCommandCodec.encodeResult(
                                RaftResult.ok(RaftCommand.Op.REVISION).revision(-1L)));
        assertNotNull(revision);
        assertEquals(-1L, revision.getRevision());
        final RaftCommand poll =
                RaftCommandCodec.decodeCommand(
                        RaftCommandCodec.encodeCommand(RaftCommand.pollChanges(-1L, 0)));
        assertNotNull(poll);
        assertEquals(-1L, poll.getSinceRevision());
    }

    @Test
    void testEncodedCommandsStartWithTheFormatVersion() {
        for (RaftCommand command :
                Arrays.asList(
                        RaftCommand.get("/v/x"),
                        RaftCommand.put("/v/p", new byte[0], 0, 1L),
                        RaftCommand.multi(
                                Arrays.asList(RaftCommand.put("/v/m", new byte[0], 0, 1L))))) {
            final byte[] payload = RaftCommandCodec.encodeCommand(command);
            // The version byte is written once per payload, so MULTI sub-commands carry none.
            assertEquals(
                    (byte) RaftCommandCodec.CURRENT_FORMAT_VERSION,
                    payload[0],
                    "payload of " + command.getOp() + " starts with the format version");
            assertNotNull(RaftCommandCodec.decodeCommand(payload));
        }
    }

    @Test
    void testLeaseListPaginationRoundTrips() {
        final RaftCommand command =
                RaftCommandCodec.decodeCommand(
                        RaftCommandCodec.encodeCommand(RaftCommand.leaseList(20L, 2L)));
        assertNotNull(command);
        assertEquals(20L, command.getStartAfterLeaseId());
        assertEquals(2L, command.getMaxResults());

        final RaftResult page = RaftResult.ok(RaftCommand.Op.LEASE_LIST);
        page.addLease(new RaftResult.Lease(30L, 5_000L, 6_000L, 42L));
        page.hasMore(true);
        final RaftResult decoded =
                RaftCommandCodec.decodeResult(RaftCommandCodec.encodeResult(page));
        assertNotNull(decoded);
        assertTrue(decoded.hasMore());
        assertEquals(1, decoded.getLeases().size());
        assertEquals(30L, decoded.getLeases().get(0).getLeaseId());
        assertEquals(5_000L, decoded.getLeases().get(0).getTtlMs());
        assertEquals(6_000L, decoded.getLeases().get(0).getExpireAtMs());
        assertEquals(42L, decoded.getLeases().get(0).getLastRenewIndex());
    }

    @Test
    void testUnknownFormatVersionIsMalformed() {
        final byte[] payload = RaftCommandCodec.encodeCommand(RaftCommand.get("/v/x"));
        payload[0] = (byte) (RaftCommandCodec.CURRENT_FORMAT_VERSION + 1);
        assertNull(RaftCommandCodec.decodeCommand(payload));
    }

    @Test
    void testPreVersionPayloadIsMalformed() {
        // the pre-version encoding started at the op byte; every such payload is rejected
        assertNull(
                RaftCommandCodec.decodeCommand(
                        new byte[] {RaftCommand.Op.GET.code(), 2, '/', 'x'}));
        final byte[] crafted = new byte[1 + 10];
        crafted[0] = RaftCommand.Op.PUT.code();
        Arrays.fill(crafted, 1, 10, (byte) 0xFF);
        crafted[10] = 0x01;
        assertNull(RaftCommandCodec.decodeCommand(crafted));
    }

    @Test
    void testHugeCountHeadersDoNotPreallocate() {
        // count headers are untrusted - never size an allocation from one before the reads reject
        assertNull(RaftCommandCodec.decodeResult(pollChangesWithHugeRecordCount()));
        // Same for a changes column-family value: [revision=1][changeCount=2^31-1].
        assertNull(
                RaftCommandCodec.decodeChangeRecord(
                        new byte[] {1, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x07}));
    }

    @Test
    void testNestedMultiResultIsMalformed() {
        final byte[] twoLevel = {
            RaftCommand.Op.MULTI.code(), RaftResult.Status.OK.code(), 1,
            RaftCommand.Op.MULTI.code(), RaftResult.Status.OK.code(), 0
        };
        assertNull(RaftCommandCodec.decodeResult(twoLevel));

        final byte[] deepChain = new byte[3 * 100_000];
        for (int i = 0; i < deepChain.length; i += 3) {
            deepChain[i] = RaftCommand.Op.MULTI.code();
            deepChain[i + 1] = RaftResult.Status.OK.code();
            deepChain[i + 2] = 1;
        }
        assertNull(RaftCommandCodec.decodeResult(deepChain));
    }

    @Test
    void testNestedMultiCommandIsRejectedBeforeRecursing() {
        // reject nesting before any recursive parse - this runs on every replica decoding a commit
        final byte[] twoLevel = {
            (byte) RaftCommandCodec.CURRENT_FORMAT_VERSION,
            RaftCommand.Op.MULTI.code(),
            0,
            1,
            RaftCommand.Op.MULTI.code(),
            0,
            0
        };
        assertNull(RaftCommandCodec.decodeCommand(twoLevel));

        final byte[] deepCommandChain = new byte[1 + 3 * 100_000];
        deepCommandChain[0] = (byte) RaftCommandCodec.CURRENT_FORMAT_VERSION;
        for (int i = 1; i < deepCommandChain.length; i += 3) {
            deepCommandChain[i] = RaftCommand.Op.MULTI.code();
            deepCommandChain[i + 1] = 0; // writerTerm
            deepCommandChain[i + 2] = 1; // one sub-command
        }
        assertNull(RaftCommandCodec.decodeCommand(deepCommandChain));
    }

    @Test
    void testMultiOpCapIsEnforcedAtComposition() {
        final List<RaftCommand> overCap = new ArrayList<>();
        for (int i = 0; i <= RaftCommand.MAX_MULTI_OPS; i++) {
            overCap.add(RaftCommand.put("/cap/" + i, new byte[0], 0, 1L));
        }
        final IllegalArgumentException e =
                assertThrows(IllegalArgumentException.class, () -> RaftCommand.multi(overCap));
        assertTrue(e.getMessage().contains(String.valueOf(RaftCommand.MAX_MULTI_OPS)));
        assertTrue(e.getMessage().contains(String.valueOf(overCap.size())));

        final List<RaftCommand> atCap = overCap.subList(0, RaftCommand.MAX_MULTI_OPS);
        final RaftCommand decoded =
                RaftCommandCodec.decodeCommand(
                        RaftCommandCodec.encodeCommand(RaftCommand.multi(atCap)));
        assertNotNull(decoded);
        assertEquals(RaftCommand.MAX_MULTI_OPS, decoded.getOps().size());
    }

    @Test
    void testCreateWithLeaseRoundTripsWithoutEphemeralFlag() {
        final RaftCommand leased =
                RaftCommandCodec.decodeCommand(
                        RaftCommandCodec.encodeCommand(
                                RaftCommand.create("/eph/x", new byte[] {9}, false, 42L, 7L)));
        assertNotNull(leased);
        assertEquals(42L, leased.getLeaseId());
        assertEquals((byte) 0, leased.getFlags());

        final RaftCommand sequentialWithParents =
                RaftCommandCodec.decodeCommand(
                        RaftCommandCodec.encodeCommand(
                                RaftCommand.create("/eph/s", new byte[0], true, 42L, 7L, true)));
        assertNotNull(sequentialWithParents);
        assertEquals(42L, sequentialWithParents.getLeaseId());
        assertEquals(
                (byte) (RaftCommand.FLAG_SEQUENTIAL | RaftCommand.FLAG_CREATE_PARENTS),
                sequentialWithParents.getFlags());
    }

    @Test
    void testVersionTwoLeaseRenewRoundTripsTtl() {
        assertEquals(2, RaftCommandCodec.CURRENT_FORMAT_VERSION);

        final RaftCommand renewed =
                RaftCommandCodec.decodeCommand(
                        RaftCommandCodec.encodeCommand(RaftCommand.leaseRenew(11L, 30L, 100L)));
        assertNotNull(renewed);
        assertEquals(11L, renewed.getLeaseId());
        assertEquals(30L, renewed.getTtlMs());
        assertEquals(100L, renewed.getExpireAtMs());

        final RaftCommand legacyFactory =
                RaftCommandCodec.decodeCommand(
                        RaftCommandCodec.encodeCommand(RaftCommand.leaseRenew(11L, 100L)));
        assertNotNull(legacyFactory);
        assertEquals(0L, legacyFactory.getTtlMs());
        assertEquals(100L, legacyFactory.getExpireAtMs());
    }

    @Test
    void testVersionOneLeaseRenewWirePayloadIsRejected() {
        // v1 LEASE_RENEW was [leaseId][expireAt], without the v2 ttl field.
        assertNull(
                RaftCommandCodec.decodeCommand(
                        new byte[] {1, RaftCommand.Op.LEASE_RENEW.code(), 11, 100}));
    }

    @Test
    void testNewResultStatusesAppendWithoutRenumbering() {
        assertEquals(12, RaftResult.Status.STALE_TERM.code());
        assertEquals(13, RaftResult.Status.RESULT_EXPIRED.code());
        assertEquals(14, RaftResult.Status.INVOCATION_MISMATCH.code());
        for (RaftResult.Status status :
                Arrays.asList(
                        RaftResult.Status.RESULT_EXPIRED, RaftResult.Status.INVOCATION_MISMATCH)) {
            final RaftResult decoded =
                    RaftCommandCodec.decodeResult(
                            RaftCommandCodec.encodeResult(
                                    RaftResult.error(RaftCommand.Op.PUT, status)));
            assertNotNull(decoded);
            assertEquals(status, decoded.getStatus());
        }
    }

    @Test
    void testCommandCodecRoundTrip() {
        final List<RaftCommand> commands =
                Arrays.asList(
                        RaftCommand.put("/c/put", bytes("d"), 7, 1234L),
                        RaftCommand.create("/c/create", bytes("d"), true, 9, 1234L),
                        RaftCommand.create("/c/create2", bytes("d"), false, 0, 1234L),
                        RaftCommand.create("/c/create3", bytes("d"), false, 0, 1234L, true),
                        RaftCommand.delete("/c/del", true),
                        RaftCommand.cas("/c/cas", bytes("d"), 4, 0, 1234L),
                        RaftCommand.multi(
                                Arrays.asList(
                                        RaftCommand.put("/c/m1", bytes("d"), 0, 1L),
                                        RaftCommand.increment("m-cnt", -3))),
                        RaftCommand.increment("c-cnt", -5),
                        RaftCommand.leaseRegister(11L, 30, 99L),
                        RaftCommand.leaseRenew(11L, 100L),
                        RaftCommand.leaseExpire(11L, 42L),
                        RaftCommand.get("/c/get"),
                        RaftCommand.list("/c/", 77),
                        RaftCommand.exists("/c/e"),
                        RaftCommand.revision(),
                        RaftCommand.pollChanges(12345, 678));

        for (RaftCommand command : commands) {
            final RaftCommand decoded =
                    RaftCommandCodec.decodeCommand(RaftCommandCodec.encodeCommand(command));
            assertNotNull(decoded, "round trip of " + command.getOp());
            assertEquals(command.getOp(), decoded.getOp());
            assertEquals(command.getPath(), decoded.getPath());
            assertEquals(command.getLeaseId(), decoded.getLeaseId());
            assertArrayEquals(command.getData(), decoded.getData());
            assertEquals(
                    command.getOps() == null ? 0 : command.getOps().size(),
                    decoded.getOps() == null ? 0 : decoded.getOps().size());
        }

        assertNull(RaftCommandCodec.decodeCommand(new byte[] {99}));
        assertNull(RaftCommandCodec.decodeCommand(new byte[] {1, 5, 'a'}));
        assertNotNull(
                RaftCommandCodec.decodeCommand(
                        RaftCommandCodec.encodeCommand(RaftCommand.get("/x"))));
        assertNull(RaftCommandCodec.decodeResult(new byte[] {99, 0, 1}));

        final RaftCommand listed =
                RaftCommandCodec.decodeCommand(
                        RaftCommandCodec.encodeCommand(RaftCommand.list("/c/", "/c/mid", 77)));
        assertEquals("/c/mid", listed.getStartAfter());
        assertNotNull(
                RaftCommandCodec.decodeCommand(
                        RaftCommandCodec.encodeCommand(RaftCommand.list("/c/", 77))));

        final RaftCommand stamped =
                RaftCommandCodec.decodeCommand(
                        RaftCommandCodec.encodeCommand(
                                RaftCommand.multi(
                                        Collections.singletonList(
                                                RaftCommand.put("/c/m2", bytes("d"), 0, 1L)),
                                        7L)));
        assertEquals(7L, stamped.getWriterTerm());
        assertEquals(
                0L,
                RaftCommandCodec.decodeCommand(
                                RaftCommandCodec.encodeCommand(
                                        RaftCommand.multi(
                                                Collections.singletonList(
                                                        RaftCommand.put("/c/m3", bytes("d"), 0, 1L)),
                                                0L)))
                        .getWriterTerm());
        final RaftCommand withParents =
                RaftCommandCodec.decodeCommand(
                        RaftCommandCodec.encodeCommand(
                                RaftCommand.create("/c/cp", bytes("d"), false, 0, 1L, true)));
        assertTrue((withParents.getFlags() & RaftCommand.FLAG_CREATE_PARENTS) != 0);
        assertTrue((withParents.getFlags() & RaftCommand.FLAG_SEQUENTIAL) == 0);

        assertEquals(
                RaftResult.Status.STALE_TERM,
                RaftCommandCodec.decodeResult(
                                RaftCommandCodec.encodeResult(
                                        RaftResult.error(
                                                RaftCommand.Op.MULTI,
                                                RaftResult.Status.STALE_TERM)))
                        .getStatus());
        final RaftResult listResult =
                RaftCommandCodec.decodeResult(
                        RaftCommandCodec.encodeResult(
                                RaftResult.ok(RaftCommand.Op.LIST)
                                        .addEntry(new RaftResult.Entry("/c/a", 1))
                                        .hasMore(true)));
        assertEquals(RaftResult.Status.OK, listResult.getStatus());
        assertEquals(1, listResult.getEntries().size());
        assertEquals("/c/a", listResult.getEntries().get(0).getPath());
        assertEquals(1L, listResult.getEntries().get(0).getVersion());
        assertTrue(listResult.hasMore());

        final ChangeEvent record =
                new ChangeEvent(
                        42,
                        Arrays.asList(
                                new ChangeEvent.Change(
                                        ChangeEvent.ChangeOp.CREATE, "/r", bytes("v")),
                                new ChangeEvent.Change(ChangeEvent.ChangeOp.DELETE, "/r", null)));
        final RaftResult result =
                RaftResult.ok(RaftCommand.Op.POLL_CHANGES)
                        .revision(99)
                        .oldestRetainedRevision(3)
                        .records(Collections.singletonList(record));
        final RaftResult decoded = RaftCommandCodec.decodeResult(RaftCommandCodec.encodeResult(result));
        assertNotNull(decoded);
        assertEquals(99L, decoded.getRevision());
        assertEquals(3L, decoded.getOldestRetainedRevision());
        assertEquals(1, decoded.getRecords().size());
        assertEquals(42L, decoded.getRecords().get(0).getRevision());
        assertEquals(2, decoded.getRecords().get(0).getChanges().size());
        assertNotNull(RaftCommandCodec.decodeChangeRecord(RaftCommandCodec.encodeChangeRecord(record)));
    }
}
