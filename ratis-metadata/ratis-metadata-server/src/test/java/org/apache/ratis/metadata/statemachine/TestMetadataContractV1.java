/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file distributed with
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.ratis.metadata.proto.MetadataProtos.Lane;
import org.apache.ratis.metadata.proto.MetadataProtos.NodeKind;
import org.apache.ratis.metadata.proto.MetadataProtos.Role;
import org.apache.ratis.metadata.protocol.GroupConfig;
import org.apache.ratis.metadata.protocol.Stamp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * T02 storage-contract tests: initialization idempotence (A05), record
 * integrity (A52), iterator failures are never EOF (A22), an interrupted
 * batch publishes nothing (A23), close/read-view races (A46), plus the
 * codec byte vectors. Every test drives the real store through the same
 * commit path production uses.
 */
public class TestMetadataContractV1 {
    private static final byte[] WRITER_ID = new byte[16];

    static {
        for (int i = 0; i < WRITER_ID.length; i++) {
            WRITER_ID[i] = (byte) (0x10 + i);
        }
    }

    // Spec-layout vectors derived independently of the Java codec.
    private static final String NODE_VECTOR_HEX =
            "0101000000000000000700000002000000000000000900000003000000000000000000000000"
                    + "0000000000000000000000000000000000000000000000000000000000000000000000"
                    + "6400000000000000c800000002abcdb1220c35603b79f0f87e7cdc8594b88cb29b67e"
                    + "43f5e3239e690d690a1a7b4ae";
    private static final String RESULT_VECTOR_HEX =
            "01000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
                    + "0000000000000003000000000000000b00000000000003e8000000000009431800"
                    + "000003010203aa2307271d1196415d33646c572a132f135596d07a00aa87697bc2b"
                    + "5d21ced89";
    private static final String LEASE_VECTOR_HEX =
            "01101112131415161718191a1b1c1d1e1f00000000000000020000000000007530"
                    + "000000000000a7f8000000000000000900000001";
    private static final String RESULT_KEY_VECTOR_HEX =
            "00101112131415161718191a1b1c1d1e1f00000000000000020000000000000005";
    private static final String APPLIED_VECTOR_HEX =
            "010000000000000001000000000000000c";

    private static GroupConfig coordConfig() {
        return GroupConfig.defaultsFor(Role.COORDINATION);
    }

    private static GroupConfig otherCoordConfig() {
        return GroupConfig.valueOf(
                GroupConfig.defaultsFor(Role.COORDINATION).toBuilder().setPageEntries(512).build());
    }

    @Test
    public void initializationIsIdempotentAndPinsRole(@TempDir Path dir) throws Exception {
        try (MetadataContractFixture fixture =
                     MetadataContractFixture.create(Role.COORDINATION, coordConfig(), dir)) {
            final RocksMetadataStore store = fixture.store();
            Assertions.assertFalse(store.initialized(), "a fresh directory is uninitialized");
            fixture.initialize(1, 1);
            Assertions.assertTrue(store.initialized());
            Assertions.assertEquals(Role.COORDINATION, store.role());
            Assertions.assertEquals(1, store.appliedIndex());
            Assertions.assertEquals(StoreStats.ZERO, store.stats());
            Assertions.assertTrue(fixture.dumpLogicalTables().length > 0);

            // advance past initialization, then check no re-initialize resets it
            try (PreparedTransition tx = fixture.newUncommittedTransition(1, 2, 500)) {
                fixture.stageTestNodeForRollback(tx, key(1), new byte[]{1});
                store.commit(tx);
            }
            Assertions.assertEquals(2, store.appliedIndex());
            Assertions.assertEquals(500, store.logicalTime());
            final byte[] dumpBefore = fixture.dumpLogicalTables();

            fixture.initialize(1, 1); // identical config: idempotent
            Assertions.assertEquals(2, store.appliedIndex(), "re-initialize must not reset state");
            Assertions.assertArrayEquals(dumpBefore, fixture.dumpLogicalTables());

            Assertions.assertThrows(MetadataStoreException.class,
                    () -> store.initialize(otherCoordConfig(), 1, Stamp.valueOf(1, 1)),
                    "a different config on the same directory is rejected");
            Assertions.assertThrows(MetadataStoreException.class,
                    () -> store.initialize(GroupConfig.defaultsFor(Role.KEYSPACE),
                            1, Stamp.valueOf(1, 1)),
                    "the role is pinned for the life of the directory");
            Assertions.assertEquals(Role.COORDINATION, store.role());
            Assertions.assertEquals(2, store.appliedIndex());
            Assertions.assertArrayEquals(dumpBefore, fixture.dumpLogicalTables());
        }
    }

    @Test
    public void keyspaceGroupInitializesOnItsOwnDirectory(@TempDir Path dir) throws Exception {
        try (MetadataContractFixture fixture = MetadataContractFixture.create(
                Role.KEYSPACE, GroupConfig.defaultsFor(Role.KEYSPACE), dir)) {
            fixture.initialize(1, 1);
            Assertions.assertEquals(Role.KEYSPACE, fixture.store().role());
            Assertions.assertTrue(fixture.store().initialized());
        }
    }

    @Test
    public void recordCodecGoldenVectors() {
        final byte[] nodeBytes = hex(NODE_VECTOR_HEX);
        final NodeRecord node = NodeRecord.newBuilder()
                .setKind(NodeKind.NORMAL)
                .setCreateStamp(Stamp.valueOf(7, 2))
                .setModifyStamp(Stamp.valueOf(9, 3))
                .setCreatedAt(100)
                .setModifiedAt(200)
                .setValue(new byte[]{(byte) 0xAB, (byte) 0xCD})
                .build();
        Assertions.assertArrayEquals(nodeBytes, StoredRecordCodec.nodeValue(node));
        Assertions.assertEquals(node, StoredRecordCodec.decodeNode(nodeBytes));

        final byte[] resultBytes = hex(RESULT_VECTOR_HEX);
        final StoredResult result = StoredResult.newBuilder()
                .setDigest(range32())
                .setFirstTerm(3)
                .setFirstIndex(11)
                .setExecutedAt(1000)
                .setExpiresAt(607_000)
                .setReply(new byte[]{1, 2, 3})
                .build();
        Assertions.assertArrayEquals(resultBytes, StoredRecordCodec.storedResultValue(result));
        Assertions.assertEquals(result, StoredRecordCodec.decodeStoredResult(resultBytes));

        final byte[] leaseBytes = hex(LEASE_VECTOR_HEX);
        final LeaseRow lease = new LeaseRow(WRITER_ID, 2, 30_000, 43_000, 9, 1);
        Assertions.assertArrayEquals(leaseBytes, StoredRecordCodec.leaseValue(lease));
        Assertions.assertEquals(lease, StoredRecordCodec.decodeLease(leaseBytes));

        Assertions.assertArrayEquals(hex(RESULT_KEY_VECTOR_HEX),
                StoredRecordCodec.businessResultKey(WRITER_ID, 2, 5));
        Assertions.assertArrayEquals(hex(APPLIED_VECTOR_HEX),
                StoredRecordCodec.appliedValue(1, 12));
        Assertions.assertArrayEquals(new byte[]{1},
                StoredRecordCodec.leasePathValue());
    }

    @Test
    public void recordCorruptionIsExplicitStorageError() {
        final byte[] node = hex(NODE_VECTOR_HEX);
        final byte[] result = hex(RESULT_VECTOR_HEX);

        // StoredResult: digest, index, expiry, reply, checksum, length, codec, trailing
        Assertions.assertThrows(MetadataStoreException.class,
                () -> StoredRecordCodec.decodeStoredResult(flip(result, 1)));   // digest byte
        Assertions.assertThrows(MetadataStoreException.class,
                () -> StoredRecordCodec.decodeStoredResult(flip(result, 41)));  // firstIndex byte
        Assertions.assertThrows(MetadataStoreException.class,
                () -> StoredRecordCodec.decodeStoredResult(flip(result, 57)));  // expiresAt byte
        Assertions.assertThrows(MetadataStoreException.class,
                () -> StoredRecordCodec.decodeStoredResult(flip(result, 69)));  // reply byte
        Assertions.assertThrows(MetadataStoreException.class,
                () -> StoredRecordCodec.decodeStoredResult(flip(result, 73)));  // checksum byte
        Assertions.assertThrows(MetadataStoreException.class,
                () -> StoredRecordCodec.decodeStoredResult(truncated(result)));
        Assertions.assertThrows(MetadataStoreException.class,
                () -> StoredRecordCodec.decodeStoredResult(withTrailing(result)));
        Assertions.assertThrows(MetadataStoreException.class,
                () -> StoredRecordCodec.decodeStoredResult(withCodec(result, 0)));
        Assertions.assertThrows(MetadataStoreException.class,
                () -> StoredRecordCodec.decodeStoredResult(withCodec(result, 2)));

        // NodeRecord: value, checksum, length, codec, kind enum, ownership pairing
        Assertions.assertThrows(MetadataStoreException.class,
                () -> StoredRecordCodec.decodeNode(flip(node, 86)));   // value byte
        Assertions.assertThrows(MetadataStoreException.class,
                () -> StoredRecordCodec.decodeNode(flip(node, 2)));    // createIndex byte
        Assertions.assertThrows(MetadataStoreException.class,
                () -> StoredRecordCodec.decodeNode(flip(node, node.length - 1)));  // checksum
        Assertions.assertThrows(MetadataStoreException.class,
                () -> StoredRecordCodec.decodeNode(truncated(node)));
        Assertions.assertThrows(MetadataStoreException.class,
                () -> StoredRecordCodec.decodeNode(withTrailing(node)));
        Assertions.assertThrows(MetadataStoreException.class,
                () -> StoredRecordCodec.decodeNode(withCodec(node, 2)));
        Assertions.assertThrows(MetadataStoreException.class,
                () -> StoredRecordCodec.decodeNode(withKind(node, 0)));  // unspecified
        Assertions.assertThrows(MetadataStoreException.class,
                () -> StoredRecordCodec.decodeNode(withKind(node, 5)));  // unknown
        // a normal node whose ownership fields are half-set is corrupt
        Assertions.assertThrows(MetadataStoreException.class, () -> NodeRecord.newBuilder()
                .setKind(NodeKind.NORMAL)
                .setCreateStamp(Stamp.valueOf(1, 1)).setModifyStamp(Stamp.valueOf(1, 1))
                .setLeaseOwnership(7, WRITER_ID, 0).build());

        // lease row: binding count negative, codec, trailing
        final byte[] lease = hex(LEASE_VECTOR_HEX);
        Assertions.assertThrows(MetadataStoreException.class,
                () -> StoredRecordCodec.decodeLease(flipSign(lease, 49)));  // bindingCount sign
        Assertions.assertThrows(MetadataStoreException.class,
                () -> StoredRecordCodec.decodeLease(withTrailing(lease)));
        Assertions.assertThrows(MetadataStoreException.class,
                () -> StoredRecordCodec.decodeLease(withCodec(lease, 0)));

        // writer row: lane enum out of range
        Assertions.assertThrows(MetadataStoreException.class,
                () -> StoredRecordCodec.decodeWriter(flip(
                        StoredRecordCodec.writerValue(new WriterRow(Lane.BUSINESS, 1, 5)), 1)));

        // markers and small rows: trailing bytes, truncation, negative values
        final byte[] applied = hex(APPLIED_VECTOR_HEX);
        Assertions.assertThrows(MetadataStoreException.class,
                () -> StoredRecordCodec.decodeApplied(withTrailing(applied)));
        Assertions.assertThrows(MetadataStoreException.class,
                () -> StoredRecordCodec.decodeApplied(truncated(applied)));
        Assertions.assertThrows(MetadataStoreException.class,
                () -> StoredRecordCodec.decodeApplied(withCodec(applied, 0)));
        Assertions.assertThrows(MetadataStoreException.class,
                () -> StoredRecordCodec.decodeApplied(flipSign(applied, 1)));  // negative term
        Assertions.assertThrows(MetadataStoreException.class,
                () -> StoredRecordCodec.decodeLogicalTime(withTrailing(
                        StoredRecordCodec.logicalTimeValue(5))));
        Assertions.assertThrows(MetadataStoreException.class,
                () -> StoredRecordCodec.decodeChangeFloor(flipSign(
                        StoredRecordCodec.changeFloorValue(1), 1)));  // negative floor
        Assertions.assertThrows(MetadataStoreException.class,
                () -> StoredRecordCodec.decodeStats(withCodec(
                        StoredRecordCodec.statsValue(StoreStats.ZERO), 2)));
        Assertions.assertThrows(MetadataStoreException.class,
                () -> StoredRecordCodec.decodeCounter(withTrailing(
                        StoredRecordCodec.counterValue(0))));
        Assertions.assertThrows(MetadataStoreException.class,
                () -> StoredRecordCodec.decodeCounter(truncated(
                        StoredRecordCodec.counterValue(0))));
        Assertions.assertThrows(MetadataStoreException.class,
                () -> StoredRecordCodec.decodeLeasePath(new byte[]{2}));
        Assertions.assertThrows(MetadataStoreException.class,
                () -> StoredRecordCodec.decodeLeasePath(truncated(new byte[]{1})));
        Assertions.assertThrows(MetadataStoreException.class,
                () -> StoredRecordCodec.decodeChangeRevision(flipSign(
                        StoredRecordCodec.changeRevisionValue(1, 10), 1)));  // negative count
        Assertions.assertThrows(MetadataStoreException.class,
                () -> StoredRecordCodec.decodeGroupConfig(withTrailing(
                        StoredRecordCodec.groupConfigValue(new byte[]{1}, new byte[32]))));
    }

    @Test
    public void markerCorruptionFailsOpen(@TempDir Path dir) throws Exception {
        try (MetadataContractFixture fixture =
                     MetadataContractFixture.create(Role.COORDINATION, coordConfig(), dir)) {
            fixture.initialize(1, 1);
            try (PreparedTransition tx = fixture.newUncommittedTransition(1, 2, 100)) {
                fixture.stageTestNodeForRollback(tx, key(1), new byte[]{1});
                fixture.store().commit(tx);
            }
            final byte[] applied = StoredRecordCodec.appliedValue(1, 2);

            fixture.writeRawMarker("applied", withTrailing(applied));
            Assertions.assertThrows(MetadataStoreException.class, fixture::reopenStore,
                    "a trailing-byte marker is corruption at open, not a default value");

            fixture.writeRawMarker("applied", applied); // repair: the dump round-trips again
            fixture.reopenStore();
            fixture.writeRawMarker("applied", null);
            Assertions.assertThrows(MetadataStoreException.class, fixture::reopenStore,
                    "an initialized store missing one marker of the set is corruption");
        }

        final Path orphanDir = dir.resolve("orphan");
        try (MetadataContractFixture fresh =
                     MetadataContractFixture.create(Role.COORDINATION, coordConfig(), orphanDir)) {
            fresh.writeRawMarker("logical-time", StoredRecordCodec.logicalTimeValue(5));
            Assertions.assertThrows(MetadataStoreException.class, fresh::reopenStore,
                    "a marker without a group-config record is corruption, not a partial open");
        }
    }

    @Test
    public void iteratorFailureIsNotEofNorPartialSuccess(@TempDir Path dir) throws Exception {
        final MetadataContractFixture fixture =
                MetadataContractFixture.create(Role.COORDINATION, coordConfig(), dir);
        fixture.initialize(1, 1);
        final RocksMetadataStore store = fixture.store();
        try (PreparedTransition tx = fixture.newUncommittedTransition(1, 2, 100)) {
            for (int i = 0; i < 96; i++) {
                fixture.stageTestNodeForRollback(tx, key(i), new byte[4096]);
            }
            store.commit(tx);
        }
        store.checkpoint(); // flushes memtables so the data lives in SST files
        fixture.close();
        corruptLargestSst(dir);

        final RocksMetadataStore reopened = fixture.reopenStore();
        Assertions.assertTrue(reopened.initialized(), "open itself succeeds; the damage bites on read");
        Assertions.assertThrows(MetadataStoreException.class, fixture::dumpLogicalTables,
                "a scan hitting a corrupt block is an error, never a short dump");
        int threw = 0;
        for (int i = 0; i < 96; i++) {
            try {
                reopened.getNode(key(i));
            } catch (MetadataStoreException expected) {
                threw++;
            }
        }
        Assertions.assertTrue(threw > 0, "at least one point read must hit the corrupt block");
        fixture.close();
    }

    @Test
    public void discardedTransitionPublishesNothing(@TempDir Path dir) throws Exception {
        try (MetadataContractFixture fixture =
                     MetadataContractFixture.create(Role.COORDINATION, coordConfig(), dir)) {
            fixture.initialize(1, 1);
            final byte[] before = fixture.dumpLogicalTables();
            try (PreparedTransition tx = fixture.newUncommittedTransition(1, 2, 100)) {
                fixture.stageTestNodeForRollback(tx, key(1), new byte[]{1});
                fixture.stageStoredResult(tx, WRITER_ID, 1, 1, sampleResult());
                tx.stageChangeFloor(2);
            }
            Assertions.assertArrayEquals(before, fixture.dumpLogicalTables(),
                    "closing without commit discards every staged effect");
        }
    }

    @Test
    public void failedWriteBeforeBatchChangesNothing(@TempDir Path dir) throws Exception {
        try (MetadataContractFixture fixture =
                     MetadataContractFixture.create(Role.COORDINATION, coordConfig(), dir)) {
            final RocksMetadataStore store = fixture.store();
            fixture.initialize(1, 1);
            final byte[] before = fixture.dumpLogicalTables();

            final PreparedTransition tx = fixture.newUncommittedTransition(1, 2, 100);
            try {
                fixture.stageTestNodeForRollback(tx, key(1), new byte[]{9, 9});
                fixture.stageStoredResult(tx, WRITER_ID, 1, 1, sampleResult());
                fixture.failNextWriteBeforeBatch();
                Assertions.assertThrows(MetadataStoreException.class, () -> store.commit(tx));
            } finally {
                tx.close();
            }
            Assertions.assertArrayEquals(before, fixture.dumpLogicalTables(),
                    "no result, expiry row, marker or node may appear from a failed write");
            Assertions.assertEquals(1, store.appliedIndex(), "the marker did not advance");

            // the store remains usable and the same content commits afterwards
            try (PreparedTransition tx2 = fixture.newUncommittedTransition(1, 2, 100)) {
                fixture.stageTestNodeForRollback(tx2, key(1), new byte[]{9, 9});
                store.commit(tx2);
            }
            Assertions.assertFalse(Arrays.equals(before, fixture.dumpLogicalTables()));
            Assertions.assertEquals(2, store.appliedIndex());
        }
    }

    @Test
    public void closeTimeoutKeepsHandlesAliveUntilDrained(@TempDir Path dir) throws Exception {
        final MetadataContractFixture fixture =
                MetadataContractFixture.create(Role.COORDINATION, coordConfig(), dir);
        final RocksMetadataStore store = fixture.store();
        fixture.initialize(1, 1);
        try (PreparedTransition tx = fixture.newUncommittedTransition(1, 2, 100)) {
            fixture.stageTestNodeForRollback(tx, key(1), new byte[]{1});
            store.commit(tx);
        }
        final byte[] nodeKey = key(1);
        final ReadView held = store.openReadView();
        final AtomicReference<Throwable> closeOutcome = new AtomicReference<>();
        final Thread closer = new Thread(() -> {
            try {
                store.close();
                closeOutcome.set(new AssertionError("close unexpectedly completed"));
            } catch (Throwable t) {
                closeOutcome.set(t);
            }
        });
        closer.start();
        closer.join(20_000);

        final Throwable outcome = closeOutcome.get();
        Assertions.assertTrue(outcome instanceof MetadataStoreException, "close reports: " + outcome);
        final MetadataStoreException timeout = (MetadataStoreException) outcome;
        Assertions.assertEquals(MetadataStoreException.Reason.LIFECYCLE, timeout.reason());
        Assertions.assertTrue(timeout.getMessage().contains("CLOSE_TIMEOUT"), timeout.getMessage());
        Assertions.assertSame(RocksMetadataStore.State.CLOSING, store.state(),
                "a timed-out close stays CLOSING with every handle alive");

        // the reader's native handle is still usable and new leases are fenced
        Assertions.assertNotNull(held.get(MetadataCf.NODES, nodeKey));
        Assertions.assertThrows(MetadataStoreException.class, store::openReadView);
        Assertions.assertThrows(MetadataStoreException.class,
                () -> store.getNode(nodeKey), "point reads are fenced too once closing");

        held.close();
        store.close(); // readers drained: the retry completes
        Assertions.assertSame(RocksMetadataStore.State.CLOSED, store.state());
        fixture.close(); // no-op on a closed store
    }

    @Test
    public void workBudgetArithmeticIsChecked() {
        final WorkBudget budget = new WorkBudget(10, 100);
        Assertions.assertTrue(budget.tryCharge(9, 99));
        Assertions.assertTrue(budget.tryCharge(1, 1));
        Assertions.assertFalse(budget.tryCharge(0, 1), "the byte cap refuses one more byte");
        Assertions.assertEquals(10, budget.chargedItems());
        Assertions.assertEquals(100, budget.chargedBytes());

        final WorkBudget huge = new WorkBudget(Long.MAX_VALUE, Long.MAX_VALUE);
        Assertions.assertTrue(huge.tryCharge(Long.MAX_VALUE, 0));
        Assertions.assertFalse(huge.tryCharge(1, 0), "overflow refuses instead of wrapping");
        Assertions.assertEquals(Long.MAX_VALUE, huge.chargedItems());

        Assertions.assertThrows(IllegalArgumentException.class, () -> budget.tryCharge(-1, 0));
        Assertions.assertThrows(IllegalArgumentException.class, () -> new WorkBudget(-1, 0));
    }

    @Test
    public void checkpointFreezesTheAppliedMarkerIndex(@TempDir Path dir) throws Exception {
        try (MetadataContractFixture fixture =
                     MetadataContractFixture.create(Role.COORDINATION, coordConfig(), dir)) {
            final RocksMetadataStore store = fixture.store();
            fixture.initialize(1, 1);
            try (PreparedTransition tx = fixture.newUncommittedTransition(1, 2, 300)) {
                fixture.stageTestNodeForRollback(tx, key(1), new byte[]{1});
                store.commit(tx);
            }
            final StoreCheckpoint cut = store.checkpoint();
            Assertions.assertEquals(store.appliedTerm(), cut.term());
            Assertions.assertEquals(store.appliedIndex(), cut.index());
            Assertions.assertTrue(Files.exists(cut.directory()), "the cut has a directory");
            Assertions.assertTrue(directoryFileCount(cut.directory()) > 0);
            // a second cut under an open read view still freezes consistently
            try (ReadView ignored = store.openReadView()) {
                Assertions.assertDoesNotThrow(store::checkpoint);
            }
        }
    }

    // ---- helpers ----

    private static byte[] key(int i) {
        return String.format("/n%04d", i).getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static byte[] range32() {
        final byte[] digest = new byte[32];
        for (int i = 0; i < digest.length; i++) {
            digest[i] = (byte) i;
        }
        return digest;
    }

    private static StoredResult sampleResult() {
        return StoredResult.newBuilder()
                .setDigest(range32())
                .setFirstTerm(1)
                .setFirstIndex(2)
                .setExecutedAt(100)
                .setExpiresAt(700_000)
                .setReply(new byte[]{7, 8})
                .build();
    }

    private static byte[] flip(byte[] bytes, int at) {
        final byte[] out = bytes.clone();
        out[at] ^= 0x5A;
        return out;
    }

    /** Sets the sign bit of the big-endian field at {@code at}: a negative value. */
    private static byte[] flipSign(byte[] bytes, int at) {
        final byte[] out = bytes.clone();
        out[at] ^= (byte) 0x80;
        return out;
    }

    private static byte[] truncated(byte[] bytes) {
        return Arrays.copyOf(bytes, bytes.length - 1);
    }

    private static byte[] withTrailing(byte[] bytes) {
        return Arrays.copyOf(bytes, bytes.length + 1);
    }

    private static byte[] withCodec(byte[] bytes, int codecVersion) {
        final byte[] out = bytes.clone();
        out[0] = (byte) codecVersion;
        return out;
    }

    private static byte[] withKind(byte[] nodeBytes, int kind) {
        final byte[] out = nodeBytes.clone();
        out[1] = (byte) kind;
        return out;
    }

    private static byte[] hex(String hex) {
        final byte[] out = new byte[hex.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        }
        return out;
    }

    /** Flips the middle bytes of the largest SST file: a corrupted data block. */
    private static void corruptLargestSst(Path dir) throws IOException {
        final List<Path> ssts;
        try (java.util.stream.Stream<Path> walk = Files.walk(dir)) {
            ssts = walk.filter(p -> p.getFileName().toString().endsWith(".sst"))
                    .collect(java.util.stream.Collectors.toList());
        }
        Assertions.assertFalse(ssts.isEmpty(), "flushed data must live in SST files");
        Path largest = null;
        long largestSize = -1;
        for (Path sst : ssts) {
            final long size = Files.size(sst);
            if (size > largestSize) {
                largest = sst;
                largestSize = size;
            }
        }
        final byte[] contents = Files.readAllBytes(largest);
        final int middle = contents.length / 2;
        for (int i = middle; i < middle + 16 && i < contents.length; i++) {
            contents[i] ^= 0x5A;
        }
        Files.write(largest, contents);
    }

    private static long directoryFileCount(Path dir) throws IOException {
        try (java.util.stream.Stream<Path> walk = Files.walk(dir)) {
            return walk.filter(Files::isRegularFile).count();
        }
    }
}
