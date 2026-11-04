/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.ratis.metadata.util.Clock;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.ClientInvocationId;
import org.apache.ratis.server.protocol.TermIndex;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Direct storage tests of invocation result selection and caller-owned atomic staging. */
public class TestInvocationResultStore {

    private static final UUID CLIENT_UUID =
            UUID.fromString("12345678-1234-4678-9234-567812345678");
    private static final ClientId CLIENT_ID = ClientId.valueOf(CLIENT_UUID);
    private static final byte[] RESULTS_CF = "command-results".getBytes(StandardCharsets.UTF_8);
    private static final byte[] APPLIED_REVISION_KEY =
            "applied-revision".getBytes(StandardCharsets.UTF_8);
    private static final byte[] CUTOFF_KEY = {0};
    private static final byte RECORD_KEY_TAG = 1;
    private static final byte TIME_KEY_TAG = 2;

    @TempDir File tempDir;

    private DBOptions dbOptions;
    private ColumnFamilyOptions defaultOptions;
    private ColumnFamilyOptions resultsOptions;
    private WriteOptions writeOptions;
    private RocksDB db;
    private ColumnFamilyHandle defaultHandle;
    private ColumnFamilyHandle resultsHandle;
    private InvocationResultStore store;

    @BeforeEach
    void open() throws Exception {
        dbOptions =
                new DBOptions()
                        .setCreateIfMissing(true)
                        .setCreateMissingColumnFamilies(true)
                        .setAtomicFlush(true);
        defaultOptions = new ColumnFamilyOptions();
        resultsOptions = new ColumnFamilyOptions();
        writeOptions = new WriteOptions().setDisableWAL(true);
        final List<ColumnFamilyDescriptor> descriptors =
                Arrays.asList(
                        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, defaultOptions),
                        new ColumnFamilyDescriptor(RESULTS_CF, resultsOptions));
        final List<ColumnFamilyHandle> handles = new ArrayList<>();
        db = RocksDB.open(dbOptions, tempDir.getAbsolutePath(), descriptors, handles);
        defaultHandle = handles.get(0);
        resultsHandle = handles.get(1);
        store = new InvocationResultStore(db, defaultHandle, resultsHandle);
    }

    @AfterEach
    void close() {
        if (resultsHandle != null) {
            resultsHandle.close();
        }
        if (defaultHandle != null) {
            defaultHandle.close();
        }
        if (db != null) {
            db.close();
        }
        if (writeOptions != null) {
            writeOptions.close();
        }
        if (resultsOptions != null) {
            resultsOptions.close();
        }
        if (defaultOptions != null) {
            defaultOptions.close();
        }
        if (dbOptions != null) {
            dbOptions.close();
        }
    }

    @Test
    void testFirstExecutionAndDuplicateRefreshPreserveFirstFields() throws Exception {
        final ClientInvocationId invocation = invocation(10L);
        final MetadataLogEnvelope firstEnvelope =
                envelope(RaftCommand.increment("first", 7L), 1_000L, 100L);
        final byte[] firstResult =
                RaftCommandCodec.encodeResult(
                        RaftResult.ok(RaftCommand.Op.INCREMENT).counterValue(7L).revision(10L));
        final InvocationResultStore.Disposition first =
                store.lookup(invocation, TermIndex.valueOf(1L, 10L), firstEnvelope);
        assertEquals(InvocationResultStore.Disposition.Kind.EXECUTE, first.getKind());
        assertEquals(900L, first.getEffectiveCutoff());
        assertEquals(1_000L, first.getRetentionTime());

        try (WriteBatch batch = new WriteBatch()) {
            store.stageRetention(batch, first);
            store.stageResult(batch, first, firstResult);
            batch.put(defaultHandle, APPLIED_REVISION_KEY, longBytes(10L));
            assertNull(
                    db.get(resultsHandle, invocationKey(invocation)),
                    "staging must not commit or close the caller's batch");
            db.write(writeOptions, batch);
        }
        assertRecord(invocation, 1L, 10L, 1_000L, 1_000L, firstEnvelope, firstResult);

        final MetadataLogEnvelope duplicateEnvelope =
                envelope(RaftCommand.increment("first", 7L), 2_000L, 100L);
        final InvocationResultStore.Disposition duplicate =
                store.lookup(invocation, TermIndex.valueOf(1L, 11L), duplicateEnvelope);
        assertEquals(InvocationResultStore.Disposition.Kind.RETURN_RESULT, duplicate.getKind());
        assertArrayEquals(firstResult, duplicate.getEncodedResult());
        assertEquals(1_900L, duplicate.getEffectiveCutoff());
        assertEquals(2_000L, duplicate.getRetentionTime());
        commit(duplicate, null, 11L);

        assertNull(db.get(resultsHandle, timeKey(1_000L, invocation)));
        assertArrayEquals(
                invocationKey(invocation), db.get(resultsHandle, timeKey(2_000L, invocation)));
        assertRecord(invocation, 1L, 10L, 1_000L, 2_000L, firstEnvelope, firstResult);

        final InvocationResultStore.Disposition replay =
                store.lookup(invocation, TermIndex.valueOf(1L, 10L), firstEnvelope);
        assertEquals(InvocationResultStore.Disposition.Kind.RETURN_RESULT, replay.getKind());
        assertArrayEquals(firstResult, replay.getEncodedResult());
    }

    @Test
    void testMismatchReturnsCurrentOpWithoutOverwritingOriginalResult() throws Exception {
        final ClientInvocationId invocation = invocation(20L);
        final MetadataLogEnvelope originalEnvelope =
                envelope(RaftCommand.put("/mismatch", bytes("first"), 0L, 1L), 1_000L, 100L);
        final byte[] originalResult =
                RaftCommandCodec.encodeResult(RaftResult.ok(RaftCommand.Op.PUT).version(1L));
        final InvocationResultStore.Disposition first =
                store.lookup(invocation, TermIndex.valueOf(1L, 5L), originalEnvelope);
        commit(first, originalResult, 5L);

        final MetadataLogEnvelope mismatchEnvelope =
                envelope(RaftCommand.increment("different", 1L), 2_000L, 100L);
        final InvocationResultStore.Disposition mismatch =
                store.lookup(invocation, TermIndex.valueOf(1L, 6L), mismatchEnvelope);
        final RaftResult mismatchResult = decode(mismatch);
        assertEquals(InvocationResultStore.Disposition.Kind.RETURN_RESULT, mismatch.getKind());
        assertEquals(RaftResult.Status.INVOCATION_MISMATCH, mismatchResult.getStatus());
        assertEquals(RaftCommand.Op.INCREMENT, mismatchResult.getOp());
        commit(mismatch, null, 6L);

        assertRecord(invocation, 1L, 5L, 1_000L, 2_000L, originalEnvelope, originalResult);
        assertArrayEquals(
                originalResult,
                store.lookup(invocation, TermIndex.valueOf(1L, 5L), originalEnvelope)
                        .getEncodedResult());
        assertEquals(
                RaftResult.Status.INVOCATION_MISMATCH,
                decode(store.lookup(invocation, TermIndex.valueOf(1L, 6L), mismatchEnvelope))
                        .getStatus());
    }

    @Test
    void testReplayGenerationFenceAndMissingRecordSelection() throws Exception {
        setMarker(10L);
        final ClientInvocationId missing = invocation(30L);
        final MetadataLogEnvelope oldEnvelope =
                envelope(RaftCommand.increment("missing", 1L), 1_000L, 100L);
        assertThrows(
                IOException.class,
                () -> store.lookup(missing, TermIndex.valueOf(1L, 10L), oldEnvelope),
                "a missing result at or above the persisted cutoff is corruption");

        final ClientInvocationId cutoffWriter = invocation(31L);
        final InvocationResultStore.Disposition cutoff =
                store.lookup(
                        cutoffWriter,
                        TermIndex.valueOf(1L, 11L),
                        envelope(RaftCommand.increment("cutoff", 1L), 2_000L, 100L));
        commit(
                cutoff,
                RaftCommandCodec.encodeResult(RaftResult.ok(RaftCommand.Op.INCREMENT)),
                11L);
        final InvocationResultStore.Disposition expired =
                store.lookup(missing, TermIndex.valueOf(1L, 10L), oldEnvelope);
        assertEquals(InvocationResultStore.Disposition.Kind.RESULT_EXPIRED, expired.getKind());
        assertEquals(RaftResult.Status.RESULT_EXPIRED, decode(expired).getStatus());

        final ClientInvocationId newerGeneration = invocation(32L);
        final MetadataLogEnvelope generationEnvelope =
                envelope(RaftCommand.increment("generation", 1L), 3_000L, 100L);
        final InvocationResultStore.Disposition first =
                store.lookup(newerGeneration, TermIndex.valueOf(2L, 30L), generationEnvelope);
        commit(
                first,
                RaftCommandCodec.encodeResult(
                        RaftResult.ok(RaftCommand.Op.INCREMENT).counterValue(99L)),
                30L);
        final InvocationResultStore.Disposition generationReplay =
                store.lookup(newerGeneration, TermIndex.valueOf(1L, 10L), generationEnvelope);
        assertEquals(
                InvocationResultStore.Disposition.Kind.RESULT_EXPIRED,
                generationReplay.getKind());
        assertEquals(RaftResult.Status.RESULT_EXPIRED, decode(generationReplay).getStatus());
    }

    @Test
    void testCurrentRecordWithMissingOrMismatchingTimeIndexIsCorruption() throws Exception {
        final ClientInvocationId invocation = invocation(40L);
        final MetadataLogEnvelope envelope =
                envelope(RaftCommand.increment("index", 1L), 1_000L, 100L);
        final InvocationResultStore.Disposition first =
                store.lookup(invocation, TermIndex.valueOf(1L, 1L), envelope);
        commit(first, RaftCommandCodec.encodeResult(RaftResult.ok(RaftCommand.Op.INCREMENT)), 1L);
        try (WriteBatch batch = new WriteBatch()) {
            batch.delete(resultsHandle, timeKey(1_000L, invocation));
            db.write(writeOptions, batch);
        }

        assertThrows(
                IOException.class,
                () -> store.lookup(invocation, TermIndex.valueOf(1L, 2L), envelope));
    }

    @Test
    void testMalformedRecordAndCutoffFailClearly() throws Exception {
        final ClientInvocationId invocation = invocation(41L);
        final MetadataLogEnvelope envelope =
                envelope(RaftCommand.increment("corrupt", 1L), 1_000L, 100L);
        try (WriteBatch batch = new WriteBatch()) {
            batch.put(resultsHandle, invocationKey(invocation), new byte[] {1, 2, 3});
            db.write(writeOptions, batch);
        }
        assertThrows(
                IOException.class,
                () -> store.lookup(invocation, TermIndex.valueOf(1L, 1L), envelope));

        try (WriteBatch batch = new WriteBatch()) {
            batch.delete(resultsHandle, invocationKey(invocation));
            batch.put(resultsHandle, CUTOFF_KEY, new byte[] {1});
            db.write(writeOptions, batch);
        }
        assertThrows(RocksDBException.class, store::loadCutoff);
    }

    @Test
    void testCutoffIsMonotonicAndRetentionTimeUsesEffectiveCutoffFloor() throws Exception {
        final ClientInvocationId invocation = invocation(50L);
        final MetadataLogEnvelope firstEnvelope =
                envelope(RaftCommand.increment("clock", 1L), 10_000L, 1_000L);
        final InvocationResultStore.Disposition first =
                store.lookup(invocation, TermIndex.valueOf(1L, 1L), firstEnvelope);
        commit(first, RaftCommandCodec.encodeResult(RaftResult.ok(RaftCommand.Op.INCREMENT)), 1L);

        final InvocationResultStore.Disposition duplicate =
                store.lookup(
                        invocation,
                        TermIndex.valueOf(1L, 2L),
                        envelope(RaftCommand.increment("clock", 1L), 5_000L, 1_000L));
        assertEquals(9_000L, duplicate.getEffectiveCutoff());
        assertEquals(9_000L, duplicate.getRetentionTime());
        commit(duplicate, null, 2L);
        assertEquals(9_000L, store.loadCutoff());
        assertRecord(
                invocation,
                1L,
                1L,
                10_000L,
                9_000L,
                firstEnvelope,
                RaftCommandCodec.encodeResult(RaftResult.ok(RaftCommand.Op.INCREMENT)));
    }

    @Test
    void testMissingReplayAtPersistedCutoffIsNotExpired() throws Exception {
        setMarker(10L);
        final ClientInvocationId cutoffWriter = invocation(51L);
        final InvocationResultStore.Disposition cutoff =
                store.lookup(
                        cutoffWriter,
                        TermIndex.valueOf(1L, 11L),
                        envelope(RaftCommand.increment("cutoff", 1L), 2_000L, 1_000L));
        commit(
                cutoff,
                RaftCommandCodec.encodeResult(RaftResult.ok(RaftCommand.Op.INCREMENT)),
                11L);
        assertEquals(1_000L, store.loadCutoff());

        final ClientInvocationId missing = invocation(52L);
        assertThrows(
                IOException.class,
                () ->
                        store.lookup(
                                missing,
                                TermIndex.valueOf(1L, 10L),
                                envelope(RaftCommand.increment("missing", 1L), 1_000L, 100L)),
                "serverTime equal to the persisted cutoff is not expired");
    }

    @Test
    void testTimeIndexSortsSignedLongsNumerically() throws Exception {
        final long[] times = {-1L, 0L, 1L};
        for (int i = 0; i < times.length; i++) {
            final ClientInvocationId invocation = invocation(100L + i);
            final MetadataLogEnvelope envelope =
                    envelope(RaftCommand.increment("ordered-" + i, 1L), times[i], 10L);
            final InvocationResultStore.Disposition first =
                    store.lookup(invocation, TermIndex.valueOf(1L, i + 1L), envelope);
            commit(
                    first,
                    RaftCommandCodec.encodeResult(RaftResult.ok(RaftCommand.Op.INCREMENT)),
                    i + 1L);
        }

        final List<Long> ordered = new ArrayList<>();
        try (RocksIterator iterator = db.newIterator(resultsHandle)) {
            iterator.seek(new byte[] {TIME_KEY_TAG});
            while (iterator.isValid() && iterator.key()[0] == TIME_KEY_TAG) {
                ordered.add(decodeSortableLong(iterator.key(), 1));
                iterator.next();
            }
        }
        assertEquals(Arrays.asList(-1L, 0L, 1L), ordered);
    }

    @Test
    void testRefreshCollectsSiblingBelowNewCutoffButKeepsRefreshedOriginalReply() throws Exception {
        final ClientInvocationId invocation = invocation(200L);
        final ClientInvocationId sibling = invocation(201L);
        final RaftCommand command = RaftCommand.put("/refresh-boundary", bytes("v1"), 0, 1L);
        final byte[] original =
                RaftCommandCodec.encodeResult(RaftResult.ok(RaftCommand.Op.PUT).version(1L));
        commit(
                store.lookup(invocation, TermIndex.valueOf(1L, 5L), envelope(command, 1_000L, 100L)),
                original,
                5L);
        commit(
                store.lookup(
                        sibling,
                        TermIndex.valueOf(1L, 6L),
                        envelope(RaftCommand.put("/refresh-sibling", bytes("s"), 0, 1L), 1_000L, 100L)),
                RaftCommandCodec.encodeResult(RaftResult.ok(RaftCommand.Op.PUT).version(1L)),
                6L);

        // The duplicate's own proposed cutoff passes both records' old retentionTime: the old
        // first result would have expired, so ordinary GC must not delete the refreshed record
        // in its own transition - only the sibling below the cutoff is collected.
        final MetadataLogEnvelope refreshEnvelope = envelope(command, 3_000L, 1_500L);
        final InvocationResultStore.Disposition refresh =
                store.lookup(invocation, TermIndex.valueOf(1L, 7L), refreshEnvelope);
        assertEquals(InvocationResultStore.Disposition.Kind.RETURN_RESULT, refresh.getKind());
        assertEquals(1_500L, refresh.getEffectiveCutoff());
        assertEquals(3_000L, refresh.getRetentionTime());
        assertArrayEquals(original, refresh.getEncodedResult());
        commit(refresh, null, 7L);

        assertNull(db.get(resultsHandle, timeKey(1_000L, invocation)), "the old time index moved");
        assertArrayEquals(
                invocationKey(invocation), db.get(resultsHandle, timeKey(3_000L, invocation)));
        assertRecord(invocation, 1L, 5L, 1_000L, 3_000L, refreshEnvelope, original);
        assertNull(
                db.get(resultsHandle, invocationKey(sibling)),
                "the sibling below the new cutoff is collected");
        assertNull(db.get(resultsHandle, timeKey(1_000L, sibling)));
        assertEquals(1_500L, store.loadCutoff());
        assertArrayEquals(
                original,
                store.lookup(invocation, TermIndex.valueOf(1L, 5L), refreshEnvelope)
                        .getEncodedResult());
    }

    @Test
    void testMismatchRefreshAdvancingTheCutoffSurvivesItsOwnTransition() throws Exception {
        final ClientInvocationId invocation = invocation(202L);
        final ClientInvocationId sibling = invocation(203L);
        final MetadataLogEnvelope firstEnvelope =
                envelope(RaftCommand.put("/mismatch-refresh", bytes("o"), 0, 1L), 1_000L, 100L);
        final byte[] original =
                RaftCommandCodec.encodeResult(RaftResult.ok(RaftCommand.Op.PUT).version(1L));
        commit(store.lookup(invocation, TermIndex.valueOf(1L, 8L), firstEnvelope), original, 8L);
        commit(
                store.lookup(
                        sibling,
                        TermIndex.valueOf(1L, 9L),
                        envelope(
                                RaftCommand.put("/mismatch-refresh-sibling", bytes("s"), 0, 1L),
                                1_000L,
                                100L)),
                RaftCommandCodec.encodeResult(RaftResult.ok(RaftCommand.Op.PUT).version(1L)),
                9L);

        final InvocationResultStore.Disposition mismatch =
                store.lookup(
                        invocation,
                        TermIndex.valueOf(1L, 10L),
                        envelope(RaftCommand.increment("mismatch-other", 1L), 3_000L, 1_500L));
        assertEquals(InvocationResultStore.Disposition.Kind.RETURN_RESULT, mismatch.getKind());
        final RaftResult mismatchResult = decode(mismatch);
        assertEquals(RaftResult.Status.INVOCATION_MISMATCH, mismatchResult.getStatus());
        assertEquals(RaftCommand.Op.INCREMENT, mismatchResult.getOp());
        commit(mismatch, null, 10L);

        assertRecord(invocation, 1L, 8L, 1_000L, 3_000L, firstEnvelope, original);
        assertNull(db.get(resultsHandle, invocationKey(sibling)), "only the sibling is collected");
        assertNull(db.get(resultsHandle, timeKey(1_000L, sibling)));
        assertArrayEquals(
                original,
                store.lookup(invocation, TermIndex.valueOf(1L, 8L), firstEnvelope)
                        .getEncodedResult());
    }

    @Test
    void testCollectionFollowsTheReplicatedCutoffOnly() throws Exception {
        final ClientInvocationId first = invocation(204L);
        commit(
                store.lookup(
                        first,
                        TermIndex.valueOf(1L, 1L),
                        envelope(RaftCommand.put("/gc-1", bytes("1"), 0, 1L), 1_000L, 100L)),
                RaftCommandCodec.encodeResult(RaftResult.ok(RaftCommand.Op.PUT).version(1L)),
                1L);
        final ClientInvocationId second = invocation(205L);
        commit(
                store.lookup(
                        second,
                        TermIndex.valueOf(1L, 2L),
                        envelope(RaftCommand.put("/gc-2", bytes("2"), 0, 1L), 2_000L, 100L)),
                RaftCommandCodec.encodeResult(RaftResult.ok(RaftCommand.Op.PUT).version(1L)),
                2L);
        assertNull(
                db.get(resultsHandle, invocationKey(first)),
                "collected once the replicated cutoff passes its retentionTime");
        assertNull(db.get(resultsHandle, timeKey(1_000L, first)));

        final ClientInvocationId third = invocation(206L);
        commit(
                store.lookup(
                        third,
                        TermIndex.valueOf(1L, 3L),
                        envelope(RaftCommand.put("/gc-3", bytes("3"), 0, 1L), 3_000L, 1_000L)),
                RaftCommandCodec.encodeResult(RaftResult.ok(RaftCommand.Op.PUT).version(1L)),
                3L);
        assertRecord(
                second,
                1L,
                2L,
                2_000L,
                2_000L,
                envelope(RaftCommand.put("/gc-2", bytes("2"), 0, 1L), 2_000L, 100L),
                RaftCommandCodec.encodeResult(RaftResult.ok(RaftCommand.Op.PUT).version(1L)));

        final ClientInvocationId fourth = invocation(207L);
        commit(
                store.lookup(
                        fourth,
                        TermIndex.valueOf(1L, 4L),
                        envelope(RaftCommand.put("/gc-4", bytes("4"), 0, 1L), 4_000L, 100L)),
                RaftCommandCodec.encodeResult(RaftResult.ok(RaftCommand.Op.PUT).version(1L)),
                4L);
        assertNull(db.get(resultsHandle, invocationKey(second)));
        assertNull(db.get(resultsHandle, timeKey(2_000L, second)));
        assertNull(db.get(resultsHandle, invocationKey(third)));
        assertNull(db.get(resultsHandle, timeKey(3_000L, third)));
        assertRecord(
                fourth,
                1L,
                4L,
                4_000L,
                4_000L,
                envelope(RaftCommand.put("/gc-4", bytes("4"), 0, 1L), 4_000L, 100L),
                RaftCommandCodec.encodeResult(RaftResult.ok(RaftCommand.Op.PUT).version(1L)));
        assertEquals(3_900L, store.loadCutoff());
    }

    @Test
    void testCandidateExactlyAtTheCutoffSurvivesCollection() throws Exception {
        final ClientInvocationId atCutoff = invocation(210L);
        commit(
                store.lookup(
                        atCutoff,
                        TermIndex.valueOf(1L, 1L),
                        envelope(RaftCommand.put("/eq-a", bytes("a"), 0, 1L), 1_000L, 100L)),
                RaftCommandCodec.encodeResult(RaftResult.ok(RaftCommand.Op.PUT).version(1L)),
                1L);
        final ClientInvocationId above = invocation(211L);
        commit(
                store.lookup(
                        above,
                        TermIndex.valueOf(1L, 2L),
                        envelope(RaftCommand.put("/eq-b", bytes("b"), 0, 1L), 2_000L, 1_000L)),
                RaftCommandCodec.encodeResult(RaftResult.ok(RaftCommand.Op.PUT).version(1L)),
                2L);
        // The second commit's effective cutoff is exactly 1_000: the candidate at 1_000 survives.
        assertArrayEquals(
                invocationKey(atCutoff), db.get(resultsHandle, timeKey(1_000L, atCutoff)));

        final ClientInvocationId collector = invocation(212L);
        commit(
                store.lookup(
                        collector,
                        TermIndex.valueOf(1L, 3L),
                        envelope(RaftCommand.put("/eq-c", bytes("c"), 0, 1L), 2_500L, 500L)),
                RaftCommandCodec.encodeResult(RaftResult.ok(RaftCommand.Op.PUT).version(1L)),
                3L);
        assertNull(
                db.get(resultsHandle, invocationKey(atCutoff)),
                "strictly below the new cutoff 2_000 is collected");
        assertNull(db.get(resultsHandle, timeKey(1_000L, atCutoff)));
        assertArrayEquals(
                invocationKey(above), db.get(resultsHandle, timeKey(2_000L, above)),
                "a candidate exactly at the cutoff 2_000 survives");
        assertEquals(2_000L, store.loadCutoff());
    }

    @Test
    void testClockBehindPersistedCutoffTransitionsCollectNothingAndKeepTheFloor() throws Exception {
        final ClientInvocationId ahead = invocation(220L);
        commit(
                store.lookup(
                        ahead,
                        TermIndex.valueOf(1L, 1L),
                        envelope(RaftCommand.put("/cb-ahead", bytes("a"), 0, 1L), 10_000L, 1_000L)),
                RaftCommandCodec.encodeResult(RaftResult.ok(RaftCommand.Op.PUT).version(1L)),
                1L);
        assertEquals(9_000L, store.loadCutoff());

        // A new result whose envelope serverTime is behind the persisted cutoff: its retention
        // lands on the cutoff floor, so it can never be collected in its own transition, and the
        // regressed clock must not sweep anything (collection follows the cutoff, not the clock).
        final ClientInvocationId behind = invocation(221L);
        final MetadataLogEnvelope behindEnvelope =
                envelope(RaftCommand.put("/cb-behind", bytes("b"), 0, 1L), 8_900L, 100L);
        final byte[] behindResult =
                RaftCommandCodec.encodeResult(RaftResult.ok(RaftCommand.Op.PUT).version(1L));
        commit(
                store.lookup(behind, TermIndex.valueOf(1L, 2L), behindEnvelope),
                behindResult,
                2L);
        assertRecord(behind, 1L, 2L, 8_900L, 9_000L, behindEnvelope, behindResult);
        assertArrayEquals(invocationKey(behind), db.get(resultsHandle, timeKey(9_000L, behind)));
        assertArrayEquals(invocationKey(ahead), db.get(resultsHandle, timeKey(10_000L, ahead)));

        // A refresh while the clock is still behind the persisted cutoff: the old time key sits
        // exactly at the cutoff, so the refresh survives its own transition.
        final InvocationResultStore.Disposition behindRefresh =
                store.lookup(
                        behind,
                        TermIndex.valueOf(1L, 3L),
                        envelope(RaftCommand.put("/cb-behind", bytes("b"), 0, 1L), 8_700L, 100L));
        assertEquals(InvocationResultStore.Disposition.Kind.RETURN_RESULT, behindRefresh.getKind());
        assertArrayEquals(behindResult, behindRefresh.getEncodedResult());
        commit(behindRefresh, null, 3L);
        assertRecord(behind, 1L, 2L, 8_900L, 9_000L, behindEnvelope, behindResult);
        assertArrayEquals(invocationKey(ahead), db.get(resultsHandle, timeKey(10_000L, ahead)));
        assertEquals(9_000L, store.loadCutoff());

        final ClientInvocationId advanced = invocation(222L);
        commit(
                store.lookup(
                        advanced,
                        TermIndex.valueOf(1L, 4L),
                        envelope(RaftCommand.put("/cb-advanced", bytes("c"), 0, 1L), 12_000L, 100L)),
                RaftCommandCodec.encodeResult(RaftResult.ok(RaftCommand.Op.PUT).version(1L)),
                4L);
        assertNull(db.get(resultsHandle, invocationKey(behind)), "the floor result is collected");
        assertNull(db.get(resultsHandle, timeKey(9_000L, behind)));
        assertNull(db.get(resultsHandle, invocationKey(ahead)));
        assertNull(db.get(resultsHandle, timeKey(10_000L, ahead)));
        assertEquals(11_900L, store.loadCutoff());
    }

    @Test
    void testInconsistentUnrefreshedIndexFailsCommitAsCorruption() throws Exception {
        final RaftCommand advance = RaftCommand.put("/corrupt-advance", bytes("z"), 0, 1L);
        final byte[] advanceResult =
                RaftCommandCodec.encodeResult(RaftResult.ok(RaftCommand.Op.PUT).version(1L));

        // Each mode corrupts exactly one victim at retentionTime 1_000, asserts the cutoff-1_900
        // commit fails with THAT mode's own corruption message, then removes the victim. Without
        // the removal a later assertThrows could be satisfied by an earlier mode's leftover
        // victim (scan order among equal time keys follows random clientId bytes); the message
        // fragment pins each assertion to exactly one production corruption check, so removing
        // any single check fails only its own mode.
        final ClientInvocationId staleTimestamp = invocation(230L);
        commitVictim(staleTimestamp, "/corrupt-1", 30L);
        patchRecordRetentionTime(staleTimestamp, 999L);
        assertThrowsCorruptingCommit(
                advance, advanceResult, 31L, "disagrees with its time index");
        assertEquals(900L, store.loadCutoff(), "a failed corrupting commit writes nothing");
        removeVictim(staleTimestamp);

        // A time index key whose record is missing.
        final ClientInvocationId missingRecord = invocation(231L);
        commitVictim(missingRecord, "/corrupt-2", 32L);
        try (WriteBatch batch = new WriteBatch()) {
            batch.delete(resultsHandle, invocationKey(missingRecord));
            db.write(writeOptions, batch);
        }
        assertThrowsCorruptingCommit(advance, advanceResult, 33L, "Missing result record");
        removeVictim(missingRecord);

        // A record that no longer decodes.
        final ClientInvocationId garbageRecord = invocation(232L);
        commitVictim(garbageRecord, "/corrupt-3", 34L);
        try (WriteBatch batch = new WriteBatch()) {
            batch.put(resultsHandle, invocationKey(garbageRecord), new byte[] {9, 9, 9});
            db.write(writeOptions, batch);
        }
        assertThrowsCorruptingCommit(
                advance, advanceResult, 35L, "Truncated invocation result record");
        removeVictim(garbageRecord);

        // A time index key whose value does not rebuild the invocation key.
        final ClientInvocationId tamperedIndex = invocation(233L);
        commitVictim(tamperedIndex, "/corrupt-4", 36L);
        try (WriteBatch batch = new WriteBatch()) {
            batch.put(resultsHandle, timeKey(1_000L, tamperedIndex), new byte[] {1});
            db.write(writeOptions, batch);
        }
        assertThrowsCorruptingCommit(
                advance, advanceResult, 37L, "does not rebuild its invocation key");
        removeVictim(tamperedIndex);
        assertEquals(900L, store.loadCutoff(), "no corrupting commit ever reached the database");
    }

    @Test
    void testOldGenerationReplayAfterNewerGenerationReturnsExpiredAndNewReply() throws Exception {
        final ClientInvocationId invocation = invocation(240L);
        final RaftCommand command = RaftCommand.put("/generation", bytes("g"), 0, 1L);
        final MetadataLogEnvelope firstEnvelope = envelope(command, 1_000L, 100L);
        final byte[] firstResult =
                RaftCommandCodec.encodeResult(RaftResult.ok(RaftCommand.Op.PUT).version(1L));
        commit(store.lookup(invocation, TermIndex.valueOf(1L, 60L), firstEnvelope), firstResult, 60L);

        final ClientInvocationId cutoffWriter = invocation(241L);
        commit(
                store.lookup(
                        cutoffWriter,
                        TermIndex.valueOf(1L, 61L),
                        envelope(RaftCommand.put("/generation-cutoff", bytes("w"), 0, 1L), 2_000L, 100L)),
                RaftCommandCodec.encodeResult(RaftResult.ok(RaftCommand.Op.PUT).version(1L)),
                61L);
        assertNull(
                db.get(resultsHandle, invocationKey(invocation)),
                "the first generation was collected below the advanced cutoff");

        // A post-retention retry legitimately runs as a new invocation (documented unsupported
        // boundary): the durable record's generation moves to the newer index.
        final MetadataLogEnvelope secondEnvelope = envelope(command, 2_500L, 100L);
        final byte[] secondResult =
                RaftCommandCodec.encodeResult(RaftResult.ok(RaftCommand.Op.PUT).version(2L));
        final InvocationResultStore.Disposition second =
                store.lookup(invocation, TermIndex.valueOf(1L, 62L), secondEnvelope);
        assertEquals(InvocationResultStore.Disposition.Kind.EXECUTE, second.getKind());
        commit(second, secondResult, 62L);
        assertRecord(invocation, 1L, 62L, 2_500L, 2_500L, secondEnvelope, secondResult);

        final InvocationResultStore.Disposition oldGenerationReplay =
                store.lookup(invocation, TermIndex.valueOf(1L, 60L), firstEnvelope);
        assertEquals(
                InvocationResultStore.Disposition.Kind.RESULT_EXPIRED, oldGenerationReplay.getKind());
        assertEquals(RaftResult.Status.RESULT_EXPIRED, decode(oldGenerationReplay).getStatus());
        commit(oldGenerationReplay, null, 63L);
        assertRecord(
                invocation,
                1L,
                62L,
                2_500L,
                2_500L,
                secondEnvelope,
                secondResult,
                "the expired replay staged nothing over generation two");
        assertEquals(2_400L, store.loadCutoff(), "the expired replay does not advance the cutoff");

        assertArrayEquals(
                secondResult,
                store.lookup(invocation, TermIndex.valueOf(1L, 62L), secondEnvelope)
                        .getEncodedResult());
        assertArrayEquals(
                secondResult,
                store.lookup(invocation, TermIndex.valueOf(1L, 64L), secondEnvelope)
                        .getEncodedResult(),
                "a within-contract duplicate still returns generation two's original reply");
    }

    private void commitVictim(ClientInvocationId victim, String path, long revision)
            throws Exception {
        commit(
                store.lookup(
                        victim,
                        TermIndex.valueOf(1L, revision),
                        envelope(RaftCommand.put(path, bytes("v"), 0, 1L), 1_000L, 100L)),
                RaftCommandCodec.encodeResult(RaftResult.ok(RaftCommand.Op.PUT).version(1L)),
                revision);
    }

    /**
     * The committing invocation's cutoff 1_900 must sweep the one inconsistent 1_000 victim and
     * fail with the caller's corruption mode: the expected message fragment pins the assertion to
     * exactly one production corruption check, so deleting any single check fails only its own
     * mode (a leftover victim from another mode can no longer satisfy the assertThrows).
     */
    private void assertThrowsCorruptingCommit(
            RaftCommand advance, byte[] result, long revision, String expectedMessage) {
        final IOException failure =
                assertThrows(
                        IOException.class,
                        () ->
                                commit(
                                        store.lookup(
                                                invocation(250L + (int) (revision - 31L)),
                                                TermIndex.valueOf(1L, revision),
                                                envelope(advance, 2_000L, 100L)),
                                        result,
                                        revision),
                        "an inconsistent unrefreshed index fails as corruption, never silently skipped");
        assertTrue(
                failure.getMessage().contains(expectedMessage),
                "expected corruption <"
                        + expectedMessage
                        + "> but was <"
                        + failure.getMessage()
                        + ">");
    }

    /** Deletes a mode's victim so each assertThrows sees exactly its own corruption. */
    private void removeVictim(ClientInvocationId victim) throws Exception {
        try (WriteBatch batch = new WriteBatch()) {
            batch.delete(resultsHandle, invocationKey(victim));
            batch.delete(resultsHandle, timeKey(1_000L, victim));
            db.write(writeOptions, batch);
        }
    }

    /** Rewrites only the embedded retentionTime, leaving the time index key untouched. */
    private void patchRecordRetentionTime(ClientInvocationId victim, long patchedRetentionTime)
            throws Exception {
        final byte[] encoded = db.get(resultsHandle, invocationKey(victim));
        final byte[] patched = encoded.clone();
        // Layout: version(1) firstTerm(8) firstIndex(8) firstSeen(8) retentionTime(8) ...
        ByteBuffer.wrap(patched, 1 + 3 * Long.BYTES, Long.BYTES).putLong(patchedRetentionTime);
        try (WriteBatch batch = new WriteBatch()) {
            batch.put(resultsHandle, invocationKey(victim), patched);
            db.write(writeOptions, batch);
        }
    }

    private void commit(
            InvocationResultStore.Disposition disposition, byte[] firstResult, long revision)
            throws Exception {
        try (WriteBatch batch = new WriteBatch()) {
            store.stageRetention(batch, disposition);
            store.stageResult(batch, disposition, firstResult);
            batch.put(defaultHandle, APPLIED_REVISION_KEY, longBytes(revision));
            db.write(writeOptions, batch);
        }
    }

    private void setMarker(long revision) throws Exception {
        try (WriteBatch batch = new WriteBatch()) {
            batch.put(defaultHandle, APPLIED_REVISION_KEY, longBytes(revision));
            db.write(writeOptions, batch);
        }
    }

    private void assertRecord(
            ClientInvocationId invocation,
            long firstTerm,
            long firstIndex,
            long firstSeen,
            long retentionTime,
            MetadataLogEnvelope originalEnvelope,
            byte[] originalResult)
            throws Exception {
        assertRecord(
                invocation,
                firstTerm,
                firstIndex,
                firstSeen,
                retentionTime,
                originalEnvelope,
                originalResult,
                "the record is present");
    }

    private void assertRecord(
            ClientInvocationId invocation,
            long firstTerm,
            long firstIndex,
            long firstSeen,
            long retentionTime,
            MetadataLogEnvelope originalEnvelope,
            byte[] originalResult,
            String message)
            throws Exception {
        final byte[] encoded = db.get(resultsHandle, invocationKey(invocation));
        assertTrue(encoded != null, message);
        final ByteBuffer in = ByteBuffer.wrap(encoded);
        assertEquals(1, in.get() & 0xFF);
        assertEquals(firstTerm, in.getLong());
        assertEquals(firstIndex, in.getLong());
        assertEquals(firstSeen, in.getLong());
        assertEquals(retentionTime, in.getLong());
        final byte[] digest = new byte[32];
        in.get(digest);
        assertArrayEquals(originalEnvelope.getRawCommandDigest(), digest);
        final int resultLength = in.getInt();
        assertEquals(resultLength, in.remaining());
        final byte[] result = new byte[resultLength];
        in.get(result);
        assertArrayEquals(originalResult, result);
    }

    private static RaftResult decode(InvocationResultStore.Disposition disposition) {
        final RaftResult result = RaftCommandCodec.decodeResult(disposition.getEncodedResult());
        assertTrue(result != null, "disposition result must decode");
        return result;
    }

    private static ClientInvocationId invocation(long callId) {
        return ClientInvocationId.valueOf(CLIENT_ID, callId);
    }

    private static MetadataLogEnvelope envelope(
            RaftCommand command, long serverTime, long retentionMillis) {
        return MetadataLogEnvelope.create(
                RaftCommandCodec.encodeCommand(command), clockAt(serverTime), retentionMillis);
    }

    private static Clock clockAt(long milliseconds) {
        return new Clock() {
            @Override
            public long milliseconds() {
                return milliseconds;
            }

            @Override
            public long nanoseconds() {
                return TimeUnit.MILLISECONDS.toNanos(milliseconds);
            }
        };
    }

    private static byte[] invocationKey(ClientInvocationId invocation) {
        final byte[] clientId = invocation.getClientId().toByteString().toByteArray();
        return ByteBuffer.allocate(1 + 16 + 8)
                .put(RECORD_KEY_TAG)
                .put(clientId)
                .putLong(invocation.getLongId() ^ Long.MIN_VALUE)
                .array();
    }

    private static byte[] timeKey(long retentionTime, ClientInvocationId invocation) {
        final byte[] clientId = invocation.getClientId().toByteString().toByteArray();
        return ByteBuffer.allocate(1 + 8 + 16 + 8)
                .put(TIME_KEY_TAG)
                .putLong(retentionTime ^ Long.MIN_VALUE)
                .put(clientId)
                .putLong(invocation.getLongId() ^ Long.MIN_VALUE)
                .array();
    }

    private static long decodeSortableLong(byte[] source, int offset) {
        return ByteBuffer.wrap(source, offset, 8).getLong() ^ Long.MIN_VALUE;
    }

    private static byte[] longBytes(long value) {
        return ByteBuffer.allocate(8).putLong(value).array();
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
