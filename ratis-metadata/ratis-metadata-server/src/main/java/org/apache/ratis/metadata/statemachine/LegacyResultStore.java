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

import org.apache.ratis.metadata.protocol.RaftCommandCodec;
import org.apache.ratis.metadata.protocol.RaftResult;
import org.apache.ratis.protocol.ClientInvocationId;
import org.apache.ratis.server.protocol.TermIndex;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 * Durable invocation results and their deterministic replay selection. This class reads RocksDB
 * and stages into caller-owned batches; the state machine remains the sole writer and batch owner.
 *
 * <p>Legacy path: serves only the pre-redesign {@link MetadataStateMachine}; the canonical
 * {@link InvocationResultStore} owns the layout-3 result contract. Removed together with the
 * legacy state machine.</p>
 */
final class LegacyResultStore {

    private static final byte[] CUTOFF_KEY = {0};
    private static final byte RECORD_KEY_TAG = 1;
    private static final byte TIME_KEY_TAG = 2;
    private static final int CLIENT_ID_LENGTH = 16;
    private static final int RECORD_VERSION = 1;
    private static final int RECORD_HEADER_LENGTH =
            1 + 4 * Long.BYTES + MetadataLogEnvelope.DIGEST_LENGTH + Integer.BYTES;

    /**
     * One apply's share of expired-result collection. A write burst followed by an idle
     * retention window can put the whole backlog below the cutoff at once; without a cap the
     * first write back carries all of it - unbounded batch work and native memory on the
     * applier. The survivors stay candidates; the next applies resume at them.
     */
    static final int MAX_COLLECTIONS_PER_APPLY = 1024;

    private final RocksDB db;
    private final ColumnFamilyHandle defaultHandle;
    private final ColumnFamilyHandle resultsHandle;

    /**
     * Lower bound on every live time index's retention time, maintained from staged and
     * committed state alone: each staged result lowers it, each collection scan raises it to
     * the first key it leaves at or above the cutoff. {@link Long#MIN_VALUE} means unknown -
     * the first scan after open establishes it. Only the applier thread touches this store,
     * so the floor needs no synchronization.
     */
    private long earliestLiveRetentionTime = Long.MIN_VALUE;

    LegacyResultStore(
            RocksDB db, ColumnFamilyHandle defaultHandle, ColumnFamilyHandle resultsHandle) {
        this.db = Objects.requireNonNull(db, "db");
        this.defaultHandle = Objects.requireNonNull(defaultHandle, "defaultHandle");
        this.resultsHandle = Objects.requireNonNull(resultsHandle, "resultsHandle");
    }

    /** The durable outcome selected before any business execution or term fence. */
    static final class Disposition {
        enum Kind {
            EXECUTE,
            RETURN_RESULT,
            RESULT_EXPIRED
        }

        private final Kind kind;
        private final ClientInvocationId invocation;
        private final TermIndex termIndex;
        private final MetadataLogEnvelope envelope;
        private final Record previous;
        private final byte[] invocationKey;
        private final byte[] encodedResult;
        private final long effectiveCutoff;
        private final long retentionTime;

        @SuppressWarnings("checkstyle:ParameterNumber")
        private Disposition(Kind kind, ClientInvocationId invocation, TermIndex termIndex, MetadataLogEnvelope envelope,
                Record previous, byte[] invocationKey, byte[] encodedResult, long effectiveCutoff, long retentionTime) {
            this.kind = kind;
            this.invocation = invocation;
            this.termIndex = termIndex;
            this.envelope = envelope;
            this.previous = previous;
            this.invocationKey = invocationKey;
            this.encodedResult = encodedResult == null ? null : encodedResult.clone();
            this.effectiveCutoff = effectiveCutoff;
            this.retentionTime = retentionTime;
        }

        Kind getKind() {
            return kind;
        }

        byte[] getEncodedResult() {
            return encodedResult == null ? null : encodedResult.clone();
        }

        long getEffectiveCutoff() {
            return effectiveCutoff;
        }

        long getRetentionTime() {
            return retentionTime;
        }
    }

    /** Immutable first-execution fields plus the mutable logical retention timestamp. */
    private static final class Record {
        private final byte[] encodedResult;
        private final long firstTerm;
        private final long firstIndex;
        private final long firstSeen;
        private final long retentionTime;
        private final byte[] rawDigest;

        private Record(byte[] encodedResult, long firstTerm, long firstIndex, long firstSeen, long retentionTime,
                       byte[] rawDigest) {
            this.encodedResult = encodedResult.clone();
            this.firstTerm = firstTerm;
            this.firstIndex = firstIndex;
            this.firstSeen = firstSeen;
            this.retentionTime = retentionTime;
            this.rawDigest = rawDigest.clone();
        }

        private Record refresh(long newRetentionTime) {
            return new Record(encodedResult, firstTerm, firstIndex, firstSeen, newRetentionTime, rawDigest);
        }

        private byte[] encode() {
            return ByteBuffer.allocate(RECORD_HEADER_LENGTH + encodedResult.length)
                    .put((byte) RECORD_VERSION)
                    .putLong(firstTerm)
                    .putLong(firstIndex)
                    .putLong(firstSeen)
                    .putLong(retentionTime)
                    .put(rawDigest)
                    .putInt(encodedResult.length)
                    .put(encodedResult)
                    .array();
        }

        private static Record decode(byte[] encoded) throws IOException {
            if (encoded == null || encoded.length < RECORD_HEADER_LENGTH) {
                throw new IOException("Truncated invocation result record");
            }
            try {
                final ByteBuffer in = ByteBuffer.wrap(encoded);
                final int version = in.get() & 0xFF;
                if (version != RECORD_VERSION) {
                    throw new IOException("Unknown invocation result record version " + version);
                }
                final long firstTerm = in.getLong();
                final long firstIndex = in.getLong();
                final long firstSeen = in.getLong();
                final long retentionTime = in.getLong();
                final byte[] digest = new byte[MetadataLogEnvelope.DIGEST_LENGTH];
                in.get(digest);
                final int resultLength = in.getInt();
                if (resultLength <= 0 || resultLength != in.remaining()) {
                    throw new IOException("Invalid encoded result length " + resultLength
                            + " with " + in.remaining() + " bytes remaining");
                }
                if (firstTerm < 0 || firstIndex < 0) {
                    throw new IOException("Invalid first term/index " + firstTerm + "/" + firstIndex);
                }
                final byte[] result = new byte[resultLength];
                in.get(result);
                if (RaftCommandCodec.decodeResult(result) == null) {
                    throw new IOException("Invocation record contains an invalid encoded result");
                }
                return new Record(result, firstTerm, firstIndex, firstSeen, retentionTime, digest);
            } catch (RuntimeException malformed) {
                throw new IOException("Malformed invocation result record", malformed);
            }
        }
    }

    /**
     * Selects the transition using only durable pre-transition marker, cutoff, and result
     * state. The {@code appliedRevision} caller argument is that durable marker: the state
     * machine keeps it equal to the persisted applied-revision key - loaded on open, advanced
     * right after each durable write, always on this store's single applier thread.
     */
    Disposition lookup(ClientInvocationId invocation, TermIndex termIndex, MetadataLogEnvelope envelope,
            long appliedRevision) throws IOException, RocksDBException {
        Objects.requireNonNull(invocation, "invocation");
        Objects.requireNonNull(termIndex, "termIndex");
        Objects.requireNonNull(envelope, "envelope");

        final byte[] invocationKey = invocationKey(invocation);
        final long persistedCutoff = loadCutoff();
        final long effectiveCutoff = Math.max(persistedCutoff, envelope.getProposedCutoffMillis());
        final long retentionTime = Math.max(envelope.getServerTimeMillis(), effectiveCutoff);
        final boolean replay = termIndex.getIndex() <= appliedRevision;
        final byte[] encodedRecord = db.get(resultsHandle, invocationKey);

        if (encodedRecord == null) {
            if (replay) {
                if (envelope.getServerTimeMillis() < persistedCutoff) {
                    return expired(
                            invocation, termIndex, envelope, null, invocationKey, effectiveCutoff, retentionTime);
                }
                throw new IOException("Missing unexpired invocation result for " + invocation
                                + " at replay revision " + termIndex.getIndex()
                                + " with serverTime " + envelope.getServerTimeMillis()
                                + " and persisted cutoff " + persistedCutoff);
            }
            return new Disposition(Disposition.Kind.EXECUTE, invocation, termIndex, envelope, null, invocationKey,
                    null, effectiveCutoff, retentionTime);
        }

        final Record record = Record.decode(encodedRecord);
        verifyCurrentTimeIndex(invocation, invocationKey, record.retentionTime);
        // A refresh may only extend retention. A duplicate preprocessed under a lagging leader
        // clock must never pull the record below the retention its committed predecessors chose:
        // collection deletes strictly below the cutoff, so a lowered retention can drop the record
        // while a replay of its own log entry still expects an unexpired result.
        final long refreshedRetentionTime = Math.max(record.retentionTime, retentionTime);
        if (replay && record.firstIndex > termIndex.getIndex()) {
            return expired(invocation, termIndex, envelope, record, invocationKey, effectiveCutoff, retentionTime);
        }

        final byte[] selected = Arrays.equals(record.rawDigest, envelope.getRawCommandDigest()) ? record.encodedResult
                : RaftCommandCodec.encodeResult(RaftResult.error(
                envelope.getCommand().getOp(), RaftResult.Status.INVOCATION_MISMATCH));
        return new Disposition(Disposition.Kind.RETURN_RESULT, invocation, termIndex, envelope, record, invocationKey,
                selected, effectiveCutoff, refreshedRetentionTime);
    }

    /**
     * Stages, against pre-transition state only: the deterministic collection of expired results,
     * the cutoff advance, and the current invocation's old time-index deletion, in that order.
     * Collection is driven exclusively by the disposition's replicated effective cutoff - never a
     * local clock - and only a candidate strictly below the cutoff is collected. The caller stages
     * the refreshed/new record and its new time index afterwards via {@link #stageResult}.
     */
    void stageRetention(WriteBatch batch, Disposition disposition)
            throws IOException, RocksDBException {
        Objects.requireNonNull(batch, "batch");
        Objects.requireNonNull(disposition, "disposition");
        if (disposition.kind == Disposition.Kind.RESULT_EXPIRED) {
            return;
        }
        stageExpiredCandidateDeletions(batch, disposition);
        batch.put(resultsHandle, CUTOFF_KEY, longBytes(disposition.effectiveCutoff));
        if (disposition.previous != null) {
            batch.delete(resultsHandle, timeKey(disposition.previous.retentionTime, disposition.invocation));
        }
    }

    /**
     * Stages a first result or an immutable-field-preserving refresh, and last its new
     * time-index key, without committing either. The refreshed retention is floored at the
     * existing record's retention in {@link #lookup}, so the time index only moves forward.
     *
     * <p>The {@code firstExecutionResult} bytes must be {@link RaftCommandCodec#encodeResult}
     * output from this same apply; the record stores them verbatim and only the reply path
     * ever decodes them, so validity is the caller's precondition rather than re-parsed here.
     */
    void stageResult(WriteBatch batch, Disposition disposition, byte[] firstExecutionResult) throws RocksDBException {
        Objects.requireNonNull(batch, "batch");
        Objects.requireNonNull(disposition, "disposition");
        if (disposition.kind == Disposition.Kind.RESULT_EXPIRED) {
            return;
        }
        final Record result;
        if (disposition.previous != null) {
            result = disposition.previous.refresh(disposition.retentionTime);
        } else {
            if (firstExecutionResult == null) {
                throw new IllegalArgumentException("First execution requires an encoded RaftResult");
            }
            result = new Record(firstExecutionResult, disposition.termIndex.getTerm(), disposition.termIndex.getIndex(),
                    disposition.envelope.getServerTimeMillis(), disposition.retentionTime,
                    disposition.envelope.getRawCommandDigest());
        }
        batch.put(resultsHandle, disposition.invocationKey, result.encode());
        batch.put(resultsHandle, timeKey(disposition.retentionTime, disposition.invocation), disposition.invocationKey);
        earliestLiveRetentionTime = Math.min(earliestLiveRetentionTime, disposition.retentionTime);
    }

    /**
     * Deletes every result whose time index sits strictly below the disposition's effective
     * cutoff. The currently refreshed invocation is never an ordinary candidate: its old time
     * key leaves via the refresh staging itself, so both a matching and a mismatching refresh
     * survive their own transition. Any inconsistency on a non-refreshed key the scan reaches -
     * a malformed or value-mismatching index key, a missing or undecodable record, or a record
     * whose embedded retention time disagrees with its index - fails as corruption and is never
     * silently skipped. A scan the retention floor proves empty is skipped wholesale, so an
     * inconsistency that appears above the floor surfaces only once collection catches up to it.
     */
    private void stageExpiredCandidateDeletions(WriteBatch batch, Disposition disposition)
            throws IOException, RocksDBException {
        final long effectiveCutoff = disposition.effectiveCutoff;
        if (earliestLiveRetentionTime >= effectiveCutoff) {
            // No live time index can sit below the cutoff: the scan would collect nothing,
            // so skip its native iterator entirely - this runs once per committed entry.
            return;
        }
        final boolean refreshing = disposition.previous != null;
        try (RocksIterator iterator = db.newIterator(resultsHandle)) {
            int collected = 0;
            boolean drained = true;
            // Time keys are the highest tag, so this seek lands on the first one; they sort by
            // retention time first, so iteration can stop at the first key at or above the cutoff.
            iterator.seek(new byte[]{TIME_KEY_TAG});
            while (iterator.isValid()) {
                final byte[] timeKey = iterator.key();
                if (timeKey.length == 0 || timeKey[0] != TIME_KEY_TAG) {
                    break;
                }
                if (timeKey.length != 1 + Long.BYTES + CLIENT_ID_LENGTH + Long.BYTES) {
                    throw new IOException("Malformed time index key of length " + timeKey.length + " below cutoff "
                            + effectiveCutoff);
                }
                final long candidateTime = sortableLong(timeKey, 1);
                if (candidateTime >= effectiveCutoff) {
                    // Keys sort by retention time first; nothing further is strictly below.
                    break;
                }
                final byte[] recordKey = invocationKeyFromTimeKey(timeKey);
                if (refreshing && Arrays.equals(recordKey, disposition.invocationKey)) {
                    iterator.next();
                    continue;
                }
                if (!Arrays.equals(recordKey, iterator.value())) {
                    throw new IOException("Time index at retentionTime " + candidateTime
                            + " does not rebuild its invocation key");
                }
                final byte[] encodedRecord = db.get(resultsHandle, recordKey);
                if (encodedRecord == null) {
                    throw new IOException("Missing result record for the time index at retentionTime " + candidateTime);
                }
                final Record record = Record.decode(encodedRecord);
                if (record.retentionTime != candidateTime) {
                    throw new IOException("Result record retentionTime " + record.retentionTime
                            + " disagrees with its time index " + candidateTime);
                }
                batch.delete(resultsHandle, timeKey);
                batch.delete(resultsHandle, recordKey);
                if (++collected >= MAX_COLLECTIONS_PER_APPLY) {
                    drained = false;
                    break;
                }
                iterator.next();
            }
            // isValid()==false also covers a scan stopped by a storage error; only status()
            // tells that apart from a clean end, so a collection never commits off a partial
            // scan (the floor logic below relies on having truly seen the last candidate).
            iterator.status();
            // Where the loop stopped - the first time key at or above the cutoff, or past the
            // time keys - no key below the cutoff survives this batch, so the floor rises to
            // it. A truncated or failed scan leaves the floor untouched: the uncollected
            // candidates below the cutoff still live, and the next apply's seek lands on them
            // (the collected keys are gone) - the floor stays a lower bound either way.
            if (drained) {
                if (iterator.isValid() && iterator.key().length > 0 && iterator.key()[0] == TIME_KEY_TAG) {
                    earliestLiveRetentionTime = sortableLong(iterator.key(), 1);
                } else {
                    earliestLiveRetentionTime = Long.MAX_VALUE;
                }
            }
        }
    }

    /** Loads the replicated monotone result cutoff; absent means no expiration has been proposed. */
    long loadCutoff() throws RocksDBException {
        final byte[] encoded = db.get(resultsHandle, CUTOFF_KEY);
        if (encoded == null) {
            return Long.MIN_VALUE;
        }
        if (encoded.length != Long.BYTES) {
            throw new RocksDBException("Corrupt invocation result cutoff: expected 8 bytes, got " + encoded.length);
        }
        return ByteBuffer.wrap(encoded).getLong();
    }

    private void verifyCurrentTimeIndex(ClientInvocationId invocation, byte[] invocationKey, long retentionTime)
            throws IOException, RocksDBException {
        final byte[] indexed = db.get(resultsHandle, timeKey(retentionTime, invocation));
        if (!Arrays.equals(invocationKey, indexed)) {
            throw new IOException("Missing or mismatching time index for " + invocation
                    + " at retentionTime " + retentionTime);
        }
    }

    private static Disposition expired(ClientInvocationId invocation, TermIndex termIndex, MetadataLogEnvelope envelope,
            Record previous, byte[] invocationKey, long effectiveCutoff, long retentionTime) {
        return new Disposition(Disposition.Kind.RESULT_EXPIRED, invocation, termIndex, envelope, previous,
                invocationKey, RaftCommandCodec.encodeResult(RaftResult.error(
                        envelope.getCommand().getOp(), RaftResult.Status.RESULT_EXPIRED)),
                effectiveCutoff, retentionTime);
    }

    private static byte[] invocationKey(ClientInvocationId invocation) throws IOException {
        final byte[] clientId = invocation.getClientId().toByteString().toByteArray();
        if (clientId.length != CLIENT_ID_LENGTH) {
            throw new IOException("Invalid clientId width " + clientId.length + " for " + invocation);
        }
        return ByteBuffer.allocate(1 + CLIENT_ID_LENGTH + Long.BYTES)
                .put(RECORD_KEY_TAG)
                .put(clientId)
                .putLong(invocation.getLongId() ^ Long.MIN_VALUE)
                .array();
    }

    private static byte[] timeKey(long retentionTime, ClientInvocationId invocation) {
        final byte[] clientId = invocation.getClientId().toByteString().toByteArray();
        if (clientId.length != CLIENT_ID_LENGTH) {
            // lookup validates this before a Disposition can be constructed.
            throw new IllegalArgumentException("Invalid clientId width " + clientId.length + " for " + invocation);
        }
        return ByteBuffer.allocate(1 + Long.BYTES + CLIENT_ID_LENGTH + Long.BYTES)
                .put(TIME_KEY_TAG)
                .putLong(retentionTime ^ Long.MIN_VALUE)
                .put(clientId)
                .putLong(invocation.getLongId() ^ Long.MIN_VALUE)
                .array();
    }

    /** Rebuilds the record key a time index key addresses: the same clientId and call id. */
    private static byte[] invocationKeyFromTimeKey(byte[] timeKey) {
        final byte[] recordKey = new byte[1 + CLIENT_ID_LENGTH + Long.BYTES];
        recordKey[0] = RECORD_KEY_TAG;
        System.arraycopy(timeKey, 1 + Long.BYTES, recordKey, 1, CLIENT_ID_LENGTH + Long.BYTES);
        return recordKey;
    }

    /** Decodes a sign-flipped big-endian long written by {@link #timeKey} back to its value. */
    private static long sortableLong(byte[] source, int offset) {
        return ByteBuffer.wrap(source, offset, Long.BYTES).getLong() ^ Long.MIN_VALUE;
    }

    private static byte[] longBytes(long value) {
        return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
    }
}
