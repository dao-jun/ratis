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

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import org.apache.ratis.metadata.proto.MetadataProtos.Lane;
import org.apache.ratis.metadata.protocol.Stamp;

/**
 * The layout-version-3 record codec: every key and value of every column
 * family, encoded and decoded strictly. All integers are big-endian; all
 * lengths are checked and every record must be consumed exactly - trailing
 * bytes, a wrong codec version, a negative value or an invalid enum are all
 * storage corruption. {@link NodeRecord} and {@link StoredResult} carry a
 * trailing SHA-256 over all preceding bytes; the RocksDB block checksum
 * cannot substitute for that application-level identity check.
 */
final class StoredRecordCodec {
    /** The single codec version readable by this layout. */
    static final int CODEC_VERSION = 1;
    /** The fixed width of the trailing record checksums. */
    static final int CHECKSUM_BYTES = 32;

    private static final int WRITER_ID_BYTES = 16;
    private static final int NODE_FIXED_BYTES = 86;
    private static final int RESULT_FIXED_BYTES = 69;

    private StoredRecordCodec() {
    }

    // ---- default-CF marker values ----

    /** The {@code group-config} value: canonical proto bytes then the 32-byte fingerprint. */
    static byte[] groupConfigValue(byte[] canonicalProto, byte[] fingerprint) {
        if (fingerprint.length != CHECKSUM_BYTES) {
            throw MetadataStoreException.corrupt("fingerprint must be 32 bytes");
        }
        return concat(canonicalProto, fingerprint);
    }

    /** @return the canonical GroupConfig protobuf bytes, after the fingerprint check. */
    static byte[] decodeGroupConfig(byte[] value) {
        if (value.length <= CHECKSUM_BYTES) {
            throw corruptLength("group-config", value.length);
        }
        final int protoLength = value.length - CHECKSUM_BYTES;
        final byte[] proto = Arrays.copyOf(value, protoLength);
        final byte[] fingerprint = Arrays.copyOfRange(value, protoLength, value.length);
        if (!Arrays.equals(fingerprint, sha256(proto))) {
            throw MetadataStoreException.corrupt("group-config fingerprint mismatch");
        }
        return proto;
    }

    static byte[] appliedValue(long term, long index) {
        requireNonNegative("applied term", term);
        requireNonNegative("applied index", index);
        final ByteBuffer out = header(17);
        out.putLong(term).putLong(index);
        return finish(out);
    }

    /** @return {@code {term, index}}. */
    static long[] decodeApplied(byte[] value) {
        final ByteBuffer in = open(value, "applied", 17);
        final long term = in.getLong();
        final long index = in.getLong();
        requireNonNegative("applied term", term);
        requireNonNegative("applied index", index);
        return new long[]{term, index};
    }

    static byte[] logicalTimeValue(long logicalNow) {
        requireNonNegative("logicalNow", logicalNow);
        return counterValue(logicalNow);
    }

    static long decodeLogicalTime(byte[] value) {
        final ByteBuffer in = open(value, "logical-time", 9);
        final long logicalNow = in.getLong();
        requireNonNegative("logicalNow", logicalNow);
        return logicalNow;
    }

    static byte[] rootStampValue(Stamp stamp) {
        final ByteBuffer out = header(13);
        writeStamp(out, stamp);
        return finish(out);
    }

    static Stamp decodeRootStamp(byte[] value) {
        final ByteBuffer in = open(value, "root-stamp", 13);
        return readStamp(in);
    }

    static byte[] changeFloorValue(long floorRevision) {
        requireNonNegative("changeFloor", floorRevision);
        final ByteBuffer out = header(9);
        out.putLong(floorRevision);
        return finish(out);
    }

    static long decodeChangeFloor(byte[] value) {
        final ByteBuffer in = open(value, "change-floor", 9);
        final long floor = in.getLong();
        requireNonNegative("changeFloor", floor);
        return floor;
    }

    static byte[] statsValue(StoreStats stats) {
        requireValidStats(stats);
        final ByteBuffer out = header(49);
        out.putLong(stats.businessResultBytes())
                .putLong(stats.controlResultBytes())
                .putLong(stats.visibleChangeBytes())
                .putLong(stats.physicalChangeBytes())
                .putLong(stats.writerCount())
                .putLong(stats.leaseCount());
        return finish(out);
    }

    static StoreStats decodeStats(byte[] value) {
        final ByteBuffer in = open(value, "stats", 49);
        final StoreStats stats = new StoreStats(in.getLong(), in.getLong(), in.getLong(),
                in.getLong(), in.getLong(), in.getLong());
        requireValidStats(stats);
        return stats;
    }

    // ---- row values ----

    static byte[] nodeValue(NodeRecord record) {
        final ByteBuffer out = header(NODE_FIXED_BYTES + record.value().length + CHECKSUM_BYTES);
        out.put((byte) kindCode(record.kind()));
        writeStamp(out, record.createStamp());
        writeStamp(out, record.modifyStamp());
        out.putLong(record.leaseId()).put(record.ownerWriterId())
                .putLong(record.ownerGeneration()).putLong(record.lockIndex())
                .putLong(record.createdAt()).putLong(record.modifiedAt())
                .putInt(record.value().length).put(record.value());
        final byte[] prefix = Arrays.copyOf(out.array(), out.position());
        out.put(sha256(prefix));
        return finish(out);
    }

    static NodeRecord decodeNode(byte[] value) {
        if (value.length < NODE_FIXED_BYTES + CHECKSUM_BYTES) {
            throw corruptLength("node", value.length);
        }
        final ByteBuffer in = open(value, "node", 0);
        final byte kindByte = in.get();
        final Stamp createStamp = readStamp(in);
        final Stamp modifyStamp = readStamp(in);
        final long leaseId = in.getLong();
        final byte[] owner = new byte[WRITER_ID_BYTES];
        in.get(owner);
        final long ownerGeneration = in.getLong();
        final long lockIndex = in.getLong();
        final long createdAt = in.getLong();
        final long modifiedAt = in.getLong();
        final int valueLength = in.getInt();
        if (valueLength < 0 || in.remaining() != valueLength + CHECKSUM_BYTES) {
            throw MetadataStoreException.corrupt(
                    "node value length " + valueLength + " disagrees with record size");
        }
        final byte[] nodeValue = new byte[valueLength];
        in.get(nodeValue);
        final byte[] checksum = new byte[CHECKSUM_BYTES];
        in.get(checksum);
        final byte[] prefix = Arrays.copyOf(value, value.length - CHECKSUM_BYTES);
        if (!Arrays.equals(checksum, sha256(prefix))) {
            throw MetadataStoreException.corrupt("node record checksum mismatch");
        }
        return NodeRecord.newBuilder()
                .setKind(kindOf(kindByte))
                .setCreateStamp(createStamp)
                .setModifyStamp(modifyStamp)
                .setLeaseOwnership(leaseId, owner, ownerGeneration)
                .setLockIndex(lockIndex)
                .setCreatedAt(createdAt)
                .setModifiedAt(modifiedAt)
                .setValue(nodeValue)
                .build();
    }

    static byte[] counterValue(long value) {
        final ByteBuffer out = header(9);
        out.putLong(value);
        return finish(out);
    }

    static long decodeCounter(byte[] value) {
        final ByteBuffer in = open(value, "counter", 9);
        return in.getLong(); // signed by design: counters may go down
    }

    static byte[] writerValue(WriterRow row) {
        final ByteBuffer out = header(18);
        out.put(laneCode(row.lane()))
                .putLong(row.currentGeneration())
                .putLong(row.highWater());
        return finish(out);
    }

    static WriterRow decodeWriter(byte[] value) {
        final ByteBuffer in = open(value, "writer", 18);
        final WriterRow row = new WriterRow(laneOf(in.get()), in.getLong(), in.getLong());
        if (row.currentGeneration() < 1 || row.highWater() < 0) {
            throw MetadataStoreException.corrupt("invalid writer row " + row);
        }
        return row;
    }

    static byte[] leaseValue(LeaseRow row) {
        final ByteBuffer out = header(53);
        out.put(row.ownerWriterId())
                .putLong(row.ownerGeneration())
                .putLong(row.ttlMs())
                .putLong(row.expireAt())
                .putLong(row.lastRenewIndex())
                .putInt(row.bindingCount());
        return finish(out);
    }

    static LeaseRow decodeLease(byte[] value) {
        final ByteBuffer in = open(value, "lease", 53);
        final byte[] owner = new byte[WRITER_ID_BYTES];
        in.get(owner);
        final LeaseRow row = new LeaseRow(owner, in.getLong(), in.getLong(),
                in.getLong(), in.getLong(), in.getInt());
        if (row.ownerGeneration() < 1 || row.ttlMs() < 1 || row.expireAt() < 0
                || row.lastRenewIndex() < 0 || row.bindingCount() < 0) {
            throw MetadataStoreException.corrupt("invalid lease row " + row);
        }
        return row;
    }

    static byte[] leasePathValue() {
        return new byte[]{(byte) CODEC_VERSION};
    }

    static void decodeLeasePath(byte[] value) {
        open(value, "lease-path", 1);
    }

    static byte[] leaseExpiryValue(long lastRenewIndex) {
        requireNonNegative("lastRenewIndex", lastRenewIndex);
        final ByteBuffer out = header(9);
        out.putLong(lastRenewIndex);
        return finish(out);
    }

    static long decodeLeaseExpiry(byte[] value) {
        final ByteBuffer in = open(value, "lease-expiry", 9);
        final long lastRenewIndex = in.getLong();
        requireNonNegative("lastRenewIndex", lastRenewIndex);
        return lastRenewIndex;
    }

    static byte[] storedResultValue(StoredResult result) {
        final ByteBuffer out = header(
                RESULT_FIXED_BYTES + result.reply().length + CHECKSUM_BYTES);
        out.put(result.digest())
                .putLong(result.firstTerm())
                .putLong(result.firstIndex())
                .putLong(result.executedAt())
                .putLong(result.expiresAt())
                .putInt(result.reply().length)
                .put(result.reply());
        final byte[] prefix = Arrays.copyOf(out.array(), out.position());
        out.put(sha256(prefix));
        return finish(out);
    }

    static StoredResult decodeStoredResult(byte[] value) {
        if (value.length < RESULT_FIXED_BYTES + CHECKSUM_BYTES) {
            throw corruptLength("stored-result", value.length);
        }
        final ByteBuffer in = open(value, "stored-result", 0);
        final byte[] digest = new byte[StoredResult.DIGEST_BYTES];
        in.get(digest);
        final long firstTerm = in.getLong();
        final long firstIndex = in.getLong();
        final long executedAt = in.getLong();
        final long expiresAt = in.getLong();
        final int replyLength = in.getInt();
        if (replyLength < 0 || in.remaining() != replyLength + CHECKSUM_BYTES) {
            throw MetadataStoreException.corrupt(
                    "reply length " + replyLength + " disagrees with record size");
        }
        final byte[] reply = new byte[replyLength];
        in.get(reply);
        final byte[] checksum = new byte[CHECKSUM_BYTES];
        in.get(checksum);
        final byte[] prefix = Arrays.copyOf(value, value.length - CHECKSUM_BYTES);
        if (!Arrays.equals(checksum, sha256(prefix))) {
            throw MetadataStoreException.corrupt("stored-result checksum mismatch");
        }
        return StoredResult.newBuilder()
                .setDigest(digest)
                .setFirstTerm(firstTerm)
                .setFirstIndex(firstIndex)
                .setExecutedAt(executedAt)
                .setExpiresAt(expiresAt)
                .setReply(reply)
                .build();
    }

    static byte[] resultExpiryValue(long firstIndex) {
        requireNonNegative("firstIndex", firstIndex);
        final ByteBuffer out = header(9);
        out.putLong(firstIndex);
        return finish(out);
    }

    static long decodeResultExpiry(byte[] value) {
        final ByteBuffer in = open(value, "result-expiry", 9);
        final long firstIndex = in.getLong();
        requireNonNegative("firstIndex", firstIndex);
        return firstIndex;
    }

    static byte[] changeRevisionValue(int eventCount, long encodedBytes) {
        if (eventCount < 1) {
            throw MetadataStoreException.corrupt(
                    "a change-revisions row needs at least one event: " + eventCount);
        }
        requireNonNegative("encodedBytes", encodedBytes);
        final ByteBuffer out = header(13);
        out.putInt(eventCount).putLong(encodedBytes);
        return finish(out);
    }

    /** @return {@code {eventCount, encodedBytes}}. */
    static long[] decodeChangeRevision(byte[] value) {
        final ByteBuffer in = open(value, "change-revisions", 13);
        final long eventCount = in.getInt();
        final long encodedBytes = in.getLong();
        if (eventCount < 1 || encodedBytes < 0) {
            throw MetadataStoreException.corrupt(
                    "invalid change-revision row: " + eventCount + " events, " + encodedBytes);
        }
        return new long[]{eventCount, encodedBytes};
    }

    // ---- keys ----

    static byte[] userCounterKey(byte[] name) {
        final byte[] out = new byte[1 + name.length];
        out[0] = 0;
        System.arraycopy(name, 0, out, 1, name.length);
        return out;
    }

    static byte[] seqCounterKey(byte[] parentKey, Stamp parentCreateStamp) {
        final ByteBuffer out = ByteBuffer.allocate(1 + 4 + parentKey.length + 12);
        out.put((byte) 1).putInt(parentKey.length);
        out.put(parentKey);
        writeStamp(out, parentCreateStamp);
        return out.array();
    }

    static byte[] writerKey(byte[] writerId) {
        // the writers key is the bare writer id
        return requireWriterId(writerId).clone();
    }

    static byte[] leaseKey(long leaseId) {
        requireNonNegative("leaseId", leaseId);
        final ByteBuffer out = ByteBuffer.allocate(8);
        out.putLong(leaseId);
        return out.array();
    }

    static byte[] leasePathKey(long leaseId, byte[] nodeKey) {
        if (nodeKey.length == 0) {
            throw MetadataStoreException.corrupt("a lease-path tail must not be empty");
        }
        final ByteBuffer out = ByteBuffer.allocate(8 + nodeKey.length);
        out.putLong(leaseId).put(nodeKey);
        return out.array();
    }

    static byte[] leaseExpiryKey(long expireAt, long leaseId) {
        final ByteBuffer out = ByteBuffer.allocate(16);
        out.putLong(expireAt).putLong(leaseId);
        return out.array();
    }

    static byte[] businessResultKey(byte[] writerId, long generation, long sequence) {
        final ByteBuffer out = ByteBuffer.allocate(1 + WRITER_ID_BYTES + 16);
        out.put((byte) 0).put(requireWriterId(writerId))
                .putLong(generation).putLong(sequence);
        return out.array();
    }

    static byte[] takeoverResultKey(byte[] writerId, long expectedGeneration, byte[] nonce) {
        if (nonce.length != WRITER_ID_BYTES) {
            throw MetadataStoreException.corrupt(
                    "nonce must be " + WRITER_ID_BYTES + " bytes, got " + nonce.length);
        }
        final ByteBuffer out = ByteBuffer.allocate(1 + WRITER_ID_BYTES + 8 + WRITER_ID_BYTES);
        out.put((byte) 1).put(requireWriterId(writerId))
                .putLong(expectedGeneration).put(nonce);
        return out.array();
    }

    static byte[] resultExpiryKey(long expiresAt, byte[] resultKey) {
        final ByteBuffer out = ByteBuffer.allocate(8 + resultKey.length);
        out.putLong(expiresAt).put(resultKey);
        return out.array();
    }

    static byte[] changeKey(long revision, int ordinal) {
        final ByteBuffer out = ByteBuffer.allocate(12);
        out.putLong(revision).putInt(ordinal);
        return out.array();
    }

    static byte[] changeRevisionKey(long revision) {
        final ByteBuffer out = ByteBuffer.allocate(8);
        out.putLong(revision);
        return out.array();
    }

    // ---- shared plumbing ----

    private static ByteBuffer header(int capacity) {
        final ByteBuffer out = ByteBuffer.allocate(capacity);
        out.put((byte) CODEC_VERSION);
        return out;
    }

    private static byte[] finish(ByteBuffer out) {
        if (out.position() != out.capacity()) {
            throw new IllegalStateException("encoder size mismatch");
        }
        return out.array();
    }

    /** Opens a fixed-width record: codec byte checked, exact total size enforced. */
    private static ByteBuffer open(byte[] value, String what, int width) {
        if (value.length < 1) {
            throw corruptLength(what, value.length);
        }
        final ByteBuffer in = ByteBuffer.wrap(value);
        if (in.get() != CODEC_VERSION) {
            throw MetadataStoreException.corrupt(
                    what + " carries codec version " + value[0] + ", expected " + CODEC_VERSION);
        }
        if (width > 0 && value.length != width) {
            throw MetadataStoreException.corrupt(
                    what + " is " + value.length + " bytes, expected " + width);
        }
        return in;
    }

    private static void writeStamp(ByteBuffer out, Stamp stamp) {
        out.putLong(stamp.logIndex()).putInt(stamp.ordinal());
    }

    private static Stamp readStamp(ByteBuffer in) {
        final long logIndex = in.getLong();
        final int ordinal = in.getInt();
        if (logIndex < 0 || ordinal < 1) {
            throw MetadataStoreException.corrupt(
                    "stamp must have index >= 0 and ordinal > 0: " + logIndex + "/" + ordinal);
        }
        return Stamp.valueOf(logIndex, ordinal);
    }

    private static byte kindCode(org.apache.ratis.metadata.proto.MetadataProtos.NodeKind kind) {
        switch (kind) {
            case NORMAL:
                return 1;
            case EPHEMERAL_LOCK:
                return 2;
            default:
                throw MetadataStoreException.corrupt("unknown node kind " + kind);
        }
    }

    private static org.apache.ratis.metadata.proto.MetadataProtos.NodeKind kindOf(byte code) {
        switch (code) {
            case 1:
                return org.apache.ratis.metadata.proto.MetadataProtos.NodeKind.NORMAL;
            case 2:
                return org.apache.ratis.metadata.proto.MetadataProtos.NodeKind.EPHEMERAL_LOCK;
            default:
                throw MetadataStoreException.corrupt("unknown node kind code " + code);
        }
    }

    private static byte laneCode(Lane lane) {
        switch (lane) {
            case BUSINESS:
                return 1;
            case CONTROL:
                return 2;
            default:
                throw MetadataStoreException.corrupt("unknown lane " + lane);
        }
    }

    private static Lane laneOf(byte code) {
        switch (code) {
            case 1:
                return Lane.BUSINESS;
            case 2:
                return Lane.CONTROL;
            default:
                throw MetadataStoreException.corrupt("unknown lane code " + code);
        }
    }

    private static byte[] requireWriterId(byte[] writerId) {
        if (writerId.length != WRITER_ID_BYTES) {
            throw MetadataStoreException.corrupt(
                    "writerId must be " + WRITER_ID_BYTES + " bytes, got " + writerId.length);
        }
        return writerId;
    }

    private static void requireNonNegative(String name, long value) {
        if (value < 0) {
            throw MetadataStoreException.corrupt(name + " must not be negative: " + value);
        }
    }

    /** Encoders and decoders share the domain: no negative stats component. */
    private static void requireValidStats(StoreStats stats) {
        if (stats.businessResultBytes() < 0 || stats.controlResultBytes() < 0
                || stats.visibleChangeBytes() < 0 || stats.physicalChangeBytes() < 0
                || stats.writerCount() < 0 || stats.leaseCount() < 0) {
            throw MetadataStoreException.corrupt("negative stats component in " + stats);
        }
    }

    private static MetadataStoreException corruptLength(String what, int length) {
        return MetadataStoreException.corrupt(what + " record is too short: " + length + " bytes");
    }

    private static byte[] concat(byte[] first, byte[] second) {
        final byte[] out = new byte[first.length + second.length];
        System.arraycopy(first, 0, out, 0, first.length);
        System.arraycopy(second, 0, out, first.length, second.length);
        return out;
    }

    /** The one SHA-256 of the package: record checksums and domain digests. */
    static byte[] sha256(byte[] payload) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(payload);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }
}
