/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.ratis.metadata.protocol;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The TLV wire codec of the metadata commands and results (the protobuf seam). Strings on the wire
 * are UTF-8 and lossless by construction: the encoder rejects unpaired UTF-16 surrogates instead
 * of letting UTF-8 substitute {@code '?'}, which would alias distinct names. Decode-side,
 * malformed bytes fold to U+FFFD, so raw hand-crafted payloads can still alias - writing log
 * entries directly already implies full trust (see the module README).
 */
public final class RaftCommandCodec {

    /** The format version every payload starts with; any other version is malformed. */
    public static final int CURRENT_FORMAT_VERSION = 3;

    /** Default cap of LIST results when the command passes 0. */
    public static final long DEFAULT_MAX_LIST_RESULTS = 1024;

    /** Default byte budget of POLL_CHANGES when the command passes 0. */
    public static final long DEFAULT_MAX_POLL_BYTES = 4L * 1024 * 1024;

    private RaftCommandCodec() {}

    // ------------------------------------------------------------------
    // Commands
    // ------------------------------------------------------------------

    /**
     * Encodes one command (write or read) onto the wire.
     *
     * @throws IllegalArgumentException when a string field (path, counter name, start-after
     *     cursor) carries an unpaired surrogate, which UTF-8 cannot represent losslessly
     */
    public static byte[] encodeCommand(RaftCommand command) {
        Encoder out = new Encoder();
        out.writeByte(CURRENT_FORMAT_VERSION);
        writeCommand(out, command);
        return out.toBytes();
    }

    private static void writeCommand(Encoder out, RaftCommand command) {
        final RaftCommand.Op op = command.getOp();
        out.writeByte(op.code());
        switch (op) {
            case PUT:
                out.writeString(command.getPath());
                out.writeBytes(command.getData());
                out.writeVarlong(command.getLeaseId());
                out.writeVarlong(command.getTimestamp());
                break;
            case CREATE:
                out.writeString(command.getPath());
                out.writeBytes(command.getData());
                out.writeByte(command.getFlags());
                out.writeVarlong(command.getLeaseId());
                out.writeVarlong(command.getTimestamp());
                break;
            case DELETE:
                out.writeString(command.getPath());
                out.writeBoolean(command.isRecursive());
                break;
            case CAS:
                out.writeString(command.getPath());
                out.writeBytes(command.getData());
                out.writeVarlong(command.getExpectedVersion());
                out.writeVarlong(command.getLeaseId());
                out.writeVarlong(command.getTimestamp());
                break;
            case MULTI:
                final List<RaftCommand> ops = command.getOps();
                // The writer's term precedes the sub-commands: a stamped MULTI commits only under a
                // leader the writer knew of (see RaftCommand#multi(List, long)).
                out.writeVarlong(command.getWriterTerm());
                out.writeVarlong(ops.size());
                for (RaftCommand sub : ops) {
                    writeCommand(out, sub);
                }
                break;
            case INCREMENT:
                out.writeString(command.getPath());
                out.writeZigzag(command.getDelta());
                // v3: the lease guard (0 = plain counter arithmetic); v2 payloads die on the
                // version byte, never as a silent unguarded increment.
                out.writeVarlong(command.getLeaseId());
                break;
            case LEASE_REGISTER:
                out.writeVarlong(command.getLeaseId());
                out.writeVarlong(command.getTtlMs());
                out.writeVarlong(command.getExpireAtMs());
                break;
            case LEASE_RENEW:
                out.writeVarlong(command.getLeaseId());
                out.writeVarlong(command.getTtlMs());
                out.writeVarlong(command.getExpireAtMs());
                break;
            case LEASE_EXPIRE:
                out.writeVarlong(command.getLeaseId());
                out.writeZigzag(command.getExpectedLastRenewIndex());
                break;
            case GET:
            case EXISTS:
                out.writeString(command.getPath());
                break;
            case LIST:
                out.writeString(command.getPath());
                out.writeString(command.getStartAfter());
                out.writeVarlong(command.getMaxResults());
                break;
            case REVISION:
                break;
            case LEASE_LIST:
                out.writeVarlong(command.getStartAfterLeaseId());
                out.writeVarlong(command.getMaxResults());
                break;
            case POLL_CHANGES:
                out.writeVarlong(command.getSinceRevision());
                out.writeVarlong(command.getMaxBytes());
                break;
            default:
                throw new IllegalArgumentException("Cannot encode op " + op);
        }
    }

    /** Decodes a command payload; {@code null} if malformed (version, op, truncation, nesting). */
    public static RaftCommand decodeCommand(byte[] payload) {
        try {
            final Decoder in = new Decoder(payload);
            if (in.readByte() != CURRENT_FORMAT_VERSION) {
                return null;
            }
            final RaftCommand command = readCommand(in);
            return in.remaining() == 0 ? command : null;
        } catch (RuntimeException e) {
            // The malformed signal is expected; anything else is a bug - either way undecodable.
            return null;
        }
    }

    private static RaftCommand readCommand(Decoder in) {
        final RaftCommand.Op op = RaftCommand.Op.forCode(in.readByte());
        if (op == null) {
            throw new MalformedPayloadException();
        }
        final RaftCommand command = RaftCommand.newForDecode(op);
        switch (op) {
            case PUT:
                return command.path(in.readString())
                        .data(in.readBytes())
                        .leaseId(in.readVarlong())
                        .timestamp(in.readVarlong());
            case CREATE:
                return command.path(in.readString())
                        .data(in.readBytes())
                        .flags((byte) in.readByte())
                        .leaseId(in.readVarlong())
                        .timestamp(in.readVarlong());
            case DELETE:
                return command.path(in.readString()).recursive(in.readBoolean());
            case CAS:
                return command.path(in.readString())
                        .data(in.readBytes())
                        .expectedVersion(in.readVarlong())
                        .leaseId(in.readVarlong())
                        .timestamp(in.readVarlong());
            case MULTI:
                final long writerTerm = in.readVarlong();
                if (writerTerm < 0) {
                    throw new MalformedPayloadException();
                }
                final int count = in.readCount();
                if (count > RaftCommand.MAX_MULTI_OPS) {
                    throw new MalformedPayloadException();
                }
                final List<RaftCommand> ops = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    // A nested MULTI must die before the recursion, not after the sub-command
                    // parses: parsing first recurses once per crafted [MULTI][count=1] level,
                    // stack-overflowing the decode of a committed entry on every replica.
                    if (in.peekByte() == RaftCommand.Op.MULTI.code()) {
                        throw new MalformedPayloadException();
                    }
                    final RaftCommand sub = readCommand(in);
                    if (!sub.getOp().isWrite()) {
                        throw new MalformedPayloadException();
                    }
                    ops.add(sub);
                }
                return command.writerTerm(writerTerm).ops(ops);
            case INCREMENT:
                return command.path(in.readString())
                        .delta(in.readZigzag())
                        .leaseId(in.readVarlong());
            case LEASE_REGISTER:
                return command.leaseId(in.readVarlong())
                        .ttlMs(in.readVarlong())
                        .expireAtMs(in.readVarlong());
            case LEASE_RENEW:
                return command.leaseId(in.readVarlong())
                        .ttlMs(in.readVarlong())
                        .expireAtMs(in.readVarlong());
            case LEASE_EXPIRE:
                return command.leaseId(in.readVarlong()).expectedLastRenewIndex(in.readZigzag());
            case GET:
            case EXISTS:
                return command.path(in.readString());
            case LIST:
                return command.path(in.readString())
                        .startAfter(in.readString())
                        .maxResults(in.readVarlong());
            case REVISION:
                return command;
            case LEASE_LIST:
                return command.startAfterLeaseId(in.readVarlong()).maxResults(in.readVarlong());
            case POLL_CHANGES:
                return command.sinceRevision(in.readVarlong()).maxBytes(in.readVarlong());
            default:
                throw new MalformedPayloadException();
        }
    }

    // ------------------------------------------------------------------
    // Results
    // ------------------------------------------------------------------

    /**
     * Encodes one command result onto the wire.
     *
     * @throws IllegalArgumentException when a string field (a result or list-entry path) carries
     *     an unpaired surrogate, which UTF-8 cannot represent losslessly
     */
    public static byte[] encodeResult(RaftResult result) {
        final Encoder out = new Encoder();
        writeResult(out, result);
        return out.toBytes();
    }

    private static void writeResult(Encoder out, RaftResult result) {
        out.writeByte(result.getOp().code());
        out.writeByte(result.getStatus().code());
        if (result.getStatus() != RaftResult.Status.OK) {
            writeErrorPayload(out, result);
            return;
        }
        switch (result.getOp()) {
            case PUT:
            case CAS:
                out.writeVarlong(result.getVersion());
                break;
            case CREATE:
                out.writeString(result.getResultPath());
                out.writeVarlong(result.getVersion());
                break;
            case DELETE:
                out.writeVarlong(result.getDeletedCount());
                break;
            case MULTI:
                final List<RaftResult> subs = result.getSubResults();
                out.writeVarlong(subs.size());
                for (RaftResult sub : subs) {
                    writeResult(out, sub);
                }
                break;
            case INCREMENT:
                out.writeZigzag(result.getValue());
                break;
            case LEASE_REGISTER:
            case LEASE_RENEW:
                out.writeVarlong(result.getRevision());
                break;
            case LEASE_EXPIRE:
                out.writeBoolean(result.isExpired());
                out.writeVarlong(result.getDeletedCount());
                break;
            case GET:
                out.writeVarlong(result.getVersion());
                out.writeVarlong(result.getLeaseId());
                out.writeVarlong(result.getCreateTime());
                out.writeVarlong(result.getModifyTime());
                out.writeBytes(result.getData());
                break;
            case LIST:
                final List<RaftResult.Entry> entries = result.getEntries();
                out.writeVarlong(entries.size());
                for (RaftResult.Entry entry : entries) {
                    out.writeString(entry.getPath());
                    out.writeVarlong(entry.getVersion());
                }
                out.writeBoolean(result.hasMore());
                break;
            case EXISTS:
                out.writeBoolean(result.isFound());
                break;
            case LEASE_LIST:
                final List<RaftResult.Lease> leases = result.getLeases();
                out.writeVarlong(leases.size());
                for (RaftResult.Lease lease : leases) {
                    out.writeVarlong(lease.getLeaseId());
                    out.writeVarlong(lease.getTtlMs());
                    out.writeVarlong(lease.getExpireAtMs());
                    out.writeVarlong(lease.getLastRenewIndex());
                }
                out.writeBoolean(result.hasMore());
                break;
            case REVISION:
                out.writeVarlong(result.getRevision());
                break;
            case POLL_CHANGES:
                out.writeVarlong(result.getRevision());
                out.writeVarlong(result.getOldestRetainedRevision());
                final List<ChangeEvent> records = result.getRecords();
                out.writeVarlong(records.size());
                for (ChangeEvent record : records) {
                    writeChangeRecord(out, record);
                }
                break;
            default:
                throw new IllegalArgumentException("Cannot encode result of op " + result.getOp());
        }
    }

    private static void writeErrorPayload(Encoder out, RaftResult result) {
        switch (result.getStatus()) {
            case MULTI_FAILED:
                out.writeVarlong(result.getFailIndex());
                out.writeByte(result.getFailStatus().code());
                break;
            case NEED_SNAPSHOT:
                out.writeVarlong(result.getOldestRetainedRevision());
                break;
            default:
                break;
        }
    }

    /** Decodes a result payload; returns {@code null} if the payload is malformed. */
    public static RaftResult decodeResult(byte[] payload) {
        try {
            final Decoder in = new Decoder(payload);
            final RaftResult result = readResult(in);
            return in.remaining() == 0 ? result : null;
        } catch (RuntimeException e) {
            // As decodeCommand: undecodable either way, so a bug can't kill the caller's thread.
            return null;
        }
    }

    private static RaftResult readResult(Decoder in) {
        final RaftCommand.Op op = RaftCommand.Op.forCode(in.readByte());
        final RaftResult.Status status = RaftResult.Status.forCode(in.readByte());
        if (op == null || status == null) {
            throw new MalformedPayloadException();
        }
        final RaftResult result = RaftResult.error(op, status);
        if (status != RaftResult.Status.OK) {
            switch (status) {
                case MULTI_FAILED:
                    return result.failIndex(in.readCount()).failStatus(readStatus(in));
                case NEED_SNAPSHOT:
                    return result.oldestRetainedRevision(in.readVarlong());
                default:
                    return result;
            }
        }
        switch (op) {
            case PUT:
            case CAS:
                return result.version(in.readVarlong());
            case CREATE:
                return result.resultPath(in.readString()).version(in.readVarlong());
            case DELETE:
                return result.deletedCount(in.readVarlong());
            case MULTI:
                final int count = in.readCount();
                if (count > RaftCommand.MAX_MULTI_OPS) {
                    throw new MalformedPayloadException();
                }
                final List<RaftResult> subs = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    // The command decoder rejects a nested MULTI, so the state machine never
                    // produces a MULTI sub-result: reject its op byte before recursing, or a
                    // crafted [MULTI][OK][count=1] chain would recurse once per level.
                    if (in.peekByte() == RaftCommand.Op.MULTI.code()) {
                        throw new MalformedPayloadException();
                    }
                    subs.add(readResult(in));
                }
                return result.subResults(subs);
            case INCREMENT:
                return result.counterValue(in.readZigzag());
            case LEASE_REGISTER:
            case LEASE_RENEW:
                return result.revision(in.readVarlong());
            case LEASE_EXPIRE:
                return result.expired(in.readBoolean()).deletedCount(in.readVarlong());
            case GET:
                return result.version(in.readVarlong())
                        .leaseId(in.readVarlong())
                        .createTime(in.readVarlong())
                        .modifyTime(in.readVarlong())
                        .data(in.readBytes());
            case LIST:
                final int entryCount = in.readCount();
                for (int i = 0; i < entryCount; i++) {
                    result.addEntry(new RaftResult.Entry(in.readString(), in.readVarlong()));
                }
                return result.hasMore(in.readBoolean());
            case EXISTS:
                return result.found(in.readBoolean());
            case LEASE_LIST:
                final int leaseCount = in.readCount();
                for (int i = 0; i < leaseCount; i++) {
                    result.addLease(new RaftResult.Lease(
                            in.readVarlong(), in.readVarlong(), in.readVarlong(), in.readVarlong()));
                }
                return result.hasMore(in.readBoolean());
            case REVISION:
                return result.revision(in.readVarlong());
            case POLL_CHANGES:
                final RaftResult poll =
                        result.revision(in.readVarlong()).oldestRetainedRevision(in.readVarlong());
                final int recordCount = in.readCount();
                // Each record spans 2+ bytes, so remaining() caps the capacity hint: a lying count
                // dies in the per-record reads as the malformed signal, not an OutOfMemoryError.
                final List<ChangeEvent> records = new ArrayList<>(Math.min(recordCount, in.remaining()));
                for (int i = 0; i < recordCount; i++) {
                    final ChangeEvent record = readChangeRecord(in);
                    records.add(record);
                }
                return poll.records(records);
            default:
                throw new MalformedPayloadException();
        }
    }

    private static RaftResult.Status readStatus(Decoder in) {
        final RaftResult.Status status = RaftResult.Status.forCode(in.readByte());
        if (status == null) {
            throw new MalformedPayloadException();
        }
        return status;
    }

    // ------------------------------------------------------------------
    // Change records
    // ------------------------------------------------------------------

    /**
     * Encodes a change record: the {@code changes} column family value.
     *
     * @throws IllegalArgumentException when a change path carries an unpaired surrogate, which
     *     UTF-8 cannot represent losslessly
     */
    public static byte[] encodeChangeRecord(ChangeEvent record) {
        final Encoder out = new Encoder();
        writeChangeRecord(out, record);
        return out.toBytes();
    }

    private static void writeChangeRecord(Encoder out, ChangeEvent record) {
        out.writeVarlong(record.getRevision());
        final List<ChangeEvent.Change> changes = record.getChanges();
        out.writeVarlong(changes.size());
        for (ChangeEvent.Change change : changes) {
            out.writeByte(change.getOp().code());
            out.writeString(change.getPath());
            out.writeBytes(change.getData());
        }
    }

    /** Decodes a change record ({@code changes} column family value); {@code null} if malformed. */
    public static ChangeEvent decodeChangeRecord(byte[] bytes) {
        try {
            final Decoder in = new Decoder(bytes);
            final ChangeEvent record = readChangeRecord(in);
            return in.remaining() == 0 ? record : null;
        } catch (RuntimeException e) {
            // As decodeCommand: undecodable either way, so a bug can't kill the caller's thread.
            return null;
        }
    }

    private static ChangeEvent readChangeRecord(Decoder in) {
        final long revision = in.readVarlong();
        final int count = in.readCount();
        // As in POLL_CHANGES: each change spans 2+ bytes, so capacity is capped by what can parse.
        final List<ChangeEvent.Change> changes = new ArrayList<>(Math.min(count, in.remaining()));
        for (int i = 0; i < count; i++) {
            final ChangeEvent.ChangeOp op = ChangeEvent.ChangeOp.forCode((byte) in.readByte());
            if (op == null) {
                throw new MalformedPayloadException();
            }
            final String path = in.readString();
            final byte[] data = in.readBytes();
            // INCREMENT and the lease ops carry exactly one long (counter value, lease id); a
            // wrong-length payload dies here, not as a BufferUnderflowException inside
            // Change.getCounterValue()/getLeaseId() in a listener far past this boundary.
            final boolean carriesALong = op == ChangeEvent.ChangeOp.INCREMENT
                    || op == ChangeEvent.ChangeOp.LEASE_REGISTER
                    || op == ChangeEvent.ChangeOp.LEASE_RENEW
                    || op == ChangeEvent.ChangeOp.LEASE_EXPIRE;
            if (carriesALong && data.length != Long.BYTES) {
                throw new MalformedPayloadException();
            }
            changes.add(new ChangeEvent.Change(op, path, data));
        }
        return new ChangeEvent(revision, changes);
    }

    // ------------------------------------------------------------------
    // Primitive encode/decode, one path each
    // ------------------------------------------------------------------

    /** Raised only inside this class when a payload violates its layout; never escapes it. */
    private static final class MalformedPayloadException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    /** The reject signal of {@code writeString}: the wire only carries lossless strings. */
    private static IllegalArgumentException unencodableString(int surrogateIndex) {
        return new IllegalArgumentException("Cannot encode a string with an unpaired surrogate at index "
                + surrogateIndex + ": UTF-8 would substitute it with '?', aliasing distinct names");
    }

    /** A single growable byte buffer; every encoded message is copied out exactly once. */
    private static final class Encoder {
        private byte[] buffer = new byte[64];
        private int position;

        byte[] toBytes() {
            return Arrays.copyOf(buffer, position);
        }

        void ensure(int extra) {
            if (position + extra > buffer.length) {
                buffer = Arrays.copyOf(buffer, Math.max(buffer.length * 2, position + extra));
            }
        }

        void writeByte(int value) {
            ensure(1);
            buffer[position++] = (byte) value;
        }

        void writeBoolean(boolean value) {
            writeByte(value ? 1 : 0);
        }

        /** Writes a long as an unsigned LEB128 varint (at most 10 bytes); a negative field value
         * (an empty replica's REVISION is -1) round-trips bit-exactly - only length and count
         * prefixes must be non-negative (see readBytes and readCount). */
        void writeVarlong(long value) {
            ensure(10);
            while ((value & ~0x7FL) != 0) {
                buffer[position++] = (byte) ((value & 0x7F) | 0x80);
                value >>>= 7;
            }
            buffer[position++] = (byte) value;
        }

        /** Writes a possibly-negative long zigzag-encoded, so small magnitudes stay small. */
        void writeZigzag(long value) {
            writeVarlong((value << 1) ^ (value >> 63));
        }

        void writeBytes(byte[] value) {
            writeVarlong(value.length);
            ensure(value.length);
            System.arraycopy(value, 0, buffer, position, value.length);
            position += value.length;
        }

        void writeString(String value) {
            // getBytes(UTF_8) substitutes an unpaired surrogate with '?' - a legal name character
            // - so two distinct names would encode to the same bytes and alias. Reject instead:
            // every string written here must round-trip through readString unchanged.
            for (int i = 0; i < value.length(); i++) {
                final char c = value.charAt(i);
                if (Character.isHighSurrogate(c)) {
                    if (i + 1 == value.length() || !Character.isLowSurrogate(value.charAt(i + 1))) {
                        throw unencodableString(i);
                    }
                    i++; // a surrogate pair is one code point and encodes losslessly
                } else if (Character.isLowSurrogate(c)) {
                    throw unencodableString(i);
                }
            }
            writeBytes(value.getBytes(StandardCharsets.UTF_8));
        }
    }

    /** A bounds-checked cursor over a payload; any violation raises the malformed signal. */
    private static final class Decoder {
        private final byte[] buffer;
        private int position;

        Decoder(byte[] buffer) {
            this.buffer = buffer;
        }

        int remaining() {
            return buffer.length - position;
        }

        private void need(int count) {
            if (remaining() < count) {
                throw new MalformedPayloadException();
            }
        }

        int readByte() {
            need(1);
            return buffer[position++] & 0xFF;
        }

        /** The next byte without consuming it; a caller guards a recursion on it. */
        int peekByte() {
            need(1);
            return buffer[position] & 0xFF;
        }

        boolean readBoolean() {
            return readByte() != 0;
        }

        long readVarlong() {
            long value = 0;
            int shift = 0;
            while (true) {
                need(1);
                final byte b = buffer[position++];
                value |= (long) (b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    return value;
                }
                shift += 7;
                if (shift >= 64) {
                    throw new MalformedPayloadException();
                }
            }
        }

        /** A count or index field: rejected outside [0, Integer.MAX_VALUE] before it narrows to
         * int, or a wider varlong (2^32 + 4) would truncate to a valid-looking 4. */
        int readCount() {
            final long value = readVarlong();
            if (value < 0 || value > Integer.MAX_VALUE) {
                throw new MalformedPayloadException();
            }
            return (int) value;
        }

        long readZigzag() {
            final long value = readVarlong();
            return (value >>> 1) ^ -(value & 1);
        }

        byte[] readBytes() {
            final long length = readVarlong();
            // A varlong may be negative elsewhere (empty replica's REVISION is -1), but a length
            // never is; reject it here as the malformed signal, not NegativeArraySizeException.
            if (length < 0 || length > remaining()) {
                throw new MalformedPayloadException();
            }
            final byte[] value = new byte[(int) length];
            System.arraycopy(buffer, position, value, 0, (int) length);
            position += length;
            return value;
        }

        String readString() {
            return new String(readBytes(), StandardCharsets.UTF_8);
        }
    }
}
