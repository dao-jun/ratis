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

import org.apache.ratis.metadata.protocol.RaftCommand;
import org.apache.ratis.metadata.protocol.RaftCommandCodec;
import org.apache.ratis.metadata.util.Clock;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/** The only payload accepted in a metadata raft log: a leader-normalized command and its context. */
public final class MetadataLogEnvelope {

    private static final int CURRENT_FORMAT_VERSION = 1;
    /** The sha-256 digest width; shared with {@link LegacyResultStore}'s record format. */
    static final int DIGEST_LENGTH = 32;
    private static final int COMMAND_LENGTH_OFFSET = 1 + Long.BYTES + Long.BYTES + DIGEST_LENGTH;
    private static final int HEADER_LENGTH = COMMAND_LENGTH_OFFSET + Integer.BYTES;

    private final RaftCommand command;
    private final byte[] rawCommandDigest;
    private final long serverTimeMillis;
    private final long proposedCutoffMillis;

    private MetadataLogEnvelope(RaftCommand command, byte[] rawCommandDigest, long serverTimeMillis,
                                long proposedCutoffMillis) {
        if (command == null || !command.getOp().isWrite()) {
            throw new IllegalArgumentException("A metadata log envelope must contain a write command");
        }
        if (rawCommandDigest == null || rawCommandDigest.length != DIGEST_LENGTH) {
            throw new IllegalArgumentException("A raw command digest must contain exactly 32 bytes");
        }
        this.command = command;
        this.rawCommandDigest = rawCommandDigest.clone();
        this.serverTimeMillis = serverTimeMillis;
        this.proposedCutoffMillis = proposedCutoffMillis;
    }

    /**
     * Digests the exact client bytes, then normalizes all lease expiries against one leader time.
     * The returned command is a decode-side copy; the caller's raw array is never modified.
     */
    public static MetadataLogEnvelope create(byte[] rawCommand, Clock clock, long retentionMillis) {
        if (rawCommand == null) {
            throw new IllegalArgumentException("rawCommand must not be null");
        }
        if (clock == null) {
            throw new IllegalArgumentException("clock must not be null");
        }
        if (retentionMillis <= 0) {
            throw new IllegalArgumentException("Result retention must be positive, got " + retentionMillis + " ms");
        }

        final byte[] digest = sha256(rawCommand);
        final RaftCommand command = RaftCommandCodec.decodeCommand(rawCommand);
        if (command == null || !command.getOp().isWrite()) {
            throw new IllegalArgumentException("A metadata log envelope requires a valid write command");
        }
        final long serverTimeMillis = clock.milliseconds();
        normalizeLeaseExpiries(command, serverTimeMillis);
        return new MetadataLogEnvelope(command, digest, serverTimeMillis,
                saturatingSubtract(serverTimeMillis, retentionMillis));
    }

    /** Decodes one complete envelope; malformed, old, trailing and non-write payloads return null. */
    public static MetadataLogEnvelope decode(byte[] logData) {
        if (logData == null || logData.length < HEADER_LENGTH) {
            return null;
        }
        try {
            final ByteBuffer in = ByteBuffer.wrap(logData);
            if ((in.get() & 0xFF) != CURRENT_FORMAT_VERSION) {
                return null;
            }
            final long serverTimeMillis = in.getLong();
            final long proposedCutoffMillis = in.getLong();
            final byte[] digest = new byte[DIGEST_LENGTH];
            in.get(digest);
            final int commandLength = in.getInt();
            if (commandLength < 0 || commandLength != in.remaining()) {
                return null;
            }
            final byte[] encodedCommand = new byte[commandLength];
            in.get(encodedCommand);
            final RaftCommand command = RaftCommandCodec.decodeCommand(encodedCommand);
            if (command == null || !command.getOp().isWrite()) {
                return null;
            }
            return new MetadataLogEnvelope(command, digest, serverTimeMillis, proposedCutoffMillis);
        } catch (RuntimeException malformed) {
            return null;
        }
    }

    /** The normalized command every replica applies. */
    public RaftCommand getCommand() {
        return command;
    }

    /** SHA-256 of the exact original client command, before leader normalization. */
    public byte[] getRawCommandDigest() {
        return rawCommandDigest.clone();
    }

    /** The single leader clock sample shared by this entry and all its MULTI children. */
    public long getServerTimeMillis() {
        return serverTimeMillis;
    }

    /** The leader-proposed result-retention cutoff, replicated rather than recomputed locally. */
    public long getProposedCutoffMillis() {
        return proposedCutoffMillis;
    }

    /**
     * Whether every lease expiry this command carries is the one this envelope's leader time
     * normalizes to - the same {@link #normalizedExpiryOrThrow} policy the leader applied. A
     * MULTI with a null child list carries no expiry to validate.
     */
    public boolean hasNormalizedLeaseExpiries() {
        return hasNormalizedLeaseExpiries(command, serverTimeMillis);
    }

    /** Encodes the fixed big-endian header followed by exactly one normalized command. */
    public byte[] encode() {
        final byte[] encodedCommand = RaftCommandCodec.encodeCommand(command);
        return ByteBuffer.allocate(HEADER_LENGTH + encodedCommand.length)
                .put((byte) CURRENT_FORMAT_VERSION)
                .putLong(serverTimeMillis)
                .putLong(proposedCutoffMillis)
                .put(rawCommandDigest)
                .putInt(encodedCommand.length)
                .put(encodedCommand)
                .array();
    }

    private static void normalizeLeaseExpiries(RaftCommand command, long serverTimeMillis) {
        switch (command.getOp()) {
            case LEASE_REGISTER:
            case LEASE_RENEW:
                command.expireAtMs(normalizedExpiryOrThrow(command, serverTimeMillis));
                break;
            case MULTI:
                if (command.getOps() != null) {
                    for (RaftCommand child : command.getOps()) {
                        normalizeLeaseExpiries(child, serverTimeMillis);
                    }
                }
                break;
            default:
                break;
        }
    }

    private static boolean hasNormalizedLeaseExpiries(RaftCommand command, long serverTimeMillis) {
        switch (command.getOp()) {
            case LEASE_REGISTER:
            case LEASE_RENEW:
                try {
                    return command.getExpireAtMs() == normalizedExpiryOrThrow(command, serverTimeMillis);
                } catch (IllegalArgumentException invalidTiming) {
                    return false;
                }
            case MULTI:
                if (command.getOps() == null) {
                    return true;
                }
                for (RaftCommand child : command.getOps()) {
                    if (!hasNormalizedLeaseExpiries(child, serverTimeMillis)) {
                        return false;
                    }
                }
                return true;
            default:
                return true;
        }
    }

    /** The single lease-expiry policy: a positive TTL normalized to leader time plus TTL. */
    private static long normalizedExpiryOrThrow(RaftCommand command, long serverTimeMillis) {
        if (command.getTtlMs() <= 0) {
            throw new IllegalArgumentException(command.getOp() + " TTL must be positive, got "
                    + command.getTtlMs());
        }
        try {
            return Math.addExact(serverTimeMillis, command.getTtlMs());
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException(
                    command.getOp() + " expiry overflows leader time plus TTL", overflow);
        }
    }

    private static long saturatingSubtract(long value, long positiveAmount) {
        return value < Long.MIN_VALUE + positiveAmount ? Long.MIN_VALUE : value - positiveAmount;
    }

    private static byte[] sha256(byte[] rawCommand) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(rawCommand);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is required by the Java runtime", impossible);
        }
    }

    @Override
    public String toString() {
        return "MetadataLogEnvelope{command="
                + command
                + ", serverTimeMillis="
                + serverTimeMillis
                + ", proposedCutoffMillis="
                + proposedCutoffMillis
                + ", digest="
                + Arrays.toString(rawCommandDigest)
                + '}';
    }
}
