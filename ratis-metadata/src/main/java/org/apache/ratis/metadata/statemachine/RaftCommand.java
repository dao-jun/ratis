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

package org.apache.ratis.metadata.statemachine;

import java.util.List;

/** The in-memory form of one metadata command, on either side of {@link RaftCommandCodec}. */
public final class RaftCommand {

    /** The operation; the code also selects the payload layout on the wire. */
    public enum Op {
        /**
         * Write: upsert a path (creates or bumps the version). A PUT that would create is
         * held to CREATE's parent rule - the parent must exist and must not be lease-bound;
         * updating an existing record is ungated by parents.
         */
        PUT((byte) 1, true),
        /** Write: create a path (ZooKeeper-like semantics; supports sequential and ephemeral). */
        CREATE((byte) 2, true),
        /** Write: delete a path, non-recursively or the whole subtree. */
        DELETE((byte) 3, true),
        /** Write: put conditioned on the current version of the path. */
        CAS((byte) 4, true),
        /** Write: a batch of other write ops applied atomically. */
        MULTI((byte) 5, true),
        /** Write: add a delta to a named counter. */
        INCREMENT((byte) 6, true),
        /** Write: register (or re-register) a lease. */
        LEASE_REGISTER((byte) 7, true),
        /** Write: renew a lease with a new expiry time. */
        LEASE_RENEW((byte) 8, true),
        /** Write: expire a lease and delete its ephemeral paths. */
        LEASE_EXPIRE((byte) 9, true),
        /** Read: fetch one path's record. */
        GET((byte) 20, false),
        /** Read: list the paths under a prefix. */
        LIST((byte) 21, false),
        /** Read: check whether a path exists. */
        EXISTS((byte) 22, false),
        /** Read: fetch the currently applied revision. */
        REVISION((byte) 23, false),
        /** Read: fetch the change records after a revision. */
        POLL_CHANGES((byte) 24, false),
        /** Read: list the registered leases with expiry and renew-index state. */
        LEASE_LIST((byte) 25, false),
        /** Result-only marker for payloads whose operation code could not be decoded. */
        UNKNOWN((byte) 0, false);

        private final byte code;
        private final boolean write;

        Op(byte code, boolean write) {
            this.code = code;
            this.write = write;
        }

        /** The one-byte wire code of this operation. */
        public byte code() {
            return code;
        }

        /** Whether this operation mutates state (goes through {@code applyTransaction}). */
        public boolean isWrite() {
            return write;
        }

        static Op forCode(int code) {
            for (Op op : values()) {
                if (op.code == code) {
                    return op;
                }
            }
            return null;
        }
    }

    /** Flag bit of {@link #getFlags()}: append a monotonic sequence number to the path. */
    public static final byte FLAG_SEQUENTIAL = 0x01;
    /** Flag bit of {@link #getFlags()}: materialize missing ancestors atomically. */
    public static final byte FLAG_CREATE_PARENTS = 0x04;

    /** The hard cap of MULTI sub-commands, enforced at composition and at decode. */
    public static final int MAX_MULTI_OPS = 1024;

    private final Op op;
    private String path = "";
    private byte[] data = new byte[0];
    private long leaseId;
    private long timestamp;
    private byte flags;
    private boolean recursive;
    private long expectedVersion;
    private long delta;
    private long ttlMs;
    private long expireAtMs;
    private long expectedLastRenewIndex = -1;
    private List<RaftCommand> ops;
    private long writerTerm;
    private long maxResults;
    private String startAfter = "";
    private long sinceRevision;
    private long maxBytes;
    private long startAfterLeaseId;

    private RaftCommand(Op op) {
        this.op = op;
    }

    /** Creates a PUT command: upsert {@code data} at {@code path}. */
    public static RaftCommand put(String path, byte[] data, long leaseId, long timestamp) {
        RaftCommand c = new RaftCommand(Op.PUT);
        c.path = path;
        c.data = data;
        c.leaseId = leaseId;
        c.timestamp = timestamp;
        return c;
    }

    /** Creates a CREATE: {@code sequential} numbers the path; {@code leaseId} binds a lease. */
    public static RaftCommand create(
            String path, byte[] data, boolean sequential, long leaseId, long timestamp) {
        return create(path, data, sequential, leaseId, timestamp, false);
    }

    /** Creates a CREATE command; {@code createParents} materializes missing ancestors. */
    public static RaftCommand create(
            String path,
            byte[] data,
            boolean sequential,
            long leaseId,
            long timestamp,
            boolean createParents) {
        RaftCommand c = new RaftCommand(Op.CREATE);
        c.path = path;
        c.data = data;
        c.flags = sequential ? FLAG_SEQUENTIAL : 0;
        if (createParents) {
            c.flags |= FLAG_CREATE_PARENTS;
        }
        c.leaseId = leaseId;
        c.timestamp = timestamp;
        return c;
    }

    /** Creates a DELETE command: with {@code recursive} the whole subtree under the path goes. */
    public static RaftCommand delete(String path, boolean recursive) {
        RaftCommand c = new RaftCommand(Op.DELETE);
        c.path = path;
        c.recursive = recursive;
        return c;
    }

    /** Creates a CAS: applies the put only if the current version is {@code expectedVersion}. */
    public static RaftCommand cas(
            String path, byte[] data, long expectedVersion, long leaseId, long timestamp) {
        RaftCommand c = new RaftCommand(Op.CAS);
        c.path = path;
        c.data = data;
        c.expectedVersion = expectedVersion;
        c.leaseId = leaseId;
        c.timestamp = timestamp;
        return c;
    }

    /** Creates a MULTI: {@code ops} apply atomically; nesting MULTI is rejected. */
    public static RaftCommand multi(List<RaftCommand> ops) {
        return multi(ops, 0L);
    }

    /** Creates a term-stamped MULTI (0 = unstamped), rejected under a higher entry term. */
    public static RaftCommand multi(List<RaftCommand> ops, long writerTerm) {
        if (ops.size() > MAX_MULTI_OPS) {
            throw new IllegalArgumentException(
                    "MULTI supports at most " + MAX_MULTI_OPS + " sub-commands, got " + ops.size());
        }
        RaftCommand c = new RaftCommand(Op.MULTI);
        c.ops = ops;
        c.writerTerm = writerTerm;
        return c;
    }

    /** Creates an INCREMENT command: adds {@code delta} (may be negative) to the named counter. */
    public static RaftCommand increment(String name, long delta) {
        RaftCommand c = new RaftCommand(Op.INCREMENT);
        c.path = name;
        c.delta = delta;
        return c;
    }

    /** Creates a LEASE_REGISTER; {@code expireAtMs} is proposer-computed ({@code now + ttl}). */
    public static RaftCommand leaseRegister(long leaseId, long ttlMs, long expireAtMs) {
        RaftCommand c = new RaftCommand(Op.LEASE_REGISTER);
        c.leaseId = leaseId;
        c.ttlMs = ttlMs;
        c.expireAtMs = expireAtMs;
        return c;
    }

    /**
     * Creates the legacy LEASE_RENEW form with a proposer-computed expiry and no TTL. Leaders and
     * replicas reject this form; it is retained only to keep the old factory signature unambiguous.
     */
    public static RaftCommand leaseRenew(long leaseId, long expireAtMs) {
        RaftCommand c = new RaftCommand(Op.LEASE_RENEW);
        c.leaseId = leaseId;
        c.expireAtMs = expireAtMs;
        return c;
    }

    /** Creates a LEASE_RENEW with a new TTL and proposer-computed expiry. */
    public static RaftCommand leaseRenew(long leaseId, long ttlMs, long expireAtMs) {
        RaftCommand c = new RaftCommand(Op.LEASE_RENEW);
        c.leaseId = leaseId;
        c.ttlMs = ttlMs;
        c.expireAtMs = expireAtMs;
        return c;
    }

    /** Creates a LEASE_EXPIRE command guarded by the expected last renew index (-1 = any). */
    public static RaftCommand leaseExpire(long leaseId, long expectedLastRenewIndex) {
        RaftCommand c = new RaftCommand(Op.LEASE_EXPIRE);
        c.leaseId = leaseId;
        c.expectedLastRenewIndex = expectedLastRenewIndex;
        return c;
    }

    /** Creates a GET command. */
    public static RaftCommand get(String path) {
        RaftCommand c = new RaftCommand(Op.GET);
        c.path = path;
        return c;
    }

    /** Creates a LIST from the range front; see {@link #list(String, String, long)}. */
    public static RaftCommand list(String prefix, long maxResults) {
        return list(prefix, "", maxResults);
    }

    /** Creates a LIST command resuming strictly after {@code startAfter} (empty = front). */
    public static RaftCommand list(String prefix, String startAfter, long maxResults) {
        RaftCommand c = new RaftCommand(Op.LIST);
        c.path = prefix;
        c.startAfter = startAfter == null ? "" : startAfter;
        c.maxResults = maxResults;
        return c;
    }

    /** Creates an EXISTS command. */
    public static RaftCommand exists(String path) {
        RaftCommand c = new RaftCommand(Op.EXISTS);
        c.path = path;
        return c;
    }

    /** Creates a REVISION command. */
    public static RaftCommand revision() {
        return new RaftCommand(Op.REVISION);
    }

    /** Creates a POLL_CHANGES: records after {@code sinceRevision} within {@code maxBytes}. */
    public static RaftCommand pollChanges(long sinceRevision, long maxBytes) {
        RaftCommand c = new RaftCommand(Op.POLL_CHANGES);
        c.sinceRevision = sinceRevision;
        c.maxBytes = maxBytes;
        return c;
    }

    /** Creates a LEASE_LIST from the table front; see {@link #leaseList(long, long)}. */
    public static RaftCommand leaseList() {
        return leaseList(0L, 0L);
    }

    /** Creates a LEASE_LIST command resuming after {@code startAfterLeaseId} (0 = front). */
    public static RaftCommand leaseList(long startAfterLeaseId, long maxResults) {
        RaftCommand c = new RaftCommand(Op.LEASE_LIST);
        c.startAfterLeaseId = startAfterLeaseId;
        c.maxResults = maxResults;
        return c;
    }

    /** The operation this command encodes. */
    public Op getOp() {
        return op;
    }

    /** The affected path (or list prefix, or counter name); empty for lease ops and REVISION. */
    public String getPath() {
        return path;
    }

    /** The record payload for PUT/CREATE/CAS; empty otherwise. */
    public byte[] getData() {
        return data;
    }

    /** The lease a record binds to (0 = persistent); or the lease a lease op acts on. */
    public long getLeaseId() {
        return leaseId;
    }

    /** The proposer wall clock for PUT/CREATE/CAS record times. */
    public long getTimestamp() {
        return timestamp;
    }

    /** The CREATE flags; see {@link #FLAG_SEQUENTIAL} and {@link #FLAG_CREATE_PARENTS}. */
    public byte getFlags() {
        return flags;
    }

    /** Whether DELETE removes the whole subtree. */
    public boolean isRecursive() {
        return recursive;
    }

    /** The version a CAS requires (0 = the path must not exist). */
    public long getExpectedVersion() {
        return expectedVersion;
    }

    /** The INCREMENT delta. */
    public long getDelta() {
        return delta;
    }

    /** The lease ttl of LEASE_REGISTER. */
    public long getTtlMs() {
        return ttlMs;
    }

    /** Proposer-computed expiry; the state machine never reads the wall clock. */
    public long getExpireAtMs() {
        return expireAtMs;
    }

    /** The renew-index guard of LEASE_EXPIRE (-1 = unconditional). */
    public long getExpectedLastRenewIndex() {
        return expectedLastRenewIndex;
    }

    /** The sub-commands of MULTI. */
    public List<RaftCommand> getOps() {
        return ops;
    }

    /** The raft term the MULTI writer observed (0 = the batch is not term-fenced). */
    public long getWriterTerm() {
        return writerTerm;
    }

    /** The result cap of LIST (0 = codec default). */
    public long getMaxResults() {
        return maxResults;
    }

    /** The continuation cursor of LIST: resume strictly after this path (empty = range front). */
    public String getStartAfter() {
        return startAfter;
    }

    /** The start revision (exclusive) of POLL_CHANGES. */
    public long getSinceRevision() {
        return sinceRevision;
    }

    /** The byte budget of POLL_CHANGES (0 = codec default). */
    public long getMaxBytes() {
        return maxBytes;
    }

    /** Continuation cursor of LEASE_LIST: resume strictly after this lease id (0 = front). */
    public long getStartAfterLeaseId() {
        return startAfterLeaseId;
    }

    // Decode-side setters: used only by RaftCommandCodec while parsing a wire payload.

    static RaftCommand newForDecode(Op op) {
        return new RaftCommand(op);
    }

    RaftCommand path(String value) {
        this.path = value;
        return this;
    }

    RaftCommand data(byte[] value) {
        this.data = value;
        return this;
    }

    RaftCommand leaseId(long value) {
        this.leaseId = value;
        return this;
    }

    RaftCommand timestamp(long value) {
        this.timestamp = value;
        return this;
    }

    RaftCommand flags(byte value) {
        this.flags = value;
        return this;
    }

    RaftCommand recursive(boolean value) {
        this.recursive = value;
        return this;
    }

    RaftCommand expectedVersion(long value) {
        this.expectedVersion = value;
        return this;
    }

    RaftCommand delta(long value) {
        this.delta = value;
        return this;
    }

    RaftCommand ttlMs(long value) {
        this.ttlMs = value;
        return this;
    }

    RaftCommand expireAtMs(long value) {
        this.expireAtMs = value;
        return this;
    }

    RaftCommand expectedLastRenewIndex(long value) {
        this.expectedLastRenewIndex = value;
        return this;
    }

    RaftCommand ops(List<RaftCommand> value) {
        this.ops = value;
        return this;
    }

    RaftCommand writerTerm(long value) {
        this.writerTerm = value;
        return this;
    }

    RaftCommand maxResults(long value) {
        this.maxResults = value;
        return this;
    }

    RaftCommand startAfter(String value) {
        this.startAfter = value == null ? "" : value;
        return this;
    }

    RaftCommand sinceRevision(long value) {
        this.sinceRevision = value;
        return this;
    }

    RaftCommand maxBytes(long value) {
        this.maxBytes = value;
        return this;
    }

    RaftCommand startAfterLeaseId(long value) {
        this.startAfterLeaseId = value;
        return this;
    }

    @Override
    public String toString() {
        return "RaftCommand{op=" + op + ", path='" + path + "', leaseId=" + leaseId + '}';
    }
}
