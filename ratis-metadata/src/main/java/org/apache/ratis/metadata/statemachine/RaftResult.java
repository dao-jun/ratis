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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** The outcome of one command: a status plus status-dependent payload fields. */
@SuppressWarnings("checkstyle:HiddenField")
public final class RaftResult {

    /** The outcome status of a command. */
    public enum Status {
        /** The command applied (or the query ran); the payload fields carry the outcome. */
        OK((byte) 0),
        /** GET/EXISTS/DELETE referenced a path that does not exist. */
        NOT_FOUND((byte) 1),
        /** CAS found a different version (or the path exists although absence was required). */
        VERSION_MISMATCH((byte) 2),
        /** CREATE hit an existing path. */
        NODE_EXISTS((byte) 3),
        /** Non-recursive DELETE of a path that has children. */
        HAS_CHILDREN((byte) 4),
        /** CREATE without an existing parent path (the root is always a valid parent). */
        NO_PARENT((byte) 5),
        /** LEASE_RENEW or ephemeral write bound to an unknown lease. */
        UNKNOWN_LEASE((byte) 6),
        /** A MULTI sub-command failed; the whole batch was discarded (failIndex/failStatus). */
        MULTI_FAILED((byte) 7),
        /** POLL_CHANGES from a garbage-collected revision; install a snapshot instead. */
        NEED_SNAPSHOT((byte) 8),
        /** The payload could not be decoded; only possible for client bugs or log corruption. */
        MALFORMED((byte) 9),
        /** The operation is not allowed on the root path (e.g. deleting it). */
        NOT_ALLOWED((byte) 10),
        /** The path does not satisfy the path syntax. */
        BAD_PATH((byte) 11),
        /** A term-fenced write committed under a higher term; nothing applied. */
        STALE_TERM((byte) 12),
        /** The retained invocation result is no longer available for this replayed entry. */
        RESULT_EXPIRED((byte) 13),
        /** An invocation was replayed with bytes different from its original raw command. */
        INVOCATION_MISMATCH((byte) 14),
        /** A lease-guarded INCREMENT found the path no longer owned by its lease (deleted or
         * re-bound); the counter stands untouched. */
        STALE_LEASE((byte) 15);

        private final byte code;

        Status(byte code) {
            this.code = code;
        }

        /** The one-byte wire code of this status. */
        public byte code() {
            return code;
        }

        static Status forCode(int code) {
            for (Status status : values()) {
                if (status.code == code) {
                    return status;
                }
            }
            return null;
        }
    }

    /** One entry of a LIST result. */
    public static final class Entry {

        private final String path;
        private final long version;

        Entry(String path, long version) {
            this.path = path;
            this.version = version;
        }

        /** The listed path. */
        public String getPath() {
            return path;
        }

        /** The path's current version. */
        public long getVersion() {
            return version;
        }

        @Override
        public String toString() {
            return "Entry{path='" + path + "', version=" + version + '}';
        }
    }

    /** One lease of a LEASE_LIST result, as the lease column family stores it. */
    public static final class Lease {

        private final long leaseId;
        private final long ttlMs;
        private final long expireAtMs;
        private final long lastRenewIndex;

        Lease(long leaseId, long ttlMs, long expireAtMs, long lastRenewIndex) {
            this.leaseId = leaseId;
            this.ttlMs = ttlMs;
            this.expireAtMs = expireAtMs;
            this.lastRenewIndex = lastRenewIndex;
        }

        /** The lease identifier. */
        public long getLeaseId() {
            return leaseId;
        }

        /** The latest ttl; each successful renewal stores its new ttl and expiry. */
        public long getTtlMs() {
            return ttlMs;
        }

        /** The leader-normalized replicated expiry; past due at and after this value. */
        public long getExpireAtMs() {
            return expireAtMs;
        }

        /** The revision of the lease's registration or last successful renewal. */
        public long getLastRenewIndex() {
            return lastRenewIndex;
        }

        @Override
        public String toString() {
            return "Lease{leaseId="
                    + leaseId
                    + ", ttlMs="
                    + ttlMs
                    + ", expireAtMs="
                    + expireAtMs
                    + ", lastRenewIndex="
                    + lastRenewIndex
                    + '}';
        }
    }

    private final RaftCommand.Op op;
    private Status status = Status.OK;
    private long version;
    private String resultPath = "";
    private byte[] data = new byte[0];
    private long leaseId;
    private long createTime;
    private long modifyTime;
    private long value;
    private long revision;
    private long oldestRetainedRevision = 1;
    private long deletedCount;
    private boolean expired;
    private boolean found;
    private boolean hasMore;
    private int failIndex = -1;
    private Status failStatus;
    private List<Entry> entries;
    private List<Lease> leases;
    private List<RaftResult> subResults;
    private List<ChangeEvent> records;

    private RaftResult(RaftCommand.Op op, Status status) {
        this.op = op;
        this.status = status;
    }

    /** Creates an OK result for the given operation; payload fields are filled by the caller. */
    public static RaftResult ok(RaftCommand.Op op) {
        return new RaftResult(op, Status.OK);
    }

    /** Creates an error result of the given status for the given operation. */
    public static RaftResult error(RaftCommand.Op op, Status status) {
        return new RaftResult(op, status);
    }

    /** The operation this result replies to (echoes the command, or UNKNOWN for bad payloads). */
    public RaftCommand.Op getOp() {
        return op;
    }

    /** The outcome status. */
    public Status getStatus() {
        return status;
    }

    /** The new record version of PUT/CREATE/CAS (1 for a fresh record). */
    public long getVersion() {
        return version;
    }

    /** The concrete path of a sequential CREATE (base path plus sequence number). */
    public String getResultPath() {
        return resultPath;
    }

    /** The record payload of GET. */
    public byte[] getData() {
        return data;
    }

    /** The lease binding of the GET record (0 = persistent). */
    public long getLeaseId() {
        return leaseId;
    }

    /** The creation timestamp of the GET record (proposer clock of the creating command). */
    public long getCreateTime() {
        return createTime;
    }

    /** The last modification timestamp of the GET record. */
    public long getModifyTime() {
        return modifyTime;
    }

    /** The new counter value of INCREMENT. */
    public long getValue() {
        return value;
    }

    /** The applied revision of REVISION, the renew index of lease ops, or the poll watermark. */
    public long getRevision() {
        return revision;
    }

    /** The oldest revision whose change records are guaranteed retained (NEED_SNAPSHOT payload). */
    public long getOldestRetainedRevision() {
        return oldestRetainedRevision;
    }

    /** The number of paths a DELETE (or a lease expiry) removed. */
    public long getDeletedCount() {
        return deletedCount;
    }

    /** Whether a LEASE_EXPIRE actually expired the lease (no-op expiries return false). */
    public boolean isExpired() {
        return expired;
    }

    /** The outcome of EXISTS. */
    public boolean isFound() {
        return found;
    }

    /** Whether the result was cut at its cap: continue with the last item as cursor. */
    public boolean hasMore() {
        return hasMore;
    }

    /** The index of the failing sub-command of a MULTI_FAILED result. */
    public int getFailIndex() {
        return failIndex;
    }

    /** The status the failing sub-command produced. */
    public Status getFailStatus() {
        return failStatus;
    }

    /** The entries of a LIST result. */
    public List<Entry> getEntries() {
        return entries == null ? Collections.<Entry>emptyList() : entries;
    }

    /** The lease states of a LEASE_LIST result. */
    public List<Lease> getLeases() {
        return leases == null ? Collections.<Lease>emptyList() : leases;
    }

    /** The per-sub-command results of a successful MULTI, in command order. */
    public List<RaftResult> getSubResults() {
        return subResults == null ? Collections.<RaftResult>emptyList() : subResults;
    }

    /** The change records of a POLL_CHANGES result, ordered by revision. */
    public List<ChangeEvent> getRecords() {
        return records == null ? Collections.<ChangeEvent>emptyList() : records;
    }

    // ------------------------------------------------------------------
    // Package-private fluent setters used by the state machine and codec
    // ------------------------------------------------------------------

    RaftResult version(long value) {
        this.version = value;
        return this;
    }

    RaftResult resultPath(String value) {
        this.resultPath = value;
        return this;
    }

    RaftResult data(byte[] value) {
        this.data = value;
        return this;
    }

    RaftResult leaseId(long value) {
        this.leaseId = value;
        return this;
    }

    RaftResult createTime(long value) {
        this.createTime = value;
        return this;
    }

    RaftResult modifyTime(long value) {
        this.modifyTime = value;
        return this;
    }

    RaftResult counterValue(long value) {
        this.value = value;
        return this;
    }

    RaftResult revision(long value) {
        this.revision = value;
        return this;
    }

    RaftResult oldestRetainedRevision(long value) {
        this.oldestRetainedRevision = value;
        return this;
    }

    RaftResult deletedCount(long value) {
        this.deletedCount = value;
        return this;
    }

    RaftResult expired(boolean value) {
        this.expired = value;
        return this;
    }

    RaftResult found(boolean value) {
        this.found = value;
        return this;
    }

    RaftResult hasMore(boolean value) {
        this.hasMore = value;
        return this;
    }

    RaftResult failIndex(int value) {
        this.failIndex = value;
        return this;
    }

    RaftResult failStatus(Status value) {
        this.failStatus = value;
        return this;
    }

    RaftResult addEntry(Entry entry) {
        if (entries == null) {
            entries = new ArrayList<>();
        }
        entries.add(entry);
        return this;
    }

    RaftResult addLease(Lease lease) {
        if (leases == null) {
            leases = new ArrayList<>();
        }
        leases.add(lease);
        return this;
    }

    RaftResult subResults(List<RaftResult> value) {
        this.subResults = value;
        return this;
    }

    RaftResult records(List<ChangeEvent> value) {
        this.records = value;
        return this;
    }

    @Override
    public String toString() {
        return "RaftResult{op=" + op + ", status=" + status + ", version=" + version + '}';
    }
}
