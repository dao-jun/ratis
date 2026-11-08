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

import java.util.Arrays;
import java.util.Objects;

import org.apache.ratis.metadata.proto.MetadataProtos.NodeKind;
import org.apache.ratis.metadata.protocol.Stamp;

/**
 * The immutable value of one row in the {@code nodes} column family. A valid
 * instance satisfies the record invariants of storage section 2: stamps have
 * {@code index >= 0} and {@code ordinal > 0}; lease ownership is all-or-none
 * (lease id, owner writer and generation are either all meaningful or all
 * zero); a lock node carries no value and owns a lease; a normal node never
 * carries a lock index. Timestamps are replicated logical time, metadata
 * only.
 */
final class NodeRecord {
    private static final int WRITER_ID_BYTES = 16;

    private final NodeKind kind;
    private final Stamp createStamp;
    private final Stamp modifyStamp;
    private final long leaseId;
    private final byte[] ownerWriterId;
    private final long ownerGeneration;
    private final long lockIndex;
    private final long createdAt;
    private final long modifiedAt;
    private final byte[] value;

    private NodeRecord(Builder builder) {
        this.kind = builder.kind;
        this.createStamp = builder.createStamp;
        this.modifyStamp = builder.modifyStamp;
        this.leaseId = builder.leaseId;
        this.ownerWriterId = builder.ownerWriterId;
        this.ownerGeneration = builder.ownerGeneration;
        this.lockIndex = builder.lockIndex;
        this.createdAt = builder.createdAt;
        this.modifiedAt = builder.modifiedAt;
        this.value = builder.value;
    }

    NodeKind kind() {
        return kind;
    }

    Stamp createStamp() {
        return createStamp;
    }

    Stamp modifyStamp() {
        return modifyStamp;
    }

    /** @return the owning lease id, or 0 when the node is persistent. */
    long leaseId() {
        return leaseId;
    }

    /** @return a defensive copy of the 16-byte owner writer id, zeros when unowned. */
    byte[] ownerWriterId() {
        return ownerWriterId.clone();
    }

    /** @return the owner's generation, or 0 when the node is persistent. */
    long ownerGeneration() {
        return ownerGeneration;
    }

    /** @return the log index the lock was granted at, or 0 for non-lock nodes. */
    long lockIndex() {
        return lockIndex;
    }

    long createdAt() {
        return createdAt;
    }

    long modifiedAt() {
        return modifiedAt;
    }

    /** @return a defensive copy of the value; empty for lock nodes. */
    byte[] value() {
        return value.clone();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof NodeRecord)) {
            return false;
        }
        final NodeRecord other = (NodeRecord) obj;
        return kind == other.kind
                && createStamp.equals(other.createStamp)
                && modifyStamp.equals(other.modifyStamp)
                && leaseId == other.leaseId
                && Arrays.equals(ownerWriterId, other.ownerWriterId)
                && ownerGeneration == other.ownerGeneration
                && lockIndex == other.lockIndex
                && createdAt == other.createdAt
                && modifiedAt == other.modifiedAt
                && Arrays.equals(value, other.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, createStamp, modifyStamp, leaseId,
                Arrays.hashCode(ownerWriterId), ownerGeneration, lockIndex,
                createdAt, modifiedAt, Arrays.hashCode(value));
    }

    @Override
    public String toString() {
        return "(kind=" + kind + ",create=" + createStamp + ",modify=" + modifyStamp
                + ",lease=" + leaseId + ",ownerGeneration=" + ownerGeneration
                + ",lockIndex=" + lockIndex + ",valueLength=" + value.length + ")";
    }

    static Builder newBuilder() {
        return new Builder();
    }

    /** Mutates only before {@link #build()}; the record itself is immutable. */
    static final class Builder {
        private NodeKind kind;
        private Stamp createStamp;
        private Stamp modifyStamp;
        private long leaseId;
        private byte[] ownerWriterId = new byte[WRITER_ID_BYTES];
        private long ownerGeneration;
        private long lockIndex;
        private long createdAt;
        private long modifiedAt;
        private byte[] value = new byte[0];

        private Builder() {
        }

        Builder setKind(NodeKind kind) {
            this.kind = kind;
            return this;
        }

        Builder setCreateStamp(Stamp stamp) {
            this.createStamp = stamp;
            return this;
        }

        Builder setModifyStamp(Stamp stamp) {
            this.modifyStamp = stamp;
            return this;
        }

        Builder setLeaseOwnership(long newLeaseId, byte[] newOwnerWriterId, long newOwnerGeneration) {
            this.leaseId = newLeaseId;
            this.ownerWriterId = newOwnerWriterId.clone();
            this.ownerGeneration = newOwnerGeneration;
            return this;
        }

        Builder setLockIndex(long lockIndex) {
            this.lockIndex = lockIndex;
            return this;
        }

        Builder setCreatedAt(long createdAt) {
            this.createdAt = createdAt;
            return this;
        }

        Builder setModifiedAt(long modifiedAt) {
            this.modifiedAt = modifiedAt;
            return this;
        }

        Builder setValue(byte[] value) {
            this.value = value.clone();
            return this;
        }

        NodeRecord build() {
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(createStamp, "createStamp");
            Objects.requireNonNull(modifyStamp, "modifyStamp");
            Objects.requireNonNull(value, "value");
            requireStamp("createStamp", createStamp);
            requireStamp("modifyStamp", modifyStamp);
            if (ownerWriterId.length != WRITER_ID_BYTES) {
                throw MetadataStoreException.corrupt(
                        "ownerWriterId must be " + WRITER_ID_BYTES + " bytes, got "
                                + ownerWriterId.length);
            }
            final boolean owned = leaseId != 0;
            final boolean ownerPresent = ownerGeneration != 0 || !isAllZero(ownerWriterId);
            if (owned != ownerPresent || (owned && (leaseId < 1 || ownerGeneration < 1))) {
                throw MetadataStoreException.corrupt(
                        "lease ownership must be all-or-none: leaseId=" + leaseId
                                + ", ownerGeneration=" + ownerGeneration);
            }
            switch (kind) {
                case NORMAL:
                    if (lockIndex != 0) {
                        throw MetadataStoreException.corrupt("a normal node carries no lock index");
                    }
                    break;
                case EPHEMERAL_LOCK:
                    if (!owned) {
                        throw MetadataStoreException.corrupt("a lock node must own a lease");
                    }
                    if (lockIndex < 1) {
                        throw MetadataStoreException.corrupt(
                                "a lock node's lockIndex must be meaningful, not " + lockIndex);
                    }
                    if (value.length != 0) {
                        throw MetadataStoreException.corrupt("a lock node carries no value");
                    }
                    break;
                default:
                    throw MetadataStoreException.corrupt("unknown node kind " + kind);
            }
            if (createdAt < 0 || modifiedAt < 0) {
                throw MetadataStoreException.corrupt(
                        "timestamps must not be negative: createdAt=" + createdAt
                                + ", modifiedAt=" + modifiedAt);
            }
            return new NodeRecord(this);
        }

        private static void requireStamp(String name, Stamp stamp) {
            if (stamp.logIndex() < 0 || stamp.ordinal() < 1) {
                throw MetadataStoreException.corrupt(
                        name + " must have index >= 0 and ordinal > 0: " + stamp);
            }
        }

        private static boolean isAllZero(byte[] bytes) {
            for (byte b : bytes) {
                if (b != 0) {
                    return false;
                }
            }
            return true;
        }
    }
}
