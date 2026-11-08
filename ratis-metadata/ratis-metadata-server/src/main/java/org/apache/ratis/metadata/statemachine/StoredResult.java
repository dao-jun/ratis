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

/**
 * The immutable value of one row in the {@code command-results} column
 * family: the reply of one admitted write, retained for the retention window
 * so a retried operation replays its original outcome instead of executing
 * twice. The stored bytes are the codec-level snapshot; the retention bound
 * itself is a {@link org.apache.ratis.metadata.protocol.GroupConfig} concern
 * checked when staging, not when decoding.
 */
final class StoredResult {
    static final int DIGEST_BYTES = 32;

    private final byte[] digest;
    private final long firstTerm;
    private final long firstIndex;
    private final long executedAt;
    private final long expiresAt;
    private final byte[] reply;

    private StoredResult(Builder builder) {
        this.digest = builder.digest;
        this.firstTerm = builder.firstTerm;
        this.firstIndex = builder.firstIndex;
        this.executedAt = builder.executedAt;
        this.expiresAt = builder.expiresAt;
        this.reply = builder.reply;
    }

    /** @return a defensive copy of the 32-byte operation digest. */
    byte[] digest() {
        return digest.clone();
    }

    long firstTerm() {
        return firstTerm;
    }

    long firstIndex() {
        return firstIndex;
    }

    long executedAt() {
        return executedAt;
    }

    long expiresAt() {
        return expiresAt;
    }

    /** @return a defensive copy of the original reply payload. */
    byte[] reply() {
        return reply.clone();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof StoredResult)) {
            return false;
        }
        final StoredResult other = (StoredResult) obj;
        return firstTerm == other.firstTerm
                && firstIndex == other.firstIndex
                && executedAt == other.executedAt
                && expiresAt == other.expiresAt
                && Arrays.equals(digest, other.digest)
                && Arrays.equals(reply, other.reply);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(digest), firstTerm, firstIndex,
                executedAt, expiresAt, Arrays.hashCode(reply));
    }

    @Override
    public String toString() {
        return "(firstTerm=" + firstTerm + ",firstIndex=" + firstIndex
                + ",executedAt=" + executedAt + ",expiresAt=" + expiresAt
                + ",replyLength=" + reply.length + ")";
    }

    static Builder newBuilder() {
        return new Builder();
    }

    /** Mutates only before {@link #build()}; the record itself is immutable. */
    static final class Builder {
        private byte[] digest;
        private long firstTerm;
        private long firstIndex = -1;
        private long executedAt = -1;
        private long expiresAt = -1;
        private byte[] reply;

        private Builder() {
        }

        Builder setDigest(byte[] digest) {
            this.digest = digest.clone();
            return this;
        }

        Builder setFirstTerm(long firstTerm) {
            this.firstTerm = firstTerm;
            return this;
        }

        Builder setFirstIndex(long firstIndex) {
            this.firstIndex = firstIndex;
            return this;
        }

        Builder setExecutedAt(long executedAt) {
            this.executedAt = executedAt;
            return this;
        }

        Builder setExpiresAt(long expiresAt) {
            this.expiresAt = expiresAt;
            return this;
        }

        Builder setReply(byte[] reply) {
            this.reply = reply.clone();
            return this;
        }

        StoredResult build() {
            Objects.requireNonNull(digest, "digest");
            Objects.requireNonNull(reply, "reply");
            if (digest.length != DIGEST_BYTES) {
                throw MetadataStoreException.corrupt(
                        "digest must be " + DIGEST_BYTES + " bytes, got " + digest.length);
            }
            if (firstTerm < 0 || firstIndex < 0 || executedAt < 0 || expiresAt < 0) {
                throw MetadataStoreException.corrupt("negative field in " + this);
            }
            return new StoredResult(this);
        }
    }
}
