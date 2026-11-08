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
 * One row of the {@code leases} column family. {@code expireAt} lives in the
 * replicated logical-time domain; {@code bindingCount} equals the number of
 * reverse-index rows in {@code lease-paths} for this lease.
 */
final class LeaseRow {
    static final int WRITER_ID_BYTES = 16;

    private final byte[] ownerWriterId;
    private final long ownerGeneration;
    private final long ttlMs;
    private final long expireAt;
    private final long lastRenewIndex;
    private final int bindingCount;

    LeaseRow(byte[] ownerWriterId, long ownerGeneration, long ttlMs, long expireAt,
             long lastRenewIndex, int bindingCount) {
        this.ownerWriterId = ownerWriterId.clone();
        this.ownerGeneration = ownerGeneration;
        this.ttlMs = ttlMs;
        this.expireAt = expireAt;
        this.lastRenewIndex = lastRenewIndex;
        this.bindingCount = bindingCount;
    }

    /** @return a defensive copy of the 16-byte owner writer id. */
    byte[] ownerWriterId() {
        return ownerWriterId.clone();
    }

    long ownerGeneration() {
        return ownerGeneration;
    }

    long ttlMs() {
        return ttlMs;
    }

    long expireAt() {
        return expireAt;
    }

    long lastRenewIndex() {
        return lastRenewIndex;
    }

    int bindingCount() {
        return bindingCount;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof LeaseRow)) {
            return false;
        }
        final LeaseRow other = (LeaseRow) obj;
        return ownerGeneration == other.ownerGeneration
                && ttlMs == other.ttlMs
                && expireAt == other.expireAt
                && lastRenewIndex == other.lastRenewIndex
                && bindingCount == other.bindingCount
                && Arrays.equals(ownerWriterId, other.ownerWriterId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(ownerWriterId), ownerGeneration, ttlMs,
                expireAt, lastRenewIndex, bindingCount);
    }

    @Override
    public String toString() {
        return "(ownerGeneration=" + ownerGeneration + ",ttl=" + ttlMs
                + ",expireAt=" + expireAt + ",lastRenewIndex=" + lastRenewIndex
                + ",bindings=" + bindingCount + ")";
    }
}
