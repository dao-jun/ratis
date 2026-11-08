/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import static org.apache.ratis.metadata.statemachine.MetadataStateMachine.bytesToLong;
import static org.apache.ratis.metadata.statemachine.MetadataStateMachine.putLong;

/** The lease state stored in the {@code leases} CF; three big-endian longs. */
final class LeaseState {
    private final long ttlMs;
    private final long expireAtMs;
    private final long lastRenewIndex;

    long ttlMs() {
        return ttlMs;
    }

    long expireAtMs() {
        return expireAtMs;
    }

    long lastRenewIndex() {
        return lastRenewIndex;
    }

    LeaseState(long ttlMs, long expireAtMs, long lastRenewIndex) {
        this.ttlMs = ttlMs;
        this.expireAtMs = expireAtMs;
        this.lastRenewIndex = lastRenewIndex;
    }

    byte[] encode() {
        final byte[] encoded = new byte[24];
        putLong(encoded, 0, ttlMs);
        putLong(encoded, 8, expireAtMs);
        putLong(encoded, 16, lastRenewIndex);
        return encoded;
    }

    static LeaseState decode(byte[] encoded) {
        return new LeaseState(bytesToLong(encoded, 0), bytesToLong(encoded, 8), bytesToLong(encoded, 16));
    }
}
