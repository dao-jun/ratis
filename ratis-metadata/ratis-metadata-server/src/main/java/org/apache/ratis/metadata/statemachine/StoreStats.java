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

import java.util.Objects;

/**
 * The persisted {@code stats} marker: the six counters every transition must
 * keep in step with the rows it stages, each charged and credited by the same
 * encoded-byte definition. Result byte counters are per lane; change byte
 * counters separate logical visibility ({@code visibleChangeBytes}, the quota
 * input) from physical disk ({@code physicalChangeBytes}, the disk-warning
 * input).
 */
final class StoreStats {
    static final StoreStats ZERO = new StoreStats(0, 0, 0, 0, 0, 0);

    private final long businessResultBytes;
    private final long controlResultBytes;
    private final long visibleChangeBytes;
    private final long physicalChangeBytes;
    private final long writerCount;
    private final long leaseCount;

    StoreStats(long businessResultBytes, long controlResultBytes, long visibleChangeBytes,
               long physicalChangeBytes, long writerCount, long leaseCount) {
        this.businessResultBytes = businessResultBytes;
        this.controlResultBytes = controlResultBytes;
        this.visibleChangeBytes = visibleChangeBytes;
        this.physicalChangeBytes = physicalChangeBytes;
        this.writerCount = writerCount;
        this.leaseCount = leaseCount;
    }

    long businessResultBytes() {
        return businessResultBytes;
    }

    long controlResultBytes() {
        return controlResultBytes;
    }

    long visibleChangeBytes() {
        return visibleChangeBytes;
    }

    long physicalChangeBytes() {
        return physicalChangeBytes;
    }

    long writerCount() {
        return writerCount;
    }

    long leaseCount() {
        return leaseCount;
    }

    /** @return the element-wise sum, rejecting any component overflow. */
    StoreStats plus(StoreStats delta) {
        return new StoreStats(
                Math.addExact(businessResultBytes, delta.businessResultBytes),
                Math.addExact(controlResultBytes, delta.controlResultBytes),
                Math.addExact(visibleChangeBytes, delta.visibleChangeBytes),
                Math.addExact(physicalChangeBytes, delta.physicalChangeBytes),
                Math.addExact(writerCount, delta.writerCount),
                Math.addExact(leaseCount, delta.leaseCount));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof StoreStats)) {
            return false;
        }
        final StoreStats other = (StoreStats) obj;
        return businessResultBytes == other.businessResultBytes
                && controlResultBytes == other.controlResultBytes
                && visibleChangeBytes == other.visibleChangeBytes
                && physicalChangeBytes == other.physicalChangeBytes
                && writerCount == other.writerCount
                && leaseCount == other.leaseCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(businessResultBytes, controlResultBytes, visibleChangeBytes,
                physicalChangeBytes, writerCount, leaseCount);
    }

    @Override
    public String toString() {
        return "(businessResultBytes=" + businessResultBytes
                + ",controlResultBytes=" + controlResultBytes
                + ",visibleChangeBytes=" + visibleChangeBytes
                + ",physicalChangeBytes=" + physicalChangeBytes
                + ",writers=" + writerCount + ",leases=" + leaseCount + ")";
    }
}
