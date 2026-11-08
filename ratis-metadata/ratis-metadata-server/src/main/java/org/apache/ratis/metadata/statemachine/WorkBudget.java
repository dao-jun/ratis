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

/**
 * A deterministic per-transition budget: an item count and a byte count,
 * each capped and each computed with overflow-checked arithmetic so a
 * hostile record cannot wrap the counter past its limit. Charges are
 * boolean questions - the caller decides whether "over budget" is a page
 * boundary or a capacity status - and the budget never consults wall time.
 */
final class WorkBudget {
    private final long maxItems;
    private final long maxBytes;
    private long chargedItems;
    private long chargedBytes;

    WorkBudget(long maxItems, long maxBytes) {
        if (maxItems < 0 || maxBytes < 0) {
            throw new IllegalArgumentException("budgets must not be negative: "
                    + maxItems + " items, " + maxBytes + " bytes");
        }
        this.maxItems = maxItems;
        this.maxBytes = maxBytes;
    }

    /**
     * @return whether the charge fits; {@code false} when it would exceed a
     *         cap or overflow the counter, in which case nothing is charged.
     *         The comparison form cannot overflow: cap and charged are both
     *         non-negative, so their difference stays in range.
     */
    boolean tryCharge(long items, long bytes) {
        if (items < 0 || bytes < 0) {
            throw new IllegalArgumentException("charges must not be negative: " + items + ", " + bytes);
        }
        if (items > maxItems - chargedItems || bytes > maxBytes - chargedBytes) {
            return false;
        }
        chargedItems += items;
        chargedBytes += bytes;
        return true;
    }

    long chargedItems() {
        return chargedItems;
    }

    long chargedBytes() {
        return chargedBytes;
    }

    @Override
    public String toString() {
        return "(items=" + chargedItems + "/" + maxItems + ",bytes=" + chargedBytes + "/" + maxBytes + ")";
    }
}
