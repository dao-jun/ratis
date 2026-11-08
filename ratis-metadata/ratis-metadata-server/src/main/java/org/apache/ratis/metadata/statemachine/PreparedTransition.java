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
 * One staged, not-yet-committed transition. Row mutations accumulate in two
 * Java-side {@link MutationBuffer}s - business and maintenance - so a
 * deterministic business failure can drop the business delta while keeping
 * the maintenance work. Only {@link RocksMetadataStore#commit} materializes
 * the retained buffers plus the marker records into one WriteBatch and
 * writes it once; a transition that closes uncommitted discards every
 * effect.
 */
final class PreparedTransition implements AutoCloseable {
    /** The transition lifecycle as the store sees it. */
    enum Phase {
        STAGED, COMMITTED, DISCARDED
    }

    private final RocksMetadataStore owner;
    private final long term;
    private final long index;
    private final long logicalNowAfter;
    private final MutationBuffer business = new MutationBuffer();
    private final MutationBuffer maintenance = new MutationBuffer();

    private StoreStats statsDelta = StoreStats.ZERO;
    private long changeFloor = -1;
    private Phase phase = Phase.STAGED;

    PreparedTransition(RocksMetadataStore owner, long term, long index, long logicalNowAfter) {
        this.owner = owner;
        this.term = term;
        this.index = index;
        this.logicalNowAfter = logicalNowAfter;
    }

    long term() {
        return term;
    }

    long index() {
        return index;
    }

    long logicalNowAfter() {
        return logicalNowAfter;
    }

    /** @return the buffer of business-row mutations; dropped on business failure. */
    MutationBuffer business() {
        return checkStaged(business);
    }

    /** @return the buffer of maintenance mutations; survives a business failure. */
    MutationBuffer maintenance() {
        return checkStaged(maintenance);
    }

    /** Stages a stats delta applied to the persisted marker at commit time. */
    void stageStatsDelta(StoreStats delta) {
        checkStaged(this);
        statsDelta = statsDelta.plus(delta);
    }

    /** Stages the new change floor; the commit keeps the maximum seen. */
    void stageChangeFloor(long newFloor) {
        checkStaged(this);
        if (newFloor > changeFloor) {
            changeFloor = newFloor;
        }
    }

    Phase phase() {
        return phase;
    }

    RocksMetadataStore owner() {
        return owner;
    }

    StoreStats statsDelta() {
        return statsDelta;
    }

    long changeFloor() {
        return changeFloor;
    }

    void markCommitted() {
        phase = Phase.COMMITTED;
    }

    void markDiscarded() {
        phase = Phase.DISCARDED;
    }

    private <T> T checkStaged(T subject) {
        if (phase != Phase.STAGED) {
            throw new MetadataStoreException(MetadataStoreException.Reason.LIFECYCLE,
                    "the transition is " + phase);
        }
        return subject;
    }

    @Override
    public void close() {
        phase = Phase.DISCARDED; // closing a committed transition changes nothing
    }

    @Override
    public String toString() {
        return "(term=" + term + ",index=" + index + ",phase=" + phase
                + ",business=" + business.mutationCount() + ",maintenance=" + maintenance.mutationCount() + ")";
    }
}
