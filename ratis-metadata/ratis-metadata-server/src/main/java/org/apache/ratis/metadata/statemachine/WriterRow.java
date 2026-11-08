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

import org.apache.ratis.metadata.proto.MetadataProtos.Lane;

/** One row of the {@code writers} column family: a writer's lane and FIFO state. */
final class WriterRow {
    private final Lane lane;
    private final long currentGeneration;
    private final long highWater;

    WriterRow(Lane lane, long currentGeneration, long highWater) {
        this.lane = Objects.requireNonNull(lane, "lane");
        this.currentGeneration = currentGeneration;
        this.highWater = highWater;
    }

    Lane lane() {
        return lane;
    }

    long currentGeneration() {
        return currentGeneration;
    }

    long highWater() {
        return highWater;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof WriterRow)) {
            return false;
        }
        final WriterRow other = (WriterRow) obj;
        return lane == other.lane
                && currentGeneration == other.currentGeneration
                && highWater == other.highWater;
    }

    @Override
    public int hashCode() {
        return Objects.hash(lane, currentGeneration, highWater);
    }

    @Override
    public String toString() {
        return "(lane=" + lane + ",generation=" + currentGeneration + ",highWater=" + highWater + ")";
    }
}
