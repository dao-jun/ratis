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

import java.util.Comparator;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * One Java-side staging buffer of row mutations: a value is a put, an empty
 * {@link Optional} is a tombstone. Rows are keyed in unsigned byte order and
 * visited in column-family declaration order, so materializing the buffer
 * into the {@link org.rocksdb.WriteBatch} is byte-deterministic. A
 * {@code null} value was never staged and the buffer never resurrects a row
 * another entry deleted: same-key staging keeps only the latest state.
 */
final class MutationBuffer {
    /**
     * Unsigned byte-lexicographic order, the same order RocksDB keys use;
     * {@code Arrays.compareUnsigned} is not available on Java 8.
     */
    static final Comparator<byte[]> UNSIGNED_ORDER = MutationBuffer::compareUnsigned;

    private final Map<MetadataCf, TreeMap<byte[], Optional<byte[]>>> rows = new EnumMap<>(MetadataCf.class);
    private long mutationCount;

    void put(MetadataCf cf, byte[] key, byte[] value) {
        stage(cf, key, Optional.of(value.clone()));
    }

    void delete(MetadataCf cf, byte[] key) {
        stage(cf, key, Optional.empty());
    }

    boolean isEmpty() {
        return mutationCount == 0;
    }

    long mutationCount() {
        return mutationCount;
    }

    /**
     * Visits every staged mutation exactly once: column families in
     * declaration order, keys in unsigned order.
     *
     * @param consumer receives (cf, key, value); the value is empty for a tombstone.
     */
    void forEach(MutationConsumer consumer) {
        for (Map.Entry<MetadataCf, TreeMap<byte[], Optional<byte[]>>> cfEntry : rows.entrySet()) {
            for (Map.Entry<byte[], Optional<byte[]>> row : cfEntry.getValue().entrySet()) {
                consumer.accept(cfEntry.getKey(), row.getKey(), row.getValue());
            }
        }
    }

    private void stage(MetadataCf cf, byte[] key, Optional<byte[]> value) {
        final TreeMap<byte[], Optional<byte[]>> cfRows =
                rows.computeIfAbsent(cf, ignored -> new TreeMap<>(UNSIGNED_ORDER));
        final Optional<byte[]> previous = cfRows.put(key.clone(), value);
        if (previous == null) {
            mutationCount++;
        }
    }

    /** One staged row: a put with a value, or a tombstone with none. */
    interface MutationConsumer {
        void accept(MetadataCf cf, byte[] key, Optional<byte[]> value);
    }

    /** Manual unsigned comparator; see {@link #UNSIGNED_ORDER}. */
    static int compareUnsigned(byte[] left, byte[] right) {
        final int shared = Math.min(left.length, right.length);
        for (int i = 0; i < shared; i++) {
            final int leftInt = left[i] & 0xff;
            final int rightInt = right[i] & 0xff;
            if (leftInt != rightInt) {
                return leftInt < rightInt ? -1 : 1;
            }
        }
        return Integer.compare(left.length, right.length);
    }

    @Override
    public String toString() {
        return "MutationBuffer(" + mutationCount + " rows)";
    }
}
