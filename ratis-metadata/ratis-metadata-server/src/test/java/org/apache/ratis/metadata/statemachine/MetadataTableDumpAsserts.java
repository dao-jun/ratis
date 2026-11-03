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

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Shared read-only content assertions over the leases, lease-paths and counters column
 * families of a metadata RocksDB - the tables the lock fencing tokens and the ephemeral
 * cascade depend on. Test fixture: no Test prefix, so surefire's Test*-only include rule
 * never runs it standalone.
 *
 * The layouts pinned here mirror the private encodings of MetadataStateMachine:
 * LeaseState (ttl@0, expireAtMs@8, lastRenewIndex@16), leasePathsKey (8-byte lease id +
 * path, empty value), counterKey (1 tag byte + name) and big-endian longs. A deliberate
 * format change must update this fixture in the same commit - that is the point.
 */
final class MetadataTableDumpAsserts {

    /** Mirrors MetadataStateMachine's private counter-key tags. */
    static final byte USER_COUNTER_TAG = 0x00;
    static final byte SEQUENTIAL_COUNTER_TAG = 0x01;

    private MetadataTableDumpAsserts() {}

    static ColumnFamilyHandle handleOf(List<ColumnFamilyHandle> handles, List<String> names, String name) {
        final int index = names.indexOf(name);
        assertTrue(index >= 0, "column family " + name + " is present");
        return handles.get(index);
    }

    /** Exact-content assertion over the three families of an open read-only database. */
    static void assertLeaseIndexAndCounterContent(
            RocksDB db,
            ColumnFamilyHandle leases,
            ColumnFamilyHandle leasePaths,
            ColumnFamilyHandle counters,
            Map<Long, String> expectedLeases,
            Set<String> expectedBindings,
            Map<String, Long> expectedCounters)
            throws RocksDBException {
        final Map<Long, String> leaseTable = new TreeMap<>();
        try (RocksIterator iterator = db.newIterator(leases)) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                leaseTable.put(
                        decodeLong(iterator.key(), 0),
                        decodeLong(iterator.value(), 0) + "/" + decodeLong(iterator.value(), 8)
                                + "/" + decodeLong(iterator.value(), 16));
            }
        }
        assertEquals(
                expectedLeases,
                leaseTable,
                "the exact leases table survives (ttl/expireAt/lastRenewIndex per lease)");

        final Set<String> bindings = new TreeSet<>();
        try (RocksIterator iterator = db.newIterator(leasePaths)) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                bindings.add(
                        decodeLong(iterator.key(), 0) + ":" + new String(
                                iterator.key(), 8, iterator.key().length - 8, StandardCharsets.UTF_8));
            }
        }
        assertEquals(expectedBindings, bindings, "the exact lease-paths reverse index survives");

        final Map<String, Long> counterValues = new TreeMap<>();
        try (RocksIterator iterator = db.newIterator(counters)) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                counterValues.put(
                        (iterator.key()[0] == USER_COUNTER_TAG ? "user:" : "sequential:")
                                + new String(iterator.key(), 1, iterator.key().length - 1,
                                        StandardCharsets.UTF_8),
                        decodeLong(iterator.value(), 0));
            }
        }
        assertEquals(
                expectedCounters, counterValues, "the exact counters table survives (fencing-token state)");
    }

    /** Big-endian long at offset - the shared key/value encoding of every metadata table. */
    static long decodeLong(byte[] source, int offset) {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value = (value << 8) | (source[offset + i] & 0xFFL);
        }
        return value;
    }
}
