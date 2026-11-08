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

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.ratis.metadata.proto.MetadataProtos.AdminWrite;
import org.apache.ratis.metadata.proto.MetadataProtos.ClientWrite;
import org.apache.ratis.metadata.proto.MetadataProtos.LeaseCandidate;
import org.apache.ratis.metadata.proto.MetadataProtos.NodeKind;
import org.apache.ratis.metadata.proto.MetadataProtos.Query;
import org.apache.ratis.metadata.proto.MetadataProtos.QueryReply;
import org.apache.ratis.metadata.proto.MetadataProtos.Role;
import org.apache.ratis.metadata.proto.MetadataProtos.WriteReply;
import org.apache.ratis.metadata.protocol.GroupConfig;
import org.apache.ratis.metadata.protocol.Stamp;
import org.apache.ratis.protocol.RaftGroupId;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

/**
 * The fixed contract-test harness: a real store on a real directory, driven
 * through the same commit path production uses. Engine-level entry points
 * (apply/admin/query/maintenance) arrive with T03; snapshot reinstall with
 * T04 - until then they fail loudly instead of pretending.
 */
final class MetadataContractFixture implements AutoCloseable {
    private final Path directory;
    private final Role role;
    private final GroupConfig config;
    private final RaftGroupId groupId;
    private final MetadataEngine engine;
    private long term = 1;
    private RocksMetadataStore store;

    private MetadataContractFixture(Path directory, Role role, GroupConfig config) {
        this.directory = directory;
        this.role = role;
        this.config = config;
        this.groupId = RaftGroupId.valueOf(java.util.UUID.nameUUIDFromBytes(
                ("rmd-fixture-" + role).getBytes(StandardCharsets.UTF_8)));
        this.engine = new MetadataEngine(groupId);
        this.store = RocksMetadataStore.open(directory);
    }

    static MetadataContractFixture create(Role role, GroupConfig config, Path directory) {
        return new MetadataContractFixture(directory, role, config);
    }

    RocksMetadataStore store() {
        return store;
    }

    Path directory() {
        return directory;
    }

    /** The fixture's deterministic group identity, used to freeze requests. */
    RaftGroupId groupId() {
        return groupId;
    }

    /** The term stamped onto every engine entry; tests may move it forward. */
    void setTerm(long newTerm) {
        this.term = newTerm;
    }

    long term() {
        return term;
    }

    /** Commits the initialization marker set at the given entry position. */
    void initialize(long term, long index) {
        store.initialize(config, term, Stamp.valueOf(index, 1));
    }

    /** @see RocksMetadataStore#newTransition(long, long, long) */
    PreparedTransition newUncommittedTransition(long term, long index, long logicalNow) {
        return store.newTransition(term, index, logicalNow);
    }

    /**
     * Stages one ordinary node into the transition's business buffer; a
     * test-only rollback probe, never part of the public API.
     */
    void stageTestNodeForRollback(PreparedTransition transition, byte[] key, byte[] value) {
        transition.business().put(MetadataCf.NODES, key, StoredRecordCodec.nodeValue(
                NodeRecord.newBuilder()
                        .setKind(NodeKind.NORMAL)
                        .setCreateStamp(Stamp.valueOf(1, 1))
                        .setModifyStamp(Stamp.valueOf(1, 1))
                        .setCreatedAt(1)
                        .setModifiedAt(1)
                        .setValue(value)
                        .build()));
    }

    /** Stages a stored result, its expiry-index row and the stats charge. */
    void stageStoredResult(PreparedTransition transition, byte[] writerId, long generation,
                           long sequence, StoredResult result) {
        final byte[] resultKey = StoredRecordCodec.businessResultKey(writerId, generation, sequence);
        transition.business().put(MetadataCf.COMMAND_RESULTS, resultKey,
                StoredRecordCodec.storedResultValue(result));
        transition.business().put(MetadataCf.RESULT_EXPIRY,
                StoredRecordCodec.resultExpiryKey(result.expiresAt(), resultKey),
                StoredRecordCodec.resultExpiryValue(result.firstIndex()));
        transition.stageStatsDelta(new StoreStats(
                InvocationResultStore.charge(resultKey, result), 0, 0, 0, 0, 0));
    }

    /** Makes the next {@code commit} fail after staging, before the db write. */
    void failNextWriteBeforeBatch() {
        store.setWriteGate(() -> {
            throw new IllegalStateException("injected pre-write failure");
        });
    }

    /**
     * @return every logical row of every column family: family, key and
     *         value in deterministic order - the physical SST layout never
     *         appears. Any read error propagates; a partial dump is never
     *         returned.
     */
    byte[] dumpLogicalTables() {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ReadView view = store.openReadView()) {
            for (MetadataCf cf : MetadataCf.values()) {
                writeChunk(out, cf.displayName().getBytes(StandardCharsets.UTF_8));
                final RocksIterator it = view.iterator(cf);
                int rows = 0;
                for (it.seekToFirst(); it.isValid(); it.next()) {
                    writeChunk(out, it.key());
                    writeChunk(out, it.value());
                    rows++;
                }
                // isValid()==false also covers a scan stopped by a storage
                // error; only status() tells that apart from a clean end.
                try {
                    it.status();
                } catch (RocksDBException e) {
                    throw MetadataStoreException.io("scan of " + cf + " failed after "
                            + rows + " rows", e);
                }
            }
        }
        return out.toByteArray();
    }

    private static void writeChunk(ByteArrayOutputStream out, byte[] bytes) {
        final int len = bytes.length;
        out.write((len >>> 24) & 0xff);
        out.write((len >>> 16) & 0xff);
        out.write((len >>> 8) & 0xff);
        out.write(len & 0xff);
        out.write(bytes, 0, len);
    }

    StoreStats stats() {
        return store.stats();
    }

    /**
     * Closes the store and overwrites one default-CF marker with arbitrary
     * raw bytes through a private raw handle - or, for a {@code null} value,
     * deletes the marker. The fixture's store stays closed until
     * {@link #reopenStore()}.
     */
    void writeRawMarker(String key, byte[] value) {
        if (store != null) {
            store.close();
            store = null;
        }
        final DBOptions options = new DBOptions();
        final List<ColumnFamilyOptions> cfOptions = new ArrayList<>();
        final List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
        final ColumnFamilyOptions defaultOptions = new ColumnFamilyOptions();
        cfOptions.add(defaultOptions);
        descriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, defaultOptions));
        for (MetadataCf cf : MetadataCf.values()) {
            if (cf == MetadataCf.DEFAULT) {
                continue;
            }
            final ColumnFamilyOptions options2 = new ColumnFamilyOptions();
            cfOptions.add(options2);
            descriptors.add(new ColumnFamilyDescriptor(cf.cfName(), options2));
        }
        try {
            final List<ColumnFamilyHandle> handles = new ArrayList<>();
            final RocksDB db = RocksDB.open(options,
                    directory.toAbsolutePath().toString(), descriptors, handles);
            try {
                if (value != null) {
                    db.put(handles.get(0), key.getBytes(StandardCharsets.UTF_8), value);
                } else {
                    db.delete(handles.get(0), key.getBytes(StandardCharsets.UTF_8));
                }
            } finally {
                for (ColumnFamilyHandle handle : handles) {
                    handle.close();
                }
                db.close();
            }
        } catch (RocksDBException e) {
            throw new IllegalStateException("failed to rewrite the " + key + " marker", e);
        } finally {
            cfOptions.forEach(ColumnFamilyOptions::close);
            options.close();
        }
    }

    /** Reopens the store on the same directory; a corrupt marker fails here. */
    RocksMetadataStore reopenStore() {
        if (store != null) {
            store.close();
        }
        store = RocksMetadataStore.open(directory);
        return store;
    }

    // ---- engine entry points: fixed shape, delivered with T03/T04 ----

    WriteReply apply(long index, long proposedTime, ClientWrite request) {
        return engine.apply(term, index, proposedTime, request, config, store);
    }

    WriteReply admin(long index, long proposedTime, AdminWrite request) {
        return engine.admin(term, index, proposedTime, request, config, store);
    }

    QueryReply query(Query request) {
        return engine.query(request, config, store);
    }

    void maintenance(long index, long proposedTime, LeaseCandidate candidate) {
        engine.maintenance(term, index, proposedTime, candidate, config, store);
    }

    void configEntry(long term, long index) {
        throw new UnsupportedOperationException("configEntry arrives with T04");
    }

    void reopenFromPublishedSnapshotAndLog() {
        throw new UnsupportedOperationException("reinstall arrives with T04");
    }

    @Override
    public void close() {
        if (store != null) {
            store.close();
        }
    }

    @Override
    public String toString() {
        return "MetadataContractFixture(" + role + "@" + directory + ")";
    }
}
