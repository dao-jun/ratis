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

import java.util.ArrayList;
import java.util.List;

import org.rocksdb.AbstractNativeReference;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.ReadOptions;
import org.rocksdb.Snapshot;

/**
 * A short-lived snapshot read over one consistent cut of the store, holding
 * the group lifecycle read lease for its whole life. Iterators handed out
 * are tracked: closing the view releases them first, then the ReadOptions,
 * then the snapshot, then the lease - never the reverse. Every iterator,
 * whatever ended its walk, must have {@code status()} checked before its
 * results are used.
 */
final class ReadView implements AutoCloseable {
    private final RocksMetadataStore owner;
    private final RocksDB db;
    private final ColumnFamilyHandle[] handles;
    private final ReadOptions readOptions;
    private final Snapshot snapshot;
    private final List<AbstractNativeReference> tracked = new ArrayList<>();
    private boolean closed;

    ReadView(RocksMetadataStore owner, RocksDB db, ColumnFamilyHandle[] handles) {
        this.owner = owner;
        this.db = db;
        this.handles = handles;
        this.readOptions = new ReadOptions();
        this.snapshot = db.getSnapshot();
        this.readOptions.setSnapshot(snapshot);
    }

    /**
     * @return a tracked iterator over the column family; every caller must
     *         check {@code status()} before using results, whatever ended
     *         the iteration.
     */
    RocksIterator iterator(MetadataCf cf) {
        requireOpen();
        final RocksIterator iterator = db.newIterator(handles[cf.ordinal()], readOptions);
        synchronized (tracked) {
            tracked.add(iterator);
        }
        return iterator;
    }

    /** @return the raw value bytes at the key, or {@code null} when absent. */
    byte[] get(MetadataCf cf, byte[] key) {
        requireOpen();
        try {
            return db.get(handles[cf.ordinal()], readOptions, key);
        } catch (org.rocksdb.RocksDBException e) {
            throw MetadataStoreException.io("point read of " + cf + " failed", e);
        }
    }

    private void requireOpen() {
        if (closed) {
            throw new MetadataStoreException(MetadataStoreException.Reason.LIFECYCLE,
                    "the read view is closed");
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        // release order: iterators, then ReadOptions, then the snapshot, then the lease
        synchronized (tracked) {
            for (AbstractNativeReference nativeRef : tracked) {
                nativeRef.close(); // idempotent: a caller-side close is a no-op here
            }
            tracked.clear();
        }
        readOptions.close();
        snapshot.close();
        owner.releaseReadLease();
    }
}
