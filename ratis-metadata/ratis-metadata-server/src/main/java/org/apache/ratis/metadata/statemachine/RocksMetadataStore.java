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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.ratis.metadata.IOUtils;
import org.apache.ratis.metadata.proto.MetadataProtos.Role;
import org.apache.ratis.metadata.protocol.GroupConfig;
import org.apache.ratis.metadata.protocol.MetadataProtocol;
import org.apache.ratis.metadata.protocol.MetadataProtocolException;
import org.apache.ratis.metadata.protocol.Stamp;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

/**
 * The RocksDB-backed store of one group directory, layout version 3. Owns
 * the native handles, the group lifecycle read lease (query/apply hold it;
 * close/restore waits for it to drain) and the apply/checkpoint mutex,
 * acquired only after the lifecycle side. {@link #initialize} writes
 * exactly once per directory and {@link #commit} is the only path that
 * writes afterwards; both hold the mutex. Any RocksDB or record-decoding
 * failure surfaces as a
 * {@link MetadataStoreException} and fails the group - it is never an empty
 * result and never a business status.
 */
final class RocksMetadataStore implements AutoCloseable {
    /** The lifecycle state used to fence new read leases during close. */
    enum State {
        OPEN, CLOSING, CLOSED
    }

    /** Test seam invoked inside {@link #commit} just before the single db write. */
    interface WriteGate {
        void beforeWrite();
    }

    /** How long close() waits for read leases to drain before giving up. */
    static final long CLOSE_TIMEOUT_MS = 10_000;

    private static final byte[] GROUP_CONFIG_KEY = bytes("group-config");
    private static final byte[] APPLIED_KEY = bytes("applied");
    private static final byte[] LOGICAL_TIME_KEY = bytes("logical-time");
    private static final byte[] ROOT_STAMP_KEY = bytes("root-stamp");
    private static final byte[] CHANGE_FLOOR_KEY = bytes("change-floor");
    private static final byte[] STATS_KEY = bytes("stats");

    private final Path directory;
    private final Object stateLock = new Object();
    private State state = State.OPEN;
    private int activeReadLeases;
    private final ReentrantLock applyMutex = new ReentrantLock();
    private WriteGate gate;

    // native handles, destroyed exactly once under the state monitor after drain
    private RocksDB db;
    private final List<AutoCloseable> ownedNatives = new ArrayList<>();
    private ColumnFamilyHandle[] handles;
    private WriteOptions writeOptions;

    // published markers, strict-decoded at open and after each commit
    private boolean initialized;
    private Role role;
    private GroupConfig groupConfig;
    private long appliedTermValue;
    private long appliedIndexValue;
    private long logicalTimeValue;
    private long changeFloorValue;
    private StoreStats statsValue = StoreStats.ZERO;

    private RocksMetadataStore(Path directory) {
        this.directory = directory;
    }

    private static byte[] bytes(String key) {
        return key.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Opens (creating if absent) one group directory. Markers present are
     * all strictly decoded; a partial or contradictory marker set is
     * corruption.
     *
     * @throws MetadataStoreException on any RocksDB or codec failure.
     */
    static RocksMetadataStore open(Path directory) {
        final RocksMetadataStore store = new RocksMetadataStore(directory.toAbsolutePath());
        try {
            Files.createDirectories(store.directory);
        } catch (IOException e) {
            throw MetadataStoreException.io("cannot create " + store.directory, e);
        }
        try {
            store.openDb();
            store.loadMarkers();
        } catch (RuntimeException e) {
            store.destroyNatives();
            throw e;
        }
        return store;
    }

    private void openDb() {
        final DBOptions options = new DBOptions()
                .setCreateIfMissing(true)
                // our column families do not exist on first open and must be created
                .setCreateMissingColumnFamilies(true)
                .setAtomicFlush(true)
                .setUseFsync(false)
                .setStatsDumpPeriodSec(0);
        ownedNatives.add(options);
        final List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
        final ColumnFamilyOptions defaultCfOptions = new ColumnFamilyOptions();
        ownedNatives.add(defaultCfOptions);
        descriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, defaultCfOptions));
        for (MetadataCf cf : MetadataCf.values()) {
            if (cf == MetadataCf.DEFAULT) {
                continue;
            }
            final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
            ownedNatives.add(cfOptions);
            descriptors.add(new ColumnFamilyDescriptor(cf.cfName(), cfOptions));
        }
        final List<ColumnFamilyHandle> opened = new ArrayList<>(descriptors.size());
        try {
            db = RocksDB.open(options, directory.toString(), descriptors, opened);
        } catch (RocksDBException e) {
            throw MetadataStoreException.io("failed to open metadata store at " + directory, e);
        }
        // closed after the handles and before the options on teardown
        ownedNatives.add(db);
        handles = opened.toArray(new ColumnFamilyHandle[0]);
        for (ColumnFamilyHandle handle : handles) {
            ownedNatives.add(handle);
        }
        // The raft log is the WAL; the RocksDB WAL buys nothing and recovery
        // is from a published checkpoint plus the log (storage section 6).
        writeOptions = new WriteOptions().setDisableWAL(true);
        ownedNatives.add(writeOptions);
    }

    private void loadMarkers() {
        final byte[] groupConfigRecord = getMarker(GROUP_CONFIG_KEY);
        if (groupConfigRecord == null) {
            requireNoMarker(APPLIED_KEY);
            requireNoMarker(LOGICAL_TIME_KEY);
            requireNoMarker(ROOT_STAMP_KEY);
            requireNoMarker(CHANGE_FLOOR_KEY);
            requireNoMarker(STATS_KEY);
            initialized = false;
            return;
        }
        final byte[] canonicalProto = StoredRecordCodec.decodeGroupConfig(groupConfigRecord);
        try {
            groupConfig = GroupConfig.parseFrom(canonicalProto);
        } catch (InvalidProtocolBufferException | MetadataProtocolException e) {
            throw MetadataStoreException.corrupt("the group-config record is not a valid config", e);
        }
        role = groupConfig.role();
        final long[] applied = StoredRecordCodec.decodeApplied(requireMarker(APPLIED_KEY));
        appliedTermValue = applied[0];
        appliedIndexValue = applied[1];
        logicalTimeValue = StoredRecordCodec.decodeLogicalTime(requireMarker(LOGICAL_TIME_KEY));
        StoredRecordCodec.decodeRootStamp(requireMarker(ROOT_STAMP_KEY));
        changeFloorValue = StoredRecordCodec.decodeChangeFloor(requireMarker(CHANGE_FLOOR_KEY));
        statsValue = StoredRecordCodec.decodeStats(requireMarker(STATS_KEY));
        initialized = true;
    }

    private byte[] requireMarker(byte[] key) {
        final byte[] value = getMarker(key);
        if (value == null) {
            throw MetadataStoreException.corrupt("marker " + new String(key, StandardCharsets.UTF_8)
                    + " is missing from an initialized store");
        }
        return value;
    }

    private void requireNoMarker(byte[] key) {
        if (getMarker(key) != null) {
            throw MetadataStoreException.corrupt("marker " + new String(key, StandardCharsets.UTF_8)
                    + " exists without a group-config record");
        }
    }

    private byte[] getMarker(byte[] key) {
        try {
            return db.get(handles[MetadataCf.DEFAULT.ordinal()], key);
        } catch (RocksDBException e) {
            throw MetadataStoreException.io("failed to read marker " + new String(key,
                    StandardCharsets.UTF_8), e);
        }
    }

    // ---- lifecycle lease ----

    private void acquireReadLease() {
        synchronized (stateLock) {
            if (state != State.OPEN) {
                throw new MetadataStoreException(MetadataStoreException.Reason.LIFECYCLE,
                        "the store is " + state);
            }
            activeReadLeases++;
        }
    }

    void releaseReadLease() {
        synchronized (stateLock) {
            activeReadLeases--;
            if (activeReadLeases == 0 && state == State.CLOSING) {
                stateLock.notifyAll();
            }
        }
    }

    // ---- accessors ----

    boolean initialized() {
        return initialized;
    }

    Role role() {
        return role;
    }

    GroupConfig groupConfig() {
        return groupConfig;
    }

    long appliedTerm() {
        return appliedTermValue;
    }

    long appliedIndex() {
        return appliedIndexValue;
    }

    long logicalTime() {
        return logicalTimeValue;
    }

    long changeFloor() {
        return changeFloorValue;
    }

    /** @return the persisted stats marker, strictly decoded from the db. */
    StoreStats stats() {
        acquireReadLease();
        try {
            return StoredRecordCodec.decodeStats(requireMarker(STATS_KEY));
        } finally {
            releaseReadLease();
        }
    }

    State state() {
        synchronized (stateLock) {
            return state;
        }
    }

    /** Installs the one-shot test seam invoked just before the single db write. */
    void setWriteGate(WriteGate writeGate) {
        this.gate = writeGate;
    }

    // ---- initialization ----

    /**
     * Commits the initialization marker set of a still-empty store:
     * group-config, applied, logical-time, root-stamp, change-floor and
     * stats in one batch, under the apply/checkpoint mutex so the
     * initialized check and the write are one atomic step. The role and
     * fingerprint are carried by the config. Idempotent for the identical
     * config; any different initialize attempt is rejected and the role
     * stays pinned for the life of the directory.
     *
     * @param term       the term of the initializing raft entry
     * @param rootStamp  the initialization entry's stamp; the root's permanent lifecycle
     */
    void initialize(GroupConfig config, long term, Stamp rootStamp) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(rootStamp, "rootStamp");
        acquireReadLease();
        try {
            applyMutex.lock();
            try {
                if (initialized) {
                    if (!this.groupConfig.equals(config)) {
                        throw new MetadataStoreException(MetadataStoreException.Reason.LIFECYCLE,
                                "the directory is already initialized as " + this.role
                                        + "; refusing to re-initialize as " + config.role());
                    }
                    return; // identical initialize replays as a no-op
                }
                final WriteBatch batch = new WriteBatch();
                try {
                    final ColumnFamilyHandle markerHandle = handles[MetadataCf.DEFAULT.ordinal()];
                    putMarker(batch, markerHandle, GROUP_CONFIG_KEY,
                            StoredRecordCodec.groupConfigValue(config.toProto().toByteArray(),
                                    MetadataProtocol.fingerprint(config)));
                    putMarker(batch, markerHandle, APPLIED_KEY, StoredRecordCodec.appliedValue(term,
                            rootStamp.logIndex()));
                    putMarker(batch, markerHandle, LOGICAL_TIME_KEY,
                            StoredRecordCodec.logicalTimeValue(0));
                    putMarker(batch, markerHandle, ROOT_STAMP_KEY,
                            StoredRecordCodec.rootStampValue(rootStamp));
                    putMarker(batch, markerHandle, CHANGE_FLOOR_KEY,
                            StoredRecordCodec.changeFloorValue(rootStamp.logIndex()));
                    putMarker(batch, markerHandle, STATS_KEY,
                            StoredRecordCodec.statsValue(StoreStats.ZERO));
                    write(batch);
                } finally {
                    batch.close();
                }
                this.role = config.role();
                this.groupConfig = config;
                this.appliedTermValue = term;
                this.appliedIndexValue = rootStamp.logIndex();
                this.logicalTimeValue = 0;
                this.changeFloorValue = rootStamp.logIndex();
                this.statsValue = StoreStats.ZERO;
                this.initialized = true;
            } finally {
                applyMutex.unlock();
            }
        } finally {
            releaseReadLease();
        }
    }

    // ---- transitions ----

    /**
     * @return a fresh uncommitted transition targeting the given term, index
     *         and post-apply logical time.
     */
    PreparedTransition newTransition(long term, long index, long logicalNowAfter) {
        acquireReadLease();
        try {
            if (!initialized) {
                throw new MetadataStoreException(MetadataStoreException.Reason.LIFECYCLE,
                        "the store is not initialized");
            }
            return new PreparedTransition(this, term, index, logicalNowAfter);
        } finally {
            releaseReadLease();
        }
    }

    /**
     * The only db.write path: materializes the transition's retained
     * buffers and markers into one WriteBatch and writes once, under the
     * lifecycle read lease and the apply/checkpoint mutex (in that order).
     * A failure anywhere before the write publishes nothing.
     */
    void commit(PreparedTransition transition) {
        Objects.requireNonNull(transition, "transition");
        if (transition.owner() != this) {
            throw new MetadataStoreException(MetadataStoreException.Reason.LIFECYCLE,
                    "the transition belongs to another store");
        }
        if (transition.phase() != PreparedTransition.Phase.STAGED) {
            throw new MetadataStoreException(MetadataStoreException.Reason.LIFECYCLE,
                    "the transition is " + transition.phase());
        }
        acquireReadLease();
        try {
            applyMutex.lock();
            try {
                commitLocked(transition);
            } finally {
                applyMutex.unlock();
            }
        } finally {
            releaseReadLease();
        }
    }

    private void commitLocked(PreparedTransition transition) {
        final WriteBatch batch = new WriteBatch();
        try {
            transition.maintenance().forEach((cf, key, value) -> stage(batch, cf, key, value));
            transition.business().forEach((cf, key, value) -> stage(batch, cf, key, value));
            final ColumnFamilyHandle markerHandle = handles[MetadataCf.DEFAULT.ordinal()];
            final long logicalNow = Math.max(logicalTimeValue, transition.logicalNowAfter());
            putMarker(batch, markerHandle, APPLIED_KEY,
                    StoredRecordCodec.appliedValue(transition.term(), transition.index()));
            putMarker(batch, markerHandle, LOGICAL_TIME_KEY, StoredRecordCodec.logicalTimeValue(logicalNow));
            putMarker(batch, markerHandle, STATS_KEY,
                    StoredRecordCodec.statsValue(statsValue.plus(transition.statsDelta())));
            final long floor = Math.max(changeFloorValue, transition.changeFloor());
            if (floor != changeFloorValue) {
                putMarker(batch, markerHandle, CHANGE_FLOOR_KEY, StoredRecordCodec.changeFloorValue(floor));
            }
            invokeGate();
            write(batch);
            // publish only after the single write succeeded
            appliedTermValue = transition.term();
            appliedIndexValue = transition.index();
            logicalTimeValue = logicalNow;
            statsValue = statsValue.plus(transition.statsDelta());
            changeFloorValue = floor;
            transition.markCommitted();
        } finally {
            batch.close();
        }
    }

    private void stage(WriteBatch batch, MetadataCf cf, byte[] key, Optional<byte[]> value) {
        try {
            final ColumnFamilyHandle handle = handles[cf.ordinal()];
            if (value.isPresent()) {
                batch.put(handle, key, value.get());
            } else {
                batch.delete(handle, key);
            }
        } catch (RocksDBException e) {
            throw MetadataStoreException.io("failed to stage a " + cf + " mutation", e);
        }
    }

    private void putMarker(WriteBatch batch, ColumnFamilyHandle handle, byte[] key, byte[] value) {
        try {
            batch.put(handle, key, value);
        } catch (RocksDBException e) {
            throw MetadataStoreException.io("failed to stage the " + new String(key,
                    StandardCharsets.UTF_8) + " marker", e);
        }
    }

    private void invokeGate() {
        final WriteGate pending = gate;
        if (pending == null) {
            return;
        }
        gate = null; // one shot, whatever the outcome
        try {
            pending.beforeWrite();
        } catch (RuntimeException e) {
            throw MetadataStoreException.io("the pre-write gate rejected the batch", e);
        }
    }

    private void write(WriteBatch batch) {
        try {
            db.write(writeOptions, batch);
        } catch (RocksDBException e) {
            throw MetadataStoreException.io("the batch write failed", e);
        }
    }

    // ---- reads ----

    /**
     * @return a snapshot read view holding a lifecycle read lease; rejected
     *         once the store is closing or closed.
     */
    ReadView openReadView() {
        acquireReadLease();
        try {
            return new ReadView(this, db, handles);
        } catch (RuntimeException e) {
            releaseReadLease();
            throw e;
        }
    }

    /**
     * A single-CF point read; takes only a momentary read lease, no snapshot.
     *
     * @return the raw stored bytes, or {@code null} when absent.
     */
    byte[] getNode(byte[] key) {
        acquireReadLease();
        try {
            try {
                return db.get(handles[MetadataCf.NODES.ordinal()], key);
            } catch (RocksDBException e) {
                throw MetadataStoreException.io("point read failed", e);
            }
        } finally {
            releaseReadLease();
        }
    }

    /** @return the apply/checkpoint mutex; acquire only after a lifecycle lease. */
    ReentrantLock applyMutex() {
        return applyMutex;
    }

    /**
     * Freezes a consistent cut under the lifecycle read lease and the
     * apply/checkpoint mutex: reads the applied marker, flushes every
     * column family, and creates the checkpoint directory. Publication is
     * a separate later step.
     */
    StoreCheckpoint checkpoint() {
        acquireReadLease();
        try {
            applyMutex.lock();
            try {
                final long[] applied = StoredRecordCodec.decodeApplied(requireMarker(APPLIED_KEY));
                final Path target = directory.getParent().resolve(
                        directory.getFileName() + ".checkpoint-" + applied[0] + "-" + applied[1]);
                IOUtils.deleteDirectoryQuietly(target.toFile());
                Files.createDirectories(target.getParent());
                final FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true);
                try {
                    db.flush(flushOptions, Arrays.asList(handles));
                } finally {
                    flushOptions.close();
                }
                final Checkpoint checkpoint = Checkpoint.create(db);
                try {
                    checkpoint.createCheckpoint(target.toString());
                } finally {
                    checkpoint.close();
                }
                return new StoreCheckpoint(target, applied[0], applied[1]);
            } finally {
                applyMutex.unlock();
            }
        } catch (RocksDBException e) {
            throw MetadataStoreException.io("the checkpoint failed", e);
        } catch (IOException e) {
            throw MetadataStoreException.io("the checkpoint directory failed", e);
        } finally {
            releaseReadLease();
        }
    }

    // ---- close ----

    /**
     * Closes the store: enters CLOSING to fence new read leases, waits up
     * to {@link #CLOSE_TIMEOUT_MS} for holders to drain, then releases the
     * native handles. A timeout reports CLOSE_TIMEOUT and leaves the store
     * CLOSING with all handles alive - it never frees what a reader still
     * uses; a later retry completes the close once the readers are gone.
     */
    @Override
    public void close() {
        final long deadline = System.nanoTime() + CLOSE_TIMEOUT_MS * 1_000_000L;
        synchronized (stateLock) {
            if (state == State.CLOSED) {
                return;
            }
            state = State.CLOSING;
            while (activeReadLeases > 0) {
                final long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    throw new MetadataStoreException(MetadataStoreException.Reason.LIFECYCLE,
                            "CLOSE_TIMEOUT: " + activeReadLeases + " read leases still held;"
                                    + " staying CLOSING with every native handle alive");
                }
                try {
                    // clamp so a sub-millisecond remainder never becomes the untimed wait(0)
                    stateLock.wait(Math.max(1L, remaining / 1_000_000L));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new MetadataStoreException(MetadataStoreException.Reason.LIFECYCLE,
                            "interrupted while draining read leases; staying CLOSING", e);
                }
            }
            state = State.CLOSED;
            destroyNatives();
        }
    }

    private void destroyNatives() {
        for (int i = ownedNatives.size() - 1; i >= 0; i--) {
            try {
                ownedNatives.get(i).close();
            } catch (Exception ignored) {
                // best effort during teardown; nothing below depends on it
            }
        }
        ownedNatives.clear();
        db = null;
        handles = null;
        writeOptions = null;
    }

    @Override
    public String toString() {
        return "(directory=" + directory + ",state=" + state() + ",appliedIndex=" + appliedIndexValue + ")";
    }
}
