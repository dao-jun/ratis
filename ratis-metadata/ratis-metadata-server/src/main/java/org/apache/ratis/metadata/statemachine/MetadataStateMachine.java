/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ratis.metadata.statemachine;

import org.apache.ratis.metadata.IOUtils;
import org.apache.ratis.metadata.protocol.ChangeEvent;
import org.apache.ratis.metadata.protocol.RaftCommand;
import org.apache.ratis.metadata.protocol.RaftCommandCodec;
import org.apache.ratis.metadata.protocol.RaftResult;
import org.apache.ratis.metadata.util.Clock;
import org.apache.ratis.metadata.util.SystemClock;
import org.apache.ratis.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.ClientInvocationId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.util.LifeCycle;
import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.ratis.metadata.util.Preconditions.checkArgument;
import static org.apache.ratis.metadata.util.Preconditions.checkNotNull;
import static org.apache.ratis.metadata.util.Preconditions.checkState;

/**
 * The RocksDB-backed metadata state machine: each committed raft log entry is one deterministic
 * transition, applied as one atomic RocksDB {@link WriteBatch}.
 */
public class MetadataStateMachine extends BaseStateMachine {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataStateMachine.class);

    private static final byte[] STATE_CF = "state".getBytes(StandardCharsets.UTF_8);
    private static final byte[] COUNTERS_CF = "counters".getBytes(StandardCharsets.UTF_8);
    private static final byte[] LEASES_CF = "leases".getBytes(StandardCharsets.UTF_8);
    private static final byte[] LEASE_PATHS_CF = "lease-paths".getBytes(StandardCharsets.UTF_8);
    private static final byte[] CHANGES_CF = "changes".getBytes(StandardCharsets.UTF_8);
    private static final byte[] RESULTS_CF = "command-results".getBytes(StandardCharsets.UTF_8);

    /** The column families this machine opens, in the order the identity manifest lists them. */
    static final byte[][] COLUMN_FAMILIES = {
        STATE_CF, COUNTERS_CF, LEASES_CF, LEASE_PATHS_CF, CHANGES_CF, RESULTS_CF
    };

    /** Default-family key for the applied revision; shared with {@link LegacyResultStore}. */
    static final byte[] APPLIED_REVISION_KEY = "applied-revision".getBytes(StandardCharsets.UTF_8);

    /** Default-family key holding the smallest revision whose change records are retained. */
    private static final byte[] OLDEST_RETAINED_KEY = "oldest-retained".getBytes(StandardCharsets.UTF_8);

    private static final byte USER_COUNTER_TAG = 0x00;
    private static final byte SEQUENTIAL_COUNTER_TAG = 0x01;

    /** ZooKeeper bounds path length only via jute.maxbuffer; partition specs can exceed 1024. */
    private static final int MAX_PATH_LENGTH = 8192;

    /** Revisions of change records the retention window keeps, independent of snapshot timing. */
    static final long DEFAULT_CHANGE_RECORD_RETENTION_REVISIONS = 65_536L;

    /** Every how many applied revisions the apply path advances change-record retention. */
    static final long DEFAULT_CHANGE_RECORD_GC_CADENCE_REVISIONS = 8_192L;

    /** Suffix of the staging copy a restore builds before the atomic publish. */
    private static final String RESTORE_STAGING_SUFFIX = ".restore-staging";

    /** Retries of a mid-copy restore failure against a fresh scan before the last is rethrown. */
    private static final int REINITIALIZE_RESTORE_RETRIES = 3;

    /** Suffix of the aside copy of a replaced directory; startup cleanup sweeps it too. */
    private static final String TRASH_SUFFIX = MetadataStateMachineStorage.SNAPSHOT_TRASH_SUFFIX;

    /** Sentinel overlay values marking "deleted in this batch, not yet visible to iterators". */
    private static final StateRecord RECORD_TOMBSTONE = new StateRecord(-1, 0, 0, 0, new byte[0]);

    private static final LeaseState LEASE_TOMBSTONE = new LeaseState(-1, -1, -1);

    private final File dbDir;

    /** Snapshot directory management; initialized with the raft storage in {@link #initialize}. */
    private final MetadataStateMachineStorage storage = new MetadataStateMachineStorage();

    /** RocksDB-owned objects that must outlive the db and be closed exactly once. */
    private final List<AutoCloseable> ownedOptions = new ArrayList<>();

    private RocksDB db;
    private ColumnFamilyHandle defaultHandle;
    private ColumnFamilyHandle stateHandle;
    private ColumnFamilyHandle countersHandle;
    private ColumnFamilyHandle leasesHandle;
    private ColumnFamilyHandle leasePathsHandle;
    private ColumnFamilyHandle changesHandle;
    private ColumnFamilyHandle resultsHandle;
    /** Every column-family handle in open order; set by openDb. */
    private List<ColumnFamilyHandle> cfHandles;
    private WriteOptions writeOptions;
    private LegacyResultStore resultStore;

    /** Narrow test seam immediately before the sole valid-command {@code db.write} call. */
    @VisibleForTesting
    interface CommandWriteHook {
        void beforeWrite() throws RocksDBException;
    }

    @SuppressWarnings("checkstyle:VisibilityModifier")
    @VisibleForTesting CommandWriteHook beforeCommandWrite;

    /**
     * Guards the database handle: queries hold the read lock for their whole duration (native calls
     * included); open/close transitions take the write lock.
     */
    private final ReadWriteLock dbLock = new ReentrantReadWriteLock();

    /**
     * Smallest revision whose change records are retained; persisted in the default CF, advanced
     * only by windowed GC.
     */
    private volatile long oldestRetainedRevision = 1;

    /** The last revision whose effects are durably in RocksDB; persisted with every batch. */
    private volatile long appliedRevisionMarker;

    /** Retention window and GC cadence in effect; instance fields so tests can shrink both. */
    private long changeRecordRetentionRevisions = DEFAULT_CHANGE_RECORD_RETENTION_REVISIONS;

    private long changeRecordGcCadenceRevisions = DEFAULT_CHANGE_RECORD_GC_CADENCE_REVISIONS;

    /**
     * Test seam between a poll's retention pre-check and its iteration - where updater-thread GC
     * can race a poll; null in production.
     */
    @SuppressWarnings("checkstyle:VisibilityModifier")
    @VisibleForTesting Runnable pollBetweenCheckAndIteration;

    /**
     * Test seam right before a restore's first file copy - where finalizeSnapshot can swap
     * directories under it; null in production.
     */
    @SuppressWarnings("checkstyle:VisibilityModifier")
    @VisibleForTesting Runnable restoreBeforeFirstFile;

    private Clock clock;
    private long resultRetentionMillis;
    private boolean configurationLocked;

    /** Creates a state machine with the system clock and seven-day result retention. */
    public MetadataStateMachine(File dbDir) {
        this(dbDir, SystemClock.getInstance(), Duration.ofDays(7));
    }

    /** Creates a state machine with the leader time source and result-retention policy. */
    public MetadataStateMachine(File dbDir, Clock clock, Duration resultRetention) {
        this.dbDir = dbDir;
        configureBeforeOpen(clock, resultRetention);
    }

    /**
     * Supplies the complete preprocessing configuration before raft opens this machine. Both values
     * are set together so an injected machine cannot silently retain only part of its builder config.
     */
    public final void configureBeforeOpen(Clock newClock, Duration resultRetention) {
        checkState(!configurationLocked,
                "Metadata state machine configuration is locked after initialization");
        final Clock validatedClock = checkNotNull(newClock, "clock");
        final Duration validatedRetention = checkNotNull(resultRetention, "resultRetention");
        final long validatedRetentionMillis = validatedRetention.toMillis();
        checkArgument(validatedRetentionMillis > 0, "resultRetention must be positive, got %s",
                validatedRetention);
        this.clock = validatedClock;
        this.resultRetentionMillis = validatedRetentionMillis;
    }

    // --- Lifecycle ---

    @Override
    public StateMachineStorage getStateMachineStorage() {
        return storage;
    }

    @Override
    public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage raftStorage)
            throws IOException {
        configurationLocked = true;
        super.initialize(server, groupId, raftStorage);
        storage.init(raftStorage);
        // Drive the LifeCycle ourselves: BaseStateMachine never starts it, yet ratis'
        // install-snapshot flow requires the contract below.
        getLifeCycle().transition(LifeCycle.State.STARTING);
        // Take the write lock even uncontended - the handle must only ever change under it.
        dbLock.writeLock().lock();
        try {
            recoverRestoreLeftovers();
            try {
                openDb();
            } catch (IOException openFailure) {
                // A torn working database (crash, full disk, power loss) heals once from the newest
                // local snapshot; the raft log replays everything above it.
                final SnapshotInfo latest = storage.getLatestSnapshot();
                if (latest == null) {
                    throw openFailure;
                }
                LOG.warn("{}: opening the metadata RocksDB at {} failed; restoring the working"
                                + " database from snapshot at index {}", this, dbDir, latest.getIndex(), openFailure);
                closeDb();
                IOUtils.deleteDirectoryQuietly(dbDir);
                restoreFromSnapshot(latest);
                openDb();
            }
            // Refuse-to-start semantics live after a successful open: a generation mismatch is an
            // operator decision, never something the self-heal may wipe.
            verifyIdentity();
            // The working database must never describe less than the newest local snapshot claimed;
            // the marker comparison is the exact condition.
            restoreIfBehindSnapshot();
        } finally {
            dbLock.writeLock().unlock();
        }
        getLifeCycle().transition(LifeCycle.State.RUNNING);
    }

    /** Called after an installed snapshot replaced the snapshot dirs; it becomes the state. */
    @Override
    public void reinitialize() throws IOException {
        // Resume the LifeCycle paused by pause(): ratis' updater asserts PAUSED when it reloads.
        final boolean resuming = getLifeCycleState() == LifeCycle.State.PAUSED;
        if (resuming) {
            getLifeCycle().transition(LifeCycle.State.STARTING);
        }
        dbLock.writeLock().lock();
        try {
            closeDb();
            // The install wrote a new snapshot directory behind this storage's back; re-scan,
            // restore, re-open - never opening the snapshot itself.
            final SnapshotInfo installed = storage.loadLatestSnapshot();
            if (installed == null) {
                throw new IOException("Reinitializing without an installed snapshot in " + dbDir);
            }
            final SnapshotInfo loaded = restoreUntilSettled(installed);
            openDb();
            verifyIdentity();
            // Entries below the install index are never replayed; the watermark floor is monotonic.
            updateLastAppliedTermIndex(loaded.getTermIndex());
        } finally {
            dbLock.writeLock().unlock();
        }
        if (resuming) {
            getLifeCycle().transition(LifeCycle.State.RUNNING);
        }
        super.reinitialize();
    }

    /** Pauses the LifeCycle as ratis' StateMachineUpdater.reload handshake requires; idempotent. */
    @Override
    public void pause() {
        if (getLifeCycleState() != LifeCycle.State.PAUSED
                && getLifeCycle().transitionIfValid(LifeCycle.State.PAUSING)) {
            getLifeCycle().transition(LifeCycle.State.PAUSED);
        }
    }

    @Override
    public void close() {
        // ratis joins the updater thread first; the lock only waits out in-flight queries.
        dbLock.writeLock().lock();
        try {
            closeDb();
        } finally {
            dbLock.writeLock().unlock();
        }
        // CLOSED reachable from any non-NEW state; best-effort (ratis closes failed machines).
        if (getLifeCycle().transitionIfValid(LifeCycle.State.CLOSING)) {
            getLifeCycle().transition(LifeCycle.State.CLOSED);
        } else {
            getLifeCycle().transitionIfValid(LifeCycle.State.CLOSED);
        }
    }

    private SnapshotInfo restoreUntilSettled(SnapshotInfo installed) throws IOException {
        SnapshotInfo loaded = installed;
        int retriesLeft = REINITIALIZE_RESTORE_RETRIES;
        while (true) {
            try {
                restoreFromSnapshot(loaded);
            } catch (IOException restoreFailure) {
                if (retriesLeft == 0) {
                    throw restoreFailure;
                }
                retriesLeft--;
                LOG.warn("{}: restoring snapshot at index {} failed; a concurrent install may"
                                + " have swapped the snapshot directory mid-restore, retrying"
                                + " against a fresh scan ({} retries left)", this, loaded.getIndex(), retriesLeft,
                        restoreFailure);
                final SnapshotInfo current = storage.loadLatestSnapshot();
                if (current != null) {
                    // Retry on whatever the scan sees now - newer, re-published, or transiently
                    // older.
                    loaded = current;
                }
                // A null scan is the same swap window from the listing side; just retry.
                continue;
            }
            final SnapshotInfo newer = storage.loadLatestSnapshot();
            if (newer == null || newer.getIndex() <= loaded.getIndex()) {
                break;
            }
            LOG.warn("{}: snapshot at index {} finalized during the restore of index {}; restoring the newer one",
                    this, newer.getIndex(), loaded.getIndex());
            loaded = newer;
        }
        return loaded;
    }

    /**
     * Restores the newest local snapshot when the working database is behind it, and lifts the
     * watermark to it in every case (an ahead database keeps the snapshot as a floor).
     */
    private void restoreIfBehindSnapshot() throws IOException {
        final SnapshotInfo latest = storage.getLatestSnapshot();
        if (latest == null) {
            return;
        }
        if (appliedRevisionMarker < latest.getIndex()) {
            closeDb();
            restoreFromSnapshot(latest);
            openDb();
            verifyIdentity();
        }
        updateLastAppliedTermIndex(latest.getTermIndex());
    }

    /**
     * Validates the snapshot belongs to this generation before a byte is copied, then restores the
     * working database from it.
     */
    private void restoreFromSnapshot(SnapshotInfo snapshot) throws IOException {
        final Path snapshotDir = snapshotDirOf(snapshot);
        StateMachineIdentity.validateSnapshotManifest(snapshotDir, getGroupId());
        restoreWorkingDbFrom(snapshot);
    }

    /**
     * Replaces the working database atomically: built in a staging sibling while the current
     * database stays untouched, published by one rename.
     */
    private void restoreWorkingDbFrom(SnapshotInfo snapshot) throws IOException {
        final Path snapshotDir = snapshotDirOf(snapshot);
        final Path staging = restoreStagingDir();
        IOUtils.deleteDirectoryQuietly(staging.toFile());
        Files.createDirectories(staging);
        final Runnable beforeFirstFile = restoreBeforeFirstFile;
        if (beforeFirstFile != null) {
            beforeFirstFile.run();
        }
        restoreTree(snapshotDir, staging);
        replaceDirectoryAtomically(staging, dbDir.toPath());
        LOG.info("{}: restored metadata RocksDB from snapshot {} at index {}", this, snapshotDir, snapshot.getIndex());
    }

    private Path snapshotDirOf(SnapshotInfo snapshot) throws IOException {
        // The snapshot files share one parent (one checkpoint dir); derive it from any listed file.
        for (FileInfo file : snapshot.getFiles()) {
            final Path parent = file.getPath().getParent();
            if (parent != null) {
                return parent;
            }
        }
        throw new IOException("Snapshot " + snapshot + " lists no file with a parent directory");
    }

    /**
     * Copies a checkpoint tree, hard-linking immutable SST files (RocksDB never rewrites an SST);
     * falls back to a plain copy.
     */
    private static void restoreTree(Path source, Path target) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(source)) {
            for (Path entry : stream) {
                final String name = entry.getFileName().toString();
                final Path copy = target.resolve(name);
                if (Files.isDirectory(entry)) {
                    Files.createDirectories(copy);
                    restoreTree(entry, copy);
                } else if (name.endsWith(".sst")) {
                    try {
                        Files.createLink(copy, entry);
                    } catch (IOException | UnsupportedOperationException linkFailure) {
                        Files.copy(entry, copy, StandardCopyOption.COPY_ATTRIBUTES);
                    }
                } else {
                    Files.copy(entry, copy, StandardCopyOption.COPY_ATTRIBUTES);
                }
            }
        }
    }

    /**
     * Publishes {@code staging} as {@code target} with no window where the target is absent or
     * half-written; rolls the old target back on failure.
     */
    private static void replaceDirectoryAtomically(Path staging, Path target) throws IOException {
        final Path trash = sibling(target, TRASH_SUFFIX);
        boolean targetAside = false;
        if (Files.exists(target)) {
            IOUtils.deleteDirectoryQuietly(trash.toFile());
            Files.move(target, trash);
            targetAside = true;
        }
        try {
            IOUtils.atomicMoveWithFallback(staging, target);
        } catch (IOException publishFailure) {
            if (targetAside) {
                Files.move(trash, target);
            }
            throw publishFailure;
        }
        if (targetAside) {
            IOUtils.deleteDirectoryQuietly(trash.toFile());
        }
    }

    /** The sibling of {@code dir} carrying the given suffix, e.g. {@code data.restore-staging}. */
    private static Path sibling(Path dir, String suffix) {
        return dir.resolveSibling(dir.getFileName().toString() + suffix);
    }

    private Path restoreStagingDir() {
        return sibling(dbDir.toPath(), RESTORE_STAGING_SUFFIX);
    }

    /**
     * Rolls an interrupted restore forward or back before anything is opened: staging is dropped;
     * trash is resolved by whether the publish happened.
     */
    private void recoverRestoreLeftovers() {
        final Path staging = restoreStagingDir();
        if (Files.exists(staging)) {
            LOG.warn("{}: deleting {} - leftover of an interrupted restore", this, staging);
            IOUtils.deleteDirectoryQuietly(staging.toFile());
        }
        final Path trash = sibling(dbDir.toPath(), TRASH_SUFFIX);
        if (!Files.exists(trash)) {
            return;
        }
        if (Files.exists(dbDir.toPath())) {
            LOG.warn("{}: deleting {} - the restore published, only its cleanup was interrupted", this, trash);
            IOUtils.deleteDirectoryQuietly(trash.toFile());
            return;
        }
        LOG.warn("{}: moving {} back to {} - the restore that replaced it never published", this, trash, dbDir);
        try {
            Files.move(trash, dbDir.toPath());
        } catch (IOException rollbackFailure) {
            LOG.warn("{}: failed to roll {} back; leaving it in place", this, trash, rollbackFailure);
        }
    }

    /**
     * Writes this build's identity into the working database on first open; refuses to serve data
     * belonging to another generation otherwise.
     */
    private void verifyIdentity() throws IOException {
        final String expected = StateMachineIdentity.encode(getGroupId());
        try {
            final byte[] persisted = db.get(defaultHandle, StateMachineIdentity.DB_KEY);
            if (persisted == null) {
                db.put(defaultHandle, writeOptions, StateMachineIdentity.DB_KEY,
                        expected.getBytes(StandardCharsets.UTF_8));
                return;
            }
            StateMachineIdentity.validate(new String(persisted, StandardCharsets.UTF_8), expected,
                    "the working database at " + dbDir + " (serving raft storage "
                            + storage.getSnapshotDir().getParentFile() + ")");
        } catch (RocksDBException e) {
            throw new IOException("Failed to read the identity record of " + dbDir, e);
        }
    }

    private void openDb() throws IOException {
        final DBOptions options = new DBOptions()
                        .setCreateIfMissing(true)
                        // our column families do not exist on first open and must be created
                        .setCreateMissingColumnFamilies(true)
                        .setAtomicFlush(true)
                        .setUseFsync(false)
                        .setStatsDumpPeriodSec(0);
        ownedOptions.add(options);

        final List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
        final ColumnFamilyOptions defaultCfOptions = new ColumnFamilyOptions();
        ownedOptions.add(defaultCfOptions);
        descriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, defaultCfOptions));
        for (byte[] cfName : COLUMN_FAMILIES) {
            final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
            ownedOptions.add(cfOptions);
            descriptors.add(new ColumnFamilyDescriptor(cfName, cfOptions));
        }

        final List<ColumnFamilyHandle> handles = new ArrayList<>(descriptors.size());
        try {
            db = RocksDB.open(options, dbDir.getAbsolutePath(), descriptors, handles);
        } catch (RocksDBException e) {
            throw new IOException("Failed to open metadata RocksDB at " + dbDir, e);
        }
        defaultHandle = handles.get(0);
        stateHandle = handles.get(1);
        countersHandle = handles.get(2);
        leasesHandle = handles.get(3);
        leasePathsHandle = handles.get(4);
        changesHandle = handles.get(5);
        resultsHandle = handles.get(6);
        cfHandles = handles;

        // The raft log is the WAL; the RocksDB WAL buys nothing (batches survive process death at
        // memtable granularity, not power loss).
        writeOptions = new WriteOptions().setDisableWAL(true);
        resultStore = new LegacyResultStore(db, defaultHandle, resultsHandle);

        try {
            final byte[] marker = db.get(defaultHandle, APPLIED_REVISION_KEY);
            appliedRevisionMarker = marker == null ? 0 : bytesToLong(marker, 0);
            final byte[] retained = db.get(defaultHandle, OLDEST_RETAINED_KEY);
            // The retention line only ever moves up; 1 is the "nothing collected" value.
            oldestRetainedRevision = retained == null ? 1 : Math.max(1, bytesToLong(retained, 0));
        } catch (RocksDBException e) {
            throw new IOException("Failed to read the applied-revision markers", e);
        }
        LOG.info("{}: metadata RocksDB opened at {}, replay marker at revision {}, oldest retained revision {}",
                this, dbDir, appliedRevisionMarker, oldestRetainedRevision);
    }

    private void closeDb() {
        // Native column-family handles must be released before their owning DB.
        if (cfHandles != null) {
            for (ColumnFamilyHandle handle : cfHandles) {
                IOUtils.closeQuietly(handle);
            }
            cfHandles = null;
        }
        defaultHandle = null;
        stateHandle = null;
        countersHandle = null;
        leasesHandle = null;
        leasePathsHandle = null;
        changesHandle = null;
        resultsHandle = null;
        resultStore = null;
        if (db != null) {
            db.close();
            db = null;
        }
        IOUtils.closeQuietly(writeOptions);
        writeOptions = null;
        for (AutoCloseable option : ownedOptions) {
            IOUtils.closeQuietly(option);
        }
        ownedOptions.clear();
    }

    /**
     * Preprocesses a leader write exactly once. The original request remains the invocation identity;
     * only the replicated log data becomes an envelope. Expected client-input failures are returned
     * through the context so the server can complete its retry-cache entry without appending.
     */
    @Override
    public TransactionContext startTransaction(RaftClientRequest request) throws IOException {
        final TransactionContext.Builder builder =
                TransactionContext.newBuilder().setStateMachine(this).setClientRequest(request);
        final byte[] rawCommand = request.getMessage().getContent().toByteArray();
        final byte[] logData;
        try {
            // create() decoded the command, so its strings are decode-origin: readString never
            // yields an unpaired surrogate and encode() cannot reject. Encoding inside the same
            // catch keeps that invariant belt-and-braces rather than load-bearing.
            logData = MetadataLogEnvelope.create(rawCommand, clock, resultRetentionMillis).encode();
        } catch (IllegalArgumentException invalidCommand) {
            return builder.build().setException(invalidCommand);
        }
        builder.setLogData(ByteString.copyFrom(logData));
        return builder.build();
    }

    // --- Apply path (leader and followers, serialized by the ratis applier thread) ---

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        final LogEntryProto entry = trx.getLogEntry();
        final TermIndex candidate = TermIndex.valueOf(entry);
        // Only state machine entries reach here; config entries arrive as notifyTermIndexUpdated.
        final byte[] reply = applyCommittedCommand(candidate, entry.getStateMachineLogEntry());
        // Advance last-applied only after the effects are in RocksDB, and only when it advances:
        // the log may replay entries below a restored snapshot, and the base asserts monotonicity.
        if (candidate.compareTo(getLastAppliedTermIndex()) >= 0) {
            updateLastAppliedTermIndex(candidate);
        }
        // GC after the watermark update so the line never exceeds the just-reported revision.
        maybeGcChangeRecords(candidate.getIndex());
        return CompletableFuture.completedFuture(Message.valueOf(ByteBuffer.wrap(reply)));
    }

    /** Handles config entries (no command): persists the marker in lockstep with last-applied. */
    @Override
    public void notifyTermIndexUpdated(long term, long index) {
        writeGapMarker(index);
        // Same retention bound as after a state machine entry; a config revision must not delay GC.
        maybeGcChangeRecords(index);
        super.notifyTermIndexUpdated(term, index);
    }

    /** Advances the marker for a config entry or an invalid committed-entry no-op. */
    private void writeGapMarker(long revision) {
        if (revision <= appliedRevisionMarker) {
            // Replayed entry whose revision is already durable.
            return;
        }
        try (WriteBatch batch = new WriteBatch()) {
            batch.put(defaultHandle, APPLIED_REVISION_KEY, longToBytes(revision));
            db.write(writeOptions, batch);
        } catch (RocksDBException e) {
            throw new CompletionException(new IOException("Failed to advance the applied-revision marker", e));
        }
        appliedRevisionMarker = revision;
    }

    /** Returns the same no-op for an invalid envelope or invocation, including same-entry replay. */
    private byte[] malformedNoop(long revision) {
        // This branch is deliberately independent of the marker: invalid old/log data can never
        // turn into a missing-result replay or a bare-command execution after the marker moves.
        writeGapMarker(revision);
        return RaftCommandCodec.encodeResult(RaftResult.error(RaftCommand.Op.UNKNOWN, RaftResult.Status.MALFORMED));
    }

    /** Applies one committed envelope; protocol failures become durable first results. */
    private byte[] applyCommittedCommand(TermIndex termIndex, StateMachineLogEntryProto stateEntry) {
        final long revision = termIndex.getIndex();
        final MetadataLogEnvelope envelope = MetadataLogEnvelope.decode(stateEntry.getLogData().toByteArray());
        if (envelope == null) {
            LOG.error("{}: malformed metadata log envelope committed at revision {}", getId(), revision);
            return malformedNoop(revision);
        }

        final ClientInvocationId invocation;
        try {
            invocation = ClientInvocationId.valueOf(stateEntry);
        } catch (IllegalStateException invalidInvocation) {
            LOG.error("{}: malformed client invocation committed at revision {}", getId(), revision, invalidInvocation);
            return malformedNoop(revision);
        }

        final RaftCommand command = envelope.getCommand();
        try {
            // Duplicate selection precedes every new term fence and all business validation.
            // The marker argument is the durable one: it is loaded on open and advanced in
            // lockstep with every write that persists it, on this same applier thread.
            final LegacyResultStore.Disposition disposition =
                    resultStore.lookup(invocation, termIndex, envelope, appliedRevisionMarker);
            if (disposition.getKind() == LegacyResultStore.Disposition.Kind.RESULT_EXPIRED) {
                return disposition.getEncodedResult();
            }
            if (disposition.getKind() == LegacyResultStore.Disposition.Kind.RETURN_RESULT) {
                final byte[] reply = disposition.getEncodedResult();
                if (revision > appliedRevisionMarker) {
                    commitControl(disposition, reply, revision);
                }
                return reply;
            }

            if (!envelope.hasNormalizedLeaseExpiries()) {
                LOG.error("{}: invalid replicated lease timing at revision {}", getId(), revision);
                final byte[] reply = RaftCommandCodec.encodeResult(
                        RaftResult.error(command.getOp(), RaftResult.Status.MALFORMED));
                commitControl(disposition, reply, revision);
                return reply;
            }
            if (command.getWriterTerm() > 0 && command.getWriterTerm() < termIndex.getTerm()) {
                LOG.warn("{}: rejected a term-fenced command at revision {}: writer term {} is behind " +
                                "the entry term {}", getId(), revision, command.getWriterTerm(), termIndex.getTerm());
                final byte[] reply = RaftCommandCodec.encodeResult(
                        RaftResult.error(command.getOp(), RaftResult.Status.STALE_TERM));
                commitControl(disposition, reply, revision);
                return reply;
            }

            try (ApplyEngine engine = new ApplyEngine(revision)) {
                final RaftResult result = engine.execute(command);
                final byte[] reply = RaftCommandCodec.encodeResult(result);
                if (result.getStatus() == RaftResult.Status.OK) {
                    engine.stageChanges();
                    commitCommandBatch(engine.batch, disposition, reply, revision);
                } else if (engine.batch.count() == 0) {
                    // Single-op rejections stage nothing before failing: the engine's empty
                    // batch doubles as the control batch - one WriteBatch, not two.
                    commitCommandBatch(engine.batch, disposition, reply, revision);
                } else {
                    // The overlay and every staged business effect are discarded with
                    // engine.batch: a failed MULTI may carry partial staging that must
                    // never commit, so this path alone opens a fresh control batch.
                    commitControl(disposition, reply, revision);
                }
                return reply;
            }
        } catch (IOException e) {
            throw new CompletionException(e);
        } catch (RocksDBException e) {
            throw new CompletionException(
                    new IOException("Failed to apply metadata command at revision " + revision, e));
        }
    }

    /** Commits a result/cutoff/marker transition without any business writes or change event. */
    private void commitControl(LegacyResultStore.Disposition disposition, byte[] reply, long revision)
            throws IOException, RocksDBException {
        try (WriteBatch control = new WriteBatch()) {
            commitCommandBatch(control, disposition, reply, revision);
        }
    }

    /** The sole commit point for a valid envelope at a new revision. */
    private void commitCommandBatch(WriteBatch batch, LegacyResultStore.Disposition disposition, byte[] reply,
                                    long revision) throws IOException, RocksDBException {
        resultStore.stageRetention(batch, disposition);
        resultStore.stageResult(batch, disposition, reply);
        batch.put(defaultHandle, APPLIED_REVISION_KEY, longToBytes(revision));
        final CommandWriteHook hook = beforeCommandWrite;
        if (hook != null) {
            hook.beforeWrite();
        }
        db.write(writeOptions, batch);
        appliedRevisionMarker = revision;
    }

    // --- Query path (runs on request threads, concurrent with the applier) ---

    @Override
    public CompletableFuture<Message> query(Message request) {
        final RaftCommand command = RaftCommandCodec.decodeCommand(request.getContent().toByteArray());
        if (command == null || command.getOp().isWrite()) {
            LOG.error("{}: malformed metadata query {}", getId(), request);
            return completed(Message.valueOf(ByteBuffer.wrap(RaftCommandCodec.encodeResult(
                    RaftResult.error(RaftCommand.Op.UNKNOWN, RaftResult.Status.MALFORMED)))));
        }
        if (getLifeCycleState() == LifeCycle.State.CLOSED) {
            // Fast-fail: after close the handle is gone for good (no queueing behind transitions).
            return notOpenQuery();
        }
        // A query losing the race to a transition blocks here, then sees the reopened database.
        dbLock.readLock().lock();
        try {
            if (db == null) {
                // Reachable before initialize and after closeDb; a failed future, not a crash.
                return notOpenQuery();
            }
            try {
                return completed(Message.valueOf(ByteBuffer.wrap(
                        RaftCommandCodec.encodeResult(executeQuery(command)))));
            } catch (RocksDBException e) {
                LOG.error("{}: metadata query {} failed", getId(), command, e);
                final CompletableFuture<Message> failed = new CompletableFuture<>();
                failed.completeExceptionally(new IOException("Metadata query failed", e));
                return failed;
            }
        } finally {
            dbLock.readLock().unlock();
        }
    }

    /** The failed future a query gets when the db is not open - a caller-side lifecycle bug. */
    private static CompletableFuture<Message> notOpenQuery() {
        final CompletableFuture<Message> failed = new CompletableFuture<>();
        failed.completeExceptionally(new IllegalStateException("Metadata state machine is not open"));
        return failed;
    }

    private RaftResult executeQuery(RaftCommand command) throws RocksDBException {
        switch (command.getOp()) {
            case GET:
                return queryGet(command);
            case EXISTS:
                return queryExists(command);
            case LIST:
                return queryList(command);
            case REVISION:
                return RaftResult.ok(RaftCommand.Op.REVISION).revision(getLastAppliedTermIndex().getIndex());
            case POLL_CHANGES:
                return queryPollChanges(command);
            case LEASE_LIST:
                return queryLeaseList(command);
            default:
                return RaftResult.error(command.getOp(), RaftResult.Status.MALFORMED);
        }
    }

    private RaftResult queryGet(RaftCommand command) throws RocksDBException {
        final RaftResult.Status pathError = checkPath(command.getPath());
        if (pathError != null) {
            return RaftResult.error(RaftCommand.Op.GET, pathError);
        }
        final byte[] encoded = db.get(stateHandle, command.getPath().getBytes(StandardCharsets.UTF_8));
        if (encoded == null) {
            return RaftResult.error(RaftCommand.Op.GET, RaftResult.Status.NOT_FOUND);
        }
        final StateRecord record = StateRecord.decode(encoded);
        return RaftResult.ok(RaftCommand.Op.GET)
                .version(record.version())
                .leaseId(record.leaseId())
                .createTime(record.createTime())
                .modifyTime(record.modifyTime())
                .data(record.data());
    }

    private RaftResult queryExists(RaftCommand command) throws RocksDBException {
        final RaftResult.Status pathError = checkPath(command.getPath());
        if (pathError != null) {
            return RaftResult.error(RaftCommand.Op.EXISTS, pathError);
        }
        if (command.getPath().equals("/")) {
            // The root always exists, ZooKeeper-style: never created/deleted, no record stored.
            return RaftResult.ok(RaftCommand.Op.EXISTS).found(true);
        }
        return RaftResult.ok(RaftCommand.Op.EXISTS).found(
                db.get(stateHandle, command.getPath().getBytes(StandardCharsets.UTF_8)) != null);
    }

    private RaftResult queryList(RaftCommand command) throws RocksDBException {
        final RaftResult.Status prefixError = checkListPrefix(command.getPath());
        if (prefixError != null) {
            return RaftResult.error(RaftCommand.Op.LIST, prefixError);
        }
        final long maxResults = command.getMaxResults() <= 0
                ? RaftCommandCodec.DEFAULT_MAX_LIST_RESULTS : command.getMaxResults();
        final RaftResult result = RaftResult.ok(RaftCommand.Op.LIST);
        final byte[] prefix = command.getPath().getBytes(StandardCharsets.UTF_8);
        final String startAfter = command.getStartAfter();
        try (RocksIterator iterator = db.newIterator(stateHandle)) {
            // Resume strictly after the cursor: seek the bytewise larger of cursor and prefix,
            // then step once off the cursor itself when it still exists.
            final boolean hasCursor = startAfter != null && !startAfter.isEmpty();
            final byte[] cursor = hasCursor ? startAfter.getBytes(StandardCharsets.UTF_8) : prefix;
            iterator.seek(compareUnsigned(prefix, cursor) >= 0 ? prefix : cursor);
            // Only a real cursor steps off its own key; the prefix key itself is in the range.
            if (hasCursor && iterator.isValid() && compareUnsigned(iterator.key(), cursor) == 0) {
                iterator.next();
            }
            boolean more = false;
            while (iterator.isValid() && startsWith(iterator.key(), prefix)) {
                if (result.getEntries().size() >= maxResults) {
                    // The cap cut the range with at least one matching key left unconsumed.
                    more = true;
                    break;
                }
                final String path = new String(iterator.key(), StandardCharsets.UTF_8);
                result.addEntry(new RaftResult.Entry(path, StateRecord.versionOf(iterator.value())));
                iterator.next();
            }
            // isValid()==false also covers a scan stopped by a storage error; only status()
            // tells that apart from a clean end of the range. Every full-range iterator in
            // this module carries this check.
            iterator.status();
            result.hasMore(more);
        }
        return result;
    }

    private RaftResult queryPollChanges(RaftCommand command) throws RocksDBException {
        final long sinceRevision = command.getSinceRevision();
        final long maxBytes = command.getMaxBytes() <= 0
                ? RaftCommandCodec.DEFAULT_MAX_POLL_BYTES : command.getMaxBytes();
        if (sinceRevision + 1 < oldestRetainedRevision) {
            // Range was garbage collected: the consumer must reload, not read a holed stream.
            return RaftResult.error(RaftCommand.Op.POLL_CHANGES, RaftResult.Status.NEED_SNAPSHOT)
                    .oldestRetainedRevision(oldestRetainedRevision);
        }
        // Read the watermark first: records <= watermark are visible to the iterator below.
        final long watermark = getLastAppliedTermIndex().getIndex();
        final Runnable betweenCheckAndIteration = pollBetweenCheckAndIteration;
        if (betweenCheckAndIteration != null) {
            betweenCheckAndIteration.run();
        }
        final List<ChangeEvent> records = new ArrayList<>();
        long accumulatedBytes = 0;
        try (RocksIterator iterator = db.newIterator(changesHandle)) {
            iterator.seek(longToBytes(sinceRevision + 1));
            while (iterator.isValid()) {
                // The applier may commit between the watermark sample and this live
                // iterator: records are ordered by revision, so the first one above the
                // sampled watermark ends this poll cleanly - the record stays in the
                // family, retrievable by the next poll. Key-only, so the check runs
                // before the value's JNI copy materializes.
                if (bytesToLong(iterator.key(), 0) > watermark) {
                    break;
                }
                final byte[] encoded = iterator.value();
                // Always return one record, else a sub-record budget can never progress.
                if (!records.isEmpty() && accumulatedBytes + encoded.length > maxBytes) {
                    break;
                }
                final ChangeEvent record = RaftCommandCodec.decodeChangeRecord(encoded);
                if (record == null) {
                    throw new IllegalStateException("Corrupt change record at revision "
                            + bytesToLong(iterator.key(), 0) + " in " + dbDir);
                }
                records.add(record);
                accumulatedBytes += encoded.length;
                iterator.next();
            }
            iterator.status();
        }
        // Re-check after iterating: the iterator pins its view, so a GC committing between the
        // pre-check and it can punch a hole; the persisted line is the airtight witness.
        final long lineAfterPoll = Math.max(oldestRetainedRevision, persistedOldestRetainedRevision());
        if (sinceRevision + 1 < lineAfterPoll) {
            return RaftResult.error(RaftCommand.Op.POLL_CHANGES, RaftResult.Status.NEED_SNAPSHOT)
                    .oldestRetainedRevision(lineAfterPoll);
        }
        return RaftResult.ok(RaftCommand.Op.POLL_CHANGES)
                .revision(watermark)
                .oldestRetainedRevision(lineAfterPoll)
                .records(records);
    }

    /**
     * The retention line as persisted in the default CF - the value GC commits before deleting
     * anything, hence the race witness for polls.
     */
    private long persistedOldestRetainedRevision() throws RocksDBException {
        final byte[] persisted = db.get(defaultHandle, OLDEST_RETAINED_KEY);
        return persisted == null ? 1 : Math.max(1, bytesToLong(persisted, 0));
    }

    private RaftResult queryLeaseList(RaftCommand command) throws RocksDBException {
        final long maxResults = command.getMaxResults() <= 0
                ? RaftCommandCodec.DEFAULT_MAX_LIST_RESULTS : command.getMaxResults();
        final RaftResult result = RaftResult.ok(RaftCommand.Op.LEASE_LIST);
        try (RocksIterator iterator = db.newIterator(leasesHandle)) {
            // Resume after the cursor like queryList; id 0 (the front sentinel) never collides.
            final boolean hasCursor = command.getStartAfterLeaseId() != 0;
            final byte[] cursor = longToBytes(command.getStartAfterLeaseId());
            iterator.seek(cursor);
            if (hasCursor && iterator.isValid() && compareUnsigned(iterator.key(), cursor) == 0) {
                iterator.next();
            }
            boolean more = false;
            while (iterator.isValid()) {
                if (result.getLeases().size() >= maxResults) {
                    // The cap cut the table with at least one lease left unconsumed.
                    more = true;
                    break;
                }
                final LeaseState state = LeaseState.decode(iterator.value());
                result.addLease(new RaftResult.Lease(
                        bytesToLong(iterator.key(), 0), state.ttlMs(), state.expireAtMs(), state.lastRenewIndex()));
                iterator.next();
            }
            iterator.status();
            result.hasMore(more);
        }
        return result;
    }

    // --- Snapshot and change-record garbage collection ---

    /** Publishes the current state as one RocksDB checkpoint; returns its last-included index. */
    @Override
    public long takeSnapshot() throws IOException {
        final TermIndex lastApplied = getLastAppliedTermIndex();
        final long snapshotIndex = lastApplied.getIndex();
        if (snapshotIndex < 0) {
            return RaftLog.INVALID_LOG_INDEX;
        }
        final SnapshotInfo latest = storage.getLatestSnapshot();
        if (latest != null && latest.getIndex() >= snapshotIndex) {
            // Nothing new was applied (ratis asks on every close); re-publishing would churn disks.
            return latest.getIndex();
        }
        if (db == null) {
            throw new IOException("Cannot snapshot a closed metadata RocksDB at " + dbDir);
        }

        final File snapshotDir = storage.snapshotDirectory(lastApplied.getTerm(), snapshotIndex);
        final File tmpDir = storage.tmpSnapshotDirectory(lastApplied.getTerm(), snapshotIndex);
        // The target must be absent: RocksDB refuses to checkpoint into an existing directory.
        IOUtils.deleteDirectoryQuietly(tmpDir);
        Files.createDirectories(storage.getSnapshotDir().toPath());

        try (FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
            db.flush(flushOptions, cfHandles);
        } catch (RocksDBException e) {
            throw new IOException("Failed to flush metadata RocksDB for snapshot " + snapshotDir, e);
        }

        try (Checkpoint checkpoint = Checkpoint.create(db)) {
            checkpoint.createCheckpoint(tmpDir.getAbsolutePath());
        } catch (RocksDBException e) {
            throw new IOException("Failed to checkpoint metadata RocksDB into " + tmpDir, e);
        }

        // The manifest is written last; its presence makes the directory a complete snapshot.
        StateMachineIdentity.writeSnapshotManifest(tmpDir.toPath(), getGroupId());

        // Never delete a pre-existing target in place; move it aside until the publish.
        replaceDirectoryAtomically(tmpDir.toPath(), snapshotDir.toPath());

        final SnapshotInfo published = storage.loadLatestSnapshot();
        if (published == null || published.getIndex() != snapshotIndex) {
            throw new IOException("Snapshot " + snapshotDir + " is not discoverable after publication");
        }
        LOG.info("{}: took metadata snapshot {} ({} files)", this, snapshotDir, published.getFiles().size());

        // The retention window must survive the snapshot.
        gcChangeRecords(snapshotIndex + 1 - changeRecordRetentionRevisions);
        return snapshotIndex;
    }

    /** Every cadence revisions, advance the line to {@code revision + 1 - window}. */
    private void maybeGcChangeRecords(long revision) {
        if (revision % changeRecordGcCadenceRevisions != 0) {
            return;
        }
        try {
            gcChangeRecords(revision + 1 - changeRecordRetentionRevisions);
        } catch (IOException e) {
            // A GC failure is a storage failure, exactly like the apply it follows.
            throw new CompletionException(e);
        }
    }

    private void gcChangeRecords(long lineTarget) throws IOException {
        final long newOldestRetained = Math.max(1, lineTarget);
        if (newOldestRetained <= oldestRetainedRevision) {
            // The line never moves down; a replay finds the target persisted and no-ops.
            return;
        }
        // Persist the new retention line first, atomically and alone.
        try (WriteBatch lineBatch = new WriteBatch()) {
            lineBatch.put(defaultHandle, OLDEST_RETAINED_KEY, longToBytes(newOldestRetained));
            db.write(writeOptions, lineBatch);
        } catch (RocksDBException e) {
            throw new IOException("Failed to persist the retention line " + newOldestRetained, e);
        }
        // Publish only after the line is durable: polls below the line fail with NEED_SNAPSHOT.
        oldestRetainedRevision = newOldestRetained;
        // Every changes-family key is an 8-byte big-endian revision long, so the doomed set
        // is one contiguous prefix range: a single range tombstone, no key iteration.
        try (WriteBatch batch = new WriteBatch()) {
            batch.deleteRange(
                    changesHandle, new byte[Long.BYTES], longToBytes(newOldestRetained));
            db.write(writeOptions, batch);
        } catch (RocksDBException e) {
            throw new IOException("Failed to garbage collect change records below " + newOldestRetained, e);
        }
        LOG.info("{}: garbage collected change records below revision {}", this, newOldestRetained);
    }

    // --- Role notifications (forwarded from ratis' optional event APIs) ---

    /** Receives the raft role notifications forwarded from ratis' optional event APIs. */
    public interface RoleListener {

        /** Leadership is usable; called on the updater thread - return promptly. */
        void onLeaderReady();

        /** Stopped being leader (deposed or closing); called on ratis' role-change thread. */
        void onNotLeader();

        /** The group elected {@code newLeaderId}; {@code selfIsLeader} marks this member. */
        void onLeaderChanged(RaftPeerId newLeaderId, boolean selfIsLeader);
    }

    /** Listeners; copy-on-write: ratis fires on its threads, embedders (un)register at edges. */
    private final CopyOnWriteArrayList<RoleListener> roleListeners = new CopyOnWriteArrayList<>();

    /** Registers a receiver of the raft role notifications this state machine forwards. */
    public void addRoleListener(RoleListener listener) {
        roleListeners.add(listener);
    }

    /** Unregisters a listener previously added by {@link #addRoleListener(RoleListener)}. */
    public void removeRoleListener(RoleListener listener) {
        roleListeners.remove(listener);
    }

    @Override
    public void notifyLeaderChanged(RaftGroupMemberId groupMemberId, RaftPeerId newLeaderId) {
        for (RoleListener listener : roleListeners) {
            listener.onLeaderChanged(newLeaderId, newLeaderId.equals(groupMemberId.getPeerId()));
        }
    }

    @Override
    public void notifyLeaderReady() {
        for (RoleListener listener : roleListeners) {
            listener.onLeaderReady();
        }
    }

    @Override
    public void notifyNotLeader(Collection<TransactionContext> pendingEntries) {
        for (RoleListener listener : roleListeners) {
            listener.onNotLeader();
        }
    }

    /** Overrides the retention line in memory to exercise NEED_SNAPSHOT without a snapshot. */
    @VisibleForTesting
    void setOldestRetainedRevision(long revision) {
        this.oldestRetainedRevision = revision;
    }

    /**
     * Overrides retention window and GC cadence in memory; retention 0 restores snapshot-frees-all,
     * cadence must stay >= 1.
     */
    @VisibleForTesting
    public void setChangeRecordRetention(long retentionRevisions, long gcCadenceRevisions) {
        checkArgument(retentionRevisions >= 0, "retention window must be >= 0, got %s", retentionRevisions);
        checkArgument(gcCadenceRevisions >= 1, "gc cadence must be >= 1, got %s", gcCadenceRevisions);
        this.changeRecordRetentionRevisions = retentionRevisions;
        this.changeRecordGcCadenceRevisions = gcCadenceRevisions;
    }

    /** Read-only testing probe of the {@code lease-paths} reverse index; no query exposes it. */
    @VisibleForTesting
    boolean leasePathsContains(long leaseId, String path) throws IOException {
        dbLock.readLock().lock();
        try {
            if (db == null) {
                throw new IllegalStateException("Metadata state machine is not open");
            }
            return db.get(leasePathsHandle, leasePathsKey(leaseId, path)) != null;
        } catch (RocksDBException e) {
            throw new IOException("Failed to probe the lease-paths index of " + dbDir, e);
        } finally {
            dbLock.readLock().unlock();
        }
    }

    // --- The per-entry apply engine ---

    /**
     * One command over committed state plus an in-memory overlay; nothing reaches RocksDB until
     * {@code commit()}.
     */
    private final class ApplyEngine implements AutoCloseable {

        private final long revision;
        private final WriteBatch batch = new WriteBatch();

        /** Path -> record or {@link #RECORD_TOMBSTONE}; tombstones hide committed records. */
        private final Map<String, StateRecord> recordOverlay = new HashMap<>();

        /** Subtree prefixes this batch deleted; staging keeps the overlay command-proportional. */
        private final List<String> subtreeTombstonePrefixes = new ArrayList<>();

        /** Counter key -> value; keys are the exact CF keys, wrapped for byte-wise equality. */
        private final Map<ByteBuffer, Long> counterOverlay = new HashMap<>();

        /** Lease id -> lease state or {@link #LEASE_TOMBSTONE}. */
        private final Map<Long, LeaseState> leaseOverlay = new HashMap<>();

        /**
         * Staged reverse-index bindings and tombstones; an expiry must union its victims out of
         * this map (a WriteBatch is invisible to iterators).
         */
        private final Map<ByteBuffer, Boolean> leasePathsOverlay = new HashMap<>();

        /** The mutations of this revision, in apply order; becomes the change record. */
        private final List<ChangeEvent.Change> changes = new ArrayList<>();

        /** Path -> its UTF-8 state-family key, so one application encodes each path once. */
        private final Map<String, byte[]> encodedPathKeys = new HashMap<>();

        ApplyEngine(long revision) {
            this.revision = revision;
        }

        @Override
        public void close() {
            // WriteBatch is native memory; release it regardless of the command outcome.
            batch.close();
        }

        RaftResult execute(RaftCommand command) throws RocksDBException {
            switch (command.getOp()) {
                case PUT:
                    return applyPut(command, false);
                case CAS:
                    return applyPut(command, true);
                case CREATE:
                    return applyCreate(command);
                case DELETE:
                    return applyDelete(command);
                case INCREMENT:
                    return applyIncrement(command);
                case LEASE_REGISTER:
                    return applyLeaseRegister(command);
                case LEASE_RENEW:
                    return applyLeaseRenew(command);
                case LEASE_EXPIRE:
                    return applyLeaseExpire(command);
                case MULTI:
                    return applyMulti(command);
                default:
                    return RaftResult.error(command.getOp(), RaftResult.Status.MALFORMED);
            }
        }

        /** Stages this revision's change record; the outer coordinator owns result/marker/commit. */
        void stageChanges() throws RocksDBException {
            if (!changes.isEmpty()) {
                batch.put(changesHandle, longToBytes(revision),
                        RaftCommandCodec.encodeChangeRecord(new ChangeEvent(revision, changes)));
            }
        }

        // ---- individual write ops ----

        private RaftResult applyPut(RaftCommand command, boolean compareAndSwap) throws RocksDBException {
            final RaftResult.Status pathError = checkPath(command.getPath());
            if (pathError != null) {
                return RaftResult.error(command.getOp(), pathError);
            }
            if (command.getPath().equals("/")) {
                // The root is never a record, like CREATE and DELETE hold; upserting it is
                // not allowed.
                return RaftResult.error(command.getOp(), RaftResult.Status.NOT_ALLOWED);
            }
            final RaftResult.Status leaseError = checkLeaseExists(command.getLeaseId());
            if (leaseError != null) {
                return RaftResult.error(command.getOp(), leaseError);
            }
            if (command.getLeaseId() != 0 && hasLiveDescendant(command.getPath())) {
                // An ephemeral record carries no descendants: committed children and ones
                // staged earlier in this batch (the MULTI overlay) both block the binding -
                // the symmetric half of checkParent's creation gate.
                return RaftResult.error(command.getOp(), RaftResult.Status.NOT_ALLOWED);
            }
            final StateRecord existing = getRecord(command.getPath());
            if (existing == null) {
                // A PUT that would create is held to CREATE's parent rule (ZooKeeper's setData
                // fails NoNode); updating an existing record stays ungated.
                final RaftResult.Status createError = checkParent(command.getPath());
                if (createError != null) {
                    return RaftResult.error(command.getOp(), createError);
                }
            }
            if (compareAndSwap) {
                final boolean matches = command.getExpectedVersion() == 0 ? existing == null
                        : existing != null && existing.version() == command.getExpectedVersion();
                if (!matches) {
                    return RaftResult.error(RaftCommand.Op.CAS, RaftResult.Status.VERSION_MISMATCH);
                }
            }
            final StateRecord updated = existing == null
                    ? StateRecord.fresh(command.getLeaseId(), command.getTimestamp(), command.getData())
                    : new StateRecord(existing.version() + 1, command.getLeaseId(), existing.createTime(),
                    command.getTimestamp(), command.getData());
            putRecord(command.getPath(), existing, updated);
            addChange(existing == null ? ChangeEvent.ChangeOp.CREATE
                    : ChangeEvent.ChangeOp.UPDATE, command.getPath(), command.getData());
            return RaftResult.ok(command.getOp()).version(updated.version());
        }

        private RaftResult applyCreate(RaftCommand command) throws RocksDBException {
            final RaftResult.Status pathError = checkPath(command.getPath());
            if (pathError != null) {
                return RaftResult.error(RaftCommand.Op.CREATE, pathError);
            }
            if (command.getPath().equals("/")) {
                return RaftResult.error(RaftCommand.Op.CREATE, RaftResult.Status.NOT_ALLOWED);
            }
            final RaftResult.Status leaseError = checkLeaseExists(command.getLeaseId());
            if (leaseError != null) {
                return RaftResult.error(RaftCommand.Op.CREATE, leaseError);
            }
            final RaftResult.Status parentError = (command.getFlags() & RaftCommand.FLAG_CREATE_PARENTS) != 0
                            ? ensureParents(command.getPath(), command.getTimestamp()) : checkParent(command.getPath());
            if (parentError != null) {
                return RaftResult.error(RaftCommand.Op.CREATE, parentError);
            }
            if ((command.getFlags() & RaftCommand.FLAG_SEQUENTIAL) != 0) {
                return applyCreateSequential(command);
            }
            if (getRecord(command.getPath()) != null) {
                return RaftResult.error(RaftCommand.Op.CREATE, RaftResult.Status.NODE_EXISTS);
            }
            final StateRecord created = StateRecord.fresh(
                    command.getLeaseId(), command.getTimestamp(), command.getData());
            putRecord(command.getPath(), null, created);
            addChange(ChangeEvent.ChangeOp.CREATE, command.getPath(), command.getData());
            return RaftResult.ok(RaftCommand.Op.CREATE).resultPath(command.getPath()).version(1);
        }

        private RaftResult applyCreateSequential(RaftCommand command) throws RocksDBException {
            final byte[] counterKey = counterKey(SEQUENTIAL_COUNTER_TAG, command.getPath());
            long sequence = getCounter(counterKey);
            String finalPath = null;
            // The counter makes collisions rare; an explicit create may still take the number.
            for (int attempts = 0; attempts < 1000; attempts++) {
                sequence++;
                final String candidate = command.getPath() + sequentialSuffix(sequence);
                // The final candidate, not the validated base, carries the length risk:
                // validate it before any counter or business-effect staging. An invalid
                // candidate advances to the next number inside the same bounded loop.
                if (checkPath(candidate) == null && getRecord(candidate) == null) {
                    finalPath = candidate;
                    break;
                }
            }
            if (finalPath == null) {
                return RaftResult.error(RaftCommand.Op.CREATE, RaftResult.Status.NODE_EXISTS);
            }
            setCounter(counterKey, sequence);
            final StateRecord created =
                    StateRecord.fresh(command.getLeaseId(), command.getTimestamp(), command.getData());
            putRecord(finalPath, null, created);
            addChange(ChangeEvent.ChangeOp.CREATE, finalPath, command.getData());
            return RaftResult.ok(RaftCommand.Op.CREATE).resultPath(finalPath).version(1);
        }

        private RaftResult applyDelete(RaftCommand command) throws RocksDBException {
            final RaftResult.Status pathError = checkPath(command.getPath());
            if (pathError != null) {
                return RaftResult.error(RaftCommand.Op.DELETE, pathError);
            }
            if (command.getPath().equals("/")) {
                return RaftResult.error(RaftCommand.Op.DELETE, RaftResult.Status.NOT_ALLOWED);
            }
            final StateRecord record = getRecord(command.getPath());
            if (record == null) {
                return RaftResult.error(RaftCommand.Op.DELETE, RaftResult.Status.NOT_FOUND);
            }
            if (command.isRecursive()) {
                return applyDeleteSubtree(command);
            }
            if (hasLiveDescendant(command.getPath())) {
                return RaftResult.error(RaftCommand.Op.DELETE, RaftResult.Status.HAS_CHILDREN);
            }
            deleteRecord(command.getPath(), record);
            return RaftResult.ok(RaftCommand.Op.DELETE).deletedCount(1);
        }

        /**
         * Recursive delete: writes and the change record stay proportional to command count, not
         * subtree size.
         */
        private RaftResult applyDeleteSubtree(RaftCommand command) throws RocksDBException {
            final String path = command.getPath();
            final String prefix = childPrefix(path);
            final byte[] prefixBytes = prefix.getBytes(StandardCharsets.UTF_8);

            long deleted = 0;
            final List<byte[]> staleReverseEntries = new ArrayList<>();
            try (RocksIterator iterator = db.newIterator(stateHandle)) {
                // One read-only scan feeds the reply's deletedCount and the lease-bound victims; a
                // victim the overlay already deleted is not counted again.
                iterator.seek(prefixBytes);
                while (iterator.isValid() && startsWith(iterator.key(), prefixBytes)) {
                    final String victim = new String(iterator.key(), StandardCharsets.UTF_8);
                    if (!isOverlayDeleted(victim)) {
                        deleted++;
                        // The lease binding sits at a fixed header offset; skip decoding it.
                        final long leaseId = bytesToLong(iterator.value(), 4);
                        if (leaseId != 0) {
                            staleReverseEntries.add(leasePathsKey(leaseId, victim));
                        }
                    }
                    iterator.next();
                }
                iterator.status();
            }

            // The root: exact delete, reverse entry, tombstone, plus the DELETE_SUBTREE event.
            final StateRecord root = getRecord(path);
            batch.delete(stateHandle, pathKey(path));
            recordOverlay.put(path, RECORD_TOMBSTONE);
            if (root != null && root.leaseId() != 0) {
                batch.delete(leasePathsHandle, leasePathsKey(root.leaseId(), pathKey(path)));
            }
            deleted++;

            // One range tombstone over [prefix, successor): earlier puts die, later re-creates win.
            batch.deleteRange(stateHandle, prefixBytes, successorOfPrefix(prefixBytes));
            for (Map.Entry<String, StateRecord> staged : recordOverlay.entrySet()) {
                if (staged.getKey().startsWith(prefix) && staged.getValue() != RECORD_TOMBSTONE) {
                    // A staged record dies with the subtree too: release its reverse entry and
                    // tombstone it. Count it only when the committed scan could not have (no
                    // committed copy, or an earlier tombstone hid it); a refresh was counted.
                    if (staged.getValue().leaseId() != 0) {
                        batch.delete(leasePathsHandle, leasePathsKey(staged.getValue().leaseId(), staged.getKey()));
                    }
                    if (isUnderSubtreeTombstone(staged.getKey())
                            || db.get(stateHandle, pathKey(staged.getKey())) == null) {
                        deleted++;
                    }
                    staged.setValue(RECORD_TOMBSTONE);
                }
            }
            subtreeTombstonePrefixes.add(prefix);
            for (byte[] entry : staleReverseEntries) {
                batch.delete(leasePathsHandle, entry);
            }
            addChange(ChangeEvent.ChangeOp.DELETE_SUBTREE, path, null);
            return RaftResult.ok(RaftCommand.Op.DELETE).deletedCount(deleted);
        }

        /** Whether a live record survives below the path - the HAS_CHILDREN decision. */
        private boolean hasLiveDescendant(String path) throws RocksDBException {
            final String prefix = childPrefix(path);
            final byte[] prefixBytes = prefix.getBytes(StandardCharsets.UTF_8);
            try (RocksIterator iterator = db.newIterator(stateHandle)) {
                iterator.seek(prefixBytes);
                while (iterator.isValid() && startsWith(iterator.key(), prefixBytes)) {
                    if (!isOverlayDeleted(new String(iterator.key(), StandardCharsets.UTF_8))) {
                        return true;
                    }
                    iterator.next();
                }
                iterator.status();
            }
            for (Map.Entry<String, StateRecord> staged : recordOverlay.entrySet()) {
                if (staged.getKey().startsWith(prefix) && staged.getValue() != RECORD_TOMBSTONE) {
                    return true;
                }
            }
            return false;
        }

        /** Whether the overlay deleted the path: an exact tombstone or a staged subtree prefix. */
        private boolean isOverlayDeleted(String path) {
            return recordOverlay.get(path) == RECORD_TOMBSTONE || isUnderSubtreeTombstone(path);
        }

        private boolean isUnderSubtreeTombstone(String path) {
            for (String prefix : subtreeTombstonePrefixes) {
                if (path.startsWith(prefix)) {
                    return true;
                }
            }
            return false;
        }

        private RaftResult applyIncrement(RaftCommand command) throws RocksDBException {
            final RaftResult.Status pathError = checkPath(command.getPath());
            if (pathError != null) {
                return RaftResult.error(RaftCommand.Op.INCREMENT, pathError);
            }
            final RaftResult.Status guard = checkIncrementGuard(command);
            if (guard != null) {
                return RaftResult.error(RaftCommand.Op.INCREMENT, guard);
            }
            final byte[] counterKey = counterKey(USER_COUNTER_TAG, command.getPath());
            // Long wrap-around on overflow is deterministic on every replica, which matters.
            final long value = getCounter(counterKey) + command.getDelta();
            setCounter(counterKey, value);
            addChange(ChangeEvent.ChangeOp.INCREMENT, command.getPath(), longToBytes(value));
            return RaftResult.ok(RaftCommand.Op.INCREMENT).counterValue(value);
        }

        /**
         * The guard of a lease-guarded INCREMENT (fencing-token traffic): a non-zero leaseId
         * must name a live lease that owns the record at the counter's path right now — the
         * replicated fact the lock's create established, read through this batch's overlay so a
         * same-batch create satisfies it. A contender whose lease expired (the cascade deletes
         * lease and record in one revision) or whose record was deleted or re-bound fails here,
         * so its late token can never overtake the current holder's. {@code null} passes.
         */
        private RaftResult.Status checkIncrementGuard(RaftCommand command) throws RocksDBException {
            if (command.getLeaseId() == 0) {
                return null; // plain counter arithmetic, no lease guard
            }
            final RaftResult.Status leaseError = checkLeaseExists(command.getLeaseId());
            if (leaseError != null) {
                return leaseError; // UNKNOWN_LEASE: the contender's lease died
            }
            final StateRecord owner = getRecord(command.getPath());
            return owner != null && owner.leaseId() == command.getLeaseId() ? null : RaftResult.Status.STALE_LEASE;
        }

        private RaftResult applyLeaseRegister(RaftCommand command) throws RocksDBException {
            if (command.getLeaseId() == 0) {
                return RaftResult.error(RaftCommand.Op.LEASE_REGISTER, RaftResult.Status.BAD_PATH);
            }
            final LeaseState state = new LeaseState(command.getTtlMs(), command.getExpireAtMs(), revision);
            batch.put(leasesHandle, longToBytes(command.getLeaseId()), state.encode());
            leaseOverlay.put(command.getLeaseId(), state);
            addChange(ChangeEvent.ChangeOp.LEASE_REGISTER, "", longToBytes(command.getLeaseId()));
            return RaftResult.ok(RaftCommand.Op.LEASE_REGISTER).revision(revision);
        }

        private RaftResult applyLeaseRenew(RaftCommand command) throws RocksDBException {
            final LeaseState lease = getLease(command.getLeaseId());
            if (lease == null) {
                return RaftResult.error(RaftCommand.Op.LEASE_RENEW, RaftResult.Status.UNKNOWN_LEASE);
            }
            // Monotone expiry: a duplicate renewal that committed late (the client gave up on
            // its dispatch and re-sent under a new call id, which no retry cache dedups across)
            // carries a stale expireAtMs; overwriting would retroactively shorten a live lease
            // while bumping lastRenewIndex. A renewal that does not extend the expiry changes
            // nothing - the expiry fence and any pending expiry proposal stay valid against the
            // unchanged state.
            if (command.getExpireAtMs() <= lease.expireAtMs()) {
                return RaftResult.ok(RaftCommand.Op.LEASE_RENEW).revision(revision);
            }
            // Renewal carries the new TTL as well as the normalized absolute expiry.
            final LeaseState renewed = new LeaseState(command.getTtlMs(), command.getExpireAtMs(), revision);
            batch.put(leasesHandle, longToBytes(command.getLeaseId()), renewed.encode());
            leaseOverlay.put(command.getLeaseId(), renewed);
            addChange(ChangeEvent.ChangeOp.LEASE_RENEW, "", longToBytes(command.getLeaseId()));
            return RaftResult.ok(RaftCommand.Op.LEASE_RENEW).revision(revision);
        }

        private RaftResult applyLeaseExpire(RaftCommand command) throws RocksDBException {
            final LeaseState lease = getLease(command.getLeaseId());
            if (lease == null) {
                // Idempotent: a duplicate expiry proposal (leader change) is not an error.
                return RaftResult.ok(RaftCommand.Op.LEASE_EXPIRE).expired(false).deletedCount(0);
            }
            // Renew/expire race guard: a lease renewed after the expirer's view must not be killed.
            if (command.getExpectedLastRenewIndex() >= 0
                    && lease.lastRenewIndex() > command.getExpectedLastRenewIndex()) {
                return RaftResult.ok(RaftCommand.Op.LEASE_EXPIRE).expired(false).deletedCount(0);
            }
            batch.delete(leasesHandle, longToBytes(command.getLeaseId()));
            leaseOverlay.put(command.getLeaseId(), LEASE_TOMBSTONE);
            long deleted = 0;
            final byte[] leasePrefix = longToBytes(command.getLeaseId());
            // Victims = committed reverse entries + bindings staged earlier in this batch (a
            // WriteBatch is invisible to the iterator); collect entries before records -
            // deleteRecord stages into the very structures enumerated here.
            final List<String> victims = new ArrayList<>();
            final Set<String> victimSet = new HashSet<>();
            final List<byte[]> staleReverseEntries = new ArrayList<>();
            try (RocksIterator iterator = db.newIterator(leasePathsHandle)) {
                iterator.seek(leasePrefix);
                while (iterator.isValid() && startsWith(iterator.key(), leasePrefix)) {
                    final String path =
                            new String(iterator.key(), 8, iterator.key().length - 8, StandardCharsets.UTF_8);
                    victims.add(path);
                    victimSet.add(path);
                    staleReverseEntries.add(iterator.key().clone());
                    iterator.next();
                }
                iterator.status();
            }
            for (Map.Entry<ByteBuffer, Boolean> staged : leasePathsOverlay.entrySet()) {
                if (staged.getValue() != Boolean.TRUE || !startsWith(staged.getKey().array(), leasePrefix)) {
                    continue;
                }
                final byte[] key = staged.getKey().array();
                final String path = new String(key, 8, key.length - 8, StandardCharsets.UTF_8);
                if (victimSet.add(path)) {
                    victims.add(path);
                }
                // The staged put must not survive its lease's expiry; later deletes win in a batch.
                staleReverseEntries.add(key.clone());
            }
            for (String path : victims) {
                final StateRecord record = getRecord(path);
                // A stale reverse entry must not delete the current record, though the entry goes.
                if (record != null && record.leaseId() == command.getLeaseId()) {
                    deleteRecord(path, record);
                    deleted++;
                }
            }
            for (byte[] entry : staleReverseEntries) {
                batch.delete(leasePathsHandle, entry);
                leasePathsOverlay.put(ByteBuffer.wrap(entry), Boolean.FALSE);
            }
            addChange(ChangeEvent.ChangeOp.LEASE_EXPIRE, "", longToBytes(command.getLeaseId()));
            return RaftResult.ok(RaftCommand.Op.LEASE_EXPIRE).expired(true).deletedCount(deleted);
        }

        private RaftResult applyMulti(RaftCommand command) throws RocksDBException {
            final List<RaftCommand> ops = command.getOps();
            if (ops == null || ops.isEmpty()) {
                return RaftResult.ok(RaftCommand.Op.MULTI);
            }
            final List<RaftResult> subResults = new ArrayList<>(ops.size());
            for (int i = 0; i < ops.size(); i++) {
                final RaftResult sub = execute(ops.get(i));
                if (sub.getStatus() != RaftResult.Status.OK) {
                    // First failure aborts the batch: nothing is written, no change record exists.
                    return RaftResult.error(RaftCommand.Op.MULTI, RaftResult.Status.MULTI_FAILED)
                            .failIndex(i).failStatus(sub.getStatus());
                }
                subResults.add(sub);
            }
            return RaftResult.ok(RaftCommand.Op.MULTI).subResults(subResults);
        }

        // ---- overlay read/write helpers ----

        /** The UTF-8 state-family key of a path, encoded once per engine like the counter keys. */
        private byte[] pathKey(String path) {
            return encodedPathKeys.computeIfAbsent(path, key -> key.getBytes(StandardCharsets.UTF_8));
        }

        /** Reads through the overlay: tombstones and prefixes hide committed records; puts win. */
        private StateRecord getRecord(String path) throws RocksDBException {
            final StateRecord staged = recordOverlay.get(path);
            if (staged == RECORD_TOMBSTONE) {
                return null;
            }
            if (staged != null) {
                return staged;
            }
            if (isUnderSubtreeTombstone(path)) {
                return null;
            }
            final byte[] encoded = db.get(stateHandle, pathKey(path));
            return encoded == null ? null : StateRecord.decode(encoded);
        }

        private void putRecord(String path, StateRecord previous, StateRecord updated) throws RocksDBException {
            batch.put(stateHandle, pathKey(path), updated.encode());
            recordOverlay.put(path, updated);
            if (updated.leaseId() != 0) {
                final byte[] binding = leasePathsKey(updated.leaseId(), pathKey(path));
                batch.put(leasePathsHandle, binding, new byte[0]);
                leasePathsOverlay.put(ByteBuffer.wrap(binding), Boolean.TRUE);
            }
            if (previous != null && previous.leaseId() != 0 && previous.leaseId() != updated.leaseId()) {
                // Re-binding releases the old reverse entry, else it grows the index forever.
                final byte[] stale = leasePathsKey(previous.leaseId(), pathKey(path));
                batch.delete(leasePathsHandle, stale);
                leasePathsOverlay.put(ByteBuffer.wrap(stale), Boolean.FALSE);
            }
        }

        private void deleteRecord(String path, StateRecord record) throws RocksDBException {
            batch.delete(stateHandle, pathKey(path));
            recordOverlay.put(path, RECORD_TOMBSTONE);
            if (record.leaseId() != 0) {
                final byte[] binding = leasePathsKey(record.leaseId(), pathKey(path));
                batch.delete(leasePathsHandle, binding);
                leasePathsOverlay.put(ByteBuffer.wrap(binding), Boolean.FALSE);
            }
            addChange(ChangeEvent.ChangeOp.DELETE, path, null);
        }

        private long getCounter(byte[] key) throws RocksDBException {
            final Long staged = counterOverlay.get(ByteBuffer.wrap(key));
            if (staged != null) {
                return staged;
            }
            final byte[] encoded = db.get(countersHandle, key);
            return encoded == null ? 0 : bytesToLong(encoded, 0);
        }

        private void setCounter(byte[] key, long value) throws RocksDBException {
            batch.put(countersHandle, key, longToBytes(value));
            counterOverlay.put(ByteBuffer.wrap(key), value);
        }

        private LeaseState getLease(long leaseId) throws RocksDBException {
            final LeaseState staged = leaseOverlay.get(leaseId);
            if (staged == LEASE_TOMBSTONE) {
                return null;
            }
            if (staged != null) {
                return staged;
            }
            final byte[] encoded = db.get(leasesHandle, longToBytes(leaseId));
            return encoded == null ? null : LeaseState.decode(encoded);
        }

        /** The creation gate: the parent must exist and must not be lease-bound. */
        private RaftResult.Status checkParent(String path) throws RocksDBException {
            final String parent = parentOf(path);
            if (parent.equals("/")) {
                return null;
            }
            final StateRecord parentRecord = getRecord(parent);
            if (parentRecord == null) {
                return RaftResult.Status.NO_PARENT;
            }
            return parentRecord.leaseId() != 0 ? RaftResult.Status.NOT_ALLOWED : null;
        }

        /** A non-zero {@code leaseId} must name a lease that exists right now, via the overlay. */
        private RaftResult.Status checkLeaseExists(long leaseId) throws RocksDBException {
            if (leaseId != 0 && getLease(leaseId) == null) {
                return RaftResult.Status.UNKNOWN_LEASE;
            }
            return null;
        }

        /** Stages missing ancestors as empty records so the create cannot fail NO_PARENT. */
        private RaftResult.Status ensureParents(String path, long timestamp) throws RocksDBException {
            final List<String> missing = new ArrayList<>();
            String current = parentOf(path);
            StateRecord terminal = null;
            while (!current.equals("/") && (terminal = getRecord(current)) == null) {
                missing.add(current);
                current = parentOf(current);
            }
            if (terminal != null && terminal.leaseId() != 0) {
                return RaftResult.Status.NOT_ALLOWED;
            }
            for (int i = missing.size() - 1; i >= 0; i--) {
                final String ancestor = missing.get(i);
                putRecord(ancestor, null, StateRecord.fresh(0L, timestamp, new byte[0]));
                addChange(ChangeEvent.ChangeOp.CREATE, ancestor, new byte[0]);
            }
            return null;
        }

        private void addChange(ChangeEvent.ChangeOp op, String path, byte[] data) {
            changes.add(new ChangeEvent.Change(op, path, data));
        }
    }

    // --- Storage codecs and helpers ---

    /** Path syntax check mirroring ZooKeeper's client-side PathUtils; error status or null. */
    private static RaftResult.Status checkPath(String path) {
        if (path == null || !path.startsWith("/") || path.length() > MAX_PATH_LENGTH) {
            return RaftResult.Status.BAD_PATH;
        }
        if (path.length() > 1 && (path.endsWith("/") || path.contains("//"))) {
            return RaftResult.Status.BAD_PATH;
        }
        int segmentOpen = 0;
        for (int i = 1; i < path.length(); i++) {
            final char c = path.charAt(i);
            if (isProhibitedPathChar(c)) {
                return RaftResult.Status.BAD_PATH;
            }
            if (c == '/') {
                if (isDotSegment(path, segmentOpen, i)) {
                    return RaftResult.Status.BAD_PATH;
                }
                segmentOpen = i;
            }
        }
        return isDotSegment(path, segmentOpen, path.length()) ? RaftResult.Status.BAD_PATH : null;
    }

    /** Whether {@code c} falls in ZooKeeper's four prohibited ranges, verbatim for parity. */
    private static boolean isProhibitedPathChar(char c) {
        return c <= '\u001F' || (c >= '\u007F' && c <= '\u009F') || (c >= '\uD800' && c <= '\uF8FF') || c >= '\uFFF0';
    }

    /** Whether the segment between {@code openSlash} (exclusive) and {@code end} is "." or "..". */
    private static boolean isDotSegment(String path, int openSlash, int end) {
        final int length = end - openSlash - 1;
        if (length == 1 && path.charAt(openSlash + 1) == '.') {
            return true;
        }
        return length == 2 && path.charAt(openSlash + 1) == '.' && path.charAt(openSlash + 2) == '.';
    }

    /** The LIST prefix rule: one trailing '/' over a legal path means "children of this node". */
    private static RaftResult.Status checkListPrefix(String prefix) {
        if (prefix == null || prefix.isEmpty()) {
            return RaftResult.Status.BAD_PATH;
        }
        if (prefix.length() > 1 && prefix.endsWith("/")) {
            return checkPath(prefix.substring(0, prefix.length() - 1));
        }
        return checkPath(prefix);
    }

    private static String parentOf(String path) {
        final int lastSlash = path.lastIndexOf('/');
        return lastSlash <= 0 ? "/" : path.substring(0, lastSlash);
    }

    /** The key prefix of every child of the path ("/" is the prefix of all paths). */
    private static String childPrefix(String path) {
        return path.equals("/") ? "/" : path + "/";
    }

    /** Exclusive upper bound of a prefix {@code deleteRange}: trailing '/' incremented to '0'. */
    private static byte[] successorOfPrefix(byte[] prefix) {
        final byte[] successor = prefix.clone();
        successor[successor.length - 1]++;
        return successor;
    }

    private static byte[] counterKey(byte tag, String name) {
        final byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        final byte[] key = new byte[1 + nameBytes.length];
        key[0] = tag;
        System.arraycopy(nameBytes, 0, key, 1, nameBytes.length);
        return key;
    }

    /** The {@code %010d} rendering of a non-negative long, without the Formatter path. */
    private static String sequentialSuffix(long sequence) {
        final String digits = Long.toString(sequence);
        if (digits.length() >= 10) {
            return digits;
        }
        final char[] padded = new char[10];
        Arrays.fill(padded, '0');
        digits.getChars(0, digits.length(), padded, 10 - digits.length());
        return new String(padded);
    }

    private static byte[] leasePathsKey(long leaseId, String path) {
        return leasePathsKey(leaseId, path.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] leasePathsKey(long leaseId, byte[] pathBytes) {
        final byte[] key = new byte[8 + pathBytes.length];
        putLong(key, 0, leaseId);
        System.arraycopy(pathBytes, 0, key, 8, pathBytes.length);
        return key;
    }

    private static byte[] longToBytes(long value) {
        final byte[] bytes = new byte[8];
        putLong(bytes, 0, value);
        return bytes;
    }

    static void putLong(byte[] target, int offset, long value) {
        for (int i = 0; i < 8; i++) {
            target[offset + i] = (byte) (value >>> (8 * (7 - i)));
        }
    }

    static void putInt(byte[] target, int offset, int value) {
        for (int i = 0; i < 4; i++) {
            target[offset + i] = (byte) (value >>> (8 * (3 - i)));
        }
    }

    static long bytesToLong(byte[] source, int offset) {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value = (value << 8) | (source[offset + i] & 0xFFL);
        }
        return value;
    }

    static int bytesToInt(byte[] source, int offset) {
        int value = 0;
        for (int i = 0; i < 4; i++) {
            value = (value << 8) | (source[offset + i] & 0xFF);
        }
        return value;
    }

    private static boolean startsWith(byte[] key, byte[] prefix) {
        if (key.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (key[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    /** Unsigned bytewise comparison - RocksDB's default comparator's order, and LIST cursors'. */
    private static int compareUnsigned(byte[] a, byte[] b) {
        final int shared = Math.min(a.length, b.length);
        for (int i = 0; i < shared; i++) {
            final int diff = (a[i] & 0xFF) - (b[i] & 0xFF);
            if (diff != 0) {
                return diff;
            }
        }
        return a.length - b.length;
    }

    private static CompletableFuture<Message> completed(Message message) {
        return CompletableFuture.completedFuture(message);
    }
}
