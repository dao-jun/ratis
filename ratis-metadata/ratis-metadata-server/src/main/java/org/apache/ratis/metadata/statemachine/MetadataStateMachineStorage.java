/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file distributed with
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
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.SnapshotRetentionPolicy;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.impl.FileListSnapshotInfo;
import org.apache.ratis.util.MD5FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Snapshot management: one RocksDB checkpoint directory per snapshot under the sm dir. */
public class MetadataStateMachineStorage implements StateMachineStorage {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataStateMachineStorage.class);

    /** Name prefix of a snapshot directory: {@code snapshot.<term>_<index>}. */
    static final String SNAPSHOT_DIR_PREFIX = "snapshot";

    /** Snapshot dir pattern: group 1 = term, group 2 = last-included index (ratis' convention). */
    static final Pattern SNAPSHOT_DIR_REGEX =
            Pattern.compile(SNAPSHOT_DIR_PREFIX + "\\.(\\d+)_(\\d+)");

    /** Suffix of the temporary directory a checkpoint is built in before being published. */
    static final String SNAPSHOT_TMP_SUFFIX = ".tmp";

    /** Suffix of the aside copy of a replaced target; deleted after publish and at startup. */
    static final String SNAPSHOT_TRASH_SUFFIX = ".trash";

    /** Startup scan attempts and pause between them: transient disk errors get a short chance. */
    private static final int STARTUP_SCAN_ATTEMPTS = 3;
    private static final long STARTUP_SCAN_RETRY_INTERVAL_MS = 250L;

    /** The raft storage's state machine directory; set by {@link #init(RaftStorage)}. */
    private volatile File stateMachineDir;

    /** The latest snapshot, discovered lazily and refreshed by the state machine on publish. */
    private final AtomicReference<FileListSnapshotInfo> latestSnapshot = new AtomicReference<>();

    @Override
    public void init(RaftStorage raftStorage) throws IOException {
        this.stateMachineDir = raftStorage.getStorageDir().getStateMachineDir();
        IOException lastFailure = null;
        for (int attempt = 1; attempt <= STARTUP_SCAN_ATTEMPTS; attempt++) {
            try {
                cleanupCrashLeftovers();
                scanLatestSnapshot();
                verifySnapshotAreaScannable();
                return;
            } catch (IOException failure) {
                lastFailure = failure;
                LOG.warn("Scanning {} for snapshots failed (attempt {}/{})", stateMachineDir, attempt,
                        STARTUP_SCAN_ATTEMPTS, failure);
                if (attempt < STARTUP_SCAN_ATTEMPTS) {
                    try {
                        Thread.sleep(STARTUP_SCAN_RETRY_INTERVAL_MS);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Interrupted while scanning " + stateMachineDir
                                + " for snapshots; refusing to start", interrupted);
                    }
                }
            }
        }
        // Startup fails loud: the raft log seeds its snapshot index from this scan, and a member
        // whose log was purged into its snapshots has nothing else to replay. An unverifiable
        // snapshot area is an operator decision - fix the storage (permissions, disk) or rebuild
        // the rocksdb/raft pair - never something the server may degrade through.
        LOG.error("Failed to scan {} for snapshots after {} attempts; refusing to start. Fix the"
                        + " storage (permissions or disk) or rebuild the rocksdb/raft pair.",
                stateMachineDir, STARTUP_SCAN_ATTEMPTS, lastFailure);
        throw new IOException("Failed to scan " + stateMachineDir + " for snapshots after " + STARTUP_SCAN_ATTEMPTS
                + " attempts; refusing to start", lastFailure);
    }

    @Override
    public SnapshotInfo getLatestSnapshot() {
        final FileListSnapshotInfo snapshot = latestSnapshot.get();
        if (snapshot != null) {
            return snapshot;
        }
        return loadLatestSnapshot();
    }

    /** Re-scans and caches the newest snapshot; never regresses the cache. */
    FileListSnapshotInfo loadLatestSnapshot() {
        final File dir = stateMachineDir;
        if (dir == null) {
            return null;
        }
        try {
            return scanLatestSnapshot();
        } catch (IOException e) {
            // A broken listing must not kill a running server; ratis logs and returns null
            // here too. Startup is the exception - see init().
            LOG.warn("Failed to scan {} for snapshots", dir, e);
            return null;
        }
    }

    /** The scan itself; the caller decides how a failure lands. */
    private FileListSnapshotInfo scanLatestSnapshot() throws IOException {
        final File newest = findLatestSnapshotDir(stateMachineDir);
        if (newest == null) {
            return null;
        }
        final TermIndex termIndex = termIndexOf(newest);
        final List<FileInfo> files = listSnapshotFiles(newest.toPath());
        if (files.isEmpty()) {
            // A manifested snapshot always lists at least the manifest itself; empty means the
            // listing raced a vanishing dir. Treat it as a scan failure, not "no snapshot".
            throw new IOException("Snapshot directory " + newest + " listed no files");
        }
        final FileListSnapshotInfo info = new FileListSnapshotInfo(files, termIndex);
        latestSnapshot.updateAndGet(previous ->
                previous == null || info.getIndex() >= previous.getIndex() ? info : previous);
        return info;
    }

    /**
     * Startup-only companion to the scan. java.io.File#isFile() returns false both when the
     * manifest is absent and when its stat failed, so a snapshot directory stripped of its
     * execute bit would be invisible to the scan with no exception anywhere - the exact
     * member this module must refuse to start. Every snapshot-shaped entry must be
     * verifiable: manifest present as a regular file, or confirmed absent (a torn
     * publication cleanup already removed).
     */
    private void verifySnapshotAreaScannable() throws IOException {
        final File dir = stateMachineDir;
        if (dir == null || !dir.isDirectory()) {
            return;
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir.toPath())) {
            for (Path entry : stream) {
                if (!SNAPSHOT_DIR_REGEX.matcher(entry.getFileName().toString()).matches()) {
                    continue; // .tmp/.trash leftovers and junk are cleanup's business
                }
                final Path manifest = entry.resolve(StateMachineIdentity.SNAPSHOT_FILE);
                try {
                    if (!Files.readAttributes(manifest, BasicFileAttributes.class)
                            .isRegularFile()) {
                        throw new IOException(manifest + " is not a regular file");
                    }
                } catch (NoSuchFileException confirmedAbsent) {
                    // Torn publication or junk: cleanupCrashLeftovers already handled it.
                } catch (IOException cannotVerify) {
                    throw new IOException("Snapshot directory " + entry
                            + " exists but its manifest cannot be verified; refusing" + " to start", cannotVerify);
                }
            }
        }
    }

    /** Removes interrupted-publication leftovers: .tmp, .trash, manifest-less dirs. */
    private void cleanupCrashLeftovers() {
        final File dir = stateMachineDir;
        if (dir == null || !dir.isDirectory()) {
            return;
        }
        final File[] entries = dir.listFiles();
        if (entries == null) {
            return;
        }
        for (File entry : entries) {
            if (!entry.isDirectory()) {
                continue;
            }
            final String name = entry.getName();
            final boolean leftoverSuffix = name.startsWith(SNAPSHOT_DIR_PREFIX + ".")
                    && (name.endsWith(SNAPSHOT_TMP_SUFFIX) || name.endsWith(SNAPSHOT_TRASH_SUFFIX));
            if (leftoverSuffix) {
                LOG.warn("Deleting {} - leftover of an interrupted snapshot publication", entry);
                IOUtils.deleteDirectoryQuietly(entry);
            } else if (SNAPSHOT_DIR_REGEX.matcher(name).matches() && manifestConfirmedAbsent(entry)) {
                LOG.warn("Deleting {} - snapshot-shaped directory without the {} manifest",
                        entry, StateMachineIdentity.SNAPSHOT_FILE);
                IOUtils.deleteDirectoryQuietly(entry);
            }
        }
    }

    /** True only when the manifest's absence is confirmed - a failed stat is not absence. */
    private static boolean manifestConfirmedAbsent(File snapshotDir) {
        try {
            Files.readAttributes(new File(snapshotDir, StateMachineIdentity.SNAPSHOT_FILE).toPath(),
                    BasicFileAttributes.class);
            return false;
        } catch (NoSuchFileException absent) {
            return true;
        } catch (IOException cannotTell) {
            LOG.warn("Cannot verify the manifest of {}; leaving it in place - the startup scan"
                            + " will refuse if this persists", snapshotDir, cannotTell);
            return false;
        }
    }

    @Override
    public void format() throws IOException {
        // Raft storage format() wipes the state machine dir; nothing to remove here (as ratis).
    }

    @Override
    public void cleanupOldSnapshots(SnapshotRetentionPolicy snapshotRetentionPolicy)
            throws IOException {
        final File dir = stateMachineDir;
        if (dir == null || snapshotRetentionPolicy == null) {
            return;
        }
        final int retained = snapshotRetentionPolicy.getNumSnapshotsRetained();
        if (retained <= 0) {
            // Retain everything: the default policy of "no limit configured".
            return;
        }
        final List<File> snapshots = findSnapshotDirs(dir);
        // Newest first (by index; a newer snapshot always has a greater last-included index).
        try {
            snapshots.sort(Comparator.comparingLong((File snapshot) -> {
                try {
                    return termIndexOf(snapshot).getIndex();
                } catch (IOException unparseable) {
                    // Bridge the checked exception through the comparator; rethrown below.
                    throw new UncheckedIOException(unparseable);
                }
            }).reversed());
        } catch (UncheckedIOException unparseable) {
            throw unparseable.getCause();
        }
        for (int i = retained; i < snapshots.size(); i++) {
            final File stale = snapshots.get(i);
            LOG.info("Deleting old metadata snapshot {}", stale);
            IOUtils.deleteDirectoryQuietly(stale);
        }
    }

    /** The directory ratis installs snapshots into and relativizes outgoing files against. */
    @Override
    public File getSnapshotDir() {
        return Objects.requireNonNull(stateMachineDir, "storage is not initialized yet");
    }

    /** Directory the snapshot with this last-included term/index lives in. */
    File snapshotDirectory(long term, long index) {
        return new File(getSnapshotDir(), SNAPSHOT_DIR_PREFIX + "." + term + "_" + index);
    }

    /** Not-yet-published directory the term/index checkpoint is built in. */
    File tmpSnapshotDirectory(long term, long index) {
        return new File(getSnapshotDir(), SNAPSHOT_DIR_PREFIX + "." + term + "_" + index + SNAPSHOT_TMP_SUFFIX);
    }

    private static File findLatestSnapshotDir(File dir) throws IOException {
        final List<File> snapshots = findSnapshotDirs(dir);
        File newest = null;
        long newestIndex = -1;
        for (File snapshot : snapshots) {
            final long index = termIndexOf(snapshot).getIndex();
            if (index > newestIndex) {
                newest = snapshot;
                newestIndex = index;
            }
        }
        return newest;
    }

    private static List<File> findSnapshotDirs(File dir) throws IOException {
        final List<File> snapshots = new ArrayList<>();
        if (!dir.isDirectory()) {
            return snapshots;
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir.toPath())) {
            for (Path entry : stream) {
                if (!entry.toFile().isDirectory()
                        || !SNAPSHOT_DIR_REGEX.matcher(entry.getFileName().toString()).matches()) {
                    continue;
                }
                // The manifest is the last file of a publication, so only a dir confirmed
                // without it (a torn remainder) is skipped - a failed stat is not absence.
                if (manifestConfirmedAbsent(entry.toFile())) {
                    continue;
                }
                snapshots.add(entry.toFile());
            }
        }
        return snapshots;
    }

    /**
     * @throws IllegalArgumentException when the directory name does not match the pattern.
     * @throws IOException when the name's term or index digits do not parse as a long (a digit
     *         run beyond {@link Long#MAX_VALUE}), so the caller's designed IOException path -
     *         startup's retry-then-refuse included - applies unchanged.
     */
    private static TermIndex termIndexOf(File snapshotDir) throws IOException {
        final Matcher matcher = SNAPSHOT_DIR_REGEX.matcher(snapshotDir.getName());
        if (!matcher.matches()) {
            throw new IllegalArgumentException(snapshotDir + " is not a snapshot directory");
        }
        try {
            return TermIndex.valueOf(Long.parseLong(matcher.group(1)), Long.parseLong(matcher.group(2)));
        } catch (NumberFormatException e) {
            throw new IOException("Snapshot directory " + snapshotDir
                    + " has an out-of-range term or index in its name", e);
        }
    }

    /** Lists regular files (excl. md5 sidecars), path-sorted for deterministic chunk transfer. */
    private static List<FileInfo> listSnapshotFiles(Path snapshotDir) throws IOException {
        final List<Path> files = new ArrayList<>();
        collectFiles(snapshotDir, files);
        files.sort(Comparator.comparing(Path::toString));
        final List<FileInfo> infos = new ArrayList<>(files.size());
        for (Path file : files) {
            // Null digest: ratis computes md5 while chunking (FileChunkReader).
            infos.add(new FileInfo(file, null));
        }
        return infos;
    }

    private static void collectFiles(Path dir, List<Path> collected) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path entry : stream) {
                if (Files.isDirectory(entry)) {
                    collectFiles(entry, collected);
                } else if (!entry.getFileName().toString().endsWith(MD5FileUtil.MD5_SUFFIX)) {
                    collected.add(entry);
                }
            }
        }
    }
}
