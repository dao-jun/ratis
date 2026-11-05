/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
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

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.metadata.util.Clock;
import org.apache.ratis.metadata.util.FileUtils;
import org.apache.ratis.metadata.util.ManualClock;
import org.apache.ratis.metadata.util.SystemClock;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.server.storage.StorageImplUtils;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.TransactionContextImpl;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.SizeInBytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Storage-safety tests of the metadata state machine on hand-driven, thread-free standalone
 * members.
 */
public class TestMetadataStateMachineStorageSafety {

    private static final UUID GROUP_A = UUID.fromString("11111111-1111-4111-8111-111111111111");
    private static final UUID GROUP_B = UUID.fromString("22222222-2222-4222-8222-222222222222");

    /**
     * 64 recognizable marker bytes, so a test can assert an artifact is really gone (not merely
     * renamed).
     */
    private static final byte[] JUNK_BYTES = new byte[64];

    static {
        Arrays.fill(JUNK_BYTES, (byte) 0x7f);
    }

    @TempDir File tempDir;

    /** One standalone member: a built-not-started server plus its state machine. */
    private final class Member implements AutoCloseable {
        final RaftGroupId groupId;
        final File raftDir;
        final File dbDir;
        final MetadataStateMachine stateMachine;
        private RaftServer server;

        Member(String name, UUID groupUuid, File dbDir) {
            this(name, groupUuid, dbDir, SystemClock.getInstance());
        }

        Member(String name, UUID groupUuid, File dbDir, Clock clock) {
            this.groupId = RaftGroupId.valueOf(groupUuid);
            this.raftDir = new File(tempDir, name + "-raft");
            this.dbDir = dbDir;
            this.stateMachine = new MetadataStateMachine(dbDir, clock, Duration.ofDays(7));
        }

        /**
         * Building registers no group (impls are created on start()), so the state machine is
         * initialized by hand.
         */
        void open() throws IOException {
            this.server = buildServer();
            final RaftStorage storage =
                    StorageImplUtils.newRaftStorage(
                            raftDir,
                            SizeInBytes.valueOf("0MB"),
                            // RECOVER covers the first open and the reopen (FORMAT would wipe the
                            // raft-meta files, and with them the snapshots).
                            RaftStorage.StartupOption.RECOVER,
                            null);
            try {
                storage.initialize();
                stateMachine.initialize(server, groupId, storage);
            } finally {
                // The state machine keeps only the sm dir path, so the lock can be released;
                // members open strictly one after another.
                try {
                    storage.close();
                } catch (IOException ignore) {
                }
            }
        }

        private RaftServer buildServer() throws IOException {
            final RaftPeerId selfId = RaftPeerId.valueOf("s0");
            final RaftGroup group =
                    RaftGroup.valueOf(
                            groupId,
                            RaftPeer.newBuilder().setId(selfId).setAddress("localhost:0").build());
            final RaftProperties properties = new RaftProperties();
            RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.GRPC);
            // the machine is never started; the rpc binds an ephemeral port at build time
            GrpcConfigKeys.Server.setPort(properties, 0);
            RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(raftDir));
            return RaftServer.newBuilder()
                    .setServerId(selfId)
                    .setGroup(group)
                    .setStateMachine(stateMachine)
                    .setProperties(properties)
                    .build();
        }

        File stateMachineDir() {
            return new File(raftDir, "sm");
        }

        File snapshotDir(long term, long index) {
            return new File(
                    stateMachineDir(),
                    MetadataStateMachineStorage.SNAPSHOT_DIR_PREFIX + "." + term + "_" + index);
        }

        @Override
        public void close() {
            if (server != null) {
                try {
                    server.close();
                } catch (Exception ignore) {
                }
            }
            stateMachine.close();
        }
    }

    /**
     * Opens the member, lifts its watermark to (1, 1) the way ratis notifies non-state-machine
     * entries, and snapshots.
     */
    private static void openAndSnapshotAtOne(Member member) throws IOException {
        member.open();
        member.stateMachine.notifyTermIndexUpdated(1L, 1L);
        assertEquals(1L, member.stateMachine.takeSnapshot());
    }

    /**
     * Opens the member and applies seven hand-numbered entries - two leases, two lease
     * bindings, a user counter and a sequential cursor - then snapshots at (1, 7). The
     * fixed clock makes every stored lease literal: the log envelope normalizes each
     * expiry to clock + ttl (MetadataLogEnvelope), so lease 11 reads 5000/15000/2 and
     * lease 12 reads 7000/17000/3 (lastRenewIndex = the registering revision).
     */
    private static long populateLeasesReverseIndexAndCounters(Member member) throws Exception {
        member.open();
        apply(member, 1L, RaftCommand.create("/data", "root".getBytes(StandardCharsets.UTF_8), false, 0L, 1L));
        apply(member, 2L, RaftCommand.leaseRegister(11L, 5_000L, 0L));
        apply(member, 3L, RaftCommand.leaseRegister(12L, 7_000L, 0L));
        apply(member, 4L,
                RaftCommand.create("/data/ephemeral-a", "a".getBytes(StandardCharsets.UTF_8), false, 11L, 4L));
        apply(member, 5L,
                RaftCommand.create("/data/ephemeral-b", "b".getBytes(StandardCharsets.UTF_8), false, 11L, 5L));
        apply(member, 6L, RaftCommand.increment("/counters/hits", 5L));
        apply(member, 7L, RaftCommand.create("/data/seq-", "s".getBytes(StandardCharsets.UTF_8), true, 0L, 7L));
        assertEquals(7L, member.stateMachine.takeSnapshot());
        return 7L;
    }

    private static void apply(Member member, long revision, RaftCommand command) throws Exception {
        final byte[] reply = applyEntry(member, prepareEntry(member, 1L, revision, revision, command));
        assertEquals(
                RaftResult.Status.OK,
                RaftCommandCodec.decodeResult(reply).getStatus(),
                () -> command.getOp() + " at revision " + revision + " applies");
    }

    private static LogEntryProto prepareEntry(
            Member member, long term, long revision, long callId, RaftCommand command)
            throws IOException {
        final RaftClientRequest request =
                RaftClientRequest.newBuilder()
                        .setClientId(ClientId.valueOf(GROUP_A))
                        .setServerId(member.stateMachine.getId())
                        .setGroupId(member.groupId)
                        .setCallId(callId)
                        .setMessage(
                                Message.valueOf(
                                        ByteBuffer.wrap(RaftCommandCodec.encodeCommand(command))))
                        .setType(RaftClientRequest.writeRequestType())
                        .build();
        final TransactionContext context = member.stateMachine.startTransaction(request);
        ((TransactionContextImpl) context).initLogEntry(term, revision);
        return context.getLogEntry();
    }

    private static byte[] applyEntry(Member member, LogEntryProto entry) throws Exception {
        final Message reply =
                member.stateMachine
                        .applyTransaction(
                                TransactionContext.newBuilder()
                                        .setStateMachine(member.stateMachine)
                                        .setLogEntry(entry)
                                        .build())
                        .get(10, TimeUnit.SECONDS);
        return reply.getContent().toByteArray();
    }

    private static RaftResult revisionOf(MetadataStateMachine stateMachine) {
        try {
            final CompletableFuture<Message> reply =
                    stateMachine.query(
                            Message.valueOf(
                                    ByteBuffer.wrap(
                                            RaftCommandCodec.encodeCommand(
                                                    RaftCommand.revision()))));
            return RaftCommandCodec.decodeResult(
                    reply.get(10, TimeUnit.SECONDS).getContent().toByteArray());
        } catch (Exception e) {
            throw new AssertionError("Direct revision query failed", e);
        }
    }

    /** {@code name -> length} of every regular file below the directory, for untouched-checks. */
    private static Map<String, Long> fileInventory(File dir) throws IOException {
        final Map<String, Long> inventory = new TreeMap<>();
        try (Stream<Path> files = Files.walk(dir.toPath())) {
            for (Path file : files.collect(Collectors.toList())) {
                if (Files.isRegularFile(file)) {
                    inventory.put(dir.toPath().relativize(file).toString(), Files.size(file));
                }
            }
        }
        return inventory;
    }

    private static void writeJunk(File file) throws IOException {
        Files.createDirectories(file.getParentFile().toPath());
        Files.write(file.toPath(), JUNK_BYTES);
    }

    /** Copies a directory tree, the way a test simulates an artifact appearing on disk. */
    private static void copyTree(File source, File target) throws IOException {
        try (Stream<Path> files = Files.walk(source.toPath())) {
            for (Path file : files.collect(Collectors.toList())) {
                final Path copy =
                        target.toPath().resolve(source.toPath().relativize(file).toString());
                if (Files.isDirectory(file)) {
                    Files.createDirectories(copy);
                } else {
                    Files.copy(file, copy, StandardCopyOption.COPY_ATTRIBUTES);
                }
            }
        }
    }

    /**
     * A junk MANIFEST makes open fail - the file exists, so it can never be mistaken for a missing
     * database.
     */
    private static void corruptManifest(File dbDir) throws IOException {
        final File[] manifests = dbDir.listFiles((dir, name) -> name.startsWith("MANIFEST-"));
        assertNotNull(manifests, "the working database holds a MANIFEST file");
        assertTrue(manifests.length > 0, "the working database holds a MANIFEST file");
        Files.write(manifests[0].toPath(), JUNK_BYTES);
    }

    /** The exact three-family content of a closed metadata database, dumped read-only. */
    private static void assertLeasesReverseIndexAndCounters(File dbDir) throws IOException {
        final Options options = new Options();
        final List<AutoCloseable> toClose = new ArrayList<>();
        toClose.add(options);
        try {
            final List<byte[]> cfNames = RocksDB.listColumnFamilies(options, dbDir.getAbsolutePath());
            final List<String> columnFamilies = new ArrayList<>();
            for (byte[] name : cfNames) {
                columnFamilies.add(new String(name, StandardCharsets.UTF_8));
            }
            final List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
            for (byte[] name : cfNames) {
                final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
                toClose.add(cfOptions);
                descriptors.add(new ColumnFamilyDescriptor(name, cfOptions));
            }
            final List<ColumnFamilyHandle> handles = new ArrayList<>();
            final RocksDB db = RocksDB.openReadOnly(dbDir.getAbsolutePath(), descriptors, handles);
            toClose.add(db);
            try {
                MetadataTableDumpAsserts.assertLeaseIndexAndCounterContent(
                        db,
                        MetadataTableDumpAsserts.handleOf(handles, columnFamilies, "leases"),
                        MetadataTableDumpAsserts.handleOf(handles, columnFamilies, "lease-paths"),
                        MetadataTableDumpAsserts.handleOf(handles, columnFamilies, "counters"),
                        expectedLeases(),
                        new TreeSet<>(Arrays.asList("11:/data/ephemeral-a", "11:/data/ephemeral-b")),
                        expectedCounters());
            } finally {
                for (ColumnFamilyHandle handle : handles) {
                    handle.close();
                }
            }
        } catch (RocksDBException e) {
            throw new IOException("Failed to inspect the metadata tables of " + dbDir, e);
        } finally {
            for (int i = toClose.size() - 1; i >= 0; i--) {
                quietClose(toClose.get(i));
            }
        }
    }

    private static Map<Long, String> expectedLeases() {
        final Map<Long, String> leases = new TreeMap<>();
        leases.put(11L, "5000/15000/2");
        leases.put(12L, "7000/17000/3");
        return leases;
    }

    private static Map<String, Long> expectedCounters() {
        final Map<String, Long> counters = new TreeMap<>();
        counters.put("user:/counters/hits", 5L);
        counters.put("sequential:/data/seq-", 1L);
        return counters;
    }

    private static void quietClose(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception ignored) {
        }
    }

    @Test
    void testWorkingDatabaseRefusesToServeAnotherGroup() throws Exception {
        final File dbDir = new File(tempDir, "shared-rocksdb");
        try (Member first = new Member("first", GROUP_A, dbDir)) {
            openAndSnapshotAtOne(first);
            final File manifest =
                    new File(first.snapshotDir(1, 1), StateMachineIdentity.SNAPSHOT_FILE);
            assertTrue(manifest.isFile(), "the published snapshot carries its identity manifest");
            final String manifestText =
                    new String(Files.readAllBytes(manifest.toPath()), StandardCharsets.UTF_8);
            assertTrue(manifestText.contains("group-id=" + GROUP_A));
            assertTrue(manifestText.contains("layout-version=" + StateMachineIdentity.LAYOUT_VERSION));
            assertTrue(
                    manifestText.contains(
                            "codec-format-version=" + RaftCommandCodec.CURRENT_FORMAT_VERSION));
            // The column-families line is pinned so the families the DB opens and the
            // manifest line cannot drift apart.
            assertTrue(
                    manifestText.contains(
                            "column-families=default,state,counters,leases,lease-paths,changes,"
                                    + "command-results"));
        }

        try (Member rebuilt = new Member("rebuilt", GROUP_B, dbDir)) {
            final IOException failure = assertThrows(IOException.class, rebuilt::open);
            assertTrue(failure.getMessage().contains(GROUP_A.toString()));
            assertTrue(failure.getMessage().contains(GROUP_B.toString()));
            assertTrue(failure.getMessage().contains(dbDir.getPath()));
        }

        try (Member reopened = new Member("first", GROUP_A, dbDir)) {
            reopened.open();
            assertEquals(LifeCycle.State.RUNNING, reopened.stateMachine.getLifeCycleState());
            assertEquals(
                    1L,
                    revisionOf(reopened.stateMachine).getRevision(),
                    "the reopened database keeps its applied-revision marker");
        }
    }

    @Test
    void testSnapshotOfAnotherLayoutVersionIsRefused() throws Exception {
        final File dbDir = new File(tempDir, "rocksdb");
        final Member prototype = new Member("member", GROUP_A, dbDir);
        try {
            openAndSnapshotAtOne(prototype);
        } finally {
            prototype.close();
        }
        final File snapshotDir = prototype.snapshotDir(1, 1);
        final File manifest = new File(snapshotDir, StateMachineIdentity.SNAPSHOT_FILE);
        final String manifestText =
                new String(Files.readAllBytes(manifest.toPath()), StandardCharsets.UTF_8);
        assertTrue(manifestText.contains("layout-version=" + StateMachineIdentity.LAYOUT_VERSION));
        Files.write(
                manifest.toPath(),
                manifestText
                        .replace(
                                "layout-version=" + StateMachineIdentity.LAYOUT_VERSION,
                                "layout-version=99")
                        .getBytes(StandardCharsets.UTF_8));
        final Map<String, Long> refusedInventory = fileInventory(snapshotDir);

        // A lost working database forces the restore path at startup; the manifest check must stop
        // it before any copy.
        FileUtils.deleteDirectoryQuietly(dbDir);
        try (Member member = new Member("member", GROUP_A, dbDir)) {
            final IOException failure = assertThrows(IOException.class, member::open);
            assertTrue(failure.getMessage().contains(snapshotDir.getPath()));
            assertTrue(
                    failure
                            .getMessage()
                            .contains(
                                    "layout-version: found 99, expected "
                                            + StateMachineIdentity.LAYOUT_VERSION));
            assertEquals(
                    refusedInventory,
                    fileInventory(snapshotDir),
                    "nothing was copied out of, or written into, the refused snapshot");
        }
    }

    @Test
    void testTornSnapshotDirectoryIsIgnoredAndCleanedAtStartup() throws Exception {
        final File dbDir = new File(tempDir, "rocksdb");
        final Member prototype = new Member("member", GROUP_A, dbDir);
        try {
            openAndSnapshotAtOne(prototype);
        } finally {
            prototype.close();
        }
        final File smDir = prototype.stateMachineDir();
        writeJunk(new File(smDir, "snapshot.1_2/CURRENT"));
        writeJunk(new File(smDir, "snapshot.1_3.tmp/CURRENT"));
        writeJunk(new File(smDir, "snapshot.1_0.trash/CURRENT"));

        // A fresh working database below the snapshots: startup must restore, from index 1 only.
        FileUtils.deleteDirectoryQuietly(dbDir);
        try (Member member = new Member("member", GROUP_A, dbDir)) {
            member.open();
            assertEquals(LifeCycle.State.RUNNING, member.stateMachine.getLifeCycleState());
            assertEquals(
                    1L,
                    revisionOf(member.stateMachine).getRevision(),
                    "restored from the complete snapshot, not the torn index-2 remainder");
            assertTrue(member.snapshotDir(1, 1).isDirectory(), "the complete snapshot survives");
            assertFalse(new File(smDir, "snapshot.1_2").exists(), "the torn remainder is cleaned");
            assertFalse(new File(smDir, "snapshot.1_3.tmp").exists());
            assertFalse(new File(smDir, "snapshot.1_0.trash").exists());
        }
    }

    @Test
    void testPublishOverExistingTargetLeavesNoTornLeftovers() throws Exception {
        final File dbDir = new File(tempDir, "rocksdb");
        try (Member member = new Member("member", GROUP_A, dbDir)) {
            // The pre-existing target a crashed publication could have left behind.
            writeJunk(new File(member.stateMachineDir(), "snapshot.1_1/CURRENT"));
            member.open();
            member.stateMachine.notifyTermIndexUpdated(1L, 1L);
            assertEquals(1L, member.stateMachine.takeSnapshot());

            assertTrue(
                    new File(member.snapshotDir(1, 1), StateMachineIdentity.SNAPSHOT_FILE).isFile(),
                    "the target was replaced by the published snapshot");
            assertTrue(
                    new File(member.snapshotDir(1, 1), "CURRENT").isFile(),
                    "the checkpoint content replaced the junk");
            try (Stream<Path> entries = Files.list(member.stateMachineDir().toPath())) {
                final List<String> names =
                        entries.map(path -> path.getFileName().toString())
                                .collect(Collectors.toList());
                assertEquals(
                        1, names.size(), "no aside copy, temporary directory or remainder survives");
                assertEquals(MetadataStateMachineStorage.SNAPSHOT_DIR_PREFIX + ".1_1", names.get(0));
            }
            assertEquals(1L, revisionOf(member.stateMachine).getRevision());
        }
    }

    @Test
    void testTornWorkingDatabaseSelfHealsFromLocalSnapshot() throws Exception {
        final File dbDir = new File(tempDir, "rocksdb");
        try (Member member = new Member("member", GROUP_A, dbDir)) {
            openAndSnapshotAtOne(member);
        }
        corruptManifest(dbDir);

        try (Member member = new Member("member", GROUP_A, dbDir)) {
            member.open();
            assertEquals(LifeCycle.State.RUNNING, member.stateMachine.getLifeCycleState());
            assertEquals(
                    1L,
                    revisionOf(member.stateMachine).getRevision(),
                    "the healed database carries the snapshot's applied-revision marker");
            final File[] manifests = dbDir.listFiles((dir, name) -> name.startsWith("MANIFEST-"));
            assertNotNull(manifests);
            assertTrue(manifests.length > 0);
            for (File manifest : manifests) {
                assertNotEquals(
                        JUNK_BYTES.length,
                        Files.size(manifest.toPath()),
                        "the healed database was rebuilt, not opened over the corruption");
            }
        }
    }

    @Test
    void testInterruptedRestoreRollsBackFromAsideCopyAtStartup() throws Exception {
        final File dbDir = new File(tempDir, "rocksdb");
        try (Member member = new Member("member", GROUP_A, dbDir)) {
            openAndSnapshotAtOne(member);
        }
        // The exact artifacts replaceDirectoryAtomically leaves behind in that window.
        final File aside = new File(dbDir.getPath() + ".trash");
        final File staging = new File(dbDir.getPath() + ".restore-staging");
        Files.move(dbDir.toPath(), aside.toPath());
        writeJunk(new File(staging, "CURRENT"));

        try (Member member = new Member("member", GROUP_A, dbDir)) {
            member.open();
            assertEquals(LifeCycle.State.RUNNING, member.stateMachine.getLifeCycleState());
            assertFalse(aside.exists(), "the aside copy is rolled into place and consumed");
            assertFalse(staging.exists(), "the staging copy is dropped");
            assertTrue(
                    new File(dbDir, "CURRENT").isFile(),
                    "the rolled-back database is the one the restore displaced");
            assertEquals(
                    1L,
                    revisionOf(member.stateMachine).getRevision(),
                    "the rolled-back database keeps its marker");
        }
    }

    @Test
    void testReinitializeRestoresFromInstalledSnapshotAndLeavesItIntact() throws Exception {
        // The receiver "receives" the snapshot the way ratis finalizes an install: the directory
        // appears below its own sm dir.
        final File donorDb = new File(tempDir, "donor-rocksdb");
        final Member donor = new Member("donor", GROUP_A, donorDb);
        try {
            openAndSnapshotAtOne(donor);
        } finally {
            donor.close();
        }
        final File receiverDb = new File(tempDir, "receiver-rocksdb");
        final Member receiver = new Member("receiver", GROUP_A, receiverDb);
        try {
            receiver.open();
            assertTrue(
                    revisionOf(receiver.stateMachine).getRevision() < 0,
                    "a fresh member has applied nothing yet");

            final File installed = receiver.snapshotDir(1, 1);
            copyTree(donor.snapshotDir(1, 1), installed);
            final Map<String, Long> installedInventory = fileInventory(installed);

            receiver.stateMachine.pause();
            receiver.stateMachine.reinitialize();

            assertEquals(
                    1L,
                    receiver.stateMachine.getLastAppliedTermIndex().getIndex(),
                    "the watermark lifts to the installed snapshot's index");
            assertEquals(1L, revisionOf(receiver.stateMachine).getRevision());
            assertEquals(
                    installedInventory,
                    fileInventory(installed),
                    "the installed snapshot is an immutable artifact");
        } finally {
            receiver.close();
        }
    }

    @Test
    void testReinitializeConvergesWhenAFinalizeSwapsTheSnapshotMidRestore() throws Exception {
        final File donorDb = new File(tempDir, "donor-rocksdb");
        final Member donor = new Member("donor", GROUP_A, donorDb);
        try {
            openAndSnapshotAtOne(donor);
            donor.stateMachine.notifyTermIndexUpdated(1L, 2L);
            assertEquals(2L, donor.stateMachine.takeSnapshot());
        } finally {
            donor.close();
        }

        final File receiverDb = new File(tempDir, "receiver-rocksdb");
        final Member receiver = new Member("receiver", GROUP_A, receiverDb);
        try {
            receiver.open();
            copyTree(donor.snapshotDir(1, 1), receiver.snapshotDir(1, 1));

            // The finalize of the second install, performed where the real one can land.
            final AtomicBoolean swapped = new AtomicBoolean();
            receiver.stateMachine.restoreBeforeFirstFile =
                    () -> {
                        if (swapped.compareAndSet(false, true)) {
                            try {
                                copyTree(donor.snapshotDir(1, 2), receiver.snapshotDir(1, 2));
                                FileUtils.deleteDirectoryQuietly(receiver.snapshotDir(1, 1));
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        }
                    };

            receiver.stateMachine.pause();
            receiver.stateMachine.reinitialize();

            assertTrue(swapped.get(), "the seam really fired inside the restore");
            assertEquals(LifeCycle.State.RUNNING, receiver.stateMachine.getLifeCycleState());
            assertEquals(
                    2L,
                    receiver.stateMachine.getLastAppliedTermIndex().getIndex(),
                    "the retry restored the snapshot the finalize installed");
            assertEquals(2L, revisionOf(receiver.stateMachine).getRevision());
        } finally {
            receiver.close();
        }
    }

    @Test
    void testSnapshotRestoreAndReinitializeReplayOriginalInvocationResult() throws Exception {
        final File donorDb = new File(tempDir, "result-donor-rocksdb");
        final Member donor = new Member("result-donor", GROUP_A, donorDb);
        final LogEntryProto originalEntry;
        final byte[] originalReply;
        try {
            donor.open();
            originalEntry =
                    prepareEntry(
                            donor,
                            1L,
                            1L,
                            91L,
                            RaftCommand.put("/snapshot-replay", "v".getBytes(StandardCharsets.UTF_8), 0L, 1L));
            originalReply = applyEntry(donor, originalEntry);
            assertEquals(
                    RaftResult.Status.OK,
                    RaftCommandCodec.decodeResult(originalReply).getStatus());
            assertEquals(1L, donor.stateMachine.takeSnapshot());
        } finally {
            donor.close();
        }

        FileUtils.deleteDirectoryQuietly(donorDb);
        try (Member restored = new Member("result-donor", GROUP_A, donorDb)) {
            restored.open();
            assertEquals(1L, revisionOf(restored.stateMachine).getRevision());
            assertArrayEquals(
                    originalReply,
                    applyEntry(restored, originalEntry),
                    "startup restore carries the original result, cutoff and time index");
        }

        final File receiverDb = new File(tempDir, "result-receiver-rocksdb");
        try (Member receiver = new Member("result-receiver", GROUP_A, receiverDb)) {
            receiver.open();
            copyTree(donor.snapshotDir(1, 1), receiver.snapshotDir(1, 1));
            receiver.stateMachine.pause();
            receiver.stateMachine.reinitialize();

            assertArrayEquals(
                    originalReply,
                    applyEntry(receiver, originalEntry),
                    "reinitialize rebuilds the same invocation lookup path");
        }
    }

    @Test
    void testCheckpointCarriesLeasesReverseIndexAndCounters() throws Exception {
        final File dbDir = new File(tempDir, "rocksdb");
        final File snapshotDir;
        try (Member member = new Member("member", GROUP_A, dbDir, new ManualClock(10_000L))) {
            assertEquals(7L, populateLeasesReverseIndexAndCounters(member));
            snapshotDir = member.snapshotDir(1L, 7L);
        }
        assertLeasesReverseIndexAndCounters(dbDir); // the closed working database
        assertLeasesReverseIndexAndCounters(snapshotDir); // the published checkpoint
    }

    @Test
    void testStartupRestoreBehindSnapshotKeepsLeasesReverseIndexAndCounters() throws Exception {
        final File dbDir = new File(tempDir, "rocksdb");
        try (Member member = new Member("member", GROUP_A, dbDir, new ManualClock(10_000L))) {
            populateLeasesReverseIndexAndCounters(member);
        }
        // A lost working database forces restoreIfBehindSnapshot at startup.
        FileUtils.deleteDirectoryQuietly(dbDir);
        try (Member member = new Member("member", GROUP_A, dbDir, new ManualClock(10_000L))) {
            member.open();
            assertEquals(
                    7L,
                    revisionOf(member.stateMachine).getRevision(),
                    "the restored database keeps its applied-revision marker");
        }
        assertLeasesReverseIndexAndCounters(dbDir);
    }

    @Test
    void testTornWorkingDatabaseSelfHealKeepsLeasesReverseIndexAndCounters() throws Exception {
        final File dbDir = new File(tempDir, "rocksdb");
        try (Member member = new Member("member", GROUP_A, dbDir, new ManualClock(10_000L))) {
            populateLeasesReverseIndexAndCounters(member);
        }
        corruptManifest(dbDir);
        try (Member member = new Member("member", GROUP_A, dbDir, new ManualClock(10_000L))) {
            member.open();
            assertEquals(
                    7L,
                    revisionOf(member.stateMachine).getRevision(),
                    "the healed database carries the snapshot's applied-revision marker");
        }
        assertLeasesReverseIndexAndCounters(dbDir);
    }

    @Test
    void testReinitializeFromInstalledSnapshotKeepsLeasesReverseIndexAndCounters() throws Exception {
        // The receiver "receives" the snapshot the way ratis finalizes an install: the directory
        // appears below its own sm dir, then reinitialize() rebuilds the working database from it.
        final File donorDb = new File(tempDir, "donor-rocksdb");
        final Member donor = new Member("donor", GROUP_A, donorDb, new ManualClock(10_000L));
        try {
            populateLeasesReverseIndexAndCounters(donor);
        } finally {
            donor.close();
        }
        final File receiverDb = new File(tempDir, "receiver-rocksdb");
        final Member receiver = new Member("receiver", GROUP_A, receiverDb, new ManualClock(10_000L));
        try {
            receiver.open();
            copyTree(donor.snapshotDir(1L, 7L), receiver.snapshotDir(1L, 7L));
            receiver.stateMachine.pause();
            receiver.stateMachine.reinitialize();
            assertEquals(
                    7L,
                    receiver.stateMachine.getLastAppliedTermIndex().getIndex(),
                    "the watermark lifts to the installed snapshot's index");
        } finally {
            receiver.close();
        }
        assertLeasesReverseIndexAndCounters(receiverDb);
    }

    @Test
    void testReinitializeRethrowsWhenTheSnapshotStaysBroken() throws Exception {
        final File donorDb = new File(tempDir, "donor-rocksdb");
        final Member donor = new Member("donor", GROUP_A, donorDb);
        try {
            openAndSnapshotAtOne(donor);
        } finally {
            donor.close();
        }

        final File receiverDb = new File(tempDir, "receiver-rocksdb");
        final Member receiver = new Member("receiver", GROUP_A, receiverDb);
        try {
            receiver.open();
            copyTree(donor.snapshotDir(1, 1), receiver.snapshotDir(1, 1));
            // Every attempt loses its snapshot before the first copy - a finalize that never
            // installs anything newer.
            receiver.stateMachine.restoreBeforeFirstFile =
                    () -> FileUtils.deleteDirectoryQuietly(receiver.snapshotDir(1, 1));

            receiver.stateMachine.pause();
            final IOException failure =
                    assertThrows(IOException.class, () -> receiver.stateMachine.reinitialize());
            assertFalse(failure.getMessage().contains("Reinitializing without an installed snapshot"));
            assertTrue(failure.getMessage().contains(StateMachineIdentity.SNAPSHOT_FILE));
        } finally {
            receiver.close();
        }
    }

    private static void assumePosixOwnerPermissions() {
        assumeTrue(
                FileSystems.getDefault().supportedFileAttributeViews().contains("posix"),
                "the scan failure is produced by an owner-permission flip (POSIX only)");
        assumeFalse("root".equals(System.getProperty("user.name")), "root ignores permission bits");
    }

    /** Door 1: the snapshot dir can be traversed but not listed - the scan itself throws. */
    @Test
    void testUnscannableSnapshotDirRefusesStartup() throws Exception {
        assumePosixOwnerPermissions();
        final File dbDir = new File(tempDir, "rocksdb");
        final Member prototype = new Member("member", GROUP_A, dbDir);
        try {
            openAndSnapshotAtOne(prototype);
        } finally {
            prototype.close();
        }
        final File snapshotDir = prototype.snapshotDir(1, 1);
        final Set<PosixFilePermission> original =
                Files.getPosixFilePermissions(snapshotDir.toPath());
        // Execute without read: the dir stays discoverable and its manifest stays stat-able,
        // but Files.newDirectoryStream inside it throws.
        Files.setPosixFilePermissions(
                snapshotDir.toPath(), EnumSet.of(PosixFilePermission.OWNER_EXECUTE));
        try {
            try (Member member = new Member("member", GROUP_A, dbDir)) {
                final IOException failure = assertThrows(IOException.class, member::open);
                assertTrue(failure.getMessage().contains(prototype.stateMachineDir().getPath()));
            }

            // The refusal is about the scan, not the artifacts: the same pair opens once the
            // snapshot area is readable again.
            Files.setPosixFilePermissions(snapshotDir.toPath(), original);
            try (Member member = new Member("member", GROUP_A, dbDir)) {
                member.open();
                assertEquals(LifeCycle.State.RUNNING, member.stateMachine.getLifeCycleState());
                assertEquals(1L, revisionOf(member.stateMachine).getRevision());
            }
        } finally {
            Files.setPosixFilePermissions(snapshotDir.toPath(), original);
        }
    }

    /**
     * Door 2: no execute bit at all - File.isFile() cannot distinguish an absent manifest
     * from an unstat-able one, so the dir is invisible to the scan with no exception. The
     * verify pass must refuse, and cleanup must NOT have deleted what it could not verify.
     */
    @Test
    void testUnverifiableSnapshotDirRefusesStartup() throws Exception {
        assumePosixOwnerPermissions();
        final File dbDir = new File(tempDir, "rocksdb");
        final Member prototype = new Member("member", GROUP_A, dbDir);
        try {
            openAndSnapshotAtOne(prototype);
        } finally {
            prototype.close();
        }
        final File snapshotDir = prototype.snapshotDir(1, 1);
        final Set<PosixFilePermission> original =
                Files.getPosixFilePermissions(snapshotDir.toPath());
        Files.setPosixFilePermissions(
                snapshotDir.toPath(), EnumSet.noneOf(PosixFilePermission.class));
        try {
            try (Member member = new Member("member", GROUP_A, dbDir)) {
                final IOException failure = assertThrows(IOException.class, member::open);
                assertTrue(failure.getMessage().contains(prototype.stateMachineDir().getPath()));
                assertNotNull(failure.getCause(), "the refusal chains the verification failure");
                assertTrue(String.valueOf(failure.getCause()).contains(snapshotDir.getPath()));
            }
        } finally {
            Files.setPosixFilePermissions(snapshotDir.toPath(), original);
        }
        assertTrue(
                snapshotDir.isDirectory(), "cleanup did not delete a snapshot it could not verify");
        try (Member member = new Member("member", GROUP_A, dbDir)) {
            member.open();
            assertEquals(LifeCycle.State.RUNNING, member.stateMachine.getLifeCycleState());
            assertEquals(1L, revisionOf(member.stateMachine).getRevision());
        }
    }

    /** At runtime the same scan failure degrades to "no snapshot" - never kills a server. */
    @Test
    void testRuntimeScanFailureDegradesToNoSnapshot() throws Exception {
        assumePosixOwnerPermissions();
        final File dbDir = new File(tempDir, "rocksdb");
        try (Member member = new Member("member", GROUP_A, dbDir)) {
            member.open();
            final MetadataStateMachineStorage storage =
                    (MetadataStateMachineStorage) member.stateMachine.getStateMachineStorage();
            final File smDir = member.stateMachineDir();
            Files.createDirectories(smDir.toPath());
            final Set<PosixFilePermission> original =
                    Files.getPosixFilePermissions(smDir.toPath());
            Files.setPosixFilePermissions(
                    smDir.toPath(), EnumSet.of(PosixFilePermission.OWNER_EXECUTE));
            try {
                assertNull(storage.loadLatestSnapshot());
            } finally {
                Files.setPosixFilePermissions(smDir.toPath(), original);
            }
        }
    }
}
