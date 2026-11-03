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

import org.apache.ratis.metadata.MetadataCluster;
import org.apache.ratis.metadata.util.FileUtils;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;

import static org.apache.ratis.metadata.util.TestWait.waitUntil;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Snapshot contract: checkpoint content, install-snapshot catch-up, change-record GC. */
public class TestRaftMetadataSnapshot {

    private static final int NUM_SERVERS = 3;
    private static final String GROUP_UUID = "7d1e4a9b-2c5f-4b8e-a3d6-9f0c7e5b1a84";
    private static final Duration CATCH_UP_TIMEOUT = Duration.ofSeconds(120);
    private static final long POLL_BUDGET_BYTES = 4 * 1024 * 1024;

    private static final byte[] NO_DATA = new byte[0];

    @TempDir File tempDir;

    @Test
    void testSnapshotCheckpointContentPollRetentionAndContinuedWrites() throws Exception {
        try (MetadataCluster cluster =
                MetadataCluster.newBuilder(tempDir)
                        .setNumServers(NUM_SERVERS)
                        .setPurgeForSnapshot(true)
                        .setGroupUuid(UUID.fromString(GROUP_UUID))
                        .build()) {
            cluster.start();
            final int leaderIndex = cluster.indexOf(cluster.awaitLeader());

            assertEquals(
                    RaftResult.Status.OK,
                    send(cluster, RaftCommand.put("/snap", NO_DATA, 0, 100L)).getStatus());
            assertEquals(
                    RaftResult.Status.OK,
                    send(cluster, RaftCommand.put("/snap/root", NO_DATA, 0, 100L)).getStatus());
            assertEquals(
                    RaftResult.Status.OK,
                    send(cluster, RaftCommand.leaseRegister(42L, 60_000L, 100_000L)).getStatus());
            assertEquals(
                    RaftResult.Status.OK,
                    send(cluster,
                            RaftCommand.put(
                                    "/snap/root/ephemeral", bytes("lease-bound"), 42L, 100L))
                            .getStatus());
            assertEquals(
                    RaftResult.Status.OK,
                    send(cluster,
                            RaftCommand.multi(
                                    Arrays.asList(
                                            RaftCommand.put(
                                                    "/snap/root/multi-a", bytes("a"), 0, 100L),
                                            RaftCommand.put(
                                                    "/snap/root/multi-b", bytes("b"), 0, 100L))))
                            .getStatus());
            final long preSnapshotRevision = committedRevision(cluster);
            awaitAllReplicasAt(cluster, preSnapshotRevision);

            final long snapshotIndex = cluster.takeSnapshotOnLeader();
            assertTrue(
                    snapshotIndex >= preSnapshotRevision,
                    "the snapshot covers everything applied so far");

            assertCheckpointContent(
                    findSnapshotDirectory(raftDirOf(tempDir, leaderIndex), snapshotIndex),
                    snapshotIndex);

            // NEED_SNAPSHOT is sinceRevision + 1 < line; snapshotIndex is the first live poll.
            final RaftResult collected =
                    sendReadOnly(cluster, RaftCommand.pollChanges(0, POLL_BUDGET_BYTES));
            assertEquals(RaftResult.Status.NEED_SNAPSHOT, collected.getStatus());
            assertEquals(
                    snapshotIndex + 1,
                    collected.getOldestRetainedRevision(),
                    "the retention line advances past the snapshot point");
            assertEquals(
                    RaftResult.Status.NEED_SNAPSHOT,
                    sendReadOnly(
                                    cluster,
                                    RaftCommand.pollChanges(snapshotIndex - 1, POLL_BUDGET_BYTES))
                            .getStatus());

            assertEquals(
                    RaftResult.Status.OK,
                    send(cluster,
                            RaftCommand.put("/snap/root/after-snapshot", bytes("later"), 0, 200L))
                            .getStatus());
            assertArrayEquals(
                    bytes("later"),
                    sendReadOnly(cluster, RaftCommand.get("/snap/root/after-snapshot")).getData());
            final long postWriteRevision = committedRevision(cluster);
            awaitAllReplicasAt(cluster, postWriteRevision);

            final RaftResult livePoll =
                    sendReadOnly(cluster, RaftCommand.pollChanges(snapshotIndex, POLL_BUDGET_BYTES));
            assertEquals(RaftResult.Status.OK, livePoll.getStatus());
            assertEquals(snapshotIndex + 1, livePoll.getOldestRetainedRevision());
            assertEquals(
                    1,
                    livePoll.getRecords().size(),
                    "a poll from the snapshot point returns exactly the post-snapshot write");
            final ChangeEvent record = livePoll.getRecords().get(0);
            // Two-sided bound: a ratis configuration entry may sit between snapshot and write.
            assertTrue(record.getRevision() > snapshotIndex);
            assertTrue(record.getRevision() <= postWriteRevision);
            assertEquals(1, record.getChanges().size());
            assertEquals("/snap/root/after-snapshot", record.getChanges().get(0).getPath());
            assertArrayEquals(bytes("later"), record.getChanges().get(0).getData());
        }
    }

    @Test
    void testWipedFollowerCatchesUpViaInstallSnapshot() throws Exception {
        try (MetadataCluster cluster =
                MetadataCluster.newBuilder(tempDir)
                        .setNumServers(NUM_SERVERS)
                        .setPurgeForSnapshot(true)
                        .setGroupUuid(UUID.fromString(GROUP_UUID))
                        .build()) {
            cluster.start();
            final RaftPeerId leaderId = cluster.awaitLeader();
            final int leaderIndex = cluster.indexOf(leaderId);
            final int followerIndex = (leaderIndex + 1) % NUM_SERVERS;

            send(cluster, RaftCommand.put("/catchup", NO_DATA, 0, 100L));
            for (int i = 1; i <= 3; i++) {
                send(cluster, RaftCommand.put("/catchup/a" + i, bytes("v" + i), 0, 100L));
            }
            send(cluster, RaftCommand.leaseRegister(7L, 60_000L, 200_000L));
            send(cluster, RaftCommand.put("/catchup/ephemeral", bytes("ephemeral"), 7L, 100L));
            send(cluster,
                    RaftCommand.multi(
                            Arrays.asList(
                                    RaftCommand.put("/catchup/m1", bytes("m1"), 0, 100L),
                                    RaftCommand.put("/catchup/m2", bytes("m2"), 0, 100L))));
            final long phase1Revision = committedRevision(cluster);
            awaitAllReplicasAt(cluster, phase1Revision);

            cluster.killServer(followerIndex);
            // The leader's committed revision bounds the follower's durable end (every log entry
            // is one of the writes above); past bound + 1, no replay.
            final long followerLogEndBound = committedRevision(cluster);
            for (int i = 1; i <= 3; i++) {
                send(cluster, RaftCommand.put("/catchup/b" + i, bytes("w" + i), 0, 300L));
            }
            send(cluster, RaftCommand.put("/catchup/purge-push", NO_DATA, 0, 300L));
            long latestSnapshotIndex = cluster.takeSnapshotOnLeader();
            for (int purgePush = 0;
                    logStartOnDisk(raftDirOf(tempDir, leaderIndex)) <= followerLogEndBound + 1;
                    purgePush++) {
                assertTrue(
                        purgePush < 100,
                        "the leader's log start crosses the follower's captured-end bound");
                send(cluster,
                        RaftCommand.put("/catchup/purge-push/" + purgePush, bytes("push"), 0, 300L));
                latestSnapshotIndex = cluster.takeSnapshotOnLeader();
            }
            // No write follows the loop, so the installed snapshot sits at the last taken index.
            final long snapshotIndex = latestSnapshotIndex;
            // Captured pre-restart: catch-up wait runs without client traffic while leaderless.
            final long targetRevision = committedRevision(cluster);

            // Turn leadership over: a ratis appender retries past the purge; new leaders drop it.
            final int otherFollowerIndex = (leaderIndex + 2) % NUM_SERVERS;
            cluster.killServer(otherFollowerIndex);
            cluster.killServer(leaderIndex);
            cluster.restartServer(leaderIndex);

            // Wipe the follower's RocksDB working directory only: its consensus log stays.
            FileUtils.deleteDirectoryQuietly(metaDirOf(tempDir, followerIndex));

            final MetadataStateMachine leaderMachine = cluster.stateMachine(leaderIndex);
            cluster.restartServer(followerIndex);
            final MetadataStateMachine followerMachine = cluster.stateMachine(followerIndex);

            // A failed probe means "not yet": the install transiently closes the RocksDB.
            waitUntil(
                    () -> {
                        try {
                            return MetadataCluster.directQuery(
                                            followerMachine, RaftCommand.revision())
                                            .getRevision()
                                    >= targetRevision;
                        } catch (RuntimeException notOpenYet) {
                            return false;
                        }
                    },
                    CATCH_UP_TIMEOUT,
                    "The wiped follower did not catch up to revision " + targetRevision);

            assertEquals(
                    listAsMap(followerMachine, "/catchup/"),
                    listAsMap(leaderMachine, "/catchup/"),
                    "follower state equals leader state");
            final RaftResult followerEphemeral =
                    MetadataCluster.directQuery(
                            followerMachine, RaftCommand.get("/catchup/ephemeral"));
            final RaftResult leaderEphemeral =
                    MetadataCluster.directQuery(
                            leaderMachine, RaftCommand.get("/catchup/ephemeral"));
            assertEquals(leaderEphemeral.getLeaseId(), followerEphemeral.getLeaseId());
            assertEquals(7L, followerEphemeral.getLeaseId());
            assertArrayEquals(bytes("ephemeral"), followerEphemeral.getData());

            // Only install creates this: snapshots need a leader's updater; auto trigger is off.
            try {
                waitUntil(
                        () -> {
                            try {
                                return findSnapshotDirectory(
                                                raftDirOf(tempDir, followerIndex), snapshotIndex)
                                        .isDirectory();
                            } catch (AssertionError notInstalledYet) {
                                return false;
                            }
                        },
                        CATCH_UP_TIMEOUT,
                        "The installed snapshot directory did not appear at index "
                                + snapshotIndex);
            } catch (AssertionError timeout) {
                throw new AssertionError(
                        "The installed snapshot directory did not appear at index "
                                + snapshotIndex
                                + "; leaderStart="
                                + logStartOnDisk(raftDirOf(tempDir, leaderIndex))
                                + ", followerLogEndBound="
                                + followerLogEndBound
                                + ", followerSm="
                                + Arrays.toString(
                                        new File(
                                                        new File(
                                                                raftDirOf(tempDir, followerIndex),
                                                                GROUP_UUID),
                                                        "sm")
                                                .list())
                                + ", leaderSm="
                                + Arrays.toString(
                                        new File(
                                                        new File(
                                                                raftDirOf(tempDir, leaderIndex),
                                                                GROUP_UUID),
                                                        "sm")
                                                .list()),
                        timeout);
            }

            send(cluster, RaftCommand.put("/catchup/after-catchup", bytes("ok"), 0, 400L));
            final long afterCatchupRevision = committedRevision(cluster);
            waitUntil(
                    () ->
                            MetadataCluster.directQuery(followerMachine, RaftCommand.revision())
                                            .getRevision()
                                    >= afterCatchupRevision,
                    CATCH_UP_TIMEOUT,
                    "The caught-up follower did not apply revision " + afterCatchupRevision);
            assertArrayEquals(
                    bytes("ok"),
                    MetadataCluster.directQuery(
                                    followerMachine, RaftCommand.get("/catchup/after-catchup"))
                            .getData());

            // Full restart from latest snapshot plus replay above - the production startup path.
            for (int i = 0; i < NUM_SERVERS; i++) {
                cluster.killServer(i);
            }
            for (int i = 0; i < NUM_SERVERS; i++) {
                cluster.restartServer(i);
            }
            cluster.awaitLeader();

            assertArrayEquals(
                    bytes("ok"),
                    sendReadOnly(cluster, RaftCommand.get("/catchup/after-catchup")).getData());
            assertTrue(committedRevision(cluster) >= afterCatchupRevision);
            // Query the line on the snapshot-taking replica: a restored checkpoint lags it by one
            // generation (checkpoints publish before the GC advances it); election winner varies.
            assertEquals(
                    RaftResult.Status.NEED_SNAPSHOT,
                    MetadataCluster.directQuery(
                                    cluster.stateMachine(leaderIndex),
                                    RaftCommand.pollChanges(snapshotIndex - 1, POLL_BUDGET_BYTES))
                            .getStatus(),
                    "the retention line survives the restart");
        }
    }

    private static RaftResult send(MetadataCluster cluster, RaftCommand command)
            throws IOException {
        return decodeReply(cluster.adminClient().io().send(toMessage(command)), command);
    }

    private static RaftResult sendReadOnly(MetadataCluster cluster, RaftCommand command)
            throws IOException {
        return decodeReply(
                cluster.adminClient().io().sendReadOnly(toMessage(command)), command);
    }

    /** The linearizable last-applied revision, observed through the leader. */
    private static long committedRevision(MetadataCluster cluster) throws IOException {
        return sendReadOnly(cluster, RaftCommand.revision()).getRevision();
    }

    private static void awaitAllReplicasAt(MetadataCluster cluster, long revision) {
        waitUntil(
                () -> {
                    for (int i = 0; i < NUM_SERVERS; i++) {
                        final MetadataStateMachine stateMachine = cluster.stateMachine(i);
                        if (stateMachine != null
                                && MetadataCluster.directQuery(
                                                stateMachine, RaftCommand.revision())
                                                .getRevision()
                                        < revision) {
                            return false;
                        }
                    }
                    return true;
                },
                CATCH_UP_TIMEOUT,
                "State machines did not reach revision " + revision);
    }

    /** Member {@code i}'s raft storage directory (the harness data dir is {@code node-n<i>}). */
    private static File raftDirOf(File tempDir, int i) {
        return new File(new File(tempDir, "node-n" + i), "raft");
    }

    /** Member {@code i}'s RocksDB working directory, below the same harness data dir. */
    private static File metaDirOf(File tempDir, int i) {
        return new File(new File(tempDir, "node-n" + i), "rocksdb");
    }

    /**
     * The leader's log start, witnessed through its on-disk segment files: the purge deletes
     * whole closed segments, so the smallest segment start below {@code current/} is the first
     * index the log still holds - the quantity {@code RaftLog#getStartIndex()} reports, which
     * the cluster harness does not expose. Closed segments are named {@code log_<start>-<end>},
     * the open one {@code log_inprogress_<start>}.
     */
    private static long logStartOnDisk(File raftStorageDir) {
        final File currentDir = new File(new File(raftStorageDir, GROUP_UUID), "current");
        final File[] segments = currentDir.listFiles();
        assertNotNull(segments, "log segment directory " + currentDir + " exists");
        long start = Long.MAX_VALUE;
        for (File segment : segments) {
            final String name = segment.getName();
            if (!name.startsWith("log_")) {
                continue;
            }
            final String digits =
                    name.startsWith("log_inprogress_")
                            ? name.substring("log_inprogress_".length())
                            : name.substring("log_".length());
            int end = 0;
            while (end < digits.length() && Character.isDigit(digits.charAt(end))) {
                end++;
            }
            if (end > 0) {
                start = Math.min(start, Long.parseLong(digits.substring(0, end)));
            }
        }
        // No segment left means the log holds nothing: its start is beyond every purged index.
        return start;
    }

    private static void assertCheckpointContent(File snapshotDir, long snapshotIndex)
            throws IOException {
        final Options options = new Options();
        final List<AutoCloseable> toClose = new ArrayList<>();
        toClose.add(options);
        try {
            final List<byte[]> columnFamilyNames =
                    RocksDB.listColumnFamilies(options, snapshotDir.getAbsolutePath());
            final List<String> columnFamilies = new ArrayList<>();
            for (byte[] name : columnFamilyNames) {
                columnFamilies.add(new String(name, StandardCharsets.UTF_8));
            }
            assertTrue(
                    columnFamilies.containsAll(
                            Arrays.asList("state", "counters", "leases", "lease-paths", "changes")),
                    "the checkpoint contains every column family");

            final List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
            for (byte[] name : columnFamilyNames) {
                final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
                toClose.add(cfOptions);
                descriptors.add(new ColumnFamilyDescriptor(name, cfOptions));
            }
            final List<ColumnFamilyHandle> handles = new ArrayList<>();
            final RocksDB checkpoint =
                    RocksDB.openReadOnly(snapshotDir.getAbsolutePath(), descriptors, handles);
            toClose.add(checkpoint);
            try {
                final ColumnFamilyHandle state = handleOf(handles, columnFamilies, "state");
                final ColumnFamilyHandle changes = handleOf(handles, columnFamilies, "changes");
                final ColumnFamilyHandle defaults = handleOf(handles, columnFamilies, "default");

                assertNotNull(
                        checkpoint.get(
                                state,
                                "/snap/root/multi-a".getBytes(StandardCharsets.UTF_8)),
                        "the checkpoint holds the flushed documents");

                // The changes family key is the 8-byte big-endian revision of the record.
                final RocksIterator iterator = checkpoint.newIterator(changes);
                try {
                    iterator.seekToFirst();
                    assertTrue(
                            iterator.isValid(), "the checkpoint holds the change records");
                    assertTrue(
                            decodeLong(iterator.key()) <= snapshotIndex,
                            "no change record above the snapshot index is in the checkpoint");
                } finally {
                    iterator.close();
                }

                // The marker equals the snapshot index: takeSnapshot runs after every apply.
                final byte[] marker =
                        checkpoint.get(
                                defaults, "applied-revision".getBytes(StandardCharsets.UTF_8));
                assertNotNull(
                        marker,
                        "the applied-revision marker rides in the default column family");
                assertEquals(
                        snapshotIndex,
                        decodeLong(marker),
                        "the marker describes exactly the snapshot prefix");
            } finally {
                for (ColumnFamilyHandle handle : handles) {
                    handle.close();
                }
            }
        } catch (RocksDBException e) {
            throw new IOException("Failed to inspect snapshot " + snapshotDir, e);
        } finally {
            for (int i = toClose.size() - 1; i >= 0; i--) {
                quietClose(toClose.get(i));
            }
        }
    }

    private static ColumnFamilyHandle handleOf(
            List<ColumnFamilyHandle> handles, List<String> names, String name) {
        final int index = names.indexOf(name);
        assertTrue(index >= 0, "column family " + name + " is present");
        return handles.get(index);
    }

    private static Map<String, Long> listAsMap(MetadataStateMachine stateMachine, String prefix) {
        final RaftResult list =
                MetadataCluster.directQuery(stateMachine, RaftCommand.list(prefix, 1000));
        assertEquals(RaftResult.Status.OK, list.getStatus());
        final Map<String, Long> paths = new LinkedHashMap<>();
        for (RaftResult.Entry entry : list.getEntries()) {
            paths.put(entry.getPath(), entry.getVersion());
        }
        return paths;
    }

    private static RaftResult decodeReply(RaftClientReply reply, RaftCommand command) {
        final RaftResult result =
                RaftCommandCodec.decodeResult(reply.getMessage().getContent().toByteArray());
        assertNotNull(
                result, "reply of " + command.getOp() + " decodes (op echo must match)");
        assertEquals(command.getOp(), result.getOp());
        return result;
    }

    private static Message toMessage(RaftCommand command) {
        return Message.valueOf(ByteBuffer.wrap(RaftCommandCodec.encodeCommand(command)));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /** The change-record key encoding: an 8-byte big-endian revision. */
    private static long decodeLong(byte[] bytes) {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value = (value << 8) | (bytes[i] & 0xFFL);
        }
        return value;
    }

    private static void quietClose(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception ignored) {
        }
    }

    private static File findSnapshotDirectory(File raftStorageDir, long snapshotIndex) {
        final File smDir = new File(new File(raftStorageDir, GROUP_UUID), "sm");
        final File[] candidates = smDir.listFiles();
        assertNotNull(candidates, "sm directory of " + smDir + " exists");
        for (File candidate : candidates) {
            final Matcher matcher =
                    MetadataStateMachineStorage.SNAPSHOT_DIR_REGEX.matcher(candidate.getName());
            if (candidate.isDirectory()
                    && matcher.matches()
                    && Long.parseLong(matcher.group(2)) == snapshotIndex) {
                return candidate;
            }
        }
        throw new AssertionError(
                "No snapshot directory at index " + snapshotIndex + " below " + smDir);
    }
}
