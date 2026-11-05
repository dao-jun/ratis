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

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.metadata.MetadataCluster;
import org.apache.ratis.metadata.util.FileUtils;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
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
            assertEquals(
                    RaftResult.Status.OK,
                    send(cluster, RaftCommand.increment("/snap/counter", 5L)).getStatus());
            assertEquals(
                    RaftResult.Status.OK,
                    send(cluster, RaftCommand.create("/snap/root/seq-", bytes("s"), true, 0L, 100L))
                            .getStatus());
            final long preSnapshotRevision = committedRevision(cluster);
            awaitAllReplicasAt(cluster, preSnapshotRevision);
            final List<RaftResult.Lease> leasesAtSnapshotPoint =
                    MetadataCluster.directQuery(
                                    cluster.stateMachine(leaderIndex), RaftCommand.leaseList())
                            .getLeases();
            assertEquals(1, leasesAtSnapshotPoint.size());

            final long snapshotIndex = cluster.takeSnapshotOnLeader();
            assertTrue(
                    snapshotIndex >= preSnapshotRevision,
                    "the snapshot covers everything applied so far");

            assertCheckpointContent(
                    findSnapshotDirectory(raftDirOf(tempDir, leaderIndex), snapshotIndex),
                    snapshotIndex,
                    leasesAtSnapshotPoint);

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
            assertEquals(
                    RaftResult.Status.OK,
                    send(cluster, RaftCommand.increment("/catchup/counter", 5L)).getStatus());
            assertEquals(
                    RaftResult.Status.OK,
                    send(cluster, RaftCommand.create("/catchup/seq-", bytes("s"), true, 0L, 100L))
                            .getStatus());
            final long phase1Revision = committedRevision(cluster);
            awaitAllReplicasAt(cluster, phase1Revision);
            final List<RaftResult.Lease> leasesAtFollowerKill =
                    MetadataCluster.directQuery(
                                    cluster.stateMachine(leaderIndex), RaftCommand.leaseList())
                            .getLeases();
            assertEquals(1, leasesAtFollowerKill.size());

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
            assertEquals(
                    leaseTableOf(leasesAtFollowerKill),
                    leaseTableOf(
                            MetadataCluster.directQuery(followerMachine, RaftCommand.leaseList())
                                    .getLeases()),
                    "the follower's own leases CF carries the exact leader table (all four fields)");

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

            // The installed artifact itself carries the exact transferred three-family content.
            assertCatchupThreeFamilies(
                    findSnapshotDirectory(raftDirOf(tempDir, followerIndex), snapshotIndex),
                    leaseTableOf(leasesAtFollowerKill));

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

            // The follower's working database, built by the real install -> reinitialize ->
            // live-apply -> restart path. killServer closes each server (and its RocksDB),
            // so the dump sees a cleanly closed database; no restart follows.
            for (int i = 0; i < NUM_SERVERS; i++) {
                cluster.killServer(i);
            }
            assertCatchupThreeFamilies(metaDirOf(tempDir, followerIndex), leaseTableOf(leasesAtFollowerKill));
        }
    }

    @Test
    void testWithinContractRetrySurvivesSnapshotPurgeRestartAndCacheExpiry() throws Exception {
        // Within the retry contract (before retention expiry): a resubmission of the same
        // invocation returns the original encoded reply with no second business effect, and the
        // guarantee survives snapshot, log purge, a full controlled restart and the memory retry
        // cache being expired/invalidated.
        final File base = new File(tempDir, "within-contract-retry");
        try (MetadataCluster cluster =
                MetadataCluster.newBuilder(base)
                        .setNumServers(NUM_SERVERS)
                        .setPurgeForSnapshot(true)
                        .setGroupUuid(UUID.fromString(GROUP_UUID))
                        .setResultRetention(Duration.ofMinutes(1))
                        .setRetryCacheExpiry(Duration.ofMillis(300))
                        .build()) {
            cluster.start();
            RaftPeerId leader = cluster.awaitLeader();
            final int leaderIndex = cluster.indexOf(leader);
            // Warm the leader up: an ordered admin write retries internally until the leader is
            // ready, keeping the raw unordered stream below safe from a LeaderNotReady reply.
            assertEquals(
                    RaftResult.Status.OK,
                    send(cluster, RaftCommand.put("/within-contract-warmup", bytes("w"), 0, 100L))
                            .getStatus());
            final ClientId clientId = ClientId.valueOf(UUID.randomUUID());
            final RaftCommand command = RaftCommand.put("/within-contract", bytes("v1"), 0, 100L);
            final RaftClientRequest request =
                    rawWriteRequest(cluster, leader, clientId, 42L, command);

            final RaftClientReply first = sendRaw(cluster, request);
            final byte[] original = first.getMessage().getContent().toByteArray();
            assertEquals(RaftResult.Status.OK, decodeResult(original).getStatus());
            assertEquals(1L, decodeResult(original).getVersion());
            final long firstIndex = first.getLogIndex();

            // While the memory cache still holds the reply, a retry is served without appending.
            final RaftClientReply cached = sendRaw(cluster, request);
            assertEquals(firstIndex, cached.getLogIndex());
            assertArrayEquals(original, cached.getMessage().getContent().toByteArray());

            Thread.sleep(800L); // memory retry-cache expiry

            final RaftClientReply retry = sendRaw(cluster, request);
            assertTrue(
                    retry.getLogIndex() > firstIndex,
                    "the expired-cache retry appended a duplicate-refresh entry");
            assertArrayEquals(
                    original,
                    retry.getMessage().getContent().toByteArray(),
                    "the durable store returned the original encoded reply");
            assertEquals(
                    1L,
                    sendReadOnly(cluster, RaftCommand.get("/within-contract")).getVersion(),
                    "no second business effect");

            // Snapshot and purge until the leader log no longer holds either duplicate entry.
            long snapshotIndex = cluster.takeSnapshotOnLeader();
            for (int push = 0;
                    logStartOnDisk(raftDirOf(base, leaderIndex)) <= retry.getLogIndex();
                    push++) {
                assertTrue(push < 100, "the purge crossed the duplicate entries");
                send(
                        cluster,
                        RaftCommand.put(
                                "/within-contract/purge-push-" + push, bytes("p"), 0, 100L));
                snapshotIndex = cluster.takeSnapshotOnLeader();
            }
            assertTrue(snapshotIndex >= retry.getLogIndex());

            // Full controlled restart invalidates every memory retry cache, but the new leader's
            // startup replay re-seeds its own cache; waiting out the expiry proves the record
            // itself - snapshotted and purged with the log - still answers with the original
            // reply.
            for (int i = 0; i < NUM_SERVERS; i++) {
                cluster.killServer(i);
            }
            for (int i = 0; i < NUM_SERVERS; i++) {
                cluster.restartServer(i);
            }
            leader = cluster.awaitLeader();
            Thread.sleep(800L);
            assertEquals(
                    RaftResult.Status.OK,
                    send(cluster,
                            RaftCommand.put(
                                    "/within-contract-warmup-after-restart", bytes("w"), 0, 100L))
                            .getStatus(),
                    "warm the restarted leader up before the next raw send");
            final RaftClientReply afterRestart =
                    sendRaw(cluster, rawWriteRequest(cluster, leader, clientId, 42L, command));
            assertTrue(afterRestart.getLogIndex() > retry.getLogIndex());
            assertArrayEquals(
                    original,
                    afterRestart.getMessage().getContent().toByteArray(),
                    "the original encoded reply survives restart, snapshot and purge");
            final RaftResult state = sendReadOnly(cluster, RaftCommand.get("/within-contract"));
            assertEquals(1L, state.getVersion(), "still exactly one business effect");
            assertArrayEquals(bytes("v1"), state.getData());
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

    private static void assertCheckpointContent(
            File snapshotDir, long snapshotIndex, List<RaftResult.Lease> leasesAtSnapshotPoint)
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
                            Arrays.asList(
                                    "state",
                                    "counters",
                                    "leases",
                                    "lease-paths",
                                    "changes",
                                    "command-results")),
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
                final ColumnFamilyHandle state =
                        MetadataTableDumpAsserts.handleOf(handles, columnFamilies, "state");
                final ColumnFamilyHandle changes =
                        MetadataTableDumpAsserts.handleOf(handles, columnFamilies, "changes");
                final ColumnFamilyHandle defaults =
                        MetadataTableDumpAsserts.handleOf(handles, columnFamilies, "default");
                final ColumnFamilyHandle results =
                        MetadataTableDumpAsserts.handleOf(
                                handles, columnFamilies, "command-results");

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

                boolean invocationRecord = false;
                boolean timeIndex = false;
                try (RocksIterator resultIterator = checkpoint.newIterator(results)) {
                    for (resultIterator.seekToFirst();
                            resultIterator.isValid();
                            resultIterator.next()) {
                        invocationRecord |= resultIterator.key()[0] == 1;
                        timeIndex |= resultIterator.key()[0] == 2;
                    }
                }
                assertNotNull(
                        checkpoint.get(results, new byte[] {0}),
                        "the checkpoint holds the replicated result cutoff");
                assertTrue(invocationRecord, "the checkpoint holds original invocation results");
                assertTrue(timeIndex, "the checkpoint holds invocation retention indices");

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

                MetadataTableDumpAsserts.assertLeaseIndexAndCounterContent(
                        checkpoint,
                        MetadataTableDumpAsserts.handleOf(handles, columnFamilies, "leases"),
                        MetadataTableDumpAsserts.handleOf(handles, columnFamilies, "lease-paths"),
                        MetadataTableDumpAsserts.handleOf(handles, columnFamilies, "counters"),
                        leaseTableOf(leasesAtSnapshotPoint),
                        Collections.singleton("42:/snap/root/ephemeral"),
                        expectedCounters("user:/snap/counter", 5L, "sequential:/snap/root/seq-", 1L));
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

    /** The leases CF rows expected from a captured LEASE_LIST, formatted ttl/expireAt/renew. */
    private static Map<Long, String> leaseTableOf(List<RaftResult.Lease> captured) {
        final Map<Long, String> expected = new TreeMap<>();
        for (RaftResult.Lease lease : captured) {
            expected.put(
                    lease.getLeaseId(),
                    lease.getTtlMs() + "/" + lease.getExpireAtMs() + "/" + lease.getLastRenewIndex());
        }
        return expected;
    }

    private static Map<String, Long> expectedCounters(
            String userName, long userValue, String seqName, long seqValue) {
        final Map<String, Long> expected = new TreeMap<>();
        expected.put(userName, userValue);
        expected.put(seqName, seqValue);
        return expected;
    }

    /**
     * Opens a closed snapshot or working directory read-only (the assertCheckpointContent
     * skeleton) and asserts the /catchup scenario's exact three-family content.
     */
    private static void assertCatchupThreeFamilies(File dir, Map<Long, String> expectedLeases)
            throws IOException {
        final Options options = new Options();
        final List<AutoCloseable> toClose = new ArrayList<>();
        toClose.add(options);
        try {
            final List<byte[]> columnFamilyNames =
                    RocksDB.listColumnFamilies(options, dir.getAbsolutePath());
            final List<String> columnFamilies = new ArrayList<>();
            for (byte[] name : columnFamilyNames) {
                columnFamilies.add(new String(name, StandardCharsets.UTF_8));
            }
            final List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
            for (byte[] name : columnFamilyNames) {
                final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
                toClose.add(cfOptions);
                descriptors.add(new ColumnFamilyDescriptor(name, cfOptions));
            }
            final List<ColumnFamilyHandle> handles = new ArrayList<>();
            final RocksDB db = RocksDB.openReadOnly(dir.getAbsolutePath(), descriptors, handles);
            toClose.add(db);
            try {
                MetadataTableDumpAsserts.assertLeaseIndexAndCounterContent(
                        db,
                        MetadataTableDumpAsserts.handleOf(handles, columnFamilies, "leases"),
                        MetadataTableDumpAsserts.handleOf(handles, columnFamilies, "lease-paths"),
                        MetadataTableDumpAsserts.handleOf(handles, columnFamilies, "counters"),
                        expectedLeases,
                        Collections.singleton("7:/catchup/ephemeral"),
                        expectedCounters("user:/catchup/counter", 5L, "sequential:/catchup/seq-", 1L));
            } finally {
                for (ColumnFamilyHandle handle : handles) {
                    handle.close();
                }
            }
        } catch (RocksDBException e) {
            throw new IOException("Failed to inspect the metadata tables of " + dir, e);
        } finally {
            for (int i = toClose.size() - 1; i >= 0; i--) {
                quietClose(toClose.get(i));
            }
        }
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

    /** A fixed write request: resending it re-uses one ClientInvocationId across attempts. */
    private static RaftClientRequest rawWriteRequest(
            MetadataCluster cluster,
            RaftPeerId server,
            ClientId clientId,
            long callId,
            RaftCommand command) {
        return RaftClientRequest.newBuilder()
                .setClientId(clientId)
                .setServerId(server)
                .setGroupId(cluster.groupId())
                .setCallId(callId)
                .setMessage(
                        Message.valueOf(
                                ByteBuffer.wrap(RaftCommandCodec.encodeCommand(command))))
                .setType(RaftClientRequest.writeRequestType())
                .build();
    }

    /**
     * Sends a fixed raw write request on a fresh admin client and asserts it succeeded. Callers
     * must warm the addressed leader up with an ordered admin write first: a raw unordered send
     * has no client-side retry, and a LeaderNotReady reply closes its unordered stream.
     */
    private static RaftClientReply sendRaw(MetadataCluster cluster, RaftClientRequest request)
            throws Exception {
        try (RaftClient client = cluster.newAdminClient()) {
            final CompletableFuture<RaftClientReply> future =
                    client.getClientRpc().sendRequestAsyncUnordered(request);
            final RaftClientReply reply = future.get(30, TimeUnit.SECONDS);
            assertNotNull(reply, "the raw write returned no reply");
            assertTrue(reply.isSuccess(), () -> "the raw write failed: " + reply);
            return reply;
        }
    }

    private static RaftResult decodeResult(byte[] encoded) {
        final RaftResult result = RaftCommandCodec.decodeResult(encoded);
        assertNotNull(result, "the raw reply decodes");
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
        return MetadataTableDumpAsserts.decodeLong(bytes, 0);
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
