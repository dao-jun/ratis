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
package org.apache.ratis.metadata.lease;

import org.apache.ratis.metadata.MetadataCluster;
import org.apache.ratis.metadata.statemachine.ChangeEvent;
import org.apache.ratis.metadata.statemachine.MetadataStateMachine;
import org.apache.ratis.metadata.statemachine.RaftCommand;
import org.apache.ratis.metadata.statemachine.RaftCommandCodec;
import org.apache.ratis.metadata.statemachine.RaftResult;

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
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.apache.ratis.metadata.util.TestWait.waitUntil;

/**
 * End-to-end test of the leader-driven lease expiry scheduler on the shared {@link
 * MetadataCluster} harness: every member runs its built-in scheduler on the cluster's {@link
 * org.apache.ratis.metadata.util.ManualClock}, so the suites assert through state-machine
 * effects (lease listings, record states, the change stream) instead of scheduler counters.
 */
public class TestRaftMetadataLease {

    private static final int NUM_SERVERS = 3;
    private static final String GROUP_UUID = "b5e2f7a8-1c4d-4e9b-8a3f-6d0c7e2b9f15";
    private static final Duration OPERATION_TIMEOUT = Duration.ofSeconds(30);
    private static final long POLL_BUDGET_BYTES = 4 * 1024 * 1024;
    private static final long LEADER_ELECTION_CALL_TIMEOUT_MS = 60_000L;

    private static final Duration SCAN_INTERVAL = Duration.ofMillis(50);

    /**
     * Wall time the "nothing proposed" invariants must hold through: six scan intervals, so the
     * leader's scheduler certainly completed several scans against the advanced clock. This
     * replaces the source suite's per-scheduler scan counters, which the harness does not
     * expose (the members own their schedulers internally).
     */
    private static final Duration STEADY_SCAN_WINDOW = SCAN_INTERVAL.multipliedBy(6);

    @TempDir File tempDir;

    private MetadataCluster newCluster() {
        return MetadataCluster.newBuilder(tempDir)
                .setNumServers(NUM_SERVERS)
                .setGroupUuid(UUID.fromString(GROUP_UUID))
                .setLeaseScanInterval(SCAN_INTERVAL)
                .build();
    }

    @Test
    void testSessionLifecycleRenewalAndExpiryCascade() throws Exception {
        try (MetadataCluster cluster = newCluster()) {
            cluster.start();
            final long leaseId = 101L;
            final long ttlMs = TimeUnit.SECONDS.toMillis(30);

            final long registeredAt = cluster.clock.milliseconds();
            assertEquals(
                    RaftResult.Status.OK,
                    send(cluster, RaftCommand.put("/lease101", bytes(""), 0L, registeredAt))
                            .getStatus());
            assertEquals(
                    RaftResult.Status.OK,
                    send(cluster, RaftCommand.leaseRegister(leaseId, ttlMs, registeredAt + ttlMs))
                            .getStatus());
            assertEquals(
                    RaftResult.Status.OK,
                    send(
                            cluster,
                            RaftCommand.put(
                                    "/lease101/a", bytes("a"), leaseId, registeredAt))
                            .getStatus());
            assertEquals(
                    RaftResult.Status.OK,
                    send(
                            cluster,
                            RaftCommand.put(
                                    "/lease101/b", bytes("b"), leaseId, registeredAt))
                            .getStatus());
            assertEquals(
                    RaftResult.Status.OK,
                    send(
                            cluster,
                            RaftCommand.put(
                                    "/lease101/keep", bytes("keep"), 0L, registeredAt))
                            .getStatus());

            long previousRenewIndex = 0;
            for (int round = 1; round <= 3; round++) {
                cluster.clock.advanceTime(10, TimeUnit.SECONDS);
                final long renewedExpireAt = cluster.clock.milliseconds() + ttlMs;
                assertEquals(
                        RaftResult.Status.OK,
                        send(cluster, RaftCommand.leaseRenew(leaseId, renewedExpireAt))
                                .getStatus());
                final long expectedRenewIndex = renewIndexOf(cluster, leaseId, renewedExpireAt);
                assertTrue(
                        expectedRenewIndex > previousRenewIndex,
                        "round " + round + " advances the renew index");
                previousRenewIndex = expectedRenewIndex;
            }

            // the 45s advance clears the renewed expiry plus the ttl/3 grace
            cluster.clock.advanceTime(45, TimeUnit.SECONDS);
            MetadataCluster.waitThroughTransientFailure(
                    () ->
                            leaseOf(sendReadOnly(cluster, RaftCommand.leaseList()), leaseId) == null
                                    && statusOf(cluster, "/lease101/a")
                                            == RaftResult.Status.NOT_FOUND
                                    && statusOf(cluster, "/lease101/b")
                                            == RaftResult.Status.NOT_FOUND
                                    && statusOf(cluster, "/lease101/keep")
                                            == RaftResult.Status.OK,
                    OPERATION_TIMEOUT,
                    "The expired lease and its ephemeral records were not removed");
            // (the source asserted the leader scheduler's proposalsSubmitted >= 1 here; the
            // waited-for effect - expiry applied, ephemerals cascaded, persistent record kept -
            // is the proposal's only observable outcome and is asserted above)

            final RaftResult poll =
                    sendReadOnly(cluster, RaftCommand.pollChanges(0, POLL_BUDGET_BYTES));
            assertEquals(RaftResult.Status.OK, poll.getStatus());
            assertEquals(
                    1L,
                    poll.getOldestRetainedRevision(),
                    "no snapshot was taken, so nothing was collected");

            final List<ChangeEvent> records = poll.getRecords();
            final List<Long> revisions = revisionsOf(records);
            for (int i = 1; i < revisions.size(); i++) {
                assertTrue(
                        revisions.get(i) > revisions.get(i - 1),
                        "record revisions are strictly increasing");
            }

            ChangeEvent expiryRecord = null;
            int renewEvents = 0;
            for (ChangeEvent record : records) {
                for (ChangeEvent.Change change : record.getChanges()) {
                    if (change.getOp() == ChangeEvent.ChangeOp.LEASE_RENEW
                            && change.getLeaseId() == leaseId) {
                        renewEvents++;
                    }
                    if (change.getOp() == ChangeEvent.ChangeOp.LEASE_EXPIRE
                            && change.getLeaseId() == leaseId) {
                        expiryRecord = record;
                    }
                }
            }
            assertEquals(3, renewEvents, "every renewal round left its event");
            assertNotNull(expiryRecord, "the expiry is in the stream");

            final List<String> cascadeDeletes = new ArrayList<>();
            int expiryMarks = 0;
            for (ChangeEvent.Change change : expiryRecord.getChanges()) {
                if (change.getOp() == ChangeEvent.ChangeOp.DELETE) {
                    cascadeDeletes.add(change.getPath());
                }
                if (change.getOp() == ChangeEvent.ChangeOp.LEASE_EXPIRE) {
                    expiryMarks++;
                }
            }
            final List<String> expectedCascadeDeletes =
                    new ArrayList<>(Arrays.asList("/lease101/a", "/lease101/b"));
            Collections.sort(cascadeDeletes);
            Collections.sort(expectedCascadeDeletes);
            assertEquals(
                    expectedCascadeDeletes,
                    cascadeDeletes,
                    "exactly the ephemeral paths cascade away with the lease");
            assertEquals(1, expiryMarks);

            for (ChangeEvent record : records) {
                if (record.getRevision() > expiryRecord.getRevision()) {
                    for (ChangeEvent.Change change : record.getChanges()) {
                        assertTrue(
                                change.getLeaseId() != leaseId,
                                "no zombie lease event after the expiry");
                        assertTrue(
                                !change.getPath().equals("/lease101/a")
                                        && !change.getPath().equals("/lease101/b"),
                                "no zombie path event after the expiry");
                    }
                }
            }
            assertTrue(
                    poll.getRevision() >= expiryRecord.getRevision(),
                    "the watermark covers the expiry");
        }
    }

    @Test
    void testRenewExpireRaceResolvesSelfConsistently() throws Exception {
        try (MetadataCluster cluster = newCluster()) {
            cluster.start();

            // side 1: a completed renewal protects; the clock passes the original, not the
            // renewed expiry
            final long leaseId = 201L;
            final long start = cluster.clock.milliseconds();
            assertEquals(
                    RaftResult.Status.OK,
                    send(cluster, RaftCommand.put("/race", bytes(""), 0L, start)).getStatus());
            assertEquals(
                    RaftResult.Status.OK,
                    send(
                                    cluster,
                                    RaftCommand.leaseRegister(
                                            leaseId,
                                            TimeUnit.SECONDS.toMillis(5),
                                            start + TimeUnit.SECONDS.toMillis(5)))
                            .getStatus());
            assertEquals(
                    RaftResult.Status.OK,
                    send(cluster, RaftCommand.put("/race/protected", bytes("p"), leaseId, start))
                            .getStatus());
            final long renewedExpireAt = start + TimeUnit.SECONDS.toMillis(60);
            assertEquals(
                    RaftResult.Status.OK,
                    send(cluster, RaftCommand.leaseRenew(leaseId, renewedExpireAt)).getStatus());

            cluster.clock.advanceTime(10, TimeUnit.SECONDS);
            // The source waited for the leader scheduler to complete three further scans and
            // then asserted it had proposed nothing; the members own their schedulers without
            // exposing them, so "scans happened and proposed nothing" becomes the invariant
            // holding through several scan intervals of wall time.
            holdThrough(
                    STEADY_SCAN_WINDOW,
                    () ->
                            leaseOf(sendReadOnly(cluster, RaftCommand.leaseList()), leaseId) != null
                                    && statusOf(cluster, "/race/protected")
                                            == RaftResult.Status.OK,
                    "A lease past its original but before its renewed expiry is not proposed");
            assertNotNull(
                    leaseOf(sendReadOnly(cluster, RaftCommand.leaseList()), leaseId),
                    "the renewed lease is still listed");
            assertEquals(RaftResult.Status.OK, statusOf(cluster, "/race/protected"));

            // side 2: past the renewed expiry plus the ttl/3 grace, the scheduler does propose
            cluster.clock.advanceTime(55, TimeUnit.SECONDS);
            MetadataCluster.waitThroughTransientFailure(
                    () ->
                            leaseOf(sendReadOnly(cluster, RaftCommand.leaseList()), leaseId) == null
                                    && statusOf(cluster, "/race/protected")
                                            == RaftResult.Status.NOT_FOUND,
                    OPERATION_TIMEOUT,
                    "The lease past its renewed expiry was not expired");

            // The genuine race, one lease per round: expiry view and renewal interleave.
            for (int round = 0; round < 8; round++) {
                final long racingLeaseId = 300L + round;
                final String ephemeral = "/race/round-" + round;
                final long registeredAt = cluster.clock.milliseconds();
                assertEquals(
                        RaftResult.Status.OK,
                        send(
                                        cluster,
                                        RaftCommand.leaseRegister(
                                                racingLeaseId,
                                                TimeUnit.SECONDS.toMillis(1),
                                                registeredAt
                                                        + TimeUnit.SECONDS.toMillis(1)))
                                .getStatus());
                assertEquals(
                        RaftResult.Status.OK,
                        send(
                                        cluster,
                                        RaftCommand.put(
                                                ephemeral,
                                                bytes("e"),
                                                racingLeaseId,
                                                registeredAt))
                                .getStatus());

                // due now; a pre-renew expiry proposal may already be in flight
                cluster.clock.advanceTime(2, TimeUnit.SECONDS);
                final long racingRenewExpireAt =
                        cluster.clock.milliseconds() + TimeUnit.SECONDS.toMillis(60);
                final RaftResult renewal =
                        send(cluster, RaftCommand.leaseRenew(racingLeaseId, racingRenewExpireAt));

                if (renewal.getStatus() == RaftResult.Status.OK) {
                    // the in-flight pre-renew expiry proposal now loses its renew-index guard
                    MetadataCluster.waitThroughTransientFailure(
                            () -> {
                                final RaftResult.Lease lease =
                                        leaseOf(
                                                sendReadOnly(cluster, RaftCommand.leaseList()),
                                                racingLeaseId);
                                return lease != null
                                        && lease.getExpireAtMs() == racingRenewExpireAt;
                            },
                            OPERATION_TIMEOUT,
                            "round " + round + ": the winning renewal's expiry did not apply");
                    assertEquals(
                            RaftResult.Status.OK,
                            statusOf(cluster, ephemeral),
                            "round " + round + ": the ephemeral record of a live lease survives");
                    assertEquals(
                            RaftResult.Status.OK,
                            send(
                                            cluster,
                                            RaftCommand.leaseRenew(
                                                    racingLeaseId,
                                                    cluster.clock.milliseconds()
                                                            + TimeUnit.SECONDS.toMillis(60)))
                                    .getStatus(),
                            "round " + round
                                    + ": a lease that won a renewal answers another renewal");
                } else {
                    // The expiry committed first: the renewal loses with UNKNOWN_LEASE.
                    assertEquals(
                            RaftResult.Status.UNKNOWN_LEASE,
                            renewal.getStatus(),
                            "round " + round + ": a lost race is rejected with UNKNOWN_LEASE");
                    MetadataCluster.waitThroughTransientFailure(
                            () ->
                                    leaseOf(
                                                    sendReadOnly(cluster, RaftCommand.leaseList()),
                                                    racingLeaseId)
                                            == null,
                            OPERATION_TIMEOUT,
                            "round " + round + ": the expired lease is not listed");
                    assertEquals(
                            RaftResult.Status.NOT_FOUND,
                            statusOf(cluster, ephemeral),
                            "round " + round + ": the cascade deleted the ephemeral record");
                    assertEquals(
                            RaftResult.Status.UNKNOWN_LEASE,
                            send(
                                            cluster,
                                            RaftCommand.leaseRenew(
                                                    racingLeaseId,
                                                    cluster.clock.milliseconds()
                                                            + TimeUnit.SECONDS.toMillis(60)))
                                    .getStatus(),
                            "round " + round + ": an expired lease stays dead");
                }
            }
        }
    }

    @Test
    void testKillLeaderFailoverTakeoverAndWatchContinuity() throws Exception {
        try (MetadataCluster cluster = newCluster()) {
            cluster.start();
            final RaftPeerId leader = cluster.awaitLeader();
            final int leaderIndex = cluster.indexOf(leader);
            final List<RaftPeerId> survivors = cluster.peerIds();
            survivors.remove(leader);

            final long liveLeaseId = 501L;
            final long deadLeaseId = 502L;
            final long ttlMs = TimeUnit.SECONDS.toMillis(60);
            final long now = cluster.clock.milliseconds();

            final List<Mutation> expectedBeforeCheckpoint = new ArrayList<>();
            assertEquals(
                    RaftResult.Status.OK,
                    send(cluster, RaftCommand.put("/fo", bytes(""), 0L, now)).getStatus());
            expectedBeforeCheckpoint.add(Mutation.path(ChangeEvent.ChangeOp.CREATE, "/fo"));
            assertEquals(
                    RaftResult.Status.OK,
                    send(cluster, RaftCommand.leaseRegister(liveLeaseId, ttlMs, now + ttlMs))
                            .getStatus());
            expectedBeforeCheckpoint.add(
                    Mutation.lease(ChangeEvent.ChangeOp.LEASE_REGISTER, liveLeaseId));
            assertEquals(
                    RaftResult.Status.OK,
                    send(cluster, RaftCommand.leaseRegister(deadLeaseId, ttlMs, now + ttlMs))
                            .getStatus());
            expectedBeforeCheckpoint.add(
                    Mutation.lease(ChangeEvent.ChangeOp.LEASE_REGISTER, deadLeaseId));
            assertEquals(
                    RaftResult.Status.OK,
                    send(cluster, RaftCommand.put("/fo/live-eph", bytes("live"), liveLeaseId, now))
                            .getStatus());
            expectedBeforeCheckpoint.add(
                    Mutation.path(ChangeEvent.ChangeOp.CREATE, "/fo/live-eph"));
            assertEquals(
                    RaftResult.Status.OK,
                    send(cluster, RaftCommand.put("/fo/dead-eph", bytes("dead"), deadLeaseId, now))
                            .getStatus());
            expectedBeforeCheckpoint.add(
                    Mutation.path(ChangeEvent.ChangeOp.CREATE, "/fo/dead-eph"));
            assertEquals(
                    RaftResult.Status.OK,
                    send(cluster, RaftCommand.put("/fo/persistent", bytes("p"), 0L, now))
                            .getStatus());
            expectedBeforeCheckpoint.add(
                    Mutation.path(ChangeEvent.ChangeOp.CREATE, "/fo/persistent"));

            final RaftResult firstLeg =
                    sendReadOnly(cluster, RaftCommand.pollChanges(0, POLL_BUDGET_BYTES));
            assertEquals(RaftResult.Status.OK, firstLeg.getStatus());
            assertEquals(1L, firstLeg.getOldestRetainedRevision());
            assertEquals(
                    expectedBeforeCheckpoint,
                    projectionsOf(firstLeg.getRecords()),
                    "the first poll leg carries every mutation in order, without a gap");
            final long checkpoint = firstLeg.getRevision();

            // the listener dies with the leader: query survivors. The source asserted the
            // killed leader's scan loop had started and then waited for it to stop; killServer
            // closes the member's scheduler synchronously (its close joins the loop thread), so
            // the stop needs no wait here, and the takeover expiry below proves loop liveness.
            cluster.killServer(leaderIndex);
            cluster.awaitLeader(survivors);

            // renew BEFORE the advance that makes the dead session due - only it can expire
            // after
            final long liveRenewExpireAt = cluster.clock.milliseconds() + 2 * ttlMs;
            assertEquals(
                    RaftResult.Status.OK,
                    send(cluster, RaftCommand.leaseRenew(liveLeaseId, liveRenewExpireAt))
                            .getStatus(),
                    "the live session renews through the new leader");
            final List<Mutation> expectedTail = new ArrayList<>();
            expectedTail.add(Mutation.lease(ChangeEvent.ChangeOp.LEASE_RENEW, liveLeaseId));

            // past expiry plus ttl/3 grace, far inside the live renewal: the NEW leader takes
            // over
            cluster.clock.advanceTime(
                    ttlMs + ttlMs / 3 + TimeUnit.SECONDS.toMillis(5), TimeUnit.MILLISECONDS);
            expectedTail.add(Mutation.path(ChangeEvent.ChangeOp.DELETE, "/fo/dead-eph"));
            expectedTail.add(Mutation.lease(ChangeEvent.ChangeOp.LEASE_EXPIRE, deadLeaseId));
            MetadataCluster.waitThroughTransientFailure(
                    () ->
                            leaseOf(sendReadOnly(cluster, RaftCommand.leaseList()), deadLeaseId)
                                            == null
                                    && statusOf(cluster, "/fo/dead-eph")
                                            == RaftResult.Status.NOT_FOUND,
                    OPERATION_TIMEOUT,
                    "The new leader did not expire the abandoned session");

            MetadataCluster.waitThroughTransientFailure(
                    () -> {
                        final RaftResult tail =
                                sendReadOnly(
                                        cluster,
                                        RaftCommand.pollChanges(checkpoint, POLL_BUDGET_BYTES));
                        return tail.getStatus() == RaftResult.Status.OK
                                && tail.getOldestRetainedRevision() == 1L
                                && expectedTail.equals(projectionsOf(tail.getRecords()));
                    },
                    OPERATION_TIMEOUT,
                    "The consumer did not observe the post-failover mutations without a gap");

            final RaftResult.Lease liveLease =
                    leaseOf(sendReadOnly(cluster, RaftCommand.leaseList()), liveLeaseId);
            assertNotNull(liveLease, "the live session survived the failover");
            assertEquals(liveRenewExpireAt, liveLease.getExpireAtMs());
            assertEquals(RaftResult.Status.OK, statusOf(cluster, "/fo/live-eph"));

            cluster.restartServer(leaderIndex);
            final MetadataStateMachine rejoined = cluster.stateMachine(leaderIndex);
            final long targetRevision = committedRevision(cluster);
            waitUntil(
                    () -> {
                        try {
                            return MetadataCluster.directQuery(rejoined, RaftCommand.revision())
                                            .getRevision()
                                    >= targetRevision;
                        } catch (RuntimeException notOpenYet) {
                            return false;
                        }
                    },
                    OPERATION_TIMEOUT,
                    "The rejoined node did not catch up to revision " + targetRevision);
            assertArrayEquals(
                    bytes("p"),
                    MetadataCluster.directQuery(rejoined, RaftCommand.get("/fo/persistent"))
                            .getData());
            assertEquals(
                    RaftResult.Status.NOT_FOUND,
                    MetadataCluster.directQuery(rejoined, RaftCommand.get("/fo/dead-eph"))
                            .getStatus());
            assertNotNull(
                    leaseOf(
                            MetadataCluster.directQuery(rejoined, RaftCommand.leaseList()),
                            liveLeaseId),
                    "the rejoined node sees the live session");
        }
    }

    @Test
    void testSchedulerProposesOnLeaderOnlyAndFollowsLeadershipTransfer() throws Exception {
        try (MetadataCluster cluster = newCluster()) {
            cluster.start();
            final int leaderIndex = cluster.indexOf(cluster.awaitLeader());

            // A due lease is expired by the leader, and by nobody else.
            final long leaseId = 601L;
            final long now = cluster.clock.milliseconds();
            assertEquals(
                    RaftResult.Status.OK,
                    send(
                                    cluster,
                                    RaftCommand.leaseRegister(
                                            leaseId,
                                            TimeUnit.SECONDS.toMillis(2),
                                            now + TimeUnit.SECONDS.toMillis(2)))
                            .getStatus());
            cluster.clock.advanceTime(3, TimeUnit.SECONDS);
            MetadataCluster.waitThroughTransientFailure(
                    () -> leaseOf(sendReadOnly(cluster, RaftCommand.leaseList()), leaseId) == null,
                    OPERATION_TIMEOUT,
                    "The due lease was not expired");
            // The source asserted the leader scheduler's proposalsSubmitted >= 1 and every
            // follower's == 0 with no follower scan loop running; the members own their
            // schedulers without exposing them, so "the leader alone proposed" is re-expressed
            // through the change stream: exactly one expiry event for the lease, which no
            // follower proposal could ever duplicate.
            assertEquals(
                    1,
                    expiryEventsOf(
                            sendReadOnly(cluster, RaftCommand.pollChanges(0, POLL_BUDGET_BYTES))
                                    .getRecords(),
                            leaseId),
                    "the leader alone proposed the expiry");

            final int targetIndex = (leaderIndex + 1) % NUM_SERVERS;
            final RaftClientReply transfer =
                    cluster.adminClient()
                            .admin()
                            .transferLeadership(
                                    RaftPeerId.valueOf("n" + targetIndex),
                                    LEADER_ELECTION_CALL_TIMEOUT_MS);
            assertTrue(transfer.isSuccess(), "the leadership transfer succeeds");
            cluster.awaitLeader(Collections.singletonList(RaftPeerId.valueOf("n" + targetIndex)));
            // The source waited here for the old leader's scan loop to stop and the new
            // leader's to start; both are re-expressed behaviorally below - a stopped old loop
            // adds nothing further, and only a running new loop can expire the next due lease.

            final long nextLeaseId = 602L;
            final long nextNow = cluster.clock.milliseconds();
            assertEquals(
                    RaftResult.Status.OK,
                    send(
                                    cluster,
                                    RaftCommand.leaseRegister(
                                            nextLeaseId,
                                            TimeUnit.SECONDS.toMillis(2),
                                            nextNow + TimeUnit.SECONDS.toMillis(2)))
                            .getStatus());
            cluster.clock.advanceTime(3, TimeUnit.SECONDS);
            MetadataCluster.waitThroughTransientFailure(
                    () ->
                            leaseOf(sendReadOnly(cluster, RaftCommand.leaseList()), nextLeaseId)
                                    == null,
                    OPERATION_TIMEOUT,
                    "The new leader did not expire the due lease");
            assertEquals(
                    1,
                    expiryEventsOf(
                            sendReadOnly(cluster, RaftCommand.pollChanges(0, POLL_BUDGET_BYTES))
                                    .getRecords(),
                            nextLeaseId),
                    "the new leader proposed the expiry exactly once, the demoted leader nothing"
                            + " further");
        }
    }

    @Test
    void testExpiryProposalWaitsOutTheClockDomainGrace() throws Exception {
        try (MetadataCluster cluster = newCluster()) {
            cluster.start();
            final long leaseId = 701L;
            final long ttlMs = TimeUnit.SECONDS.toMillis(30);
            final long start = cluster.clock.milliseconds();
            assertEquals(
                    RaftResult.Status.OK,
                    send(cluster, RaftCommand.leaseRegister(leaseId, ttlMs, start + ttlMs))
                            .getStatus());

            // 9s past the stamp, still inside the 10s grace: scans happen, nothing is proposed.
            // The source counted the leader scheduler's completed scans and proposals; the
            // members own their schedulers without exposing them, so "scans happened and
            // proposed nothing" becomes the lease holding through several scan intervals of
            // wall time.
            cluster.clock.advanceTime(ttlMs + TimeUnit.SECONDS.toMillis(9), TimeUnit.MILLISECONDS);
            holdThrough(
                    STEADY_SCAN_WINDOW,
                    () -> leaseOf(sendReadOnly(cluster, RaftCommand.leaseList()), leaseId) != null,
                    "A lease inside the grace is not proposed for expiry");
            assertNotNull(
                    leaseOf(sendReadOnly(cluster, RaftCommand.leaseList()), leaseId),
                    "the lease survives inside the grace");

            // 3s more = 12s past the stamp, past the grace: the scheduler proposes and it dies
            cluster.clock.advanceTime(3, TimeUnit.SECONDS);
            MetadataCluster.waitThroughTransientFailure(
                    () -> leaseOf(sendReadOnly(cluster, RaftCommand.leaseList()), leaseId) == null,
                    OPERATION_TIMEOUT,
                    "The lease past its grace was not expired");
        }
    }

    @Test
    void testSchedulerExpiresLeasesShadowedBeyondOneListingPage() throws Exception {
        try (MetadataCluster cluster = newCluster()) {
            cluster.start();
            final long start = cluster.clock.milliseconds();

            // 1024 lowest-id live leases (one MULTI's cap), far from due, registered first to
            // shadow
            final List<RaftCommand> frontPage = new ArrayList<>();
            for (long leaseId = 1; leaseId <= 1024; leaseId++) {
                frontPage.add(
                        RaftCommand.leaseRegister(
                                leaseId,
                                TimeUnit.SECONDS.toMillis(3600),
                                start + TimeUnit.SECONDS.toMillis(3600)));
            }
            assertEquals(
                    RaftResult.Status.OK,
                    send(cluster, RaftCommand.multi(frontPage)).getStatus());

            final long shadowedLeaseId = 9000L;
            assertEquals(
                    RaftResult.Status.OK,
                    send(cluster, RaftCommand.put("/shadow", bytes(""), 0L, start)).getStatus());
            assertEquals(
                    RaftResult.Status.OK,
                    send(
                                    cluster,
                                    RaftCommand.leaseRegister(
                                            shadowedLeaseId,
                                            TimeUnit.SECONDS.toMillis(1),
                                            start + TimeUnit.SECONDS.toMillis(1)))
                            .getStatus());
            assertEquals(
                    RaftResult.Status.OK,
                    send(
                                    cluster,
                                    RaftCommand.put(
                                            "/shadow/rec", bytes("s"), shadowedLeaseId, start))
                            .getStatus());

            // sanity: one full page above id 1 ends at the shadowed lease, the old stopping
            // point
            final RaftResult front = sendReadOnly(cluster, RaftCommand.leaseList(1L, 1024L));
            assertEquals(
                    1024,
                    front.getLeases().size(),
                    "the shadowed lease sits behind one full page of lower keys");
            assertEquals(
                    shadowedLeaseId,
                    front.getLeases().get(front.getLeases().size() - 1).getLeaseId());

            // past the shadowed lease's expiry plus its grace; the front page is nowhere near
            // due
            cluster.clock.advanceTime(5, TimeUnit.SECONDS);
            MetadataCluster.waitThroughTransientFailure(
                    () ->
                            leaseOf(sendReadOnly(cluster, RaftCommand.leaseList()), shadowedLeaseId)
                                            == null
                                    && statusOf(cluster, "/shadow/rec")
                                            == RaftResult.Status.NOT_FOUND,
                    OPERATION_TIMEOUT,
                    "The lease shadowed beyond one listing page was not expired");
            // the front page is untouched by all of this: the shadowed lease alone is gone
            final RaftResult after = sendReadOnly(cluster, RaftCommand.leaseList(1L, 1024L));
            assertEquals(1023, after.getLeases().size());
            assertNull(leaseOf(after, shadowedLeaseId));
        }
    }

    /** One mutation the change stream owes the consumer: an op on a path or on a lease. */
    private static final class Mutation {
        private final ChangeEvent.ChangeOp op;
        private final String path;
        private final long leaseId;

        private Mutation(ChangeEvent.ChangeOp op, String path, long leaseId) {
            this.op = op;
            this.path = path;
            this.leaseId = leaseId;
        }

        static Mutation path(ChangeEvent.ChangeOp op, String path) {
            return new Mutation(op, path, 0L);
        }

        static Mutation lease(ChangeEvent.ChangeOp op, long leaseId) {
            return new Mutation(op, "", leaseId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final Mutation other = (Mutation) o;
            return op == other.op && leaseId == other.leaseId && path.equals(other.path);
        }

        @Override
        public int hashCode() {
            int result = op.hashCode();
            result = 31 * result + path.hashCode();
            result = 31 * result + (int) (leaseId ^ (leaseId >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return "Mutation{op=" + op + ", path='" + path + "', leaseId=" + leaseId + '}';
        }
    }

    /** Flattens records into the ordered mutation list a consumer sees. */
    private static List<Mutation> projectionsOf(List<ChangeEvent> records) {
        final List<Mutation> mutations = new ArrayList<>();
        for (ChangeEvent record : records) {
            for (ChangeEvent.Change change : record.getChanges()) {
                if (change.getPath().isEmpty()) {
                    mutations.add(Mutation.lease(change.getOp(), change.getLeaseId()));
                } else {
                    mutations.add(Mutation.path(change.getOp(), change.getPath()));
                }
            }
        }
        return mutations;
    }

    /** The lease entry of the given id, or null when the listing does not contain it. */
    private static RaftResult.Lease leaseOf(RaftResult listing, long leaseId) {
        assertEquals(RaftResult.Status.OK, listing.getStatus());
        for (RaftResult.Lease lease : listing.getLeases()) {
            if (lease.getLeaseId() == leaseId) {
                return lease;
            }
        }
        return null;
    }

    /** The GET status of a path, read through the leader. */
    private static RaftResult.Status statusOf(MetadataCluster cluster, String path)
            throws IOException {
        return sendReadOnly(cluster, RaftCommand.get(path)).getStatus();
    }

    /**
     * Holds an invariant through at least the given wall-time window, failing at once if it
     * breaks: the behavioral stand-in for the source suite's "the scheduler completed N scans
     * and proposed nothing" counter assertions, since the members own their schedulers
     * internally and expose no counters.
     */
    private static void holdThrough(
            Duration window, MetadataCluster.SupplierWithIo<Boolean> invariant, String what)
            throws Exception {
        final long deadline = System.currentTimeMillis() + window.toMillis();
        while (System.currentTimeMillis() < deadline) {
            assertTrue(invariant.get(), what);
            Thread.sleep(5);
        }
        assertTrue(invariant.get(), what);
    }

    /** The linearizable last-applied revision, observed through the current leader. */
    private static long committedRevision(MetadataCluster cluster) throws IOException {
        return sendReadOnly(cluster, RaftCommand.revision()).getRevision();
    }

    /** Waits for the lease to show exactly the expected expiry; returns its renew index. */
    private static long renewIndexOf(MetadataCluster cluster, long leaseId, long expectedExpireAt) {
        final long[] renewIndex = new long[1];
        MetadataCluster.waitThroughTransientFailure(
                () -> {
                    final RaftResult.Lease lease =
                            leaseOf(sendReadOnly(cluster, RaftCommand.leaseList()), leaseId);
                    if (lease == null || lease.getExpireAtMs() != expectedExpireAt) {
                        return false;
                    }
                    renewIndex[0] = lease.getLastRenewIndex();
                    return true;
                },
                OPERATION_TIMEOUT,
                "The lease did not show the renewed expiry " + expectedExpireAt);
        return renewIndex[0];
    }

    private static List<Long> revisionsOf(List<ChangeEvent> records) {
        final List<Long> revisions = new ArrayList<>();
        for (ChangeEvent record : records) {
            revisions.add(record.getRevision());
        }
        return revisions;
    }

    /**
     * How many LEASE_EXPIRE changes the stream carries for the lease: the behavioral stand-in
     * for the per-scheduler proposalsSubmitted counters, which the members do not expose.
     */
    private static int expiryEventsOf(List<ChangeEvent> records, long leaseId) {
        int events = 0;
        for (ChangeEvent record : records) {
            for (ChangeEvent.Change change : record.getChanges()) {
                if (change.getOp() == ChangeEvent.ChangeOp.LEASE_EXPIRE
                        && change.getLeaseId() == leaseId) {
                    events++;
                }
            }
        }
        return events;
    }

    private static RaftResult send(MetadataCluster cluster, RaftCommand command)
            throws IOException {
        return decodeReply(cluster.adminClient().io().send(toMessage(command)), command);
    }

    private static RaftResult sendReadOnly(MetadataCluster cluster, RaftCommand command)
            throws IOException {
        return decodeReply(cluster.adminClient().io().sendReadOnly(toMessage(command)), command);
    }

    private static RaftResult decodeReply(RaftClientReply reply, RaftCommand command) {
        final RaftResult result =
                RaftCommandCodec.decodeResult(reply.getMessage().getContent().toByteArray());
        assertNotNull(result, "reply of " + command.getOp() + " decodes (op echo must match)");
        assertEquals(command.getOp(), result.getOp());
        return result;
    }

    private static Message toMessage(RaftCommand command) {
        return Message.valueOf(ByteBuffer.wrap(RaftCommandCodec.encodeCommand(command)));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
