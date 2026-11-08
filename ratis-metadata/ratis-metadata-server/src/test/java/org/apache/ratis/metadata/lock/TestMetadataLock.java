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
package org.apache.ratis.metadata.lock;

import org.apache.ratis.metadata.MetadataCluster;
import org.apache.ratis.metadata.client.LeaseSession;
import org.apache.ratis.metadata.client.RaftMetadataClient;
import org.apache.ratis.metadata.client.RaftMetadataException;
import org.apache.ratis.metadata.protocol.ChangeEvent;
import org.apache.ratis.metadata.protocol.RaftCommand;
import org.apache.ratis.metadata.protocol.RaftCommandCodec;
import org.apache.ratis.metadata.protocol.RaftResult;
import org.apache.ratis.metadata.util.AsyncDriver;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.apache.ratis.metadata.util.TestWait.waitUntil;

/**
 * The lock recipe over the live cluster: exclusion and token advance, prompt release, crash
 * release through lease expiry (the waiter's own dead lease along the way), acquire timeout,
 * close of a pending contender (which must not disturb the holder), and leader failover during
 * contention. The harness clock is manual, so lease lapses are deterministic; only the waiters'
 * poll and watchdog pacing run on wall time.
 */
public class TestMetadataLock {

    private static final Duration LEASE_TTL = Duration.ofSeconds(2);
    private static final Duration SETTLE = Duration.ofSeconds(10);

    @TempDir File tempDir;

    private MetadataCluster cluster;
    private RaftMetadataClient holderClient;
    private RaftMetadataClient contenderClient;

    @BeforeEach
    void setUp() throws Exception {
        cluster = MetadataCluster.newBuilder(tempDir).setNumServers(3).build();
        cluster.start();
        holderClient = cluster.newMetadataClient();
        contenderClient = cluster.newMetadataClient();
    }

    @AfterEach
    void tearDown() {
        closeQuietly(contenderClient);
        closeQuietly(holderClient);
        cluster.close();
    }

    private MetadataLock newLock(RaftMetadataClient client, String path) {
        return MetadataLock.newBuilder().setClient(client).setPath(path).setTtl(LEASE_TTL).build();
    }

    /**
     * A contender can only complete after a release; its token is higher, and the ordering holds
     * across the next handover too. {@code isDone()} before the release is deterministic: with
     * the clock frozen no lease ever lapses, so nothing but the holder's release could let the
     * contender through.
     */
    @Test
    void testContenderWaitsForReleaseAndTokensAdvance() throws Exception {
        final MetadataLock.Hold first = newLock(holderClient, "/locks/a").acquire();
        assertTrue(first.isHeld(), "the acquirer holds");

        final CompletableFuture<MetadataLock.Hold> pending =
                newLock(contenderClient, "/locks/a").acquireAsync();
        assertFalse(pending.isDone(), "the contender cannot complete while the record exists");

        first.release();
        final MetadataLock.Hold second = pending.get(SETTLE.toMillis(), TimeUnit.MILLISECONDS);
        assertTrue(second.getToken() > first.getToken(), "tokens advance across holders");
        assertFalse(first.isHeld(), "release ends the hold");

        second.release();
        final MetadataLock.Hold third = newLock(holderClient, "/locks/a").acquire();
        assertTrue(third.getToken() > second.getToken(), "tokens keep advancing");
        third.release();
    }

    /**
     * A holder whose client has stopped renewing loses the record through the leader's expiry
     * scheduler within two ttls (the ttl plus the ttl/3 grace plus the ttl/3 skew tolerance);
     * the waiter wakes on the record's delete. Closing only the holder's client is essential:
     * a clock jump also makes every live session due for renewal.
     */
    @Test
    void testCrashReleaseExpiresRecordAndWaiterTakesOver() throws Exception {
        final MetadataLock.Hold holder = newLock(holderClient, "/locks/b").acquire();
        final CompletableFuture<MetadataLock.Hold> pending =
                newLock(contenderClient, "/locks/b").acquireAsync();
        assertFalse(pending.isDone(), "the contender waits while the holder's lease is renewed");

        holderClient.close();
        assertFalse(holder.isHeld(), "the crashed holder cannot renew its lease");
        cluster.clock.advanceTime(LEASE_TTL.multipliedBy(2).toMillis(), TimeUnit.MILLISECONDS);

        final MetadataLock.Hold taker = pending.get(SETTLE.toMillis(), TimeUnit.MILLISECONDS);
        assertTrue(taker.getToken() > holder.getToken(), "the waiter takes over with a higher token");
        taker.release();
    }

    /**
     * An external recursive delete removes the held record outside the lock protocol: the
     * change stream carries one {@code DELETE_SUBTREE} event at the deleted root and no
     * per-victim {@code DELETE}. The parked contender must wake on that event itself, not wait
     * out a watchdog tick: its 30s ttl stretches the tick to 10s while the wake rides the
     * 200ms poll, so a 3s bound separates the two. The fencing counter survives the subtree
     * delete, so the woken contender still takes a higher token.
     */
    @Test
    void testWaiterWakesOnAnExternalSubtreeDeleteCoveringThePath() throws Exception {
        final MetadataLock.Hold holder = newLock(holderClient, "/locks/d").acquire();
        final MetadataLock slowTickContender =
                MetadataLock.newBuilder()
                        .setClient(contenderClient)
                        .setPath("/locks/d")
                        .setTtl(Duration.ofSeconds(30))
                        .build();
        final long revisionAtIssue = adminRevisionOf(cluster);
        final CompletableFuture<MetadataLock.Hold> pending = slowTickContender.acquireAsync();
        // Parked provably: the contender's lease registration and its rejected create both
        // appended entries beyond the revision it took as its watch position.
        waitUntil(
                () -> adminRevisionOf(cluster) >= revisionAtIssue + 2,
                SETTLE,
                "The contender never parked on the held record");
        assertFalse(pending.isDone(), "the contender waits while the record exists");

        // The recursive delete lands exactly on the lock path: the record dies with the
        // subtree event at the root, the parent chain and the fencing counter survive.
        adminDelete("/locks/d", true);

        final MetadataLock.Hold taker = pending.get(3, TimeUnit.SECONDS);
        assertTrue(taker.getToken() > holder.getToken(), "the woken contender takes a higher token");
        taker.release();
    }

    /**
     * Deleting a STRICT ancestor of the lock path wakes the waiter (the subtree event covers
     * the path) but takes the parent chain with it: the re-attempt's create meets NO_PARENT.
     * The waiter must rebuild the chain and take the freed lock, not settle with the refusal -
     * the covering-path test deletes the lock path itself and never exercises this.
     */
    @Test
    void testAWaiterRebuildsTheParentChainAfterAStrictAncestorSubtreeDelete() throws Exception {
        final MetadataLock.Hold holder = newLock(holderClient, "/anc/child/lock").acquire();
        final MetadataLock contender = newLock(contenderClient, "/anc/child/lock");
        final CompletableFuture<MetadataLock.Hold> pending = contender.acquireAsync();
        final long revisionAtIssue = adminRevisionOf(cluster);
        waitUntil(
                () -> adminRevisionOf(cluster) >= revisionAtIssue + 2,
                SETTLE,
                "The contender never parked on the held record");
        assertFalse(pending.isDone(), "the contender waits while the record exists");

        adminDelete("/anc", true);

        final MetadataLock.Hold taker = pending.get(3, TimeUnit.SECONDS);
        assertTrue(taker.getToken() > holder.getToken(), "the woken contender takes a higher token");
        taker.release();
    }

    /** A bounded acquire times out cleanly while the lock is held, and the path stays acquirable. */
    @Test
    void testAcquireTimesOutWhileTheLockIsHeld() throws Exception {
        final MetadataLock.Hold holder = newLock(holderClient, "/locks/c").acquire();
        final CompletableFuture<MetadataLock.Hold> pending =
                newLock(contenderClient, "/locks/c").acquireAsync(Duration.ofMillis(300));
        final ExecutionException failure =
                assertThrows(
                        ExecutionException.class,
                        () -> pending.get(SETTLE.toMillis(), TimeUnit.MILLISECONDS));
        assertTrue(failure.getCause() instanceof TimeoutException, "the acquire times out");

        holder.release();
        final MetadataLock.Hold next =
                newLock(contenderClient, "/locks/c").acquire(Duration.ofSeconds(5));
        assertTrue(next.getToken() > holder.getToken(), "the path is acquirable after the timeout");
        next.release();
    }

    /**
     * Closing a pending contender cancels its acquire and - crucially - must not touch the
     * holder's record: the undo only fires when this contender's own create may have committed.
     */
    @Test
    void testCloseCancelsPendingAcquireWithoutDisturbingTheHolder() throws Exception {
        final MetadataLock.Hold holder = newLock(holderClient, "/locks/d").acquire();

        final MetadataLock contender = newLock(contenderClient, "/locks/d");
        final CompletableFuture<MetadataLock.Hold> pending = contender.acquireAsync();
        contender.close();
        final ExecutionException failure =
                assertThrows(
                        ExecutionException.class,
                        () -> pending.get(SETTLE.toMillis(), TimeUnit.MILLISECONDS));
        assertTrue(failure.getCause() instanceof IllegalStateException, "close cancels the acquire");

        assertTrue(holder.isHeld(), "the holder keeps its lease");
        final CompletableFuture<MetadataLock.Hold> nextPending =
                newLock(holderClient, "/locks/d").acquireAsync();
        assertFalse(nextPending.isDone(), "the holder's record still guards the lock");

        holder.release();
        holder.release();
        assertFalse(holder.isHeld(), "release is idempotent");

        final MetadataLock.Hold next = nextPending.get(SETTLE.toMillis(), TimeUnit.MILLISECONDS);
        next.release();
    }

    /**
     * The returned future is the caller's, and cancelling it is a settle like close(): the
     * contender's private lease must be revoked and its renewals stopped. Pre-fix the unwind's
     * ownership hinged on completing the future - a cancelled future cannot be completed - so
     * nothing ran, and the orphan lease kept renewing (and, had its create committed, kept the
     * path locked) for as long as the client lived.
     */
    @Test
    void testCancellingTheAcquireFutureUnwindsTheContender() throws Exception {
        final MetadataLock.Hold holder = newLock(holderClient, "/locks/g").acquire();
        final MetadataLock contender = newLock(contenderClient, "/locks/g");
        final CompletableFuture<MetadataLock.Hold> pending = contender.acquireAsync();
        final long revisionAtIssue = adminRevisionOf(cluster);
        waitUntil(
                () -> adminRevisionOf(cluster) >= revisionAtIssue + 2,
                SETTLE,
                "The contender never parked on the held record");
        final LeaseSession session = sessionOf(contender);
        assertTrue(session.isAlive(), "the parked contender's lease renews");

        assertTrue(pending.cancel(true), "the cancel settles the acquire");

        waitUntil(
                () -> !session.isAlive(),
                SETTLE,
                "Cancelling the acquire never unwound the contender's lease - it keeps renewing");
        assertTrue(holder.isHeld(), "the holder keeps its lease");
        holder.release();
        final MetadataLock.Hold next =
                newLock(contenderClient, "/locks/g").acquire(Duration.ofSeconds(5));
        assertTrue(next.getToken() > holder.getToken(), "the path is acquirable after the cancel");
        next.release();
    }

    /**
     * The settle hands the future to its caller off the module's shared scheduler: a listener on
     * the acquire future is caller code and may block, while that one scheduler thread runs every
     * client's lease renewals, watcher polls and lock watchdogs. Pre-fix the timeout settle ran
     * the handoff inline on the scheduler thread, so one blocking listener froze them all.
     */
    @Test
    void testABlockingListenerOnTheAcquireFutureCannotStallTheSharedScheduler() throws Exception {
        final MetadataLock.Hold holder = newLock(holderClient, "/locks/h").acquire();
        final CountDownLatch listenerEntered = new CountDownLatch(1);
        final CountDownLatch releaseListener = new CountDownLatch(1);
        final AtomicReference<String> listenerThread = new AtomicReference<>();
        final CompletableFuture<MetadataLock.Hold> pending =
                newLock(contenderClient, "/locks/h").acquireAsync(Duration.ofMillis(300));
        pending.whenComplete((hold, error) -> {
            listenerThread.set(Thread.currentThread().getName());
            listenerEntered.countDown();
            try {
                releaseListener.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        assertTrue(
                listenerEntered.await(SETTLE.toMillis(), TimeUnit.MILLISECONDS),
                "the acquire timed out and ran the listener");
        // Release the listener no matter how the assertions land: pre-fix it parks ON the
        // shared scheduler thread, and leaving it parked would wedge the whole teardown.
        try {
            assertFalse(
                    listenerThread.get().contains("ratis-metadata-scheduler"),
                    "the listener ran on the module's shared scheduler thread: " + listenerThread.get());

            final CompletableFuture<Void> unrelated = new CompletableFuture<>();
            AsyncDriver.schedule(() -> unrelated.complete(null), 0);
            assertDoesNotThrow(
                    () -> unrelated.get(2, TimeUnit.SECONDS),
                    "a blocking listener stalled the module's shared scheduler");
        } finally {
            releaseListener.countDown();
        }
        holder.release();
    }

    private static LeaseSession sessionOf(MetadataLock lock) throws Exception {
        final Field field = MetadataLock.class.getDeclaredField("session");
        field.setAccessible(true);
        return (LeaseSession) field.get(lock);
    }

    /** Killing the leader mid-contention must not lose the handover: the retries carry it. */
    @Test
    void testLeaderFailoverDuringContention() throws Exception {
        final MetadataLock.Hold holder = newLock(holderClient, "/locks/e").acquire();
        final CompletableFuture<MetadataLock.Hold> pending =
                newLock(contenderClient, "/locks/e").acquireAsync();
        assertFalse(pending.isDone(), "the contender waits");

        cluster.killServer(cluster.indexOf(cluster.awaitLeader()));

        holder.release();
        final MetadataLock.Hold taker = pending.get(30, TimeUnit.SECONDS);
        assertTrue(taker.getToken() > holder.getToken(), "the contender takes over after failover");
        taker.release();
    }

    /** A second acquire on the same instance is rejected - one acquire per lock. */
    @Test
    void testSecondAcquireOnTheSameInstanceIsRejected() throws Exception {
        final MetadataLock lock = newLock(holderClient, "/locks/f");
        final MetadataLock.Hold hold = lock.acquire();
        hold.release();
        assertThrows(IllegalStateException.class, lock::acquireAsync);
    }

    /**
     * THE ownership race: the holder's lease dies underneath it (a partition outlasted the ttl),
     * the waiter takes over with a lease of its own, and only then does the old holder release.
     * The late release may not delete the new holder's record: cleanup is scoped to the releaser's
     * own dead lease, so the record at the path - bound to the new holder's lease - survives and
     * the lock keeps excluding.
     *
     * <p>The lease's death is forced deterministically by expiring it through the admin client -
     * exactly the command the leader's scheduler proposes in production after a crashed holder
     * (see {@code testCrashReleaseExpiresRecordAndWaiterTakesOver}); the property under test is
     * what the old holder's LATE cleanup does to the record that replaced its own.
     *
     * <p>Every negative assertion here is anchored, never timed: the late release's revocation
     * commit is observed through the leader's applied revision (with the harness clock frozen it
     * is the only command that can commit), its whole window is swept through the change stream,
     * and the third contender is proven parked and past one watchdog re-attempt by revision
     * arithmetic - no fixed sleeps.
     */
    @Test
    void testLateReleaseByAnExpiredHolderCannotTakeTheNewHoldersRecord() throws Exception {
        final String path = "/locks/g1";
        final MetadataLock.Hold first = newLock(holderClient, path).acquire();
        assertTrue(first.isHeld(), "the first holder holds");
        final long deadLease = holderClient.get(path).getLeaseId();
        assertTrue(deadLease != 0L, "the lock record is bound to the holder's lease");

        // A waiter with a ttl far from due, so no clock advance of this test can lapse its lease.
        final MetadataLock contender =
                MetadataLock.newBuilder()
                        .setClient(contenderClient)
                        .setPath(path)
                        .setTtl(Duration.ofSeconds(30))
                        .build();
        final CompletableFuture<MetadataLock.Hold> pending = contender.acquireAsync();
        assertFalse(pending.isDone(), "the contender waits while the record exists");

        // The holder's lease dies underneath it; the expiry cascade deletes the record and the
        // waiter takes over with a fresh lease of its own.
        adminExpireLease(deadLease);
        final MetadataLock.Hold second = pending.get(SETTLE.toMillis(), TimeUnit.MILLISECONDS);
        assertTrue(second.isHeld(), "the waiter took over after the lease expired");
        final long takersLease = contenderClient.get(path).getLeaseId();
        assertTrue(takersLease != 0L && takersLease != deadLease, "the new record binds a new lease");

        // The dead session surfaces its lease's death once a renewal comes due (one ttl of clock).
        cluster.clock.advanceTime(LEASE_TTL.toMillis(), TimeUnit.MILLISECONDS);
        waitUntil(
                () -> !first.isHeld(),
                SETTLE,
                "The expired holder's session did not surface its lease's death");

        // The late release of the long-dead hold: whatever it does may not touch the new holder's
        // record at the shared path. Anchored deterministically, never slept past: with the
        // harness clock frozen nothing is due to expire and the holder's dead session sends
        // nothing, so the ONLY command that can commit after this point is the release's own
        // revocation - the leader's applied revision moving past this point IS that revocation
        // having committed, carrying with it every command the release issued before it.
        final long revisionAtLateRelease = adminRevisionOf(cluster);
        first.release();
        waitUntil(
                () -> adminRevisionOf(cluster) > revisionAtLateRelease,
                SETTLE,
                "The late release's revocation never committed");
        final RaftMetadataClient.MetadataRecord survivor = contenderClient.get(path);
        assertEquals(
                takersLease,
                survivor.getLeaseId(),
                "the new holder's record survives the old holder's late release");
        // The change stream closes the window behind the get: every revision up to a watermark
        // sampled after it is delivered, and none of them removes the shared record.
        final long revisionAfterSurvivor = adminRevisionOf(cluster);
        final RaftMetadataClient.PollResult lateReleaseWindow =
                contenderClient.poll(revisionAtLateRelease, Long.MAX_VALUE);
        assertTrue(
                lateReleaseWindow.getWatermark() >= revisionAfterSurvivor,
                "the change stream covered the late release's whole window");
        assertFalse(
                removesPath(lateReleaseWindow, path),
                "the late release never removed the record at the shared path");

        // And the lock still excludes: a third contender cannot take over a live holder. Its
        // parked state is proven by revision arithmetic - rejected parent create, lease
        // registration, rejected lock create - and one further committed step of its own pacing
        // (with the clock frozen that step is a watchdog re-attempt) proves the retry machinery
        // keeps cycling. The negative assertion is anchored by the record-alive read right
        // before it: with the harness clock frozen nothing is due to expire, and a contender
        // cannot complete while the holder's record exists.
        final long revisionAtThirdContender = adminRevisionOf(cluster);
        final CompletableFuture<MetadataLock.Hold> thirdPending =
                newLock(contenderClient, path).acquireAsync();
        waitUntil(
                () -> adminRevisionOf(cluster) >= revisionAtThirdContender + 3,
                SETTLE,
                "The third contender never parked on the held record");
        final long parkedRevision = adminRevisionOf(cluster);
        waitUntil(
                () -> adminRevisionOf(cluster) > parkedRevision,
                SETTLE,
                "The parked contender's own pacing went silent - no re-attempt committed");
        assertEquals(
                takersLease,
                contenderClient.get(path).getLeaseId(),
                "a live holder's record still guards the lock after another holder's late"
                        + " release");
        assertFalse(
                thirdPending.isDone(),
                "a live holder still excludes later contenders after another holder's late"
                        + " release");

        second.release();
        final MetadataLock.Hold third = thirdPending.get(SETTLE.toMillis(), TimeUnit.MILLISECONDS);
        assertTrue(third.getToken() > second.getToken(), "the handover after release still works");
        third.release();
    }

    /**
     * THE fencing-inversion scenario: a stalled contender's lease-bound CREATE commits, its lease
     * dies before its token bump applies, and a new holder takes over with a token of its own -
     * the stalled contender's late bump must then be refused by the replicated guard (a refused
     * bump leaves no effect), so the next token strictly follows the incumbent's instead of
     * overtaking it. The lease's death is forced deterministically through the admin client, as
     * in {@code testLateReleaseByAnExpiredHolderCannotTakeTheNewHoldersRecord}.
     */
    @Test
    void testALateTokenIncrementCannotOvertakeTheNewHolder() throws Exception {
        final String path = "/locks/t1";
        holderClient.create("/locks", new byte[0], true);
        final LeaseSession stalled = holderClient.openLease(LEASE_TTL);
        assertEquals(path, holderClient.create(path, new byte[0], stalled));

        final CompletableFuture<MetadataLock.Hold> pending =
                newLock(contenderClient, path).acquireAsync();
        assertFalse(
                pending.isDone(),
                "the contender parks while the stalled contender's record exists");

        // the stall outlasted the ttl: the cascade frees the path and the waiter takes over
        adminExpireLease(stalled.getLeaseId());
        final MetadataLock.Hold incumbent =
                pending.get(SETTLE.toMillis(), TimeUnit.MILLISECONDS);
        final long incumbentToken = incumbent.getToken();

        // the stalled contender's own bump, issued after the takeover: refused, never applied
        final CompletableFuture<Long> lateBump = holderClient.incrementAsync(path, 1, stalled);
        final ExecutionException rejected =
                assertThrows(
                        ExecutionException.class,
                        () -> lateBump.get(SETTLE.toMillis(), TimeUnit.MILLISECONDS));
        assertTrue(
                rejected.getCause() instanceof RaftMetadataException,
                "the late bump is refused by the store, not lost in transport");
        assertEquals(
                RaftResult.Status.UNKNOWN_LEASE,
                ((RaftMetadataException) rejected.getCause()).getStatus());
        assertEquals(
                incumbentToken + 1,
                contenderClient.increment(path, 1),
                "the refused bump left no effect: the next token strictly follows the"
                        + " incumbent's");

        incumbent.release();
        final MetadataLock.Hold next = newLock(holderClient, path).acquire();
        assertTrue(
                next.getToken() > incumbentToken,
                "a fresh acquire's token still advances past the incumbent's");
        next.release();
    }

    /**
     * A waiter whose acquire times out while parked on {@code NODE_EXISTS} owns no record: its
     * unwind may not disturb the holder's. The holder keeps its record, a follow-up acquire still
     * blocks, and the release hands over cleanly.
     */
    @Test
    void testTimedOutWaiterLeavesTheHoldersRecordUntouched() throws Exception {
        final String path = "/locks/g2";
        final MetadataLock.Hold holder = newLock(holderClient, path).acquire();
        final long holdersLease = holderClient.get(path).getLeaseId();

        final MetadataLock waiter = newLock(contenderClient, path);
        final CompletableFuture<MetadataLock.Hold> pending =
                waiter.acquireAsync(Duration.ofMillis(300));
        final ExecutionException failure =
                assertThrows(
                        ExecutionException.class,
                        () -> pending.get(SETTLE.toMillis(), TimeUnit.MILLISECONDS));
        assertTrue(failure.getCause() instanceof TimeoutException, "the waiter times out");

        assertEquals(
                holdersLease,
                holderClient.get(path).getLeaseId(),
                "the holder's record is untouched by the waiter's unwind");
        assertTrue(holder.isHeld(), "the holder still holds");
        final CompletableFuture<MetadataLock.Hold> nextPending =
                newLock(contenderClient, path).acquireAsync();
        assertFalse(nextPending.isDone(), "the holder's record still guards the lock");

        holder.release();
        final MetadataLock.Hold next = nextPending.get(SETTLE.toMillis(), TimeUnit.MILLISECONDS);
        assertTrue(next.getToken() > holder.getToken());
        next.release();
    }

    /**
     * A release whose cleanup cannot reach the cluster (the client closed first) falls back to the
     * crash-release path: no exception escapes, and the record dies through the leader's expiry
     * scheduler within one ttl, leaving the path acquirable again.
     */
    @Test
    void testReleaseOnAClosedClientStillFreesThePathWithinOneTtl() throws Exception {
        final String path = "/locks/g3";
        final MetadataLock.Hold holder = newLock(holderClient, path).acquire();

        holderClient.close();
        holder.release(); // must not throw even though the cleanup has no client to go through
        assertFalse(holder.isHeld());

        cluster.clock.advanceTime(LEASE_TTL.plus(LEASE_TTL.dividedBy(3)).plusSeconds(1).toMillis(),
                TimeUnit.MILLISECONDS);
        final RaftMetadataClient reopener = cluster.newMetadataClient();
        try {
            waitUntil(
                    () -> {
                        try {
                            reopener.get(path);
                            return false;
                        } catch (RaftMetadataException notFound) {
                            return notFound.getStatus() == RaftResult.Status.NOT_FOUND;
                        }
                    },
                    SETTLE,
                    "The released path was not freed within one ttl");
            final MetadataLock.Hold next =
                    newLock(reopener, path).acquire(Duration.ofSeconds(10));
            next.release();
        } finally {
            closeQuietly(reopener);
        }
    }

    /**
     * A waiter parked on {@code NODE_EXISTS} survives a retention-line advance: its watcher
     * stops with NEED_SNAPSHOT, reports, resumes from the line - the restart the lock requests
     * from inside the report, arbitrated last-wins - and keeps waiting, so the holder's
     * release still hands over. The purge cluster makes the snapshot advance the retention
     * line deterministically past the waiter's parked watch position.
     */
    @Test
    void testWaiterSurvivesARetentionLineAdvanceWhileParked() throws Exception {
        final File purgeDir = new File(tempDir, "purge-cluster");
        assertTrue(purgeDir.mkdirs(), "the purge cluster needs its own storage dir");
        try (MetadataCluster purged =
                MetadataCluster.newBuilder(purgeDir)
                        .setNumServers(3)
                        .setPurgeForSnapshot(true)
                        .build()) {
            purged.start();
            final RaftMetadataClient holderOnPurged = purged.newMetadataClient();
            final RaftMetadataClient contenderOnPurged = purged.newMetadataClient();
            try {
                final MetadataLock.Hold holder = newLock(holderOnPurged, "/locks/h").acquire();

                final long revisionAtPark = adminRevisionOf(purged);
                final CompletableFuture<MetadataLock.Hold> pending =
                        newLock(contenderOnPurged, "/locks/h").acquireAsync();
                // Parked provably: the contender's lease registration and its rejected create
                // both appended entries beyond the revision it took as its watch position.
                waitUntil(
                        () -> adminRevisionOf(purged) >= revisionAtPark + 2,
                        SETTLE,
                        "The contender never parked on the held record");
                assertFalse(pending.isDone(), "the contender waits while the record exists");

                // The snapshot purges below its index, so the retention line jumps past the
                // parked watch position; the watcher must report, resume and re-park.
                final long snapshotIndex = purged.takeSnapshotOnLeader();
                assertTrue(
                        snapshotIndex >= revisionAtPark + 2,
                        "the retention line passed the parked watch position");
                waitUntil(
                        () -> adminRevisionOf(purged) >= revisionAtPark + 3,
                        SETTLE,
                        "The waiter did not resume from the retention line and re-attempt");
                assertFalse(
                        pending.isDone(),
                        "the contender still waits after resuming from the retention line");

                holder.release();
                final MetadataLock.Hold taker =
                        pending.get(SETTLE.toMillis(), TimeUnit.MILLISECONDS);
                assertTrue(taker.getToken() > holder.getToken(), "the handover still works");
                taker.release();
            } finally {
                closeQuietly(contenderOnPurged);
                closeQuietly(holderOnPurged);
            }
        }
    }

    /** The leader's applied revision, read through the admin client. */
    private static long adminRevisionOf(MetadataCluster cluster) throws Exception {
        final RaftCommand revision = RaftCommand.revision();
        final RaftClientReply reply =
                cluster.adminClient()
                        .io()
                        .sendReadOnly(
                                Message.valueOf(
                                        ByteBuffer.wrap(
                                                RaftCommandCodec.encodeCommand(revision))));
        final RaftResult result =
                RaftCommandCodec.decodeResult(reply.getMessage().getContent().toByteArray());
        assertEquals(RaftResult.Status.OK, result.getStatus());
        return result.getRevision();
    }

    /**
     * Whether any change in {@code window} removes the record at {@code path}: an explicit
     * delete, or a subtree delete covering it - the two mutations that could take a later
     * holder's record at the shared path.
     */
    private static boolean removesPath(RaftMetadataClient.PollResult window, String path) {
        for (ChangeEvent event : window.getRecords()) {
            for (ChangeEvent.Change change : event.getChanges()) {
                if (change.getOp() == ChangeEvent.ChangeOp.DELETE
                        && path.equals(change.getPath())) {
                    return true;
                }
                if (change.getOp() == ChangeEvent.ChangeOp.DELETE_SUBTREE
                        && (path.equals(change.getPath())
                                || path.startsWith(change.getPath() + "/"))) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Delete through the admin client - an external actor bypassing the lock protocol; with
     * {@code recursive} the whole subtree goes and the change stream reports one
     * {@code DELETE_SUBTREE} change at the root.
     */
    private void adminDelete(String path, boolean recursive) throws Exception {
        final RaftCommand delete = RaftCommand.delete(path, recursive);
        final RaftClientReply reply =
                cluster.adminClient()
                        .io()
                        .send(
                                Message.valueOf(
                                        ByteBuffer.wrap(RaftCommandCodec.encodeCommand(delete))));
        final RaftResult result =
                RaftCommandCodec.decodeResult(reply.getMessage().getContent().toByteArray());
        assertEquals(RaftResult.Status.OK, result.getStatus());
    }

    /**
     * Expire {@code leaseId} unconditionally through the admin client - the very command the
     * leader's scheduler proposes in production; replies map like any metadata result.
     */
    private void adminExpireLease(long leaseId) throws Exception {
        final RaftCommand expire = RaftCommand.leaseExpire(leaseId, -1L);
        final RaftClientReply reply =
                cluster.adminClient()
                        .io()
                        .send(
                                Message.valueOf(
                                        ByteBuffer.wrap(RaftCommandCodec.encodeCommand(expire))));
        final RaftResult result =
                RaftCommandCodec.decodeResult(reply.getMessage().getContent().toByteArray());
        assertEquals(RaftResult.Status.OK, result.getStatus());
        assertTrue(result.isExpired(), "the forced expiry removed the lease");
    }

    private static void closeQuietly(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception ignored) {
            // best-effort teardown
        }
    }
}
