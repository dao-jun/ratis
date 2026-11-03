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
import org.apache.ratis.metadata.client.RaftMetadataClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
     * A holder that stops renewing (here: the frozen clock keeps every renewal undue) loses the
     * record through the leader's expiry scheduler; the waiter wakes on the record's delete. Its
     * own lease died with the same clock jump, so the take-over also exercises the reopen path.
     */
    @Test
    void testCrashReleaseExpiresRecordAndWaiterTakesOver() throws Exception {
        final MetadataLock.Hold holder = newLock(holderClient, "/locks/b").acquire();
        final CompletableFuture<MetadataLock.Hold> pending =
                newLock(contenderClient, "/locks/b").acquireAsync();
        assertFalse(pending.isDone(), "the contender waits while the holder's lease is renewed");

        cluster.clock.advanceTime(LEASE_TTL.plusSeconds(1).toMillis(), TimeUnit.MILLISECONDS);

        final MetadataLock.Hold taker = pending.get(SETTLE.toMillis(), TimeUnit.MILLISECONDS);
        assertTrue(taker.getToken() > holder.getToken(), "the waiter takes over with a higher token");
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

    private static void closeQuietly(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception ignored) {
            // best-effort teardown
        }
    }
}
