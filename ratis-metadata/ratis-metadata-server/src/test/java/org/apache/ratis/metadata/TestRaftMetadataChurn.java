/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.metadata;

import org.apache.ratis.metadata.client.LeaseSession;
import org.apache.ratis.metadata.client.MetadataTransportException;
import org.apache.ratis.metadata.client.MetadataWatcher;
import org.apache.ratis.metadata.client.RaftMetadataClient;
import org.apache.ratis.metadata.client.RaftMetadataException;
import org.apache.ratis.metadata.lock.MetadataLock;
import org.apache.ratis.metadata.protocol.ChangeEvent;
import org.apache.ratis.metadata.protocol.RaftCommand;
import org.apache.ratis.metadata.protocol.RaftCommandCodec;
import org.apache.ratis.metadata.protocol.RaftResult;
import org.apache.ratis.metadata.util.AsyncDriver;
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
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.apache.ratis.metadata.util.TestWait.waitUntil;

/**
 * The cross-recipe churn suite: every client-side recipe at once - plain writers with retries,
 * two fenced {@link MetadataLock} contender pairs, short-ttl {@link LeaseSession}s with bound
 * ephemeral records, and one revision-0 {@link MetadataWatcher} - against one 3-node cluster
 * whose leader is repeatedly closed and restarted (with an occasional follower bounce), all
 * under the harness's shared {@link org.apache.ratis.metadata.util.ManualClock} driven in real
 * time.
 *
 * <p>The suite asserts the interaction invariants: acknowledged mutations apply exactly once
 * (version chains, counter chains, acked-create/delete presence), the watcher's delivered
 * revisions never regress or duplicate across leader changes (NEED_SNAPSHOT stops recover via
 * {@code restartFrom}), lease-bound records of sessions that keep renewing survive the churn,
 * lock tokens on one path strictly increase across grants, and after the churn stops every
 * background step keeps making progress on the shared {@code AsyncDriver}.
 */
public class TestRaftMetadataChurn {

    /** The churn phase; the settle phase follows. Both fit far inside the surefire cap. */
    private static final Duration CHURN_PHASE = Duration.ofSeconds(55);
    private static final Duration JOIN_BUDGET = Duration.ofSeconds(35);
    private static final Duration ACQUIRE_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration LOCK_TTL = Duration.ofSeconds(6);
    private static final Duration SESSION_TTL = Duration.ofSeconds(6);
    private static final long POLL_BUDGET_BYTES = 4 * 1024 * 1024;

    private static final int WRITERS = 3;
    private static final String[] LOCK_PATHS = {"/churn/locks/l1", "/churn/locks/l2"};

    @TempDir File tempDir;

    private final AtomicBoolean stopWriters = new AtomicBoolean();
    private final AtomicBoolean stopChurn = new AtomicBoolean();
    private final AtomicBoolean stopClock = new AtomicBoolean();
    private final List<String> violations = new CopyOnWriteArrayList<>();
    private final List<String> churnLog = new CopyOnWriteArrayList<>();

    private void violation(String what) {
        violations.add(what);
    }

    private void churnEvent(String what) {
        churnLog.add(what);
        System.out.println("[churn] " + what);
    }

    @Test
    void testAllRecipesUnderRepeatedLeaderChurn() throws Exception {
        try (MetadataCluster cluster = MetadataCluster.newBuilder(tempDir)
                .setNumServers(3)
                .setPurgeForSnapshot(true)
                .build()) {
            cluster.start();
            cluster.awaitLeader();

            // One client per recipe lane: sync writers, one per lock contender, the watcher,
            // the session holder and a read-only monitor. All share one AsyncDriver.
            final RaftMetadataClient writerClient = cluster.newMetadataClient();
            final RaftMetadataClient watcherClient = cluster.newMetadataClient();
            final RaftMetadataClient sessionClient = cluster.newMetadataClient();
            final RaftMetadataClient monitorClient = cluster.newMetadataClient();
            final RaftMetadataClient[] lockClients = {
                cluster.newMetadataClient(), cluster.newMetadataClient(),
                cluster.newMetadataClient(), cluster.newMetadataClient()
            };
            try {
                // ---------------- the shared clock runs in real time ----------------
                final Thread clockDriver = new Thread(() -> {
                    while (!stopClock.get()) {
                        cluster.clock.advanceTime(25, TimeUnit.MILLISECONDS);
                        try {
                            Thread.sleep(25);
                        } catch (InterruptedException e) {
                            return;
                        }
                    }
                }, "churn-clock");
                clockDriver.setDaemon(true);
                clockDriver.start();

                // ---------------- setup namespaces ----------------
                writerClient.create("/churn", bytes("dir"));
                writerClient.create("/churn/eph", bytes("dir"));
                for (int w = 0; w < WRITERS; w++) {
                    writerClient.create("/churn/w" + w, bytes("dir"));
                }

                // ---------------- the watcher from revision 0 ----------------
                final AtomicReference<MetadataWatcher> watcherRef = new AtomicReference<>();
                final WatchLog watchLog = new WatchLog();
                final MetadataWatcher watcher =
                        MetadataWatcher.newBuilder(watcherClient, watchLog.listener(watcherRef))
                                .setSinceRevision(0)
                                .setPollInterval(Duration.ofMillis(100))
                                .setMaxPollBytes(POLL_BUDGET_BYTES)
                                .build();
                watcherRef.set(watcher);
                watcher.start();

                // ---------------- lease sessions with bound ephemeral records ----------------
                final LeaseSession[] sessions = {
                    sessionClient.openLease(SESSION_TTL), sessionClient.openLease(SESSION_TTL)
                };
                final String[][] boundPaths = {
                    {"/churn/eph/s0-a", "/churn/eph/s0-b"}, {"/churn/eph/s1-a"}
                };
                for (int s = 0; s < sessions.length; s++) {
                    for (String path : boundPaths[s]) {
                        assertEquals(
                                path,
                                sessionClient.create(path, bytes("ephemeral"), sessions[s]),
                                "the bound record of session " + s + " must create");
                    }
                }

                // ---------------- workers ----------------
                final List<Writer> accountants = new ArrayList<>();
                final List<Thread> workers = new ArrayList<>();
                for (int w = 0; w < WRITERS; w++) {
                    final Writer writer = new Writer(w, writerClient);
                    accountants.add(writer);
                    final Thread t = new Thread(writer, "churn-writer-" + w);
                    t.start();
                    workers.add(t);
                }
                final LockTracker tracker = new LockTracker();
                final AtomicBoolean stopLocks = new AtomicBoolean();
                final List<Thread> contenders = new ArrayList<>();
                for (int c = 0; c < 4; c++) {
                    final Thread t =
                            new Thread(
                                    new LockContender(
                                            c, LOCK_PATHS[c % 2], lockClients[c], tracker, stopLocks),
                                    "churn-lock-" + c);
                    t.start();
                    contenders.add(t);
                }
                final ConcurrentLinkedQueue<String> absences = new ConcurrentLinkedQueue<>();
                final SessionMonitor monitor =
                        new SessionMonitor(monitorClient, sessions, boundPaths, absences);
                final Thread monitorThread = new Thread(monitor, "churn-session-monitor");
                monitorThread.start();
                final Diagnostics diagnostics =
                        new Diagnostics(cluster, sessions, sessionClient, monitorClient);
                diagnostics.start();
                final Thread churn = new Thread(new ChurnDriver(cluster), "churn-leader-killer");
                churn.start();

                // ---------------- the churn phase ----------------
                Thread.sleep(CHURN_PHASE.toMillis());
                stopChurn.set(true);
                churn.join(JOIN_BUDGET.toMillis());
                assertFalse(churn.isAlive(), "the churn thread never finished");
                assertEquals(
                        3,
                        cluster.livePeerIds().size(),
                        "every member must be back up after the churn");
                cluster.awaitLeader();

                // Watcher progress while writers still run: the poll chain keeps advancing.
                // A poll dispatched into the tail of the last leader close may legitimately
                // sit out one ratis in-call retry (~9.5s worst case) before it lands on the
                // new leader - only an indefinite stall is a defect. Grace, then triage.
                final long cpA = watcher.lastDeliveredRevision();
                Thread.sleep(1500);
                long cpB = watcher.lastDeliveredRevision();
                if (cpB <= cpA) {
                    final long graceDeadline = System.currentTimeMillis() + 10_000;
                    while (cpB <= cpA && System.currentTimeMillis() < graceDeadline) {
                        Thread.sleep(200);
                        cpB = watcher.lastDeliveredRevision();
                    }
                    if (cpB <= cpA) {
                        // Still frozen after a full retry cycle: dump everything the later
                        // stopAndReport() would never reach, plus the watcher's own state and
                        // a same-client control read to tell a poll-chain stall from a dead
                        // pipeline.
                        diagnostics.stopAndReport();
                        final long t0 = System.currentTimeMillis();
                        String control;
                        try {
                            control = "revisionAsync=" + watcherClient.revisionAsync()
                                    + " in " + (System.currentTimeMillis() - t0) + "ms";
                        } catch (Exception e) {
                            control = "revisionAsync failed after "
                                    + (System.currentTimeMillis() - t0) + "ms: " + e;
                        }
                        churnEvent("WATCHER-STALL cpA=" + cpA + " cpB=" + cpB
                                + " running=" + watcher.isRunning()
                                + " failure=" + watcher.getFailure()
                                + " control(" + control + ")");
                    } else {
                        churnEvent("watcher recovered after a slow tail poll: cpA=" + cpA
                                + " cpB=" + cpB);
                    }
                }
                assertTrue(cpB > cpA, "the watcher stopped advancing after the churn: " + cpA);

                // ---------------- settle: writers and contenders drain ----------------
                stopWriters.set(true);
                for (Thread t : workers) {
                    t.join(JOIN_BUDGET.toMillis());
                    assertFalse(t.isAlive(), "a writer never drained: " + t.getName());
                }
                stopLocks.set(true);
                for (Thread t : contenders) {
                    t.join(JOIN_BUDGET.toMillis() + ACQUIRE_TIMEOUT.toMillis());
                    assertFalse(t.isAlive(), "a lock contender never drained: " + t.getName());
                }
                monitor.stop();
                monitorThread.join(5000);

                // A final acknowledged mutation must be visible and watched.
                writerClient.put("/churn/final-sentinel", bytes("done"));
                waitUntil(
                        () -> watchLog.sawPath("/churn/final-sentinel"),
                        Duration.ofSeconds(15),
                        "the watcher never delivered the post-churn sentinel; running="
                                + watcher.isRunning()
                                + ", failure="
                                + watcher.getFailure()
                                + ", "
                                + watchLog.summary());
                assertTrue(watcher.isRunning(), "the watcher chain must still be running");
                assertNull(watcher.getFailure(), "the watcher must carry no failure");

                // ---------------- assertions ----------------
                diagnostics.stopAndReport();
                assertTrue(violations.isEmpty(), "interaction violations: " + violations);
                watchLog.assertInvariants();
                tracker.assertInvariants();
                for (Writer accountant : accountants) {
                    accountant.assertFinalState(monitorClient);
                }

                // Sessions that kept renewing through the churn keep their bound records: a
                // record of a live session that vanished is an expiry-cascade defect, and a
                // session that dies although at most one member was ever down at a time is a
                // renewal starvation defect.
                for (int s = 0; s < sessions.length; s++) {
                    assertTrue(
                            sessions[s].isAlive(),
                            "session " + s + " stopped renewing through single-out churn; "
                                    + diagnostics.summary());
                    for (String path : boundPaths[s]) {
                        assertTrue(
                                monitorClient.exists(path),
                                "the bound record " + path + " of a live session vanished");
                    }
                }
                assertTrue(
                        absences.isEmpty(),
                        "bound records went absent while their sessions stayed alive: "
                                + absences);

                assertEquals(1L, writerClient.increment("/churn/settle-counter", 1));
                assertEquals(
                        2L,
                        writerClient.increment("/churn/settle-counter", 1),
                        "post-churn writes and reads still work");

                watcher.close();
                System.out.println("[churn] " + watchLog.summary());
                System.out.println("[churn] " + tracker.summary());
                System.out.println("[churn] lock grant overlaps: " + tracker.overlaps());
                System.out.println("[churn] log: " + churnLog);
            } finally {
                stopWriters.set(true);
                stopChurn.set(true);
                stopClock.set(true);
                closeQuietly(writerClient);
                closeQuietly(watcherClient);
                closeQuietly(sessionClient);
                closeQuietly(monitorClient);
                for (RaftMetadataClient client : lockClients) {
                    closeQuietly(client);
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // plain writers: exactly-once accounting on version and counter chains
    // ------------------------------------------------------------------

    private final class Writer implements Runnable {

        private final int id;
        private final RaftMetadataClient client;
        private final NavigableSet<String> created = new ConcurrentSkipListSet<>();
        private final Set<String> deleted = ConcurrentHashMap.newKeySet();
        private final Set<String> deletedUnknown = ConcurrentHashMap.newKeySet();

        private long putsAcked;
        private long lastPutVersion;
        private int putUnknown;
        private long incrementsAcked;
        private long lastIncrementValue;
        private int incrementUnknown;
        private long multisAcked;
        private int multiUnknown;
        private int op;

        Writer(int id, RaftMetadataClient client) {
            this.id = id;
            this.client = client;
        }

        @Override
        public void run() {
            final String base = "/churn/w" + id;
            try {
                while (!stopWriters.get()) {
                    final int kind = op++ % 5;
                    if (kind == 0) {
                        try {
                            final long v =
                                    client.put(base + "/val", bytes("w" + id + "-" + putsAcked));
                            // Each acked put bumps the version by exactly one; calls whose
                            // outcome stayed unknown (transport) may have applied unbeknownst.
                            final long min = putsAcked == 0 ? 1L : lastPutVersion + 1;
                            final long max = (putsAcked == 0 ? 0L : lastPutVersion) + 1 + putUnknown;
                            if (v < min || v > max) {
                                violation(
                                        "writer " + id + ": put version chain broke: "
                                                + lastPutVersion + " -> " + v
                                                + " (+" + putUnknown + " unknown)");
                            }
                            lastPutVersion = v;
                            putUnknown = 0;
                            putsAcked++;
                        } catch (MetadataTransportException e) {
                            putUnknown++;
                        }
                    } else if (kind == 1) {
                        try {
                            created.add(client.createSequential(base + "/item-", bytes("item")));
                        } catch (MetadataTransportException e) {
                            // unknown outcome: this create is not accounted
                        }
                    } else if (kind == 2) {
                        for (String candidate : created) {
                            if (deleted.contains(candidate) || deletedUnknown.contains(candidate)) {
                                continue;
                            }
                            deleted.add(candidate);
                            try {
                                client.delete(candidate);
                            } catch (RaftMetadataException e) {
                                violation(
                                        "writer " + id + ": delete of an acked create returned "
                                                + e.getStatus() + " for " + candidate);
                            } catch (MetadataTransportException e) {
                                deleted.remove(candidate);
                                deletedUnknown.add(candidate);
                            }
                            break;
                        }
                    } else if (kind == 3) {
                        try {
                            final List<RaftResult> applied =
                                    client.multi(
                                            Arrays.asList(
                                                    RaftCommand.put(base + "/m1", bytes("a"), 0L, 0L),
                                                    RaftCommand.put(base + "/m2", bytes("b"), 0L, 0L)));
                            assertEquals(2, applied.size(), "the multi returns one result per op");
                            for (RaftResult result : applied) {
                                assertEquals(RaftResult.Status.OK, result.getStatus());
                            }
                            multisAcked++;
                        } catch (MetadataTransportException e) {
                            multiUnknown++;
                        }
                    } else {
                        try {
                            final long v = client.increment("/churn/cnt" + id, 1);
                            // Single writer: every acked increment moves the counter by exactly
                            // one, whatever unknown-outcome increments did in between.
                            if (v < lastIncrementValue + 1
                                    || v > lastIncrementValue + 1 + incrementUnknown) {
                                violation(
                                        "writer " + id + ": increment chain broke: "
                                                + lastIncrementValue + " -> " + v + " after "
                                                + incrementsAcked + " acked increments (+"
                                                + incrementUnknown + " unknown)");
                            }
                            lastIncrementValue = v;
                            incrementUnknown = 0;
                            incrementsAcked++;
                        } catch (MetadataTransportException e) {
                            incrementUnknown++;
                        }
                    }
                }
            } catch (Throwable t) {
                violation("writer " + id + " died: " + t);
            }
        }

        void assertFinalState(RaftMetadataClient reader) {
            if (putsAcked > 0 || putUnknown > 0) {
                final long version = reader.get("/churn/w" + id + "/val").getVersion();
                assertTrue(
                        version >= lastPutVersion && version <= lastPutVersion + putUnknown,
                        "writer " + id + ": /val version " + version + " vs last acked "
                                + lastPutVersion + " (+" + putUnknown + " unknown)");
            }
            if (multisAcked > 0 || multiUnknown > 0) {
                final long m1 = reader.get("/churn/w" + id + "/m1").getVersion();
                final long m2 = reader.get("/churn/w" + id + "/m2").getVersion();
                assertTrue(
                        m1 >= multisAcked && m1 <= multisAcked + multiUnknown
                                && m2 >= multisAcked && m2 <= multisAcked + multiUnknown,
                        "writer " + id + ": multi targets at " + m1 + "/" + m2 + " vs "
                                + multisAcked + " acked multis (+" + multiUnknown + " unknown)");
            }
            if (incrementsAcked > 0 || lastIncrementValue > 0) {
                final long finalValue = reader.increment("/churn/cnt" + id, 0);
                assertTrue(
                        finalValue >= lastIncrementValue
                                && finalValue <= lastIncrementValue + incrementUnknown,
                        "writer " + id + ": counter at " + finalValue + " vs last acked "
                                + lastIncrementValue + " (+" + incrementUnknown + " unknown)");
            }
            for (String path : created) {
                if (deletedUnknown.contains(path)) {
                    continue; // outcome unknown: either final state is legal
                }
                if (deleted.contains(path)) {
                    assertFalse(
                            reader.exists(path),
                            "a deleted acked create still exists: " + path);
                } else {
                    assertTrue(reader.exists(path), "an acked create is missing: " + path);
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // the lock contenders
    // ------------------------------------------------------------------

    private final class LockContender implements Runnable {

        private final int id;
        private final String path;
        private final RaftMetadataClient client;
        private final LockTracker tracker;
        private final AtomicBoolean stop;

        LockContender(
                int id, String path, RaftMetadataClient client, LockTracker tracker,
                AtomicBoolean stop) {
            this.id = id;
            this.path = path;
            this.client = client;
            this.tracker = tracker;
            this.stop = stop;
        }

        @Override
        public void run() {
            try {
                while (!stop.get()) {
                    cycle();
                }
                cycle(); // one settled cycle after the churn: pending acquires still complete
            } catch (Throwable t) {
                violation("lock contender " + id + " died: " + t);
            }
        }

        private void cycle() throws Exception {
            final MetadataLock lock =
                    MetadataLock.newBuilder()
                            .setClient(client)
                            .setPath(path)
                            .setTtl(LOCK_TTL)
                            .build();
            final MetadataLock.Hold hold;
            try {
                hold = lock
                        .acquireAsync(ACQUIRE_TIMEOUT)
                        .get(ACQUIRE_TIMEOUT.toMillis() + 5000, TimeUnit.MILLISECONDS);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof TimeoutException) {
                    return; // a churn window swallowed this cycle's deadline
                }
                violation("lock contender " + id + " acquire failed: " + e.getCause());
                return;
            } catch (TimeoutException e) {
                return;
            }
            final long token = hold.getToken();
            final LockTracker.Grant grant = tracker.grant(id, path, token);
            try {
                // The fenced write under this holder's token: the server may legitimately
                // answer STALE_TERM once raft terms outgrow the small token values.
                client.multi(
                        Collections.singletonList(
                                RaftCommand.put(
                                        path + "-owned", bytes(id + ":" + token), 0L, 0L)),
                        token);
                tracker.wroteUnder(grant);
            } catch (RaftMetadataException e) {
                if (e.getStatus() != RaftResult.Status.STALE_TERM) {
                    violation(
                            "fenced write of holder " + id + " (token " + token + ") rejected: "
                                    + e.getStatus());
                } else {
                    tracker.countStaleTerm();
                }
            }
            Thread.sleep(60 + (id * 37) % 90);
            tracker.release(grant, hold.isHeld());
            hold.release();
            lock.close();
        }
    }

    /** Grants per lock path: token order and live-window overlap evidence. */
    private static final class LockTracker {

        final List<Grant> grants = new CopyOnWriteArrayList<>();
        int staleTermWrites;

        synchronized Grant grant(int contender, String path, long token) {
            final Grant g = new Grant(contender, path, token, System.nanoTime());
            grants.add(g);
            return g;
        }

        synchronized void wroteUnder(Grant g) {
            g.wrote = true;
        }

        synchronized void release(Grant g, boolean aliveAtRelease) {
            g.aliveAtRelease = aliveAtRelease;
            g.releaseNanos = System.nanoTime();
        }

        synchronized void countStaleTerm() {
            staleTermWrites++;
        }

        synchronized void assertInvariants() {
            for (String path : Arrays.asList(LOCK_PATHS)) {
                long lastToken = 0;
                for (Grant g : grants) {
                    if (!g.path.equals(path)) {
                        continue;
                    }
                    assertTrue(
                            g.token > lastToken,
                            "fencing tokens on " + path + " must strictly increase across grants:"
                                    + " " + lastToken + " -> " + g.token + " (all: " + grants + ")");
                    lastToken = g.token;
                }
            }
        }

        /** The live-window overlaps, for triage against the known delayed-token zombie. */
        synchronized List<String> overlaps() {
            final List<String> found = new ArrayList<>();
            for (Grant a : grants) {
                for (Grant b : grants) {
                    if (a == b || !a.path.equals(b.path) || a.releaseNanos == 0) {
                        continue;
                    }
                    if (a.grantNanos < b.grantNanos && a.releaseNanos > b.grantNanos) {
                        found.add(
                                a.contender + "(token=" + a.token + ", aliveAtRelease="
                                        + a.aliveAtRelease + ") overlapped " + b.contender
                                        + "(token=" + b.token + ", aliveAtRelease="
                                        + b.aliveAtRelease + ")");
                    }
                }
            }
            return found;
        }

        synchronized String summary() {
            return grants.size() + " grants, " + staleTermWrites + " STALE_TERM fenced writes";
        }

        static final class Grant {
            final int contender;
            final String path;
            final long token;
            final long grantNanos;
            volatile long releaseNanos;
            volatile boolean aliveAtRelease;
            volatile boolean wrote;

            Grant(int contender, String path, long token, long grantNanos) {
                this.contender = contender;
                this.path = path;
                this.token = token;
                this.grantNanos = grantNanos;
            }

            @Override
            public String toString() {
                return "Grant{c=" + contender + ", token=" + token + ", wrote=" + wrote
                        + ", aliveAtRelease=" + aliveAtRelease + "}";
            }
        }
    }

    // ------------------------------------------------------------------
    // the watcher
    // ------------------------------------------------------------------

    private final class WatchLog {

        long lastDelivered;
        long deliveries;
        int needSnapshots;
        long pendingLine = -1;
        final List<String> paths = new CopyOnWriteArrayList<>();

        MetadataWatcher.Listener listener(AtomicReference<MetadataWatcher> watcherRef) {
            return new MetadataWatcher.Listener() {
                @Override
                public void onRecord(ChangeEvent event) {
                    synchronized (WatchLog.this) {
                        if (event.getRevision() <= lastDelivered) {
                            violation(
                                    "watcher delivery regressed or duplicated: "
                                            + event.getRevision() + " after " + lastDelivered);
                        }
                        if (pendingLine >= 0 && event.getRevision() <= pendingLine) {
                            violation(
                                    "watcher delivered revision " + event.getRevision()
                                            + " at or below the retention line " + pendingLine);
                        }
                        pendingLine = -1;
                        lastDelivered = event.getRevision();
                        deliveries++;
                        for (ChangeEvent.Change change : event.getChanges()) {
                            paths.add(change.getPath());
                        }
                    }
                }

                @Override
                public void onNeedSnapshot(long oldestRetainedRevision) {
                    synchronized (WatchLog.this) {
                        needSnapshots++;
                        pendingLine = Math.max(pendingLine, oldestRetainedRevision);
                    }
                    final MetadataWatcher current = watcherRef.get();
                    if (current != null) {
                        current.restartFrom(oldestRetainedRevision);
                    }
                }
            };
        }

        synchronized boolean sawPath(String path) {
            return paths.contains(path);
        }

        synchronized String summary() {
            return "deliveries=" + deliveries + ", lastRevision=" + lastDelivered
                    + ", needSnapshots=" + needSnapshots;
        }

        synchronized void assertInvariants() {
            assertTrue(lastDelivered > 0, "the watcher never delivered anything through the churn");
        }
    }

    // ------------------------------------------------------------------
    // the session monitor
    // ------------------------------------------------------------------

    private static final class SessionMonitor implements Runnable {

        private final RaftMetadataClient client;
        private final LeaseSession[] sessions;
        private final String[][] boundPaths;
        private final ConcurrentLinkedQueue<String> absences;
        private final AtomicBoolean stop = new AtomicBoolean();

        SessionMonitor(
                RaftMetadataClient client, LeaseSession[] sessions, String[][] boundPaths,
                ConcurrentLinkedQueue<String> absences) {
            this.client = client;
            this.sessions = sessions;
            this.boundPaths = boundPaths;
            this.absences = absences;
        }

        @Override
        public void run() {
            while (!stop.get()) {
                for (int s = 0; s < sessions.length; s++) {
                    if (!sessions[s].isAlive()) {
                        continue; // a dead session's records may legitimately cascade away
                    }
                    for (String path : boundPaths[s]) {
                        try {
                            if (!client.exists(path)) {
                                absences.add(path + " at " + System.currentTimeMillis());
                            }
                        } catch (MetadataTransportException transientFailure) {
                            // a read through a re-electing group: not evidence
                        }
                    }
                }
                try {
                    Thread.sleep(300);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }

        void stop() {
            stop.set(true);
        }
    }

    // ------------------------------------------------------------------
    // the churn driver
    // ------------------------------------------------------------------

    private final class ChurnDriver implements Runnable {

        private final MetadataCluster cluster;
        private int cycle;

        ChurnDriver(MetadataCluster cluster) {
            this.cluster = cluster;
        }

        @Override
        public void run() {
            try {
                while (!stopChurn.get()) {
                    final long cycleStart = System.currentTimeMillis();
                    final RaftPeerId leader = awaitLeaderPatiently();
                    final int leaderIndex = cluster.indexOf(leader);
                    final long killStart = System.currentTimeMillis();
                    cluster.killServer(leaderIndex);
                    final long killedAt = System.currentTimeMillis();
                    final List<RaftPeerId> survivors = cluster.livePeerIds();
                    final RaftPeerId next = awaitLeaderPatiently(survivors);
                    final long ledAt = System.currentTimeMillis();
                    cycle++;
                    churnEvent(
                            "cycle " + cycle + " @wall " + cycleStart + ": leader " + leader
                                    + " closed in " + (killedAt - killStart) + "ms, new leader "
                                    + next + " after " + (ledAt - killedAt) + "ms");
                    Thread.sleep(1200);
                    final long restartStart = System.currentTimeMillis();
                    cluster.restartServer(leaderIndex);
                    churnEvent(
                            "cycle " + cycle + ": old leader restarted in "
                                    + (System.currentTimeMillis() - restartStart) + "ms");
                    Thread.sleep(1200);
                    if (cycle % 3 == 0) {
                        final List<RaftPeerId> followers = cluster.livePeerIds();
                        followers.remove(next);
                        if (!followers.isEmpty()) {
                            final RaftPeerId follower =
                                    followers.get((cycle / 3) % followers.size());
                            final int followerIndex = cluster.indexOf(follower);
                            final long bounceStart = System.currentTimeMillis();
                            cluster.killServer(followerIndex);
                            Thread.sleep(1200);
                            cluster.restartServer(followerIndex);
                            churnEvent(
                                    "cycle " + cycle + ": follower " + follower + " bounced in "
                                            + (System.currentTimeMillis() - bounceStart) + "ms");
                        }
                    }
                    Thread.sleep(2500);
                }
            } catch (Throwable t) {
                if (!stopChurn.get()) {
                    violation("churn driver died: " + t);
                }
            }
        }

        /**
         * The harness's leader probe can throw a transient grpc {@code UNAVAILABLE} while a
         * killed member's port is still draining; only a leader that stays unfindable for
         * many seconds is a real failure, so the churn loop tolerates the transients.
         */
        private RaftPeerId awaitLeaderPatiently() throws InterruptedException {
            return awaitLeaderPatiently(null);
        }

        private RaftPeerId awaitLeaderPatiently(List<RaftPeerId> scope)
                throws InterruptedException {
            RuntimeException last = null;
            final long deadline = System.currentTimeMillis() + 15_000;
            while (System.currentTimeMillis() < deadline) {
                try {
                    return scope == null ? cluster.awaitLeader() : cluster.awaitLeader(scope);
                } catch (RuntimeException e) {
                    last = e;
                    Thread.sleep(250);
                }
            }
            throw last != null ? last
                    : new IllegalStateException("no leader within the patient budget");
        }
    }

    // ------------------------------------------------------------------
    // diagnostics: what the background machinery was doing while it churned
    // ------------------------------------------------------------------

    /**
     * Samples the run so a failing invariant can be attributed: a heartbeat on the shared
     * {@link AsyncDriver} (a stall parks every periodic step at once), the liveness of every
     * session, and each lease's replicated expiry as the admin client sees it (how long a
     * renewal went unlanded before a session died).
     */
    private static final class Diagnostics implements Runnable {

        private final MetadataCluster cluster;
        private final LeaseSession[] sessions;
        private final AtomicBoolean stop = new AtomicBoolean();
        private final AtomicLong lastBeat = new AtomicLong(System.currentTimeMillis());
        private final boolean[] aliveAtLastCheck;
        private final Map<Long, Long> lastExpireAt = new ConcurrentHashMap<>();
        private final Map<Long, Long> maxOverdue = new ConcurrentHashMap<>();
        private final Map<Long, Long> renewStallSince = new ConcurrentHashMap<>();
        private volatile long maxBeatGap;
        private long lastStallLogged;
        private final List<String> stalls = new CopyOnWriteArrayList<>();
        private final List<String> renewOverdue = new CopyOnWriteArrayList<>();
        private final List<String> renewStalls = new CopyOnWriteArrayList<>();
        private final List<String> deaths = new CopyOnWriteArrayList<>();
        private final List<String> leaderEvents = new CopyOnWriteArrayList<>();
        private String lastLeader = "<unset>";
        private final RaftMetadataClient sharedClient;
        private final RaftMetadataClient genericClient;
        private volatile long sharedMax;
        private volatile long genericMax;
        private final List<String> sharedLags = new CopyOnWriteArrayList<>();
        private final List<String> genericLags = new CopyOnWriteArrayList<>();

        Diagnostics(
                MetadataCluster cluster, LeaseSession[] sessions,
                RaftMetadataClient sharedClient, RaftMetadataClient genericClient) {
            this.cluster = cluster;
            this.sessions = sessions;
            this.sharedClient = sharedClient;
            this.genericClient = genericClient;
            this.aliveAtLastCheck = new boolean[sessions.length];
            Arrays.fill(aliveAtLastCheck, true);
        }

        void start() {
            AsyncDriver.schedule(this::beat, 50);
            final Thread t = new Thread(this, "churn-diagnostics");
            t.setDaemon(true);
            t.start();
        }

        /** Runs on the shared scheduler: its timestamp is the scheduler's heartbeat. */
        private void beat() {
            lastBeat.set(System.currentTimeMillis());
            if (!stop.get()) {
                AsyncDriver.schedule(this::beat, 50);
            }
        }

        @Override
        public void run() {
            int tick = 0;
            while (!stop.get()) {
                final long now = System.currentTimeMillis();
                final long clockMs = cluster.clock.milliseconds();
                final long gap = now - lastBeat.get();
                if (gap > maxBeatGap) {
                    maxBeatGap = gap;
                }
                if (gap > 300 && now - lastStallLogged > 1000) {
                    lastStallLogged = now;
                    stalls.add("scheduler stalled " + gap + "ms (wall=" + now + ", clock=" + clockMs + ")");
                }
                for (int s = 0; s < sessions.length; s++) {
                    final boolean alive = sessions[s].isAlive();
                    if (aliveAtLastCheck[s] && !alive) {
                        final Long stallStart = renewStallSince.get(sessions[s].getLeaseId());
                        final String death =
                                "session " + s + " (lease " + sessions[s].getLeaseId()
                                        + ") died at wall=" + now + ", clock=" + clockMs
                                        + ", lastExpireAt=" + lastExpireAt
                                        + ", maxOverdue=" + maxOverdue
                                        + ", renewalStallSince=" + stallStart
                                        + ", beatGap=" + gap;
                        deaths.add(death);
                        System.out.println("[churn] " + death);
                    }
                    aliveAtLastCheck[s] = alive;
                }
                if (tick++ % 5 == 0) {
                    sampleLeases(now, clockMs);
                    sampleLeader(now, clockMs);
                    probe(now);
                }
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }

        private void sampleLeases(long now, long clockMs) {
            try {
                final RaftResult listing =
                        decode(
                                cluster.adminClient()
                                        .io()
                                        .sendReadOnly(
                                                Message.valueOf(
                                                        ByteBuffer.wrap(
                                                                RaftCommandCodec.encodeCommand(
                                                                        RaftCommand.leaseList())))));
                if (listing == null || listing.getStatus() != RaftResult.Status.OK) {
                    return;
                }
                for (RaftResult.Lease lease : listing.getLeases()) {
                    final long leaseId = lease.getLeaseId();
                    if (leaseId == 0 || !known(leaseId)) {
                        continue;
                    }
                    final Long previous = lastExpireAt.put(leaseId, lease.getExpireAtMs());
                    final long overdue = clockMs - lease.getExpireAtMs();
                    final Long worst = maxOverdue.get(leaseId);
                    if (worst == null || overdue > worst) {
                        maxOverdue.put(leaseId, overdue);
                    }
                    if (overdue > SESSION_TTL.toMillis()) {
                        renewOverdue.add(
                                "lease " + leaseId + " overdue " + overdue + "ms at wall=" + now
                                        + ", clock=" + clockMs);
                    }
                    final Long stallStart = renewStallSince.get(leaseId);
                    if (stallStart != null) {
                        renewStalls.add(
                                "lease " + leaseId + " went " + (now - stallStart)
                                        + "ms without a renewal landing (episode from wall="
                                        + stallStart + "), recovered at clock=" + clockMs);
                        renewStallSince.remove(leaseId);
                    }
                    // A renewal should land every ttl/3; one and a half intervals without
                    // a landing is the start of a stall episode.
                    final long lastLandedClock = lease.getExpireAtMs() - SESSION_TTL.toMillis();
                    if (clockMs - lastLandedClock > SESSION_TTL.toMillis() / 2
                            && !renewStallSince.containsKey(leaseId)
                            && aliveOf(leaseId)) {
                        renewStallSince.put(leaseId, now);
                    }
                }
            } catch (Exception transientFailure) {
                // through a re-electing group: not evidence
            }
        }

        private void sampleLeader(long now, long clockMs) {
            try {
                final RaftPeerId leader = cluster.leaderId();
                final String current = leader == null ? "none" : leader.toString();
                if (!current.equals(lastLeader)) {
                    leaderEvents.add(
                            "wall=" + now + ", clock=" + clockMs + ", leader=" + current);
                    lastLeader = current;
                }
            } catch (Exception transientFailure) {
                // a poll through a re-electing or closing group: not evidence
            }
        }

        /**
         * One timed probe per sample: a read on the session client's own ordered pipeline, and
         * a zero-delta increment on a separate identical client. Their latencies attribute a
         * renewal stall either to the shared pipeline or to generic client retry behavior
         * against a closing leader.
         */
        private void probe(long now) {
            final long sharedStart = System.currentTimeMillis();
            sharedClient
                    .revisionAsync()
                    .whenComplete(
                            (ignored, failure) -> {
                                final long took = System.currentTimeMillis() - sharedStart;
                                if (took > sharedMax) {
                                    sharedMax = took;
                                }
                                if (took > 1000 && failure == null) {
                                    sharedLags.add("read on the session client took " + took
                                            + "ms starting wall=" + sharedStart);
                                }
                            });
            final long genericStart = System.currentTimeMillis();
            genericClient
                    .incrementAsync("/churn/probe", 0L)
                    .whenComplete(
                            (ignored, failure) -> {
                                final long took = System.currentTimeMillis() - genericStart;
                                if (took > genericMax) {
                                    genericMax = took;
                                }
                                if (took > 1000) {
                                    genericLags.add("write on the probe client took " + took
                                            + "ms starting wall=" + genericStart
                                            + (failure == null ? "" : ", failed: " + failure));
                                }
                            });
        }

        private boolean aliveOf(long leaseId) {
            for (LeaseSession session : sessions) {
                if (session.getLeaseId() == leaseId) {
                    return session.isAlive();
                }
            }
            return false;
        }

        private boolean known(long leaseId) {
            for (LeaseSession session : sessions) {
                if (session.getLeaseId() == leaseId) {
                    return true;
                }
            }
            return false;
        }

        private static RaftResult decode(RaftClientReply reply) {
            try {
                return RaftCommandCodec.decodeResult(
                        reply.getMessage().getContent().toByteArray());
            } catch (Exception undecodable) {
                return null;
            }
        }

        void stopAndReport() {
            stop.set(true);
            System.out.println("[churn] diagnostics: maxBeatGap=" + maxBeatGap + "ms");
            System.out.println("[churn] diagnostics: stalls=" + stalls);
            System.out.println("[churn] diagnostics: probeMax: sharedClientRead=" + sharedMax
                    + "ms, genericClientWrite=" + genericMax + "ms");
            System.out.println("[churn] diagnostics: sharedLags=" + sharedLags);
            System.out.println("[churn] diagnostics: genericLags=" + genericLags);
            System.out.println("[churn] diagnostics: renewOverdue=" + renewOverdue);
            System.out.println("[churn] diagnostics: renewStalls=" + renewStalls);
            System.out.println("[churn] diagnostics: leaderEvents=" + leaderEvents);
            System.out.println("[churn] diagnostics: deaths=" + deaths);
        }

        String summary() {
            return "maxBeatGap=" + maxBeatGap + "ms, stalls=" + stalls
                    + ", probeMax: sharedClientRead=" + sharedMax
                    + "ms, genericClientWrite=" + genericMax + "ms"
                    + ", sharedLags=" + sharedLags + ", genericLags=" + genericLags
                    + ", renewStalls=" + renewStalls + ", deaths=" + deaths
                    + ", leaderEvents=" + leaderEvents;
        }
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static void closeQuietly(RaftMetadataClient client) {
        try {
            client.close();
        } catch (Throwable ignored) {
            // best-effort teardown
        }
    }
}
