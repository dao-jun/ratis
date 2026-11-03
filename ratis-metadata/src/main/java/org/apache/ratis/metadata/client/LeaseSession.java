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
package org.apache.ratis.metadata.client;

import org.apache.ratis.metadata.statemachine.RaftResult;
import org.apache.ratis.metadata.util.Clock;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

final class LeaseSession implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(LeaseSession.class);

    /** The longest gap between two due-ness checks, whatever the ttl. */
    private static final long MAX_TICK_MS = 200;

    /** The shortest gap between two due-ness checks, so short ttls still poll often enough. */
    private static final long MIN_TICK_MS = 10;

    /** How long {@link #close()} waits for an in-flight renewal proposal to settle. */
    private static final long CLOSE_JOIN_MS = TimeUnit.SECONDS.toMillis(2);

    private final RaftMetadataClient client;
    private final long leaseId;
    private final long ttlMs;
    private final Clock clock;

    /** The clock reading the last accepted renewal (or the registration) happened at. */
    private long renewBaseMs;

    private final Object lock = new Object();
    private boolean started;
    private boolean stopped;
    private Thread renewalThread;

    /**
     * Set once a renewal came back {@code UNKNOWN_LEASE}; volatile - the renewal thread writes,
     * {@link #isAlive()} reads.
     */
    private volatile boolean expired;

    LeaseSession(RaftMetadataClient client, long leaseId, long ttlMs, Clock clock) {
        this.client = client;
        this.leaseId = leaseId;
        this.ttlMs = ttlMs;
        this.clock = clock;
        // The registration itself is the first "renewal"; the session's clock-base starts there.
        this.renewBaseMs = clock.milliseconds();
    }

    /** The lease id this session keeps alive; bind ephemeral records to it. */
    public long getLeaseId() {
        return leaseId;
    }

    /**
     * Whether this session still speaks for a live lease; false after {@link #close()} or an {@code
     * UNKNOWN_LEASE} renewal.
     */
    public boolean isAlive() {
        synchronized (lock) {
            return started && !stopped && !expired;
        }
    }

    /** Starts the renewal thread; called by {@link RaftMetadataClient#openLease(Duration)}. */
    void start() {
        synchronized (lock) {
            if (started) {
                return;
            }
            started = true;
            renewalThread = new Thread(this::run, "raft-lease-session-" + leaseId);
            renewalThread.setDaemon(true);
            renewalThread.start();
        }
    }

    /**
     * Stops the renewals - the lease then dies through the leader's scheduler - and detaches from
     * the client; idempotent.
     */
    @Override
    public void close() {
        final Thread thread;
        synchronized (lock) {
            if (stopped) {
                return;
            }
            stopped = true;
            thread = renewalThread;
            lock.notifyAll();
        }
        client.sessions.remove(this);
        if (thread != null) {
            try {
                thread.join(CLOSE_JOIN_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void run() {
        final long renewIntervalMs = Math.max(1, ttlMs / 3);
        final long tickMs = Math.max(MIN_TICK_MS, Math.min(MAX_TICK_MS, renewIntervalMs / 8));
        while (true) {
            final boolean due;
            synchronized (lock) {
                if (stopped) {
                    return;
                }
                due = clock.milliseconds() - renewBaseMs >= renewIntervalMs;
                if (!due) {
                    try {
                        lock.wait(tickMs);
                    } catch (InterruptedException e) {
                        // close() interrupted us; re-check the stop flag.
                        Thread.currentThread().interrupt();
                    }
                }
            }
            if (due && !renew()) {
                return;
            }
        }
    }

    private boolean renew() {
        try {
            client.renewLease(leaseId, Duration.ofMillis(ttlMs));
            renewBaseMs = clock.milliseconds();
            return true;
        } catch (RaftMetadataException e) {
            if (e.getStatus() == RaftResult.Status.UNKNOWN_LEASE) {
                // The lease expired underneath and renewing cannot resurrect it - stop quietly.
                expired = true;
                LOG.warn(
                        "Lease {} expired before its session could renew it; stopping renewals",
                        leaseId);
                return false;
            }
            // No other protocol failure is expected from a renewal; retry on the next tick.
            LOG.debug("Renewing lease {} failed with {}; retrying", leaseId, e.getStatus(), e);
            return true;
        } catch (MetadataTransportException | IllegalStateException e) {
            // The client closed under us (IllegalStateException, or ratis AlreadyClosed) - stop.
            if (e instanceof IllegalStateException
                    || e.getCause() instanceof AlreadyClosedException) {
                return false;
            }
            // Transport trouble: retry on the next tick - a renewal within one ttl keeps the lease
            // alive.
            LOG.debug("Renewing lease {} failed; retrying", leaseId, e);
            return true;
        }
    }

    /** The lease id and ttl, for logs. */
    @Override
    public String toString() {
        return "LeaseSession{leaseId=" + leaseId + ", ttlMs=" + ttlMs + "}";
    }
}
