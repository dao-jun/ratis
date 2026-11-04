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
import org.apache.ratis.metadata.util.ExceptionUtils;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * A client-side handle keeping one registered lease alive - the ephemeral-record half of the
 * ZooKeeper session semantics this layer ports. Records written through a session (the {@code
 * LeaseSession} overloads of {@link RaftMetadataClient#put}/{@code create}) live exactly as long
 * as the lease does.
 *
 * <p>The session renews the lease every third of its ttl, judged by the monotonic
 * {@link Clock#nanoseconds()} - never by wall time, so a skewed or frozen wall clock can neither
 * starve nor flood the renewals. Renewals run on the module's shared scheduler and the client's
 * asynchronous machinery - a session owns no thread of its own, so any number of them costs no
 * extra threads. Transient transport trouble is retried, and any renewal landing within one ttl
 * keeps the lease alive - so a leader failover resolving inside the ttl is transparent. When
 * renewals stop - the holder crashed, the partition outlasted the ttl, an {@code UNKNOWN_LEASE}
 * renewal landed, or {@link #close()} ran - the session detaches from the client's registry and
 * the leader's expiry scheduler deletes every record bound to the lease after the ttl, emitting
 * {@code LEASE_EXPIRE} change events for watchers.
 *
 * <p>Lifecycle: obtained from {@link RaftMetadataClient#openLease(Duration)} or its async twin,
 * never constructed directly. {@link #close()} is idempotent and never blocks: it stops the
 * renewals and detaches from the client; a renewal still in flight completes and is ignored.
 * The bound records then die through the scheduler, at most one ttl later - closing does not
 * delete them outright. {@link #isAlive()} is the holder's checkpoint: false after {@link
 * #close()} or once a renewal returned {@code UNKNOWN_LEASE} (the lease expired underneath and
 * cannot be resurrected). {@link #close()} and {@link #isAlive()} may be called from any thread.
 */
public final class LeaseSession implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(LeaseSession.class);

    private final RaftMetadataClient client;
    private final long leaseId;
    private final long ttlMs;
    private final Clock clock;

    /**
     * The monotonic reading the last accepted renewal (or the registration) happened at; guarded
     * by {@link #lock}. Due-ness never reads wall milliseconds: a skewed or frozen wall clock
     * would starve or flood the renewals, while {@link Clock#nanoseconds()} only measures
     * elapsed time.
     */
    private long renewBaseNanos;

    /** One renewal interval in nanoseconds; a third of the ttl, never less than a millisecond. */
    private final long renewalIntervalNanos;

    private final Object lock = new Object();
    private boolean started;
    private boolean stopped;
    private boolean renewing;

    /**
     * Set once a renewal came back {@code UNKNOWN_LEASE}; volatile - the renewal completion
     * writes it, {@link #isAlive()} reads it.
     */
    private volatile boolean expired;

    LeaseSession(RaftMetadataClient client, long leaseId, long ttlMs, Clock clock) {
        this.client = client;
        this.leaseId = leaseId;
        this.ttlMs = ttlMs;
        this.clock = clock;
        // The registration itself is the first "renewal"; the session's clock-base starts there.
        this.renewBaseNanos = clock.nanoseconds();
        // Overflow-safe for any sane ttl: ttlMs/3 * 1e6 overflows a long only above ~3.05e12 ms
        // (~97 years).
        this.renewalIntervalNanos = Math.max(1, ttlMs / 3) * 1_000_000L;
    }

    /** The lease id this session keeps alive; bind ephemeral records to it. */
    public long getLeaseId() {
        return leaseId;
    }

    /**
     * Whether this session still speaks for a live lease; false after {@link #close()} or an {@
     * UNKNOWN_LEASE} renewal.
     */
    public boolean isAlive() {
        synchronized (lock) {
            return started && !stopped && !expired;
        }
    }

    /** Attaches to the client's renewal ticks; called by {@link RaftMetadataClient#openLease}. */
    void start() {
        synchronized (lock) {
            if (started) {
                return;
            }
            started = true;
        }
        client.registerSession(this);
    }

    /**
     * Marks the session dead without renewals: its client was already closed when it attached, so
     * it was never registered - nothing to detach. Consistent with the two-phase stop: state
     * under the lock, and no unregister because registration never happened.
     */
    void onDeadClient() {
        synchronized (lock) {
            stopped = true;
        }
    }

    /**
     * Stops the renewals - the lease then dies through the leader's scheduler - and detaches from
     * the client; idempotent, never blocks: a renewal still in flight completes and is ignored.
     * The stop is two-phase: the state clears under the session lock, and the unregister happens
     * only after that lock is released, so the client's registry lock is never nested under the
     * session's.
     */
    @Override
    public void close() {
        synchronized (lock) {
            if (stopped) {
                return;
            }
            stopped = true;
        }
        client.unregisterSession(this);
    }

    /**
     * One scheduler tick: renews when a third of the ttl has elapsed - by the monotonic clock, so
     * wall-clock skew cannot starve the lease - and no renewal is in flight. Overlap suppression
     * matters - the previous renewal may sit out its whole retry budget during an outage, and a
     * second one on top could complete out of order.
     */
    void tick() {
        synchronized (lock) {
            if (stopped || renewing) {
                return;
            }
            if (clock.nanoseconds() - renewBaseNanos < renewalIntervalNanos) {
                return;
            }
            renewing = true;
        }
        client.renewLeaseAsync(leaseId, Duration.ofMillis(ttlMs))
                .whenComplete(
                        (ignored, failure) -> {
                            if (failure == null) {
                                synchronized (lock) {
                                    renewing = false;
                                    if (stopped) {
                                        // A completion after the stop must not resurrect state.
                                        return;
                                    }
                                    renewBaseNanos = clock.nanoseconds();
                                }
                                return;
                            }
                            // The client's async twins wrap their failures; the statuses the
                            // session decides on live on the cause underneath.
                            onRenewalFailure(ExceptionUtils.stripCompletionException(failure));
                        });
    }

    private void onRenewalFailure(Throwable failure) {
        final boolean detach;
        synchronized (lock) {
            renewing = false;
            if (stopped) {
                // A completion after the stop must not resurrect state.
                return;
            }
            if (failure instanceof RaftMetadataException
                    && ((RaftMetadataException) failure).getStatus()
                            == RaftResult.Status.UNKNOWN_LEASE) {
                // The lease expired underneath and renewing cannot resurrect it - stop quietly.
                expired = true;
                stopped = true;
                detach = true;
                LOG.warn(
                        "Lease {} expired before its session could renew it; stopping renewals",
                        leaseId);
            } else if (failure instanceof IllegalStateException
                    || failure.getCause() instanceof AlreadyClosedException) {
                // The client closed under us (IllegalStateException, or ratis AlreadyClosed) -
                // stop. The client's own sweep unregisters its copy of the list; the detach here
                // covers a session that registered before the sweep cleared it.
                stopped = true;
                detach = true;
            } else {
                // Transport trouble: the due-ness stands, the next tick retries - a renewal
                // within one ttl keeps the lease alive. No other protocol failure is expected.
                detach = false;
                LOG.debug("Renewing lease {} failed; retrying", leaseId, failure);
            }
        }
        if (detach) {
            // Phase two of the stop, strictly outside the session lock: leaving the client's
            // registry lets the final tick die with this session instead of leaking it.
            client.unregisterSession(this);
        }
    }

    /** The lease id and ttl, for logs. */
    @Override
    public String toString() {
        return "LeaseSession{leaseId=" + leaseId + ", ttlMs=" + ttlMs + "}";
    }
}
