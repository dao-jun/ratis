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
package org.apache.ratis.metadata.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * The single daemon scheduler every periodic background step of this module runs on - lease
 * renewals, watcher poll pacing, lock watchdogs. No component owns a blocking thread of its
 * own: each drives its state machine with asynchronous calls whose completions schedule the
 * next step here. Tasks must be short and must never block: this one thread is shared by every
 * component in the process, so a task that parks here (e.g. in ratis' outstanding-request
 * backpressure) freezes every other component's ticks with it. The client's async twins
 * enforce that: {@code RaftMetadataClient.sendAsync} is the only code allowed to touch the
 * raft client, and it enqueues each send on the client's sender thread - the only thread that
 * may park in ratis' backpressure. Adding another raft-client call site here silently
 * reintroduces the defect; change this contract and the README threading model together. A
 * task that throws is logged at ERROR here and swallowed: it never surfaces through the
 * discarded future and never stops a periodic task's later runs.
 */
public final class AsyncDriver {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncDriver.class);

    private AsyncDriver() {}

    private static final ScheduledThreadPoolExecutor SCHEDULER =
            new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("ratis-metadata-scheduler"));

    static {
        // A cancelled far-future task must not linger in the queue until its due time.
        SCHEDULER.setRemoveOnCancelPolicy(true);
    }

    /** Runs {@code task} once, {@code delayMs} from now. */
    public static ScheduledFuture<?> schedule(Runnable task, long delayMs) {
        return SCHEDULER.schedule(guard(task), delayMs, TimeUnit.MILLISECONDS);
    }

    /** Runs {@code task} repeatedly, every {@code periodMs}; the returned handle cancels it. */
    public static ScheduledFuture<?> scheduleAtFixedRate(Runnable task, long periodMs) {
        return SCHEDULER.scheduleAtFixedRate(guard(task), periodMs, periodMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Isolates one task's failure: the executor captures a one-shot task's throwable in the
     * returned future - which every caller discards - while an escaped throwable would suppress
     * every later run of a periodic task. Log it here so a failing step stays observable.
     */
    private static Runnable guard(Runnable task) {
        return () -> {
            try {
                task.run();
            } catch (Throwable t) {
                LOG.error("AsyncDriver task {} failed", task.getClass().getName(), t);
            }
        };
    }
}
