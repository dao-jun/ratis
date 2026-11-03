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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * The single daemon scheduler every periodic background step of this module runs on - lease
 * renewals, watcher poll pacing, lock watchdogs. No component owns a blocking thread of its
 * own: each drives its state machine with asynchronous calls whose completions schedule the
 * next step here. Tasks must be short and must never block; a slow task only delays the
 * others' ticks, it cannot deadlock them.
 */
public final class AsyncDriver {

    private AsyncDriver() {}

    private static final ScheduledExecutorService SCHEDULER =
            Executors.newSingleThreadScheduledExecutor(
                    new NamedThreadFactory("ratis-metadata-scheduler", true));

    /** Runs {@code task} once, {@code delayMs} from now. */
    public static ScheduledFuture<?> schedule(Runnable task, long delayMs) {
        return SCHEDULER.schedule(task, delayMs, TimeUnit.MILLISECONDS);
    }

    /** Runs {@code task} repeatedly, every {@code periodMs}; the returned handle cancels it. */
    public static ScheduledFuture<?> scheduleAtFixedRate(Runnable task, long periodMs) {
        return SCHEDULER.scheduleAtFixedRate(task, periodMs, periodMs, TimeUnit.MILLISECONDS);
    }
}
