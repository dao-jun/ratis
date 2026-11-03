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

import java.time.Instant;

/**
 * The time source of the metadata module: the client stamps commands with it and the lease
 * machinery schedules against it. The state machine itself never reads it - proposers supply all
 * timestamps - which is what keeps replicas convergent.
 *
 * <p>Implementations should be thread-safe; tests inject a manual clock for deterministic lease
 * timing.
 */
public interface Clock {

    /**
     * Returns the current time in milliseconds, similar to {@link System#currentTimeMillis()}.
     */
    long milliseconds();

    /**
     * Returns the current time as an {@link Instant}.
     */
    default Instant instant() {
        return Instant.ofEpochMilli(milliseconds());
    }

    /**
     * Returns the current value of the JVM's high-resolution time source in nanoseconds, similar
     * to {@link System#nanoTime()}. Usable only for elapsed-time measurement.
     */
    long nanoseconds();
}
