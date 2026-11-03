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

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.fail;

/** Polling waits for test conditions: deterministic assertions on eventually-true states. */
public final class TestWait {

    private TestWait() {}

    /**
     * Waits until the condition holds, polling every millisecond. A condition whose evaluation
     * throws fails the wait with that throwable; a condition never holding fails the test with
     * {@code what}.
     */
    public static void waitUntil(
            Supplier<Boolean> condition, Duration timeout, String what) {
        final long timeoutMs = timeout.toMillis();
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException("The timeout must be positive.");
        }
        final long start = System.currentTimeMillis();
        boolean result;
        try {
            result = condition.get();
            while (!result && System.currentTimeMillis() - start < timeoutMs) {
                result = condition.get();
                //noinspection BusyWait
                Thread.sleep(1);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting: " + what, e);
        } catch (RuntimeException e) {
            throw e;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
        if (!result) {
            fail(what);
        }
    }

    /**
     * Waits for a value to appear: polls the optional supplier until it holds one, and returns
     * it; never holding fails the test with {@code what}.
     */
    public static <T> T waitValue(
            Supplier<Optional<T>> supplier, Duration timeout, String what) {
        final AtomicReference<T> result = new AtomicReference<>();
        waitUntil(
                () -> {
                    final Optional<T> value = supplier.get();
                    if (value.isPresent()) {
                        result.set(value.get());
                        return true;
                    }
                    return false;
                },
                timeout,
                what);
        return result.get();
    }
}
