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

import java.util.concurrent.CompletionException;

/** Exception utilities for the metadata module. */
public final class ExceptionUtils {

    private ExceptionUtils() {}

    /** Unwraps {@link CompletionException} wrappers, down to the first concrete cause. */
    public static Throwable stripCompletionException(Throwable throwable) {
        return stripException(throwable, CompletionException.class);
    }

    /** Unwraps the given exception type while it wraps another cause. */
    public static Throwable stripException(Throwable throwable, Class<? extends Throwable> toStrip) {
        Throwable cause = throwable;
        while (cause != null && toStrip.isInstance(cause) && cause.getCause() != cause) {
            final Throwable next = cause.getCause();
            if (next == null) {
                break;
            }
            cause = next;
        }
        return cause == null ? throwable : cause;
    }

    /** The most causes walked; a longer chain, or one that loops, ends the walk. */
    private static final int MAX_CAUSE_DEPTH = 64;

    /**
     * Whether the given type appears in the cause chain, the throwable itself included. The
     * walk is depth-bounded: a chain longer than {@link #MAX_CAUSE_DEPTH}, or one that loops
     * back on itself, simply ends the walk.
     */
    public static boolean causedBy(Throwable throwable, Class<? extends Throwable> type) {
        for (int depth = 0; throwable != null && depth < MAX_CAUSE_DEPTH; depth++) {
            if (type.isInstance(throwable)) {
                return true;
            }
            throwable = throwable.getCause();
        }
        return false;
    }
}
