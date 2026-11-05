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

import org.apache.ratis.metadata.statemachine.RaftCommand;

/**
 * A mutation abandoned with its outcome unknown: its retry budget ran out, or it shared the
 * fate of an exhausted ordered pipeline - ratis fails the exhausted request itself with the
 * declared {@code RaftRetryFailureException} and every other outstanding request of that
 * pipeline with an {@code AlreadyClosedException} wrapping it, sent and merely queued alike.
 * Some attempt may have committed and only its acknowledgement was lost: reconcile by reading
 * state (the store, or the change stream) before re-executing. A blind re-run of a
 * non-idempotent mutation can duplicate a committed effect, and every client call mints a
 * fresh invocation, so a re-run is NOT an exactly-once retry.
 *
 * <p>Which failures carry it: mutations only - a failed read is always safe to re-issue. The
 * label errs toward reconciliation: a mutation queued behind, or submitted after, the
 * exhausted request never transmitted yet carries this type too, because the failure surface
 * cannot tell it from one that was in flight; one reconcile read is the entire cost of that
 * imprecision. Failures that stay on the plain {@link MetadataTransportException} with an
 * unknown outcome for a mutation: a very first attempt that gave up before any retry (ratis
 * then reports the raw transport failure; the client builder refuses a retry budget below one
 * request attempt), and a mutation torn down by client {@code close()} while in flight - close
 * clients only after their in-flight mutations settle, or reconcile before re-running.
 */
public class PossiblyCommittedException extends MetadataTransportException {

    private static final long serialVersionUID = 1L;

    PossiblyCommittedException(RaftCommand.Op op, Throwable cause) {
        super(
                "Metadata "
                        + op
                        + " was abandoned with its outcome unknown (retry budget exhausted or"
                        + " pipeline torn down): it may or may not have committed - reconcile by"
                        + " reading state before re-executing it",
                cause);
    }
}
