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

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.client.RaftClientRpc;
import org.apache.ratis.client.retry.RequestTypeDependentRetryPolicy;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.metadata.MetadataCluster;
import org.apache.ratis.metadata.lock.MetadataLock;
import org.apache.ratis.metadata.protocol.ChangeEvent;
import org.apache.ratis.metadata.protocol.RaftCommand;
import org.apache.ratis.metadata.protocol.RaftCommandCodec;
import org.apache.ratis.metadata.protocol.RaftResult;
import org.apache.ratis.metadata.statemachine.MetadataStateMachine;
import org.apache.ratis.metadata.util.AsyncDriver;
import org.apache.ratis.metadata.util.Clock;
import org.apache.ratis.metadata.util.ExceptionUtils;
import org.apache.ratis.metadata.util.ManualClock;
import org.apache.ratis.metadata.util.SystemClock;
import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.protocol.exceptions.RaftRetryFailureException;
import org.apache.ratis.retry.ExponentialBackoffRetry;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.apache.ratis.metadata.util.TestWait.waitUntil;

/**
 * Unit tests of {@link RaftMetadataClient}, {@link LeaseSession} and {@link MetadataWatcher} on
 * hand-written rpc stubs: async-settlement semantics, failure translation and watcher lifecycle
 * without a live quorum - plus the cluster-dependent end-to-end suites that run the same surface
 * against a live in-process quorum via {@link MetadataCluster}.
 */
public class TestRaftMetadataClient {

    private static final String GROUP_UUID = "7e4c1d2a-9b3f-4a58-8c6e-2f1d5a7b9c34";
    private static final Duration OPERATION_TIMEOUT = Duration.ofSeconds(30);
    /**
     * The observation window of the negative watcher assertions: the defect each guards against
     * trips its latch within microseconds, never near this bound - the bound only ends the
     * wait once the absence is proven for a margin no scheduling can miss.
     */
    private static final Duration NEGATIVE_WINDOW = Duration.ofMillis(1500);
    /**
     * The observation window of the isolation assertions: a settle that can run completes
     * within milliseconds of its reply, so this bound only ends the wait once starvation is
     * proven - well below OPERATION_TIMEOUT, so a regression fails fast.
     */
    private static final Duration SETTLE_WINDOW = Duration.ofSeconds(10);
    private static final long POLL_BUDGET_BYTES = 4 * 1024 * 1024;
    private static final String SEQUENTIAL_PATH_PATTERN = "/\\w+/task-\\d{10}";

    private static final int FAILOVER_WRITES = 60;

    @TempDir File tempDir;

    /**
     * The withholding stubs of the running test, released by {@link #releaseStubs()} even when a
     * finally block was missed: {@link AsyncDriver} is JVM-wide, and an unreleased wedge parks
     * the shared scheduler (or the client's sender) for every later test of the module run.
     */
    private final List<WithholdingRpc> withholdings = new CopyOnWriteArrayList<>();

    /** The stub clients of the running test, closed belt-and-braces by {@link #releaseStubs()}. */
    private final List<RaftMetadataClient> stubClients = new CopyOnWriteArrayList<>();

    @AfterEach
    void releaseStubs() {
        for (WithholdingRpc rpc : withholdings) {
            rpc.failAllPending();
        }
        for (RaftMetadataClient client : stubClients) {
            try {
                client.close();
            } catch (RuntimeException ignored) {
                // Belt-and-braces close: a test that closed already made this a no-op.
            }
        }
    }

    @Test
    void testRetryDefaultsAreFiniteAndRetryForeverIsRejected() {
        assertEquals(Duration.ofHours(24), RaftMetadataClient.DEFAULT_RETRY_TIMEOUT);
        assertThrows(
                IllegalStateException.class, () -> RaftMetadataClient.newBuilder().setRetryForever());

        final RaftMetadataClient.Builder builder = RaftMetadataClient.newBuilder();
        assertEquals(builder, builder.setRetryTimeout(Duration.ofMillis(1)));
        assertThrows(
                IllegalArgumentException.class, () -> builder.setRetryTimeout(Duration.ZERO));
        assertThrows(
                IllegalArgumentException.class, () -> builder.setRetryTimeout(Duration.ofMillis(-1)));
    }

    /**
     * A retry budget at or below one request attempt gives up on the very first try, which ratis
     * reports as the raw transport failure - no exhaustion signal, so an ambiguous mutation can
     * no longer be told from a definitely-undelivered one. The builder refuses it at build; the
     * rejected build constructs nothing, so one builder serves both asserts.
     */
    @Test
    void testBuilderRejectsARetryBudgetBelowOneAttempt() {
        final RaftMetadataClient.Builder builder =
                RaftMetadataClient.newBuilder()
                        .setGroupId(UUID.fromString(GROUP_UUID))
                        .addQuorumMember("n0", new InetSocketAddress("localhost", 1));
        assertThrows(
                IllegalArgumentException.class,
                () -> builder.setRetryTimeout(Duration.ofSeconds(3)).build(),
                "a budget equal to one attempt gives up on the first try");
        assertThrows(
                IllegalArgumentException.class,
                () -> builder.setRetryTimeout(Duration.ofSeconds(1)).build(),
                "a budget below one attempt gives up on the first try");
    }

    @Test
    void testAsyncCallEndsWhenTheClientClosesUnderneath() throws Exception {
        final InetSocketAddress deadMember = new InetSocketAddress("localhost", refusedPort());
        final RaftMetadataClient unbounded =
                RaftMetadataClient.newBuilder()
                        .setGroupId(UUID.fromString(GROUP_UUID))
                        .addQuorumMember("n0", deadMember)
                        .setRetryTimeout(Duration.ofHours(1))
                        .build();
        final CompletableFuture<Long> pending = unbounded.putAsync("/unreachable", bytes("x"));
        unbounded.close();
        final Throwable failure =
                awaitFailureOf(pending, "Closing the client never ended the async call");
        // Close-time teardown is NOT the declared exhaustion signal: a mutation abandoned by
        // close() stays the plain type (its outcome may still be unknown - see the failure
        // contract), never widening into the ambiguity subclass.
        assertEquals(MetadataTransportException.class, failure.getClass());
    }

    @Test
    void testAsyncCallOnClosedClientFailsTheFuture() throws Exception {
        final RaftMetadataClient client = newStubClient(new FailingTransportRpc(), null);
        client.close();
        final Throwable failure =
                awaitFailureOf(
                        client.putAsync("/closed", bytes("x")),
                        "A closed client never settled the async call");
        assertInstanceOf(IllegalStateException.class, failure);
    }

    @Test
    void testAsyncArgumentErrorsSettleTheFuture() throws Exception {
        final RaftMetadataClient client = newStubClient(new FailingTransportRpc(), null);
        try {
            assertInstanceOf(
                    NullPointerException.class,
                    awaitFailureOf(
                            client.openLeaseAsync(null),
                            "A null lease ttl never settled the future"));
            assertInstanceOf(
                    IllegalArgumentException.class,
                    awaitFailureOf(
                            client.renewLeaseAsync(1L, Duration.ZERO),
                            "A zero lease ttl never settled the future"));
            assertInstanceOf(
                    NullPointerException.class,
                    awaitFailureOf(
                            client.putAsync(null, bytes("x")),
                            "A null path never settled the future"));
            assertInstanceOf(
                    NullPointerException.class,
                    awaitFailureOf(
                            client.multiAsync(null), "A null op list never settled the future"));
        } finally {
            client.close();
        }
    }

    /**
     * An unencodable name (an unpaired surrogate) must fail loudly before anything is sent: the
     * sync surface throws it, the async twin settles its future with it - not with the transport
     * failure the stub would otherwise produce for a request that did go out.
     */
    @Test
    void testUnencodableNamesFailLoudly() {
        // A finite policy, so a request that did go out (the pre-rejection behavior) would fail
        // fast instead of spinning forever against the always-failing stub.
        final RaftMetadataClient client =
                newStubClient(
                        new FailingTransportRpc(),
                        RetryPolicies.retryUpToMaximumCountWithFixedSleep(
                                2, TimeDuration.valueOf(20, TimeUnit.MILLISECONDS)));
        try {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> client.increment("locks\uD800x", 1L),
                    "an unencodable counter name must be rejected at encode, not sent");
            assertInstanceOf(
                    IllegalArgumentException.class,
                    awaitFailureOf(
                            client.incrementAsync("locks\uD800x", 1L),
                            "An unencodable counter name never settled the future"));
        } finally {
            client.close();
        }
    }

    @Test
    void testAsyncUndecodableReplyFailsTheFuture() throws Exception {
        final RaftMetadataClient poisoned = newStubClient(new UndecodableReplyRpc(), null);
        try {
            final Throwable failure =
                    awaitFailureOf(
                            poisoned.getAsync("/any"), "The async get never decoded its reply");
            assertInstanceOf(UndecodableReplyException.class, failure);
        } finally {
            poisoned.close();
        }
    }

    /**
     * A mutation whose retry budget ran out is abandoned with its outcome unknown - some attempt
     * may have committed and only its reply was lost - so both surfaces answer the ambiguity
     * subclass carrying ratis' declared budget failure. A read commits nothing and keeps the
     * plain transport failure (exact class: a subclass would satisfy an instanceof of the
     * parent). One exhausted budget permanently poisons the async window, so each case gets a
     * fresh client.
     */
    @Test
    void testExhaustedMutationBudgetSurfacesPossiblyCommitted() {
        final RetryPolicy twoAttempts =
                RetryPolicies.retryUpToMaximumCountWithFixedSleep(
                        2, TimeDuration.valueOf(20, TimeUnit.MILLISECONDS));

        final RaftMetadataClient sync = newStubClient(new FailingTransportRpc(), twoAttempts);
        try {
            final PossiblyCommittedException failure =
                    assertThrows(
                            PossiblyCommittedException.class, () -> sync.put("/p", bytes("x")));
            assertInstanceOf(RaftRetryFailureException.class, failure.getCause());
        } finally {
            sync.close();
        }

        final RaftMetadataClient single = newStubClient(new FailingTransportRpc(), twoAttempts);
        try {
            final Throwable failure =
                    awaitFailureOf(
                            single.putAsync("/p", bytes("x")),
                            "The async put never settled after its budget ran out");
            assertInstanceOf(PossiblyCommittedException.class, failure);
            assertInstanceOf(RaftRetryFailureException.class, failure.getCause());
        } finally {
            single.close();
        }

        final RaftMetadataClient reader = newStubClient(new FailingTransportRpc(), twoAttempts);
        try {
            final Throwable failure =
                    awaitFailureOf(
                            reader.getAsync("/p"),
                            "The async get never settled after its budget ran out");
            assertEquals(MetadataTransportException.class, failure.getClass());
        } finally {
            reader.close();
        }
    }

    /**
     * The exhaustion cascade: ratis fails only the exhausted head with the raw budget failure
     * and every other outstanding request of the ordered pipeline with that failure wrapped in
     * an {@code AlreadyClosedException} - sent and merely queued alike, which the failure
     * surface cannot tell apart. After one warmed reply the window pipelines every later
     * request, so both puts were genuinely in flight; whichever exhausts first, both must carry
     * the ambiguity, not just the head.
     */
    @Test
    void testPipelinedSiblingsOfAnExhaustedMutationAreAlsoFlaggedAmbiguous() throws Exception {
        final ScriptedReplyRpc rpc = new ScriptedReplyRpc();
        final RaftMetadataClient client =
                newStubClient(
                        rpc,
                        RetryPolicies.retryUpToMaximumCountWithFixedSleep(
                                2, TimeDuration.valueOf(20, TimeUnit.MILLISECONDS)));
        try {
            // One reply first: the window then pipelines every later request immediately.
            client.putAsync("/warm", bytes("w")).join();

            rpc.failAll = true; // every attempt now fails at the transport level
            final CompletableFuture<Long> first = client.putAsync("/p1", bytes("x"));
            final CompletableFuture<Long> second = client.putAsync("/p2", bytes("y"));

            for (CompletableFuture<Long> pipelined : Arrays.asList(first, second)) {
                final Throwable failure =
                        awaitFailureOf(
                                pipelined,
                                "A pipelined mutation never settled after the budget ran out");
                assertInstanceOf(PossiblyCommittedException.class, failure);
                assertTrue(
                        causedBy(failure, RaftRetryFailureException.class),
                        "the ambiguity must carry ratis' declared budget failure, got " + failure);
            }
        } finally {
            client.close();
        }
    }

    /**
     * The residual first-attempt gap, pinned as the boundary it is: with one attempt budgeted,
     * ratis reports the raw transport cause (no retry was ever made), so the client must keep it
     * the plain transport failure and never widen its budget-failure matching into a guess about
     * raw causes. Exact class, because a subclass would satisfy assertThrows of the parent.
     */
    @Test
    void testFirstAttemptGiveUpStaysThePlainTransportFailure() {
        final RaftMetadataClient client =
                newStubClient(
                        new FailingTransportRpc(),
                        RetryPolicies.retryUpToMaximumCountWithFixedSleep(
                                1, TimeDuration.valueOf(1, TimeUnit.MILLISECONDS)));
        try {
            final MetadataTransportException failure =
                    assertThrows(
                            MetadataTransportException.class, () -> client.put("/p", bytes("x")));
            assertEquals(MetadataTransportException.class, failure.getClass());
        } finally {
            client.close();
        }
    }

    /**
     * The whole-prefix enumerations page: every LIST request the client issues is bounded by the
     * wire-safe page size, the pages chain through continuation cursors that resume at the
     * previous page's last path, and the combined result is still the entire sorted prefix - on
     * the sync surface and the async one alike. The async chain must also issue every follow-on
     * page off the completion threads: a send can block its issuer under ratis'
     * outstanding-requests backpressure, and a parked completion thread stalls every async
     * settlement of the client.
     */
    @Test
    void testListEnumeratesInTransportBoundedPages() throws Exception {
        final List<String> prefix = new ArrayList<>();
        for (int i = 0; i < 3 * 1024 + 5; i++) {
            prefix.add(String.format(Locale.ROOT, "/listing/%06d", i));
        }
        final ListServingRpc rpc = new ListServingRpc(prefix);
        final RaftMetadataClient client = newStubClient(rpc, null);
        try {
            rpc.resetRecordings();
            assertEquals(prefix, pathsOf(client.list("/listing/")));
            assertBoundedChainedPages(rpc);

            rpc.resetRecordings();
            assertEquals(prefix, pathsOf(client.listAsync("/listing/").join()));
            assertBoundedChainedPages(rpc);
            final List<ListServingRpc.ServedList> served = rpc.served();
            for (int i = 1; i < served.size(); i++) {
                assertFalse(
                        served.get(i).thread.startsWith("raft-metadata-async"),
                        "follow-on page " + i + " was issued on " + served.get(i).thread);
            }
        } finally {
            client.close();
        }

        // The wire-bound guard: whatever either constant becomes, one page must stay under the
        // grpc default message cap even at the 3-bytes-per-char worst-case path.
        final long pageResults = (Long) staticFieldOf(RaftMetadataClient.class, "LIST_PAGE_RESULTS");
        final int maxPathLength =
                (Integer) staticFieldOf(MetadataStateMachine.class, "MAX_PATH_LENGTH");
        assertTrue(
                pageResults * (3L * maxPathLength + 13L) < 64L * 1024 * 1024,
                pageResults + " results of " + maxPathLength + "-char paths exceed the 64 MB cap");
    }

    /** Every recorded page is bounded, and each cursor resumes at the previous page's last path. */
    private static void assertBoundedChainedPages(ListServingRpc rpc) {
        final List<ListServingRpc.ServedList> served = rpc.served();
        for (ListServingRpc.ServedList page : served) {
            assertTrue(
                    page.maxResults > 0 && page.maxResults <= 1024,
                    "every page request must be wire-bounded, got " + page.maxResults);
        }
        assertTrue(
                served.size() >= 2,
                "a prefix beyond one page must be chained through several requests, got "
                        + served.size());
        for (int i = 1; i < served.size(); i++) {
            assertEquals(
                    served.get(i - 1).lastPath,
                    served.get(i).startAfter,
                    "page " + i + " must resume at the previous page's last path");
        }
    }

    @Test
    void testAsyncCompletionsRunOnTheClientExecutor() throws Exception {
        final ControllableReplyRpc rpc = new ControllableReplyRpc();
        final RaftMetadataClient client = newStubClient(rpc, null);
        try {
            final CompletableFuture<String> completingThread =
                    client.getAsync("/any").thenApply(record -> Thread.currentThread().getName());
            // A returned async call means the send is enqueued, not issued: gate on the stub -
            // the request reaches it on the client's sender thread, moments after the return.
            waitUntil(
                    rpc::hasPending,
                    Duration.ofSeconds(5),
                    "The get never reached the rpc");
            // The call stays pending until the test supplies the reply, so the chain provably
            // runs on whatever thread settles it - the client's executor, not the caller's.
            rpc.completePendingGet();
            assertTrue(
                    completingThread.join().startsWith("raft-metadata-async"),
                    "completions must run on the client's own executor");
        } finally {
            client.close();
        }
    }

    // ------------------------------------------------------------------
    // The sender thread: no caller, completion or scheduler thread ever
    // parks inside ratis' ordered-async machinery
    // ------------------------------------------------------------------

    /**
     * The core F5 pin: a send wedged in ratis' outstanding-request backpressure must stall only
     * the client's sender thread - never the one JVM-wide scheduler every component's periodic
     * step runs on. Both of the client's two permits are withheld forever (the first send parks
     * at the stub, the second is delayed inside ratis' sliding window holding the second permit,
     * and no retry fires), so any send issued on the scheduler thread itself would park it
     * there; the probe task behind such a renewal would never run.
     */
    @Test
    void testWedgedPipelineDoesNotStallTheSharedScheduler() throws Exception {
        final WithholdingRpc rpc = new WithholdingRpc();
        withholdings.add(rpc);
        final RaftMetadataClient client = newStubClient(rpc, RetryPolicies.noRetry(), 2);
        try {
            client.putAsync("/wedge/first", bytes("x"));
            client.putAsync("/wedge/second", bytes("x"));
            // A background step's shape: a renewal driven by the shared scheduler.
            AsyncDriver.schedule(() -> client.renewLeaseAsync(7L, Duration.ofSeconds(30)), 0L);
            final CountDownLatch probe = new CountDownLatch(1);
            AsyncDriver.schedule(probe::countDown, 50L);
            assertTrue(
                    probe.await(10, TimeUnit.SECONDS),
                    "a send wedged in the pipeline backpressure must not stall the shared"
                            + " scheduler");
        } finally {
            rpc.failAllPending();
            client.close();
        }
    }

    /**
     * The finding's failure scenario verbatim: a lock's acquire-timeout task must fire while the
     * pipeline is wedged. The timeout task runs on the shared scheduler - so its begin step must
     * never park that scheduler in the pipeline's backpressure, or the queued timeout (and every
     * other component's tick with it) never runs and the acquire hangs instead of failing.
     */
    @Test
    void testLockAcquireTimeoutFiresWhileThePipelineIsWedged() throws Exception {
        final WithholdingRpc rpc = new WithholdingRpc();
        withholdings.add(rpc);
        final RaftMetadataClient client = newStubClient(rpc, RetryPolicies.noRetry(), 2);
        try {
            client.putAsync("/wedge/first", bytes("x"));
            client.putAsync("/wedge/second", bytes("x"));
            final MetadataLock lock =
                    MetadataLock.newBuilder()
                            .setClient(client)
                            .setPath("/wedge/lock")
                            .setTtl(Duration.ofSeconds(2))
                            .build();
            final Throwable failure =
                    awaitFailureOf(
                            lock.acquireAsync(Duration.ofMillis(300)),
                            "The acquire timeout never fired while the pipeline was wedged");
            assertInstanceOf(TimeoutException.class, failure);
        } finally {
            rpc.failAllPending();
            client.close();
        }
    }

    /**
     * Close()'s bounded drain: against a transport that fails nothing on close (this stub's
     * close is a no-op and the permits stay withheld), close() itself must return at once, and
     * the sends it stranded - the one whose issue parks the sender in the backpressure acquire
     * and the ones still queued behind it - must still settle within the drain's grace. The
     * first two puts are already inside ratis' pipeline at close and keep their pre-fix
     * semantics (settled by the hygiene release, not asserted): the drain's contract covers
     * exactly the newly-introduced queued state.
     */
    @Test
    void testCloseSettlesSendsQueuedBehindAWedgedPipeline() throws Exception {
        final WithholdingRpc rpc = new WithholdingRpc();
        withholdings.add(rpc);
        final RaftMetadataClient client = newStubClient(rpc, RetryPolicies.noRetry(), 2);
        try {
            client.putAsync("/wedge/first", bytes("x"));
            client.putAsync("/wedge/second", bytes("x"));
            final CompletableFuture<Long> parking = client.putAsync("/wedge/third", bytes("x"));
            final CompletableFuture<Long> queued = client.putAsync("/wedge/fourth", bytes("x"));
            final long closeStarted = System.nanoTime();
            client.close();
            assertTrue(
                    System.nanoTime() - closeStarted < TimeUnit.SECONDS.toNanos(1),
                    "close() must never block on a wedged pipeline");
            for (CompletableFuture<Long> stranded : Arrays.asList(parking, queued)) {
                final Throwable failure =
                        awaitFailureOf(
                                stranded,
                                "A send stranded by close() never settled through the bounded"
                                        + " drain");
                assertEquals(
                        MetadataTransportException.class,
                        failure.getClass(),
                        "a stranded send settles as the transport family, got " + failure);
            }
        } finally {
            rpc.failAllPending();
        }
    }

    /**
     * The admission reservation: once the queue holds the non-lease bound, further non-lease
     * sends fail fast with the transport family - while a lease-lifecycle send issued right
     * after the flood is still admitted (its future stays pending, the reservation at work), so
     * session renewals keep entering a saturated pipeline. Asserts admission only, not the
     * renewal's latency; the bound itself is documented in the README's threading model.
     */
    @Test
    void testLeaseCriticalSendsPassAFullQueue() throws Exception {
        final WithholdingRpc rpc = new WithholdingRpc();
        withholdings.add(rpc);
        final RaftMetadataClient client = newStubClient(rpc, RetryPolicies.noRetry(), 2);
        try {
            client.putAsync("/wedge/first", bytes("x"));
            client.putAsync("/wedge/second", bytes("x"));
            client.putAsync("/wedge/third", bytes("x")); // parks the sender worker
            final List<CompletableFuture<Long>> flood = new ArrayList<>();
            for (int i = 0; i < 200; i++) {
                flood.add(client.putAsync("/wedge/flood/" + i, bytes("x")));
            }
            CompletableFuture<Long> refused = null;
            for (CompletableFuture<Long> flooded : flood) {
                if (flooded.isDone()) {
                    refused = flooded;
                }
            }
            assertNotNull(refused, "a flood past the admission bound must fast-fail at return");
            final Throwable failure =
                    awaitFailureOf(
                            refused, "The fast-failed flooded send settled with something else");
            assertEquals(MetadataTransportException.class, failure.getClass());

            final CompletableFuture<Long> renewal =
                    client.renewLeaseAsync(7L, Duration.ofSeconds(30));
            assertFalse(
                    renewal.isDone(),
                    "a lease-critical send is admitted past the non-lease bound");
        } finally {
            rpc.failAllPending();
            client.close();
        }
    }

    /**
     * Design guard of the single FIFO sender - honest label: today's synchronous sends from one
     * thread are also ordered, so this does not pin the defect; it pins the FIFO choice a naive
     * multi-threaded "fix" would break. Every send issued from one thread must reach the
     * pipeline in issue order: ratis assigns its sliding-window sequence numbers at submission,
     * so a reordered submission breaks the lock recipe's create-before-revoke guarantee. All
     * futures are joined (and the total count asserted) before the order check, so a send not
     * yet issued can neither fail nor pass the assertion vacuously.
     */
    @Test
    void testAsyncSendsReachThePipelineInIssueOrder() {
        final ScriptedReplyRpc rpc = new ScriptedReplyRpc();
        final RaftMetadataClient client = newStubClient(rpc, null);
        try {
            final int gets = 50;
            final List<CompletableFuture<RaftMetadataClient.MetadataRecord>> reads =
                    new ArrayList<>();
            for (int i = 0; i < gets; i++) {
                reads.add(client.getAsync("/order/" + i));
            }
            final CompletableFuture<Long> revocation = client.revokeLeaseAsync(7L);
            CompletableFuture.allOf(reads.toArray(new CompletableFuture[0])).join();
            revocation.join();

            assertEquals(gets + 1, rpc.sent.size(), "every issued send reached the pipeline");
            assertEquals(gets, rpc.countOf(RaftCommand.Op.GET));
            assertEquals(
                    gets,
                    rpc.firstIndexOf(RaftCommand.Op.LEASE_EXPIRE, null),
                    "the revocation follows every send issued before it");
        } finally {
            client.close();
        }
    }

    @Test
    void testWatcherStopsLoudlyOnUndecodableReply() throws Exception {
        final RaftMetadataClient poisoned = newStubClient(new UndecodableReplyRpc(), null);
        try {
            final Recorder recorder = new Recorder();
            final MetadataWatcher watcher =
                    MetadataWatcher.newBuilder(poisoned, recorder)
                            .setSinceRevision(7)
                            .setPollInterval(Duration.ofMillis(50))
                            .build();
            watcher.start();
            waitUntil(
                    () -> watcher.getFailure() != null && !watcher.isRunning(),
                    OPERATION_TIMEOUT,
                    "The watcher did not stop on the undecodable reply");
            assertInstanceOf(UndecodableReplyException.class, watcher.getFailure());
            assertEquals(
                    7L,
                    watcher.lastDeliveredRevision(),
                    "nothing decoded means nothing skipped: the checkpoint stays put");
            assertTrue(recorder.deliveredEvents().isEmpty());
            assertNull(
                    recorder.needSnapshotLine(), "an undecodable reply is not a retention report");

            // Resuming stays the caller's decision; the new loop clears the failure state.
            watcher.restartFrom(7);
            assertTrue(watcher.isRunning());
            assertNull(watcher.getFailure());
            waitUntil(
                    () -> watcher.getFailure() != null && !watcher.isRunning(),
                    OPERATION_TIMEOUT,
                    "The restarted watcher did not stop on the undecodable reply");
            assertEquals(7L, watcher.lastDeliveredRevision());
            watcher.close();
        } finally {
            poisoned.close();
        }
    }

    /**
     * Admission vs close on the report path: a restart requested while the NEED_SNAPSHOT report
     * is merely QUEUED is recorded (last-wins) rather than starting a chain on the spot, and a
     * report admitted only after {@code close()} never runs the listener at all.
     */
    @Test
    void testWatcherSubmitsNoListenerCallbackAfterClose() throws Exception {
        final GatedNeedSnapshotRpc rpc = new GatedNeedSnapshotRpc();
        final RaftMetadataClient client = newStubClient(rpc, null);
        try {
            final QueueingExecutor executor = new QueueingExecutor();
            final Recorder recorder = new Recorder();
            final MetadataWatcher watcher =
                    MetadataWatcher.newBuilder(client, recorder)
                            .setExecutor(executor)
                            .setPollInterval(Duration.ofMillis(50))
                            .build();
            watcher.start();
            waitUntil(
                    rpc::firstPollInFlight,
                    OPERATION_TIMEOUT,
                    "The watcher never issued its first poll");
            rpc.answerFirstPoll();
            waitUntil(
                    () -> executor.submitted.size() == 1,
                    OPERATION_TIMEOUT,
                    "The watcher never queued its first NEED_SNAPSHOT report");

            // The restart lands while the report is queued, not running: it is recorded, and
            // the chain stays down until the report completed - a poll now would race the
            // report's own restart decision.
            watcher.restartFrom(1);
            assertFalse(watcher.isRunning(), "the restart stays pending while the report runs");

            watcher.close();
            executor.submitted.peek().run(); // the executor drains only after the close

            assertNull(
                    recorder.needSnapshotLine(),
                    "a report admitted only after close must not run the listener");
            assertEquals(1, executor.submitted.size(), "the closed watcher must not report");
            assertFalse(watcher.isRunning());
            assertThrows(
                    IllegalStateException.class,
                    () -> watcher.restartFrom(1),
                    "a closed watcher rejects further restarts");
        } finally {
            client.close();
        }
    }

    @Test
    void testWatcherStopsWhenTheClientClosesUnderneath() throws Exception {
        final FailingTransportRpc rpc = new FailingTransportRpc();
        // Two fast attempts per poll: each poll fails within ms, so the loop re-polls, not pins.
        final RaftMetadataClient client =
                newStubClient(
                        rpc,
                        RetryPolicies.retryUpToMaximumCountWithFixedSleep(
                                2, TimeDuration.valueOf(50, TimeUnit.MILLISECONDS)));
        try {
            final MetadataWatcher watcher =
                    MetadataWatcher.newBuilder(client, new Recorder())
                            .setPollInterval(Duration.ofMillis(50))
                            .build();
            watcher.start();
            waitUntil(
                    () -> rpc.attempts.get() >= 2,
                    OPERATION_TIMEOUT,
                    "The watcher never entered its retry loop");

            client.close();

            waitUntil(
                    () -> !watcher.isRunning() && watcher.getFailure() != null,
                    OPERATION_TIMEOUT,
                    "The watcher kept idling over the closed client instead of stopping");
            final Throwable failure = watcher.getFailure();
            assertTrue(
                    failure instanceof IllegalStateException
                            || (failure instanceof MetadataTransportException
                                    && causedBy(failure, AlreadyClosedException.class)),
                    "the stop must carry the closed-client cause: " + failure);
            assertTrue(failure.getMessage().contains("closed"), "the stop must name the closing");
            watcher.close();
        } finally {
            client.close();
        }
    }

    /**
     * The no-way-back half of the watcher's transport classification: a stub client whose raft
     * client cannot be rebuilt (the injected constructor) keeps its poisoned window forever, so
     * the watcher must stop terminally instead of re-polling a window that can never deliver
     * again. Pre-fix this is the defect itself: the chain loops at DEBUG forever, isRunning()
     * stays true and getFailure() stays null.
     */
    @Test
    void testWatcherStopsWhenTheAsyncWindowIsPoisoned() throws Exception {
        final FailingTransportRpc rpc = new FailingTransportRpc();
        final RaftMetadataClient client =
                newStubClient(
                        rpc,
                        RetryPolicies.retryUpToMaximumCountWithFixedSleep(
                                2, TimeDuration.valueOf(20, TimeUnit.MILLISECONDS)));
        try {
            final Recorder recorder = new Recorder();
            final MetadataWatcher watcher =
                    MetadataWatcher.newBuilder(client, recorder)
                            .setSinceRevision(9)
                            .setPollInterval(Duration.ofMillis(50))
                            .build();
            watcher.start();
            // The first poll exhausts its finite budget, which poisons the ordered window; the
            // second poll then fails fast with AlreadyClosedException under an MTE.
            waitUntil(
                    () -> watcher.getFailure() != null && !watcher.isRunning(),
                    OPERATION_TIMEOUT,
                    "The watcher looped futilely over the poisoned async window instead of"
                            + " stopping");
            final Throwable failure = watcher.getFailure();
            assertInstanceOf(MetadataTransportException.class, failure);
            assertTrue(
                    causedBy(failure, AlreadyClosedException.class),
                    "the stop must carry the poisoned-window cause: " + failure);
            assertEquals(9L, watcher.lastDeliveredRevision(), "the checkpoint stands for a resume");
            assertTrue(recorder.deliveredEvents().isEmpty());
            assertNull(recorder.needSnapshotLine());

            // The stop is final on this client: the window fails inside submitNewRequest, before
            // the transport, so the exhausted budget of the first poll stays the only traffic.
            Thread.sleep(NEGATIVE_WINDOW.toMillis());
            assertEquals(2, rpc.attempts.get(), "the stopped chain must not hammer the dead window");

            // restartFrom cannot revive this watcher: it stays bound to the dead client, so the
            // restarted chain stops the same way (pinned by the restartFrom javadoc).
            watcher.restartFrom(9);
            assertTrue(watcher.isRunning());
            assertNull(watcher.getFailure());
            waitUntil(
                    () -> watcher.getFailure() != null && !watcher.isRunning(),
                    OPERATION_TIMEOUT,
                    "The restarted chain never met the poisoned window");
            watcher.close();

            // The documented resume: a NEW watcher on a rebuilt client, from the checkpoint.
            final ScriptedReplyRpc healthyRpc = new ScriptedReplyRpc();
            final RaftMetadataClient rebuilt = newStubClient(healthyRpc, null);
            try {
                final MetadataWatcher next =
                        MetadataWatcher.newBuilder(rebuilt, new Recorder())
                                .setSinceRevision(9)
                                .setPollInterval(Duration.ofMillis(50))
                                .build();
                next.start();
                waitUntil(
                        () -> healthyRpc.countOf(RaftCommand.Op.POLL_CHANGES) >= 2,
                        OPERATION_TIMEOUT,
                        "The rebuilt chain never resumed polling");
                assertTrue(next.isRunning(), "a rebuilt client resumes the stream");
                assertNull(next.getFailure());
                next.close();
            } finally {
                rebuilt.close();
            }
        } finally {
            client.close();
        }
    }

    /**
     * The keep-polling half: a poll answered MALFORMED is a cause-less MetadataTransportException
     * from a SUCCESSFUL raft reply, so the window stays healthy and no AlreadyClosedException can
     * ever follow - the watcher must keep polling (the README failure contract documents the
     * policy). Fails if the stop were widened to any transport failure.
     */
    @Test
    void testWatcherKeepsPollingOnTransportFailuresWithoutClose() throws Exception {
        final ScriptedReplyRpc rpc = new ScriptedReplyRpc();
        rpc.errorStatuses.put(RaftCommand.Op.POLL_CHANGES, RaftResult.Status.MALFORMED);
        final RaftMetadataClient client = newStubClient(rpc, null);
        try {
            final MetadataWatcher watcher =
                    MetadataWatcher.newBuilder(client, new Recorder())
                            .setPollInterval(Duration.ofMillis(50))
                            .build();
            watcher.start();
            waitUntil(
                    () -> rpc.countOf(RaftCommand.Op.POLL_CHANGES) >= 3,
                    OPERATION_TIMEOUT,
                    "The watcher never entered its transport-retry loop");
            assertTrue(watcher.isRunning(), "a healthy window must keep the chain polling");
            assertNull(watcher.getFailure());
            watcher.close();
        } finally {
            client.close();
        }
    }

    // ------------------------------------------------------------------
    // Watcher delivery is single-flight and admitted under the close gate
    // ------------------------------------------------------------------

    /**
     * Single-flight: while one delivery unit's callback is parked on a multithreaded executor,
     * no second poll leaves and the watermark stays put - both move only after the listener
     * actually returned, so callbacks of one watcher can never overlap.
     */
    @Test
    void testWatcherPollsAgainOnlyAfterTheListenerReturned() throws Exception {
        final GatedPollRpc rpc = new GatedPollRpc();
        final RaftMetadataClient client = newStubClient(rpc, null);
        final ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            final BlockingListener listener = new BlockingListener();
            final MetadataWatcher watcher =
                    MetadataWatcher.newBuilder(client, listener)
                            .setExecutor(pool)
                            .setSinceRevision(0)
                            .setPollInterval(Duration.ofMillis(50))
                            .build();
            watcher.start();
            waitUntil(
                    rpc::hasInFlightPoll,
                    OPERATION_TIMEOUT,
                    "The watcher never issued its first poll");
            rpc.answerRecords(1L, 1L);
            assertTrue(
                    listener.firstEntered.await(OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS),
                    "The first listener callback never started");
            assertNeverTripped(
                    rpc.anotherPollArrived,
                    NEGATIVE_WINDOW,
                    "The chain polled again while the delivery was still in flight");
            assertEquals(
                    0L,
                    watcher.lastDeliveredRevision(),
                    "the watermark moves only after the listener returned");
            assertFalse(listener.overlapped(), "listener callbacks of one watcher never overlap");

            listener.release();
            assertTrue(
                    listener.firstReturned.await(OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS),
                    "The first listener callback never returned");
            waitUntil(
                    () -> rpc.pollCount() == 2 && rpc.hasInFlightPoll(),
                    OPERATION_TIMEOUT,
                    "The chain never resumed polling after the delivery completed");
            assertEquals(
                    1L,
                    rpc.sinceRevisionOf(2),
                    "the resumed poll continues after the batch that was actually delivered");

            rpc.answerRecords(3L, 2L, 3L);
            waitUntil(
                    () -> listener.events().size() == 3,
                    OPERATION_TIMEOUT,
                    "The second batch never reached the listener");
            assertEquals(3L, watcher.lastDeliveredRevision());
            assertFalse(listener.overlapped(), "listener callbacks of one watcher never overlap");
            watcher.close();
        } finally {
            pool.shutdownNow();
            client.close();
        }
    }

    /**
     * Default-executor callbacks must be isolated from the client's machinery: two watchers
     * built without an executor park their first callbacks, and while BOTH are parked the same
     * client still settles an async read and keeps renewing a due lease. Inline delivery (the
     * old default) ran each callback on one of the client's two completion-executor threads -
     * two parked callbacks starved every settle: a renewal reply never completed,
     * {@code LeaseSession#renewing} latched, and the lease expired server-side while its holder
     * blocked on something else entirely.
     */
    @Test
    void testDefaultWatcherCallbacksCannotStallTheClientMachinery() throws Exception {
        final ImmediatePollRpc rpc = new ImmediatePollRpc();
        final RaftMetadataClient client = newStubClient(rpc, null);
        final ManualClock clock = new ManualClock(0L);
        final LeaseSession session = new LeaseSession(client, 99L, 3_000L, clock);
        final BlockingListener first = new BlockingListener();
        final BlockingListener second = new BlockingListener();
        MetadataWatcher watcherA = null;
        MetadataWatcher watcherB = null;
        try {
            watcherA =
                    MetadataWatcher.newBuilder(client, first)
                            .setSinceRevision(0)
                            .setPollInterval(Duration.ofMillis(50))
                            .build();
            watcherB =
                    MetadataWatcher.newBuilder(client, second)
                            .setSinceRevision(0)
                            .setPollInterval(Duration.ofMillis(50))
                            .build();
            watcherA.start();
            watcherB.start();
            assertTrue(
                    first.firstEntered.await(OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS),
                    "The first watcher's callback never started");
            assertTrue(
                    second.firstEntered.await(OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS),
                    "The second watcher's callback never started");

            // Pin 1 - the routing: default callbacks land on the client's delivery pool, never
            // on a completion-executor thread (those two threads settle every async reply).
            for (BlockingListener listener : Arrays.asList(first, second)) {
                assertTrue(
                        listener.firstCallbackThread().startsWith("raft-metadata-delivery"),
                        "a default-executor callback ran on " + listener.firstCallbackThread());
            }

            // Pin 2 - settles keep flowing while both callbacks are parked, on the client's own
            // completion executor exactly as before. The probe's reply is withheld until the
            // get is in flight, so the dependent stage provably runs on the settling thread -
            // never on the thread that issued the call.
            final CompletableFuture<String> settlingThread =
                    client.getAsync("/isolation-probe")
                            .thenApply(record -> Thread.currentThread().getName());
            waitUntil(
                    rpc::hasPendingGet,
                    SETTLE_WINDOW,
                    "The isolation probe never reached the rpc");
            rpc.answerPendingGet();
            waitUntil(
                    settlingThread::isDone,
                    SETTLE_WINDOW,
                    "Two parked default callbacks starved the client's async settles");
            assertTrue(
                    settlingThread.join().startsWith("raft-metadata-async"),
                    "settles keep running on the client's completion executor");

            // Pin 3 - the lease angle: a due renewal settles (its session unlatches) and the
            // NEXT renewal leaves. The stub counts at SEND time, so the second probe must keep
            // making the renewal due inside the wait loop - a single fixed advance could land
            // before the first renewal's settle and freeze the ManualClock base past all
            // due-ness.
            session.start();
            clock.advanceTime(1_100L, TimeUnit.MILLISECONDS);
            session.tick();
            waitUntil(
                    () -> rpc.renewals() == 1,
                    OPERATION_TIMEOUT,
                    "The first renewal never left the session");
            waitUntil(
                    () -> {
                        clock.advanceTime(1_100L, TimeUnit.MILLISECONDS); // a full interval past any base
                        session.tick();                                   // no-op while the settle is pending
                        return rpc.renewals() >= 2;
                    },
                    OPERATION_TIMEOUT,
                    "The parked callbacks starved the renewal settles: the session stopped"
                            + " renewing");
            assertTrue(session.isAlive(), "the lease outlived the parked callbacks");
        } finally {
            // BEFORE any close, and on every exit: unpark the delivery pool's threads whatever
            // failed - a parked callback would otherwise pin them for the client's whole life
            // and cascade into every later watcher of the client.
            first.release();
            second.release();
            if (watcherA != null) {
                watcherA.close();
            }
            if (watcherB != null) {
                watcherB.close();
            }
            session.close();
            client.close();
        }
    }

    /**
     * Admission vs close on the record path: a delivery unit queued on an executor that is
     * admitted only after {@code close()} must never run the listener nor move any state.
     */
    @Test
    void testWatcherDropsRecordCallbacksQueuedBeforeClose() throws Exception {
        final GatedPollRpc rpc = new GatedPollRpc();
        final RaftMetadataClient client = newStubClient(rpc, null);
        try {
            final QueueingExecutor executor = new QueueingExecutor();
            final Recorder recorder = new Recorder();
            final MetadataWatcher watcher =
                    MetadataWatcher.newBuilder(client, recorder)
                            .setExecutor(executor)
                            .setSinceRevision(0)
                            .setPollInterval(Duration.ofMillis(50))
                            .build();
            watcher.start();
            waitUntil(
                    rpc::hasInFlightPoll,
                    OPERATION_TIMEOUT,
                    "The watcher never issued its first poll");
            rpc.answerRecords(1L, 1L);
            waitUntil(
                    () -> executor.submitted.size() == 1,
                    OPERATION_TIMEOUT,
                    "The watcher never queued its first delivery");

            watcher.close();
            executor.submitted.poll().run(); // the executor drains only after the close

            assertTrue(
                    recorder.deliveredEvents().isEmpty(),
                    "a unit admitted only after close must not run the listener");
            assertEquals(
                    0L,
                    watcher.lastDeliveredRevision(),
                    "a dropped unit must not advance the watermark");
            assertEquals(1, rpc.pollCount(), "the closed watcher polls no further");
        } finally {
            client.close();
        }
    }

    /**
     * The other side of the admission race: a callback already admitted (running) when
     * {@code close()} lands finishes and its delivery still counts - but nothing further is
     * scheduled and the chain is down.
     */
    @Test
    void testWatcherLetsAnAdmittedCallbackFinishAfterClose() throws Exception {
        final GatedPollRpc rpc = new GatedPollRpc();
        final RaftMetadataClient client = newStubClient(rpc, null);
        final ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            final BlockingListener listener = new BlockingListener();
            final MetadataWatcher watcher =
                    MetadataWatcher.newBuilder(client, listener)
                            .setExecutor(pool)
                            .setSinceRevision(0)
                            .setPollInterval(Duration.ofMillis(50))
                            .build();
            watcher.start();
            waitUntil(
                    rpc::hasInFlightPoll,
                    OPERATION_TIMEOUT,
                    "The watcher never issued its first poll");
            rpc.answerRecords(1L, 1L);
            assertTrue(
                    listener.firstEntered.await(OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS),
                    "The listener callback never started");

            watcher.close(); // mid-callback: the admitted unit may finish
            listener.release();
            assertTrue(
                    listener.firstReturned.await(OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS),
                    "An already admitted callback must finish after close");
            assertEquals(1, listener.events().size(), "the admitted callback completed its batch");
            assertEquals(
                    1L,
                    watcher.lastDeliveredRevision(),
                    "the finished delivery still records its watermark");
            assertFalse(watcher.isRunning());
            assertNeverTripped(
                    rpc.anotherPollArrived,
                    NEGATIVE_WINDOW,
                    "no poll may leave a closed watcher");
        } finally {
            pool.shutdownNow();
            client.close();
        }
    }

    /**
     * NEED_SNAPSHOT restart arbitration: restart requests arriving while the report is merely
     * queued (or running) are recorded last-wins without exception; the completed report then
     * starts exactly one new chain from the LAST accepted checkpoint. A restart against a
     * polling chain keeps its rejection.
     */
    @Test
    void testNeedSnapshotRestartsAreLastWinsUntilTheReportCompleted() throws Exception {
        final GatedPollRpc rpc = new GatedPollRpc();
        final RaftMetadataClient client = newStubClient(rpc, null);
        try {
            final QueueingExecutor executor = new QueueingExecutor();
            final Recorder recorder = new Recorder();
            final MetadataWatcher watcher =
                    MetadataWatcher.newBuilder(client, recorder)
                            .setExecutor(executor)
                            .setSinceRevision(0)
                            .setPollInterval(Duration.ofMillis(50))
                            .build();
            watcher.start();
            waitUntil(
                    rpc::hasInFlightPoll,
                    OPERATION_TIMEOUT,
                    "The watcher never issued its first poll");
            rpc.answerNeedSnapshot(5L);
            waitUntil(
                    () -> executor.submitted.size() == 1,
                    OPERATION_TIMEOUT,
                    "The watcher never queued its NEED_SNAPSHOT report");
            assertFalse(
                    watcher.isRunning(), "the chain is stopped while the report is outstanding");

            watcher.restartFrom(7L); // queued, not running: recorded, no chain yet
            watcher.restartFrom(9L); // last wins
            assertFalse(
                    watcher.isRunning(),
                    "the restart stays pending until the report completed");
            assertNeverTripped(
                    rpc.anotherPollArrived,
                    NEGATIVE_WINDOW,
                    "the pending restart must not poll before the report completed");

            executor.submitted.poll().run(); // the report finally runs
            waitUntil(
                    () -> rpc.pollCount() == 2 && rpc.hasInFlightPoll(),
                    OPERATION_TIMEOUT,
                    "The completed report never started the pending chain");
            assertEquals(
                    9L,
                    rpc.sinceRevisionOf(2),
                    "exactly one chain leaves, from the LAST accepted checkpoint");

            assertThrows(
                    IllegalStateException.class,
                    () -> watcher.restartFrom(11L),
                    "a restart against a polling chain keeps its rejection");

            rpc.answerRecords(10L, 10L);
            // The queued executor holds the delivery until the test drains it - as it held
            // the report before.
            waitUntil(
                    () -> executor.submitted.size() == 1,
                    OPERATION_TIMEOUT,
                    "The restarted chain never queued its delivery");
            executor.submitted.peek().run();
            waitUntil(
                    () -> !recorder.deliveredEvents().isEmpty(),
                    OPERATION_TIMEOUT,
                    "The restarted chain never delivered");
            assertEquals(10L, watcher.lastDeliveredRevision());
            watcher.close();
        } finally {
            client.close();
        }
    }

    /**
     * A start() requested while the NEED_SNAPSHOT report is still outstanding defers like a
     * restart: no chain may leave before the report's callback returned, or its record
     * deliveries would overlap the report callback on the multithreaded delivery pool. The
     * completed report starts the one new chain from the stopped checkpoint.
     */
    @Test
    void testAStartWhileTheNeedSnapshotReportIsOutstandingDefersUntilItCompleted() throws Exception {
        final GatedPollRpc rpc = new GatedPollRpc();
        final RaftMetadataClient client = newStubClient(rpc, null);
        try {
            final QueueingExecutor executor = new QueueingExecutor();
            final Recorder recorder = new Recorder();
            final MetadataWatcher watcher =
                    MetadataWatcher.newBuilder(client, recorder)
                            .setExecutor(executor)
                            .setSinceRevision(0)
                            .setPollInterval(Duration.ofMillis(50))
                            .build();
            watcher.start();
            waitUntil(
                    rpc::hasInFlightPoll,
                    OPERATION_TIMEOUT,
                    "The watcher never issued its first poll");
            rpc.answerNeedSnapshot(5L);
            waitUntil(
                    () -> executor.submitted.size() == 1,
                    OPERATION_TIMEOUT,
                    "The watcher never queued its NEED_SNAPSHOT report");
            assertFalse(
                    watcher.isRunning(), "the chain is stopped while the report is outstanding");

            watcher.start(); // deferred, not started: the report callback still owns the pool
            assertFalse(
                    watcher.isRunning(),
                    "the start stays pending until the report completed");
            assertNeverTripped(
                    rpc.anotherPollArrived,
                    NEGATIVE_WINDOW,
                    "the pending start must not poll before the report completed");

            executor.submitted.poll().run(); // the report finally runs
            waitUntil(
                    () -> rpc.pollCount() == 2 && rpc.hasInFlightPoll(),
                    OPERATION_TIMEOUT,
                    "The completed report never started the deferred chain");
            assertEquals(
                    0L,
                    rpc.sinceRevisionOf(2),
                    "the deferred chain leaves from the stopped checkpoint");

            assertThrows(
                    IllegalStateException.class,
                    () -> watcher.start(),
                    "a start against a polling chain keeps its rejection");
            watcher.close();
        } finally {
            client.close();
        }
    }

    /** A report that completes with no restart request leaves the chain stopped. */
    @Test
    void testNeedSnapshotReportWithoutRestartLeavesTheChainStopped() throws Exception {
        final GatedPollRpc rpc = new GatedPollRpc();
        final RaftMetadataClient client = newStubClient(rpc, null);
        try {
            final Recorder recorder = new Recorder();
            final MetadataWatcher watcher =
                    MetadataWatcher.newBuilder(client, recorder)
                            .setSinceRevision(0)
                            .setPollInterval(Duration.ofMillis(50))
                            .build();
            watcher.start();
            waitUntil(
                    rpc::hasInFlightPoll,
                    OPERATION_TIMEOUT,
                    "The watcher never issued its first poll");
            rpc.answerNeedSnapshot(5L); // the report runs on the client's default delivery pool
            waitUntil(
                    () -> recorder.needSnapshotLine() != null && !watcher.isRunning(),
                    OPERATION_TIMEOUT,
                    "The watcher did not stop with the NEED_SNAPSHOT report");
            assertEquals(Long.valueOf(5L), recorder.needSnapshotLine());
            assertNeverTripped(
                    rpc.anotherPollArrived,
                    NEGATIVE_WINDOW,
                    "without a pending restart the chain must stay stopped");
            assertFalse(watcher.isRunning());
            watcher.close();
        } finally {
            client.close();
        }
    }

    /**
     * An executor that refuses the unit ends the chain loudly: the rejection is the watcher's
     * recorded failure - for record batches and for NEED_SNAPSHOT reports alike - and leaves
     * no pending restart behind, so a later restart takes the ordinary direct path.
     */
    @Test
    void testWatcherRecordsTheFailureWhenTheExecutorRejects() throws Exception {
        final GatedPollRpc rpc = new GatedPollRpc();
        final RaftMetadataClient client = newStubClient(rpc, null);
        try {
            final Recorder recorder = new Recorder();
            final MetadataWatcher watcher =
                    MetadataWatcher.newBuilder(client, recorder)
                            .setExecutor(new RejectingExecutor())
                            .setSinceRevision(0)
                            .setPollInterval(Duration.ofMillis(50))
                            .build();
            watcher.start();
            waitUntil(
                    rpc::hasInFlightPoll,
                    OPERATION_TIMEOUT,
                    "The watcher never issued its first poll");
            rpc.answerRecords(1L, 1L);
            waitUntil(
                    () -> watcher.getFailure() != null && !watcher.isRunning(),
                    OPERATION_TIMEOUT,
                    "The executor rejection never stopped the chain");
            assertInstanceOf(RejectedExecutionException.class, watcher.getFailure());
            assertTrue(
                    recorder.deliveredEvents().isEmpty(),
                    "a rejected unit never ran the listener");
            assertEquals(1, rpc.pollCount(), "a stopped chain polls no further");

            watcher.restartFrom(2L);
            assertTrue(watcher.isRunning(), "after the failure a restart takes the direct path");
            assertNull(watcher.getFailure());
            waitUntil(
                    () -> rpc.pollCount() == 2 && rpc.hasInFlightPoll(),
                    OPERATION_TIMEOUT,
                    "The restarted chain never polled");

            rpc.answerNeedSnapshot(20L); // the report's submission is rejected too
            waitUntil(
                    () -> watcher.getFailure() != null && !watcher.isRunning(),
                    OPERATION_TIMEOUT,
                    "The rejected NEED_SNAPSHOT report never stopped the chain");
            assertInstanceOf(RejectedExecutionException.class, watcher.getFailure());
            assertNull(recorder.needSnapshotLine(), "the rejected report never ran");

            watcher.restartFrom(21L);
            assertTrue(
                    watcher.isRunning(), "a rejected report must leave no pending restart");
            watcher.close();
        } finally {
            client.close();
        }
    }

    /**
     * A listener that dies with an Error mid-batch must not claim the undelivered tail: the
     * watermark advances only to the last record the listener completed, and the chain
     * reschedules from there, re-delivering what never arrived. The Error itself surfaces
     * wherever its executor thread puts it - here, synchronously out of the drained unit.
     */
    @Test
    void testWatcherResumesFromTheLastCompletedRecordWhenTheListenerDies() throws Exception {
        final GatedPollRpc rpc = new GatedPollRpc();
        final RaftMetadataClient client = newStubClient(rpc, null);
        try {
            final QueueingExecutor executor = new QueueingExecutor();
            final AtomicBoolean firstRecordFatal = new AtomicBoolean(true);
            final List<ChangeEvent> delivered = new CopyOnWriteArrayList<>();
            final MetadataWatcher watcher =
                    MetadataWatcher.newBuilder(
                                    client,
                                    new MetadataWatcher.Listener() {
                                        @Override
                                        public void onRecord(ChangeEvent event) {
                                            if (firstRecordFatal.compareAndSet(true, false)) {
                                                throw new UnknownError("listener dies mid-batch");
                                            }
                                            delivered.add(event);
                                        }

                                        @Override
                                        public void onNeedSnapshot(
                                                long oldestRetainedRevision) {}
                                    })
                            .setExecutor(executor)
                            .setSinceRevision(0)
                            .setPollInterval(Duration.ofMillis(50))
                            .build();
            watcher.start();
            waitUntil(
                    rpc::hasInFlightPoll,
                    OPERATION_TIMEOUT,
                    "The watcher never issued its first poll");
            rpc.answerRecords(2L, 1L, 2L); // one batch of two records
            waitUntil(
                    () -> executor.submitted.size() == 1,
                    OPERATION_TIMEOUT,
                    "The watcher never queued its first delivery");

            // The unit runs on the test's own thread, so the Error surfaces right here.
            try {
                executor.submitted.poll().run();
                fail("The listener's Error must surface through the drained unit");
            } catch (UnknownError expected) {
                // where the executor's thread puts it - here, the test's own thread
            }

            assertTrue(delivered.isEmpty(), "the dying listener kept no record");
            assertEquals(
                    0L,
                    watcher.lastDeliveredRevision(),
                    "the watermark claims only records the listener completed");
            assertTrue(watcher.isRunning(), "the chain survives the dying listener");
            assertNull(watcher.getFailure(), "a dying listener is not a chain failure");

            // The chain rescheduled from the pre-batch position: the whole batch is re-polled.
            waitUntil(
                    () -> rpc.pollCount() == 2 && rpc.hasInFlightPoll(),
                    OPERATION_TIMEOUT,
                    "The chain never rescheduled after the dying listener");
            assertEquals(
                    0L,
                    rpc.sinceRevisionOf(2),
                    "the redelivery resumes at the pre-batch position");
            rpc.answerRecords(2L, 1L, 2L);
            waitUntil(
                    () -> executor.submitted.size() == 1,
                    OPERATION_TIMEOUT,
                    "The redelivered batch never queued");
            executor.submitted.poll().run();
            assertEquals(
                    Arrays.asList(1L, 2L),
                    revisionsOf(delivered),
                    "the redelivery hands over both records exactly once");
            assertEquals(2L, watcher.lastDeliveredRevision());
            watcher.close();
        } finally {
            client.close();
        }
    }

    /**
     * A listener dying on the first record of EVERY batch is a poison pace alone cannot
     * outlive: the chain would re-deliver the identical batch forever at poll pace, running
     * but never advancing, with no failure surface. Three consecutive stalled deliveries
     * escalate into the recorded failure - the chain stops and the listener's Error becomes
     * observable.
     */
    @Test
    void testAListenerDyingOnEveryRecordEscalatesToTheRecordedFailure() throws Exception {
        final GatedPollRpc rpc = new GatedPollRpc();
        final RaftMetadataClient client = newStubClient(rpc, null);
        try {
            final QueueingExecutor executor = new QueueingExecutor();
            final UnknownError poison = new UnknownError("listener dies on every record");
            final MetadataWatcher watcher =
                    MetadataWatcher.newBuilder(
                                    client,
                                    new MetadataWatcher.Listener() {
                                        @Override
                                        public void onRecord(ChangeEvent event) {
                                            throw poison;
                                        }

                                        @Override
                                        public void onNeedSnapshot(
                                                long oldestRetainedRevision) {}
                                    })
                            .setExecutor(executor)
                            .setSinceRevision(0)
                            .setPollInterval(Duration.ofMillis(50))
                            .build();
            watcher.start();
            waitUntil(
                    rpc::hasInFlightPoll,
                    OPERATION_TIMEOUT,
                    "The watcher never issued its first poll");
            for (int stall = 0; stall < 3; stall++) {
                waitUntil(
                        rpc::hasInFlightPoll,
                        OPERATION_TIMEOUT,
                        "stalled delivery " + (stall + 1) + " never re-polled the batch");
                rpc.answerRecords(3L, 1L, 3L); // the identical batch, every time
                waitUntil(
                        () -> executor.submitted.size() == 1,
                        OPERATION_TIMEOUT,
                        "stalled delivery " + (stall + 1) + " never queued");
                try {
                    executor.submitted.poll().run();
                } catch (UnknownError expected) {
                    // where the drained unit puts it - the escalation records it too
                }
            }
            assertFalse(watcher.isRunning(), "the stalled chain must escalate, not pace forever");
            assertSame(poison, watcher.getFailure(), "the escalation carries the listener's Error");
            final int pollsAtTheStop = rpc.pollCount();
            Thread.sleep(NEGATIVE_WINDOW.toMillis());
            assertEquals(
                    pollsAtTheStop,
                    rpc.pollCount(),
                    "an escalated chain polls no more");
            watcher.close();
        } finally {
            client.close();
        }
    }

    /**
     * A record reply landing after {@code close()} is dropped whole: no delivery unit is
     * submitted, the listener never runs, no failure is recorded and nothing schedules.
     */
    @Test
    void testWatcherDropsARecordReplyLandingAfterClose() throws Exception {
        final GatedPollRpc rpc = new GatedPollRpc();
        final RaftMetadataClient client = newStubClient(rpc, null);
        try {
            final QueueingExecutor executor = new QueueingExecutor();
            final Recorder recorder = new Recorder();
            final MetadataWatcher watcher =
                    MetadataWatcher.newBuilder(client, recorder)
                            .setExecutor(executor)
                            .setSinceRevision(0)
                            .setPollInterval(Duration.ofMillis(50))
                            .build();
            watcher.start();
            waitUntil(
                    rpc::hasInFlightPoll,
                    OPERATION_TIMEOUT,
                    "The watcher never issued its first poll");

            watcher.close(); // the poll is still in flight
            rpc.answerRecords(1L, 1L); // its reply lands after the close

            assertTrue(
                    recorder.deliveredEvents().isEmpty(), "no listener callback after close");
            assertEquals(0, executor.submitted.size(), "no delivery unit is submitted");
            assertEquals(0L, watcher.lastDeliveredRevision(), "the watermark never moved");
            assertNull(watcher.getFailure(), "a dropped reply is not a failure");
            assertFalse(watcher.isRunning());
            assertNeverTripped(
                    rpc.anotherPollArrived,
                    NEGATIVE_WINDOW,
                    "nothing schedules after a post-close reply");
        } finally {
            client.close();
        }
    }

    @Test
    void testLeaseSessionStopsRenewingWhenTheClientCloses() throws Exception {
        final RaftMetadataClient client = newStubClient(new FailingTransportRpc(), null);
        client.close();
        // Constructed directly: openLease() needs a live client; here the attach faces a closed
        // client.
        final LeaseSession session = new LeaseSession(client, 77L, 30_000L, new ManualClock(0L));
        session.start();
        // The registration on a closed client turns the session away dead: no renewal is ever
        // due on the shared scheduler, and the holder's checkpoint reports it.
        assertFalse(session.isAlive(), "A session of a closed client must report dead");
    }

    /**
     * The {@code UNKNOWN_LEASE} renewal is a terminal path: state clears under the session lock
     * and the session then leaves the client's registry - outside that lock - so the final tick
     * dies with it. A session that stays registered after its lease died leaks a dead entry (and
     * keeps the scheduler tick alive) for the life of the client.
     */
    @Test
    void testUnknownLeaseRenewalDetachesTheSessionAndCancelsTheFinalTick() throws Exception {
        final ScriptedReplyRpc rpc = new ScriptedReplyRpc();
        rpc.errorStatuses.put(RaftCommand.Op.LEASE_RENEW, RaftResult.Status.UNKNOWN_LEASE);
        final RaftMetadataClient client = newStubClient(rpc, null);
        try {
            final ManualClock clock = new ManualClock(0L);
            final LeaseSession session = new LeaseSession(client, 4242L, 3_000L, clock);
            session.start();
            assertTrue(client.sessions.contains(session), "a started session is registered");
            assertNotNull(sessionTickOf(client), "the session tick is live while one is registered");

            // A third of the ttl passes; the due renewal comes back UNKNOWN_LEASE - terminal.
            clock.advanceTime(1_100L, TimeUnit.MILLISECONDS);
            session.tick();
            waitUntil(
                    () -> !session.isAlive(),
                    Duration.ofSeconds(5),
                    "The session did not surface its lease's death");

            waitUntil(
                    () -> !client.sessions.contains(session),
                    Duration.ofSeconds(5),
                    "The UNKNOWN_LEASE terminal path left the session registered");
            assertNull(
                    sessionTickOf(client),
                    "the final tick was not cancelled once the last session detached");

            // No resurrection and no further renewals: a stopped session dispatches nothing.
            final int renewals = rpc.countOf(RaftCommand.Op.LEASE_RENEW);
            session.tick();
            assertEquals(
                    renewals,
                    rpc.countOf(RaftCommand.Op.LEASE_RENEW),
                    "a stopped session must not dispatch another renewal");
            session.close(); // idempotent on the terminal path
            assertFalse(session.isAlive());
        } finally {
            client.close();
        }
    }

    /**
     * The client-close terminal path: the sweep stops and detaches every session, and a renewal
     * completion landing AFTER the stop (its reply was in flight) must not resurrect anything -
     * the session stays dead, unregistered, tick-less, and dispatches no further renewal.
     */
    @Test
    void testClientCloseDetachesSessionsAndDelayedCompletionsDoNotResurrect() throws Exception {
        final ScriptedReplyRpc rpc = new ScriptedReplyRpc();
        rpc.holdRenewals = true;
        final RaftMetadataClient client = newStubClient(rpc, null);
        final ManualClock clock = new ManualClock(0L);
        final LeaseSession session = new LeaseSession(client, 555L, 3_000L, clock);
        session.start();
        clock.advanceTime(1_100L, TimeUnit.MILLISECONDS);
        session.tick(); // the renewal is in flight; its reply is withheld
        waitUntil(
                () -> rpc.countOf(RaftCommand.Op.LEASE_RENEW) >= 1,
                Duration.ofSeconds(5),
                "The renewal never went out");

        client.close();
        assertFalse(session.isAlive(), "the sweep stopped the session");
        assertFalse(client.sessions.contains(session), "the sweep detached the session");
        assertNull(sessionTickOf(client), "the sweep cancelled the tick");

        // The delayed successful completion lands after the stop.
        rpc.completeHeld();
        Thread.sleep(200);
        assertFalse(session.isAlive(), "a completion after stop resurrected the session");
        assertFalse(client.sessions.contains(session), "a completion after stop re-registered");
        assertNull(sessionTickOf(client), "a completion after stop resurrected the tick");
        final int renewals = rpc.countOf(RaftCommand.Op.LEASE_RENEW);
        session.tick();
        assertEquals(
                renewals, rpc.countOf(RaftCommand.Op.LEASE_RENEW), "no renewal after stop");
    }

    /**
     * Renewals of a session ride the client's own ttl-sized pipeline, never the general one
     * shared with user operations: through a leader close the general pipeline's window can
     * head-of-line-park a renewal far past its lease's ttl + ttl/3 budget, the exact defect
     * class the renewal pipeline exists to bypass. Pre-fix (the dispatch on the shared {@code
     * renewLeaseAsync}) the renewal appears on the main rpc and this pin fails.
     */
    @Test
    void testSessionRenewalsRideTheirOwnPipelineNotTheGeneralOne() throws Exception {
        final ScriptedReplyRpc mainRpc = new ScriptedReplyRpc();
        final AtomicReference<ScriptedReplyRpc> renewalRpc = new AtomicReference<>();
        final RaftMetadataClient client =
                new RaftMetadataClient(
                        stubRaftClientOf(mainRpc, null),
                        SystemClock.getInstance(),
                        ttlMs -> {
                            final ScriptedReplyRpc rpc = new ScriptedReplyRpc();
                            renewalRpc.set(rpc);
                            return stubRaftClientOf(rpc, null);
                        });
        try {
            final ManualClock clock = new ManualClock(0L);
            final LeaseSession session = new LeaseSession(client, 9L, 3_000L, clock);
            session.start();
            clock.advanceTime(1_100L, TimeUnit.MILLISECONDS);
            session.tick();
            waitUntil(
                    () -> renewalRpc.get() != null
                                    && renewalRpc.get().countOf(RaftCommand.Op.LEASE_RENEW) >= 1,
                    Duration.ofSeconds(5),
                    "The renewal never went out on its own pipeline");
            assertEquals(
                    0,
                    mainRpc.countOf(RaftCommand.Op.LEASE_RENEW),
                    "a session renewal must not ride the general pipeline");
            session.close();
        } finally {
            client.close();
        }
    }

    /**
     * A renewal client that exhausted its ttl-sized budget (or whose sliding window the
     * exhaustion poisoned) is transport trouble, never session death: it is discarded and the
     * next tick mints a fresh one. Pre-fix the equivalent exhaustion on the shared pipeline
     * killed the session for good - the budget failure surfaced as {@code
     * RaftRetryFailureException}, later sends failed with its {@code AlreadyClosedException}
     * wrap, and {@code LeaseSession#onRenewalFailure} misread that cause as the client closing.
     */
    @Test
    void testSpentRenewalPipelineIsReopenedNotFatal() throws Exception {
        final List<ScriptedReplyRpc> pipelines = new CopyOnWriteArrayList<>();
        final RaftMetadataClient client =
                new RaftMetadataClient(
                        stubRaftClientOf(new ScriptedReplyRpc(), null),
                        SystemClock.getInstance(),
                        ttlMs -> {
                            final ScriptedReplyRpc rpc = new ScriptedReplyRpc();
                            rpc.failAll = true; // every attempt fails at the transport level
                            pipelines.add(rpc);
                            return stubRaftClientOf(rpc, tightRenewalPolicy());
                        });
        try {
            final ManualClock clock = new ManualClock(0L);
            final LeaseSession session = new LeaseSession(client, 11L, 3_000L, clock);
            session.start(); // registers with the 50ms scheduler tick
            clock.advanceTime(1_100L, TimeUnit.MILLISECONDS); // due forever until a success
            waitUntil(
                    () -> pipelines.size() >= 2,
                    Duration.ofSeconds(10),
                    "the spent renewal pipeline was never reopened");
            assertTrue(
                    session.isAlive(),
                    "a spent renewal pipeline is transport trouble, never session death");
            session.close();
        } finally {
            client.close();
        }
    }

    /** WRITE-only budget of 300ms, backoff 50ms - the shape the production factory builds. */
    private static RetryPolicy tightRenewalPolicy() {
        return RequestTypeDependentRetryPolicy.newBuilder()
                .setRetryPolicy(
                        RaftClientRequestProto.TypeCase.WRITE,
                        ExponentialBackoffRetry.newBuilder()
                                .setBaseSleepTime(TimeDuration.valueOf(50, TimeUnit.MILLISECONDS))
                                .build())
                .setTimeout(
                        RaftClientRequestProto.TypeCase.WRITE,
                        TimeDuration.valueOf(300, TimeUnit.MILLISECONDS))
                .build();
    }

    /**
     * The renewal sizing law: a third of the sizing ttl per attempt (floored at 250ms, capped
     * by the request timeout), backoff from a sixth of the ttl up to a third, and a whole
     * dispatch budget of the ttl plus the ttl/3 expiry grace. Any regression to the general
     * pipeline's constants (3s attempt, 200ms-5s backoff, 24h budget) fails these values.
     */
    @Test
    void testRenewalPacingIsDerivedFromTheTtl() {
        assertEquals(2_000L, RaftMetadataClient.renewalAttemptTimeoutMs(6_000L, 3_000L));
        assertEquals(3_000L, RaftMetadataClient.renewalAttemptTimeoutMs(30_000L, 3_000L));
        assertEquals(250L, RaftMetadataClient.renewalAttemptTimeoutMs(300L, 3_000L));
        assertEquals(200L, RaftMetadataClient.renewalBackoffBaseMs(6_000L));
        assertEquals(2_000L, RaftMetadataClient.renewalBackoffMaxMs(6_000L));
        assertEquals(8_000L, RaftMetadataClient.renewalRetryBudgetMs(6_000L)); // ttl + ttl/3
    }

    // ------------------------------------------------------------------
    // A poisoned async pipeline is rebuilt, not lingered in
    // ------------------------------------------------------------------

    /**
     * The M10 core, end to end on ratis' real ordered-async window (only the rpc is a stub):
     * one exhausted retry budget poisons the window permanently, and pre-fix that death was the
     * client's own - every later send failed fast with an {@code AlreadyClosedException} wrap,
     * the session's renewal read the wrap as the client closing, stopped, and the leader's
     * expiry cascade deleted its records. With the rebuild the poison costs one failed call:
     * the client swaps in a fresh pipeline, the next renewal rides it, the session survives.
     */
    @Test
    void testPoisonedAsyncPipelineIsRebuiltAndTheLeaseSessionSurvives() throws Exception {
        final ScriptedReplyRpc rpc = new ScriptedReplyRpc();
        final ManualClock clock = new ManualClock(0L);
        final AtomicInteger builds = new AtomicInteger();
        final RaftMetadataClient client =
                newRecoverableStubClient(rpc, singleAttemptPolicy(), 200L, clock, builds);
        final LeaseSession session = new LeaseSession(client, 99L, 3_000L, clock);
        session.start();
        final RaftClient first = raftClientOf(client);

        rpc.failAll = true; // the single-attempt budget exhausts; the window keeps the death
        final Throwable failure =
                awaitFailureOf(
                        client.putAsync("/burst", bytes("x")),
                        "The poisoning put never settled after its budget ran out");
        assertEquals(
                MetadataTransportException.class,
                failure.getClass(),
                "the poisoning call still fails exactly as before");

        rpc.failAll = false;
        clock.advanceTime(1_100L, TimeUnit.MILLISECONDS); // the renewal comes due
        session.tick(); // dies fast on the poisoned window - the detection that rebuilds
        // Pre-fix this times out: nothing ever replaces the dead client, and the session
        // never renews again.
        waitUntil(
                () -> raftClientOf(client) != first,
                Duration.ofSeconds(5),
                "The poisoned pipeline was never rebuilt");
        assertEquals(2, builds.get(), "one poison, one rebuild");

        session.tick(); // due-ness stands: the failed renewal left the session due
        waitUntil(
                () -> rpc.countOf(RaftCommand.Op.LEASE_RENEW) >= 1,
                Duration.ofSeconds(5),
                "The renewal never reached the healed stub through the rebuilt pipeline");
        assertTrue(session.isAlive(), "the session rode out the poison instead of dying");
        assertTrue(client.sessions.contains(session), "the surviving session stayed registered");
    }

    /**
     * The poison source is the sessions' OWN traffic this time: one renewal exhausts its
     * budget and kills the shared window, and pre-fix the sibling session died with it - its
     * next renewal hit the dead window, whose {@code AlreadyClosedException} wrap read as the
     * client closing. With the rebuild both sessions keep renewing through the fresh
     * pipeline, whichever session's renewal set the poison. The single sender thread keeps
     * the poison deterministic: the first-queued renewal fails raw at the transport, the
     * second fails fast on the window it killed.
     */
    @Test
    void testRenewalExhaustingItsOwnBudgetKeepsEverySessionAlive() throws Exception {
        final ScriptedReplyRpc rpc = new ScriptedReplyRpc();
        final ManualClock clock = new ManualClock(0L);
        final RaftMetadataClient client =
                newRecoverableStubClient(
                        rpc, singleAttemptPolicy(), 200L, clock, new AtomicInteger());
        final LeaseSession victim = new LeaseSession(client, 10L, 3_000L, clock);
        final LeaseSession sibling = new LeaseSession(client, 11L, 3_000L, clock);
        victim.start();
        sibling.start();
        final RaftClient first = raftClientOf(client);

        rpc.failAll = true;
        clock.advanceTime(1_100L, TimeUnit.MILLISECONDS);
        victim.tick(); // queued first: its renewal exhausts the budget and poisons the window
        waitUntil(
                () -> {
                    sibling.tick(); // queued second: dies fast on the dead window, and detects
                    return raftClientOf(client) != first;
                },
                Duration.ofSeconds(10),
                "The pipeline poisoned by a renewal was never rebuilt");

        rpc.failAll = false;
        waitUntil(
                () -> {
                    clock.advanceTime(1_100L, TimeUnit.MILLISECONDS); // keep both sessions due
                    victim.tick();
                    sibling.tick();
                    return rpc.countOf(RaftCommand.Op.LEASE_RENEW, 10L) >= 2
                            && rpc.countOf(RaftCommand.Op.LEASE_RENEW, 11L) >= 1;
                },
                Duration.ofSeconds(10),
                "No session renewed through the rebuilt pipeline");
        assertTrue(victim.isAlive(), "the session whose renewal poisoned the window survived");
        assertTrue(sibling.isAlive(), "the sibling session survived the poison");
        assertTrue(
                client.sessions.contains(victim) && client.sessions.contains(sibling),
                "both sessions stayed registered");
    }

    /**
     * The watcher half of the same defect: pre-fix the poisoned window turned the watcher
     * into a silent dead loop - it logged transport trouble and re-polled forever against a
     * window that could never deliver again. With the rebuild the next failing poll swaps in
     * a fresh pipeline and the poll after it delivers again; the watcher never even stops.
     */
    @Test
    void testWatcherResumesAfterThePipelineIsPoisoned() throws Exception {
        final FlakyPollRpc rpc = new FlakyPollRpc();
        rpc.failAll = true; // set before start: the very first poll must be the poison source
        final RaftMetadataClient client =
                newRecoverableStubClient(
                        rpc, singleAttemptPolicy(), 200L, SystemClock.getInstance(),
                        new AtomicInteger());
        try {
            final Recorder recorder = new Recorder();
            final MetadataWatcher watcher =
                    MetadataWatcher.newBuilder(client, recorder)
                            .setSinceRevision(5L)
                            .setPollInterval(Duration.ofMillis(50))
                            .build();
            watcher.start(); // poll 1 exhausts its budget and poisons the window
            waitUntil(
                    () -> rpc.failedSends() >= 1,
                    Duration.ofSeconds(5),
                    "The poisoning poll never reached the transport");

            rpc.failAll = false;
            // Pre-fix every later poll fails fast at the dead window: nothing is ever
            // deliverable again and this wait times out.
            waitUntil(
                    () -> watcher.lastDeliveredRevision() == 6L,
                    Duration.ofSeconds(10),
                    "The watcher never delivered through the rebuilt pipeline");
            assertTrue(watcher.isRunning(), "the watcher survived its pipeline's poison");
            assertNull(watcher.getFailure(), "no failure may stop a healed watcher");
            watcher.close();
        } finally {
            client.close();
        }
    }

    /**
     * The burst shape of one poison: ratis fails every outstanding request of the window at
     * once, so a 24-deep burst lands two dozen concurrent failure settlements on the client's
     * two completion threads. The recovery must stay single-flight - one factory invocation
     * for the whole burst, not one build per detection - and no raft client close may ever
     * run on a completion thread: the stale client's close parks in the transport, and a
     * parked completion thread starves every settle of this client, renewals included.
     */
    @Test
    void testABurstOfConcurrentFailuresRebuildsExactlyOnceAndNeverClosesOnACompletionThread()
            throws Exception {
        final ScriptedReplyRpc rpc = new ScriptedReplyRpc();
        final AtomicInteger builds = new AtomicInteger();
        final RaftMetadataClient client =
                newRecoverableStubClient(
                        rpc, singleAttemptPolicy(), 200L, SystemClock.getInstance(), builds);
        final RaftClient first = raftClientOf(client);

        rpc.failAll = true;
        final List<CompletableFuture<Long>> burst = new ArrayList<>();
        for (int i = 0; i < 24; i++) {
            burst.add(client.putAsync("/burst/" + i, bytes("x")));
        }
        for (CompletableFuture<Long> pending : burst) {
            assertInstanceOf(
                    MetadataTransportException.class,
                    awaitFailureOf(pending, "A burst send never settled"),
                    "the burst failures stay caller-visible");
        }

        waitUntil(
                () -> raftClientOf(client) != first,
                Duration.ofSeconds(5),
                "The poisoned pipeline was never rebuilt");
        assertEquals(
                2, builds.get(), "one poison burst must build exactly one replacement client");

        // The stale client closes only after the quiet-period grace, on the closer thread.
        waitUntil(
                () -> !rpc.closeThreads.isEmpty(),
                Duration.ofSeconds(5),
                "The replaced client was never closed after the grace period");
        // Snapshot before any close of the metadata client itself: its inline close of the
        // current reference legitimately runs on the caller's thread.
        final List<String> closes = new ArrayList<>(rpc.closeThreads);
        assertEquals(1, closes.size(), "only the replaced client closed");
        assertTrue(
                closes.get(0).startsWith("raft-metadata-pipeline-close"),
                "the replaced client closed on " + closes.get(0) + ", not the closer thread");
    }

    /**
     * A persistently re-poisoning root cause must not churn clients: the cool-down caps the
     * rebuild rate at one per quiet period. The replacement window poisons too (every send
     * still fails at the transport), so the pokes inside the quiet window rebuild nothing and
     * swap nothing; once the clock passes the window, the next poison still recovers - the
     * cool-down suppresses, it does not jam. Every poke's failure stays caller-visible: the
     * budget-exhaustion contract.
     */
    @Test
    void testRebuildCoolDownCapsTheChurnOfAPersistentlyPoisonedPipeline() throws Exception {
        final ScriptedReplyRpc rpc = new ScriptedReplyRpc();
        final ManualClock clock = new ManualClock(0L);
        final AtomicInteger builds = new AtomicInteger();
        final RaftMetadataClient client =
                newRecoverableStubClient(rpc, singleAttemptPolicy(), 200L, clock, builds);

        rpc.failAll = true;
        awaitFailureOf(
                client.putAsync("/poke/0", bytes("x")), "The poisoning poke never settled");
        awaitFailureOf(
                client.putAsync("/poke/1", bytes("x")),
                "The poison-detecting poke never settled"); // fails fast: the rebuild trigger
        waitUntil(
                () -> builds.get() >= 2,
                Duration.ofSeconds(5),
                "The first poison never rebuilt the pipeline");
        final RaftClient second = raftClientOf(client);

        // Poke on through the cool-down window: the first poke poisons the replacement raw,
        // every later one dies fast on it - suppressed, no rebuild, identity unchanged.
        for (int poke = 2; poke <= 5; poke++) {
            clock.advanceTime(40L, TimeUnit.MILLISECONDS);
            assertInstanceOf(
                    MetadataTransportException.class,
                    awaitFailureOf(
                            client.putAsync("/poke/" + poke, bytes("x")),
                            "A suppressed poke never settled"),
                    "suppression never hides the failure from the caller");
            assertEquals(2, builds.get(), "the cool-down window rebuilt on poke " + poke);
            assertSame(second, raftClientOf(client), "the cool-down window swapped on poke " + poke);
        }

        // Past the quiet period the same poison recovers: suppressed is not jammed.
        clock.advanceTime(240L, TimeUnit.MILLISECONDS); // 400ms past the swap: past the window
        awaitFailureOf(
                client.putAsync("/poke/last", bytes("x")), "The post-window poke never settled");
        waitUntil(
                () -> builds.get() >= 3,
                Duration.ofSeconds(5),
                "A poison past the quiet period never rebuilt the pipeline");
        assertNotSame(second, raftClientOf(client), "the post-window poison swapped nothing");
    }

    /**
     * The non-rebuildable client keeps the pre-fix terminal semantics: an injected raft client
     * (the stub seam) has no factory, so a poisoned window has no way back - its renewal
     * surfaces the poison instead of promising a reopen that can never happen, and the
     * session stops and detaches terminally rather than retrying a dead pipeline with {@code
     * isAlive()} held true while the lease is long gone server-side.
     */
    @Test
    void testPoisonedNonRebuildableClientStillStopsItsSessionTerminally() throws Exception {
        final ScriptedReplyRpc rpc = new ScriptedReplyRpc();
        final RaftMetadataClient client = newStubClient(rpc, singleAttemptPolicy());
        try {
            final ManualClock clock = new ManualClock(0L);
            final LeaseSession session = new LeaseSession(client, 77L, 3_000L, clock);
            session.start();

            rpc.failAll = true; // the put poisons the only, injected, window
            awaitFailureOf(
                    client.putAsync("/poison", bytes("x")),
                    "The poisoning put never settled");
            rpc.failAll = false;

            clock.advanceTime(1_100L, TimeUnit.MILLISECONDS);
            session.tick(); // the renewal dies fast on the dead window - terminally
            waitUntil(
                    () -> !session.isAlive(),
                    Duration.ofSeconds(5),
                    "The session never stopped on its un-rebuildable pipeline");
            waitUntil(
                    () -> !client.sessions.contains(session),
                    Duration.ofSeconds(5),
                    "The terminal session stayed registered");
            assertNull(sessionTickOf(client), "the final tick was not cancelled");
        } finally {
            client.close();
        }
    }

    /**
     * The replaced client also serves sync io() traffic, which never touches the window, so
     * its close waits out the quiet-period grace: an attempt in flight when the poison landed
     * keeps its per-attempt lifetime instead of being cut mid-retry. No close may land inside
     * the grace; the close that lands after it runs on the closer thread.
     */
    @Test
    void testTheReplacedClientIsClosedOnlyAfterTheGracePeriod() throws Exception {
        final ScriptedReplyRpc rpc = new ScriptedReplyRpc();
        final AtomicInteger builds = new AtomicInteger();
        final RaftMetadataClient client =
                newRecoverableStubClient(
                        rpc, singleAttemptPolicy(), 150L, SystemClock.getInstance(), builds);
        final RaftClient first = raftClientOf(client);

        rpc.failAll = true;
        awaitFailureOf(
                client.putAsync("/poison/0", bytes("x")), "The poisoning put never settled");
        awaitFailureOf(
                client.putAsync("/poison/1", bytes("x")),
                "The poison-detecting put never settled"); // the rebuild trigger
        waitUntil(
                () -> raftClientOf(client) != first && builds.get() == 2,
                Duration.ofSeconds(5),
                "The poisoned pipeline was never rebuilt exactly once");

        assertNeverTripped(
                rpc.closedOnce,
                Duration.ofMillis(50),
                "The replaced client was closed inside the grace period");
        waitUntil(
                () -> !rpc.closeThreads.isEmpty(),
                Duration.ofSeconds(5),
                "The replaced client was never closed after the grace period");
        assertTrue(
                rpc.closeThreads.get(0).startsWith("raft-metadata-pipeline-close"),
                "the replaced client closed on "
                        + rpc.closeThreads.get(0)
                        + ", not the closer thread");
    }

    /**
     * What the grace-period close does to a mutation still in flight on the replaced client:
     * ratis answers the cut with a bare AlreadyClosedException (the sync io() loop rethrows it
     * straight, the async window's close-time teardown fails the request with it) - no budget
     * failure underneath, unlike the poison cascade. The request may already sit applied in the
     * raft log, so the cut must answer the unknown-outcome family; the safe-to-retry family is
     * reserved for failures of a client the user's own close cut.
     */
    @Test
    void testTheGracePeriodCutOfAReplacedClientAnswersUnknownOutcome() throws Exception {
        final ScriptedReplyRpc rpc = new ScriptedReplyRpc();
        final AtomicInteger builds = new AtomicInteger();
        final RaftMetadataClient client =
                newRecoverableStubClient(
                        rpc, singleAttemptPolicy(), 150L, SystemClock.getInstance(), builds);
        final RaftClient first = raftClientOf(client);

        // A mutation in flight on the soon-to-be-replaced client. The warm-up reply unlocks
        // the ordered window (ratis delays every send behind an un-replied first), so the
        // withheld create is actually issued and parks on its rpc future, past the swap and
        // past the close - the window's fail sweep starts at the poison pair's seqNum and
        // never touches it.
        client.putAsync("/warm", bytes("x")).join();
        rpc.holdCreatePath = "/in-flight";
        final CompletableFuture<String> held = client.createAsync("/in-flight", bytes("x"));

        rpc.failAll = true;
        awaitFailureOf(
                client.putAsync("/poison/0", bytes("x")), "The poisoning put never settled");
        awaitFailureOf(
                client.putAsync("/poison/1", bytes("x")),
                "The poison-detecting put never settled"); // the rebuild trigger
        waitUntil(
                () -> raftClientOf(client) != first && builds.get() == 2,
                Duration.ofSeconds(5),
                "The poisoned pipeline was never rebuilt exactly once");
        waitUntil(
                () -> !rpc.closeThreads.isEmpty(),
                Duration.ofSeconds(5),
                "The replaced client was never closed after the grace period");

        // The transport reports the withheld reply's failure on the now-closed client - the
        // same cut a channel death delivers in production. failAll still on, so the withheld
        // reply settles as a transport failure and walks ratis' closed-client path.
        rpc.completeHeld();

        final Throwable failure = awaitFailureOf(held, "The in-flight create never settled");
        assertEquals(
                PossiblyCommittedException.class,
                failure.getClass(),
                "the grace-period cut must answer unknown outcome, not " + failure);
        assertTrue(
                causedBy(failure, AlreadyClosedException.class),
                "the cut is close-shaped: " + failure);
    }

    // ------------------------------------------------------------------
    // The lock speaks the typed revocation, never a path delete
    // ------------------------------------------------------------------

    /**
     * A successful release stops the session's renewals and then revokes the holder's private
     * lease through the ordered pipeline - the one command whose server-side cascade is scoped to
     * that lease. It must never send a {@code DELETE} of the lock path: a path delete is
     * ownership-unsafe (it cannot tell whose record sits at the path).
     */
    @Test
    void testLockReleaseRevokesTheLeaseNeverDeletesThePath() throws Exception {
        final ScriptedReplyRpc rpc = new ScriptedReplyRpc();
        final RaftMetadataClient client = newStubClient(rpc, null);
        try {
            final MetadataLock lock = newLockOn(client, "/stub/lock");
            final MetadataLock.Hold hold =
                    lock.acquireAsync().get(OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            assertTrue(hold.isHeld());
            final long registeredLease = rpc.singleLeaseIdOf();
            assertTrue(registeredLease != 0L, "the acquire registered exactly one lease");

            hold.release();
            waitUntil(
                    () ->
                            rpc.countOf(RaftCommand.Op.LEASE_EXPIRE) > 0
                                    || rpc.countOf(RaftCommand.Op.DELETE) > 0,
                    Duration.ofSeconds(5),
                    "The release never sent its cleanup command");
            assertEquals(
                    0,
                    rpc.countOf(RaftCommand.Op.DELETE),
                    "release must revoke the lease, never delete the lock path");
            assertEquals(
                    1,
                    rpc.countOf(RaftCommand.Op.LEASE_EXPIRE),
                    "release sends exactly one revocation");
            assertEquals(
                    registeredLease,
                    rpc.sentLeaseIdOf(RaftCommand.Op.LEASE_EXPIRE),
                    "the revoked lease is the holder's own private lease");
            assertFalse(hold.isHeld());
        } finally {
            client.close();
        }
    }

    /**
     * The ambiguous create: its command went out, the reply is still in flight (its outcome is
     * unknown to the contender), and the acquire is cancelled. The unwind must still speak only
     * the lease-scoped revocation - issued AFTER the create in the ordered pipeline, so the
     * server's leaseId-checked cascade covers a create that committed anyway - and never a path
     * delete, which could remove a later holder's record at the same path.
     */
    @Test
    void testAmbiguousCommittedCreateIsRetiredByRevocationNeverByAPathDelete() throws Exception {
        final ScriptedReplyRpc rpc = new ScriptedReplyRpc();
        rpc.holdCreatePath = "/stub/lock";
        final RaftMetadataClient client = newStubClient(rpc, null);
        try {
            final MetadataLock lock = newLockOn(client, "/stub/lock");
            final CompletableFuture<MetadataLock.Hold> pending = lock.acquireAsync();
            waitUntil(
                    () -> rpc.countOf(RaftCommand.Op.CREATE, "/stub/lock") == 1,
                    Duration.ofSeconds(5),
                    "The contender's create never went out");
            final long contendersLease = rpc.sentLeaseIdOf(RaftCommand.Op.CREATE, "/stub/lock");

            // Cancel while the create's outcome is unknown.
            lock.close();
            final ExecutionException failure =
                    assertThrows(
                            ExecutionException.class,
                            () -> pending.get(OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
            assertTrue(failure.getCause() instanceof IllegalStateException, "close cancels");

            waitUntil(
                    () ->
                            rpc.countOf(RaftCommand.Op.LEASE_EXPIRE) > 0
                                    || rpc.countOf(RaftCommand.Op.DELETE) > 0,
                    Duration.ofSeconds(5),
                    "The unwind never sent its cleanup command");
            assertEquals(
                    0,
                    rpc.countOf(RaftCommand.Op.DELETE),
                    "the ambiguous-undo must revoke, never delete the path");
            assertEquals(
                    contendersLease,
                    rpc.sentLeaseIdOf(RaftCommand.Op.LEASE_EXPIRE),
                    "the revoked lease is the one the ambiguous create bound its record to");
            assertTrue(
                    rpc.firstIndexOf(RaftCommand.Op.CREATE, "/stub/lock")
                            < rpc.firstIndexOf(RaftCommand.Op.LEASE_EXPIRE, null),
                    "the revocation follows the create in the ordered pipeline, so the cascade"
                            + " covers a create that committed anyway");

            // The delayed reply lands: the create had committed after all. Nothing may resurrect.
            rpc.completeHeld();
            Thread.sleep(200);
            assertEquals(
                    0, rpc.countOf(RaftCommand.Op.DELETE), "the late commit changed nothing");
            assertTrue(pending.isDone() && pending.isCompletedExceptionally());
        } finally {
            client.close();
        }
    }

    /**
     * A revocation that cannot reach the cluster (transport dead) is logged and the release falls
     * back to scheduler expiry within one ttl - the stopped renewals already doom the lease. It
     * must never fall back to a path delete.
     */
    @Test
    void testFailedRevocationFallsBackToExpiryNeverToAPathDelete() throws Exception {
        final ScriptedReplyRpc rpc = new ScriptedReplyRpc();
        final RaftMetadataClient client =
                newStubClient(
                        rpc,
                        RetryPolicies.retryUpToMaximumCountWithFixedSleep(
                                2, TimeDuration.valueOf(50, TimeUnit.MILLISECONDS)));
        try {
            final MetadataLock lock = newLockOn(client, "/stub/lock");
            final MetadataLock.Hold hold =
                    lock.acquireAsync().get(OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            rpc.failAll = true; // the revocation cannot get through anymore
            hold.release(); // must not throw
            waitUntil(
                    () -> rpc.countOf(RaftCommand.Op.LEASE_EXPIRE) > 0,
                    Duration.ofSeconds(5),
                    "The revocation never went out despite the failing transport");
            assertEquals(
                    0,
                    rpc.countOf(RaftCommand.Op.DELETE),
                    "a failed revocation must fall back to scheduler expiry, never to a path"
                            + " delete");
            assertFalse(hold.isHeld());
        } finally {
            client.close();
        }
    }

    // ------------------------------------------------------------------
    // Cluster tests
    // ------------------------------------------------------------------

    @Test
    void testWriteReadRoundtripAndConflictMapping() throws Exception {
        try (MetadataCluster cluster = newCluster(false)) {
            cluster.start();
            final RaftMetadataClient client = cluster.newMetadataClient();
            try {
                // A creating put requires the parent record (as ZooKeeper's setData fails NoNode).
                assertEquals("/a", client.create("/a", bytes("dir")));

                assertEquals(1L, client.put("/a/b", bytes("v1")));
                assertEquals(2L, client.put("/a/b", bytes("v2")));
                final RaftMetadataClient.MetadataRecord record = client.get("/a/b");
                assertArrayEquals(bytes("v2"), record.getData());
                assertEquals(2L, record.getVersion());
                assertEquals(
                        0L, record.getLeaseId(), "an unbound record carries no lease");

                assertEquals("/a/c", client.create("/a/c", bytes("c")));
                assertEquals(
                        RaftResult.Status.NODE_EXISTS,
                        expectMetadataFailure(() -> client.create("/a/c", bytes("x"))));
                assertEquals(
                        RaftResult.Status.NO_PARENT,
                        expectMetadataFailure(() -> client.create("/no/parent", bytes("x"))));

                // compare-and-swap: the matching version wins, a stale one loses, 0 means create.
                assertEquals(3L, client.compareAndPut("/a/b", bytes("v3"), 2L));
                assertArrayEquals(bytes("v3"), client.get("/a/b").getData());
                assertEquals(
                        RaftResult.Status.VERSION_MISMATCH,
                        expectMetadataFailure(
                                () -> client.compareAndPut("/a/b", bytes("v4"), 2L)));
                assertEquals(1L, client.compareAndPut("/a/fresh", bytes("f"), 0L));

                // Zero-padded numbers: lexicographic equals numeric at fixed width, never
                // colliding.
                client.create("/seq", bytes("dir"));
                final String first = client.createSequential("/seq/task-", bytes("1"));
                final String second = client.createSequential("/seq/task-", bytes("2"));
                final String third = client.createSequential("/seq/task-", bytes("3"));
                assertTrue(first.matches(SEQUENTIAL_PATH_PATTERN));
                assertTrue(
                        first.compareTo(second) < 0,
                        "sequential numbers grow monotonically");
                assertTrue(second.compareTo(third) < 0);

                // listing is a prefix scan in lexicographic order (the prefix record included).
                assertEquals(
                        Arrays.asList("/a", "/a/b", "/a/c", "/a/fresh"),
                        pathsOf(client.list("/a")));
                assertEquals(
                        Arrays.asList(first, second, third), pathsOf(client.list("/seq/task-")));

                assertTrue(client.exists("/a/b"));
                assertFalse(client.exists("/missing"));
                assertEquals(
                        RaftResult.Status.NOT_FOUND,
                        expectMetadataFailure(() -> client.get("/missing")));

                assertEquals(
                        RaftResult.Status.NOT_FOUND,
                        expectMetadataFailure(() -> client.delete("/never")));
                assertEquals(
                        RaftResult.Status.HAS_CHILDREN,
                        expectMetadataFailure(() -> client.delete("/a")));
                assertEquals(
                        4L,
                        client.deleteRecursive("/a"),
                        "the recursive delete removes the subtree and its root record");
                assertFalse(client.exists("/a"));
                assertFalse(client.exists("/a/b"));

                client.create("/m", bytes("dir"));
                final List<RaftResult> applied =
                        client.multi(
                                Arrays.asList(
                                        RaftCommand.put("/m/x", bytes("x"), 0L, 0L),
                                        RaftCommand.put("/m/y", bytes("y"), 0L, 0L)));
                assertEquals(2, applied.size());
                assertEquals(RaftResult.Status.OK, applied.get(0).getStatus());
                assertEquals(RaftResult.Status.OK, applied.get(1).getStatus());
                try {
                    client.multi(
                            Arrays.asList(
                                    RaftCommand.put("/m/z", bytes("z"), 0L, 0L),
                                    RaftCommand.cas("/m/x", bytes("clash"), 99L, 0L, 0L)));
                    throw new AssertionError("The failing MULTI was expected to throw");
                } catch (RaftMetadataException e) {
                    assertEquals(RaftResult.Status.MULTI_FAILED, e.getStatus());
                    assertEquals(1, e.getFailIndex());
                    assertEquals(
                            RaftResult.Status.VERSION_MISMATCH,
                            e.getFailStatus(),
                            "/m/x holds version 1, so the stale expected version loses");
                }
                assertFalse(
                        client.exists("/m/z"),
                        "a failed MULTI applies none of its sub-commands");
                assertTrue(client.exists("/m/x"));

                assertEquals(5L, client.increment("/rounds", 5));
                assertEquals(3L, client.increment("/rounds", -2));

                final RaftMetadataClient.PollResult everything = client.poll(0, POLL_BUDGET_BYTES);
                assertTrue(
                        everything.getWatermark() >= client.revision() - 1L,
                        "the watermark covers every write this test made");
                assertStrictlyIncreasing(revisionsOf(everything.getRecords()));
                assertEquals(
                        1L,
                        everything.getOldestRetainedRevision(),
                        "no snapshot was taken, so nothing was collected");
            } finally {
                client.close();
            }
        }
    }

    @Test
    void testAsyncRoundtripAndConcurrentWrites() throws Exception {
        try (MetadataCluster cluster = newCluster(false)) {
            cluster.start();
            final RaftMetadataClient client = cluster.newMetadataClient();
            try {
                assertEquals("/async", client.createAsync("/async", bytes("dir")).join());
                assertEquals(1L, client.putAsync("/async/b", bytes("v1")).join());
                assertEquals(2L, client.putAsync("/async/b", bytes("v2")).join());
                final RaftMetadataClient.MetadataRecord record =
                        client.getAsync("/async/b").join();
                assertArrayEquals(bytes("v2"), record.getData());
                assertEquals(2L, record.getVersion());

                // Concurrent async writes share one ordered pipeline but apply as separate
                // entries.
                client.createAsync("/async/p", bytes("dir")).join();
                final List<CompletableFuture<Long>> writes = new ArrayList<>();
                for (int i = 0; i < 10; i++) {
                    writes.add(client.putAsync("/async/p/" + i, bytes("v" + i)));
                }
                CompletableFuture.allOf(writes.toArray(new CompletableFuture[0])).join();
                for (CompletableFuture<Long> write : writes) {
                    assertEquals(1L, write.join());
                }

                final List<String> expected = new ArrayList<>();
                expected.add("/async");
                expected.add("/async/b");
                expected.add("/async/p");
                for (int i = 0; i < 10; i++) {
                    expected.add("/async/p/" + i);
                    assertTrue(client.existsAsync("/async/p/" + i).join());
                }
                assertEquals(expected, pathsOf(client.listAsync("/async").join()));

                // The status mapping of the sync surface holds in the async one.
                final RaftMetadataException conflict =
                        (RaftMetadataException)
                                awaitFailureOf(
                                        client.compareAndPutAsync("/async/b", bytes("v3"), 99L),
                                        "The stale compare-and-put never completed");
                assertEquals(RaftResult.Status.VERSION_MISMATCH, conflict.getStatus());

                assertEquals(5L, client.incrementAsync("/async-rounds", 5).join());
                assertEquals(3L, client.incrementAsync("/async-rounds", -2).join());

                // The async-opened session is a live session: renewable, and closed with the
                // client.
                final LeaseSession asyncLease = client.openLeaseAsync(Duration.ofSeconds(30)).join();
                assertTrue(
                        client.renewLeaseAsync(asyncLease.getLeaseId(), Duration.ofSeconds(30))
                                .join()
                                > 0);

                final long revisionBefore = client.revisionAsync().join();
                client.putAsync("/async/watermark", bytes("w")).join();
                final RaftMetadataClient.PollResult polled =
                        client.pollAsync(0L, POLL_BUDGET_BYTES).join();
                assertTrue(
                        polled.getWatermark() > revisionBefore,
                        "the async write is committed and applied");
                assertStrictlyIncreasing(revisionsOf(polled.getRecords()));
            } finally {
                client.close();
            }
        }
    }

    @Test
    void testConcurrentSequentialCreatesAreUnique() throws Exception {
        try (MetadataCluster cluster = newCluster(false)) {
            cluster.start();
            final RaftMetadataClient client = cluster.newMetadataClient();
            try {
                client.create("/jobs", bytes("dir"));

                final int threads = 4;
                final int createsPerThread = 25;
                final ExecutorService pool = Executors.newFixedThreadPool(threads);
                try {
                    final List<Future<List<String>>> futures = new ArrayList<>();
                    for (int t = 0; t < threads; t++) {
                        futures.add(
                                pool.submit(
                                        () -> {
                                            final List<String> created = new ArrayList<>();
                                            for (int i = 0; i < createsPerThread; i++) {
                                                created.add(
                                                        client.createSequential(
                                                                "/jobs/task-",
                                                                bytes("payload-" + i)));
                                            }
                                            return created;
                                        }));
                    }
                    final Set<String> unique = new HashSet<>();
                    for (Future<List<String>> future : futures) {
                        unique.addAll(
                                future.get(OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
                    }
                    assertEquals(
                            threads * createsPerThread,
                            unique.size(),
                            "every concurrent create got its own sequence number");
                    for (String path : unique) {
                        assertTrue(path.matches(SEQUENTIAL_PATH_PATTERN));
                    }
                    assertEquals(
                            new ArrayList<>(new TreeSet<>(unique)),
                            pathsOf(client.list("/jobs/task-")),
                            "the store lists exactly the acknowledged creates, in order");
                } finally {
                    pool.shutdownNow();
                }
            } finally {
                client.close();
            }
        }
    }

    @Test
    void testListPagesBeyondOneResultCap() throws Exception {
        try (MetadataCluster cluster = newCluster(false)) {
            cluster.start();
            final RaftMetadataClient client = cluster.newMetadataClient();
            try {
                client.create("/pages", bytes("dir"));

                // one MULTI is capped at RaftCommand.MAX_MULTI_OPS, so seed in batches
                final List<String> expected = new ArrayList<>();
                for (int batch = 0; batch < 2; batch++) {
                    final List<RaftCommand> puts = new ArrayList<>();
                    for (int i = 0; i < 600; i++) {
                        final String path =
                                String.format(Locale.ROOT, "/pages/%04d", batch * 600 + i);
                        expected.add(path);
                        puts.add(RaftCommand.put(path, new byte[0], 0L, 1L));
                    }
                    client.multi(puts);
                }

                final RaftMetadataClient.ListPage firstPage =
                        client.listPage("/pages/", "", 128);
                assertEquals(expected.subList(0, 128), pathsOf(firstPage.getEntries()));
                assertTrue(firstPage.hasMore());
                assertEquals(
                        expected,
                        pathsOf(client.list("/pages/")),
                        "the paging client enumerates the whole prefix");
            } finally {
                client.close();
            }
        }
    }

    @Test
    void testListEnumeratesOnePointInTimeCommand() throws Exception {
        try (MetadataCluster cluster = newCluster(false)) {
            cluster.start();
            final RaftMetadataClient client = cluster.newMetadataClient();
            try {
                client.create("/pit", bytes("dir"));

                final List<String> expected = new ArrayList<>();
                for (int batch = 0; batch < 2; batch++) {
                    final List<RaftCommand> puts = new ArrayList<>();
                    for (int i = 0; i < 600; i++) {
                        final String path =
                                String.format(Locale.ROOT, "/pit/%04d", batch * 600 + i);
                        expected.add(path);
                        puts.add(RaftCommand.put(path, new byte[0], 0L, 1L));
                    }
                    client.multi(puts);
                }
                // Sorts inside what a first 1024-cap page consumes; a resumed later page would
                // skip it.
                final String late = "/pit/0500x";
                final List<String> expectedWithLate = new ArrayList<>(expected);
                expectedWithLate.add(late);
                Collections.sort(expectedWithLate);

                // The prototype observed the single LIST request through a forwarding client
                // rpc; the stock transports admit no such seam, so the same write now races the
                // enumeration from a second thread and the outcome is asserted directly:
                // whatever the interleaving, the paged enumeration is one consistent view - it
                // never misses a pre-existing record and never fabricates one.
                final AtomicReference<Throwable> lateFailure = new AtomicReference<>();
                final Thread lateWriter =
                        new Thread(
                                () -> {
                                    try {
                                        client.put(late, bytes("late"));
                                    } catch (Throwable t) {
                                        lateFailure.set(t);
                                    }
                                },
                                "late-metadata-writer");
                lateWriter.start();
                final List<String> listed = pathsOf(client.list("/pit/"));
                lateWriter.join(OPERATION_TIMEOUT.toMillis());
                assertFalse(lateWriter.isAlive());
                assertNull(lateFailure.get());
                assertTrue(
                        expectedWithLate.containsAll(listed),
                        "the paged enumeration is one consistent view: it never misses a"
                                + " pre-existing record and never fabricates one");
                assertTrue(
                        listed.containsAll(expected),
                        "the enumeration never misses a pre-existing record");
                assertArrayEquals(bytes("late"), client.get(late).getData());
            } finally {
                client.close();
            }
        }
    }

    @Test
    void testTransparentLeaderFailoverDuringWrites() throws Exception {
        try (MetadataCluster cluster = newCluster(false)) {
            cluster.start();
            final RaftMetadataClient client = cluster.newMetadataClient();
            try {
                // Cached once up front: leaderId() may return null while an election runs.
                final RaftPeerId leaderId = cluster.awaitLeader();
                final int leaderIndex = cluster.indexOf(leaderId);
                client.create("/fo", bytes("dir"));

                final ConcurrentLinkedQueue<String> created = new ConcurrentLinkedQueue<>();
                final AtomicReference<Throwable> writerFailure = new AtomicReference<>();
                final Thread writer =
                        new Thread(
                                () -> {
                                    try {
                                        for (int i = 0; i < FAILOVER_WRITES; i++) {
                                            created.add(
                                                    client.createSequential(
                                                            "/fo/item-", bytes("item-" + i)));
                                        }
                                    } catch (Throwable t) {
                                        writerFailure.set(t);
                                    }
                                },
                                "failover-writer");
                writer.start();

                waitUntil(
                        () -> created.size() >= 5,
                        OPERATION_TIMEOUT,
                        "The writer did not start producing through the first leader");
                final List<RaftPeerId> survivors = cluster.peerIds();
                survivors.remove(leaderId);
                cluster.killServer(leaderIndex);
                cluster.awaitLeader(survivors);

                writer.join(OPERATION_TIMEOUT.toMillis());
                assertFalse(writer.isAlive(), "the writer completed after the failover");
                assertNull(
                        writerFailure.get(),
                        "no exception may leak: the failover is transparent");
                assertEquals(FAILOVER_WRITES, created.size());
                assertEquals(
                        FAILOVER_WRITES,
                        new HashSet<>(created).size(),
                        "no sequential number was handed out twice");

                for (String path : created) {
                    assertTrue(client.exists(path));
                }
                assertEquals(
                        new ArrayList<>(new TreeSet<>(created)),
                        pathsOf(client.list("/fo/item-")));

                client.put("/fo/after-failover", bytes("ok"));
                assertArrayEquals(bytes("ok"), client.get("/fo/after-failover").getData());

                cluster.restartServer(leaderIndex);
                final MetadataStateMachine rejoined = cluster.stateMachine(leaderIndex);
                final long targetRevision = client.revision();
                waitUntil(
                        () ->
                                MetadataCluster.directQuery(rejoined, RaftCommand.revision())
                                                .getRevision()
                                        >= targetRevision,
                        OPERATION_TIMEOUT,
                        "The rejoined node did not catch up to revision " + targetRevision);
                assertArrayEquals(
                        bytes("ok"),
                        MetadataCluster.directQuery(rejoined, RaftCommand.get("/fo/after-failover"))
                                .getData());
            } finally {
                client.close();
            }
        }
    }

    @Test
    void testLeaseSessionLifecycleFromTheClientView() throws Exception {
        try (MetadataCluster cluster = newCluster(false)) {
            cluster.start();
            final RaftMetadataClient client = cluster.newMetadataClient();
            try {
                final long ttlMs = TimeUnit.SECONDS.toMillis(30);

                final LeaseSession session = client.openLease(Duration.ofSeconds(30));
                final long leaseId = session.getLeaseId();
                final long originalExpireAt = cluster.clock.milliseconds() + ttlMs;

                client.create("/eph", bytes("dir"));
                assertEquals("/eph/a", client.create("/eph/a", bytes("a"), session));
                client.put("/eph/b", bytes("b"), session);
                client.create("/eph/keep", bytes("keep"));
                assertEquals(leaseId, client.get("/eph/a").getLeaseId());

                for (int round = 1; round <= 4; round++) {
                    cluster.clock.advanceTime(10, TimeUnit.SECONDS);
                    final long expectedExpireAt = cluster.clock.milliseconds() + ttlMs;
                    MetadataCluster.waitThroughTransientFailure(
                            () -> {
                                final RaftResult.Lease lease =
                                        leaseOf(
                                                adminSendReadOnly(cluster, RaftCommand.leaseList()),
                                                leaseId);
                                return lease != null
                                        && lease.getExpireAtMs() == expectedExpireAt;
                            },
                            OPERATION_TIMEOUT,
                            "round "
                                    + round
                                    + ": the session did not renew to "
                                    + expectedExpireAt);
                }
                assertTrue(
                        cluster.clock.milliseconds() > originalExpireAt,
                        "the session outlived the lease's original expiry");
                assertTrue(client.exists("/eph/a"));
                assertTrue(client.exists("/eph/b"));

                // Past the last expiry plus the clock-domain grace (ttl/3) plus the default
                // skew tolerance (ttl/3), the scheduler takes over.
                session.close();
                cluster.clock.advanceTime(55, TimeUnit.SECONDS);
                waitUntil(
                        () ->
                                !client.exists("/eph/a")
                                        && !client.exists("/eph/b")
                                        && client.exists("/eph/keep"),
                        OPERATION_TIMEOUT,
                        "The closed session's lease and ephemeral records were not expired");
                final RaftPeerId expiringLeader = cluster.awaitLeader(cluster.peerIds());
                // The harness exposes no per-server scheduler proposal counter; the lease
                // vanishing from the new leader's listing is the same expiry, observed from the
                // client side instead of the leader's scheduler metrics.
                MetadataCluster.waitThroughTransientFailure(
                        () ->
                                leaseOf(adminSendReadOnly(cluster, RaftCommand.leaseList()), leaseId)
                                        == null,
                        OPERATION_TIMEOUT,
                        "The leader "
                                + expiringLeader
                                + " did not process the expiry of the closed session");
            } finally {
                client.close();
            }
        }
    }

    @Test
    void testLeaseSessionLivenessSurfacesTheLeasesDeath() throws Exception {
        try (MetadataCluster cluster = newCluster(false)) {
            cluster.start();
            final RaftMetadataClient client = cluster.newMetadataClient();
            try {
                final long ttlMs = TimeUnit.SECONDS.toMillis(30);
                final long now = cluster.clock.milliseconds();
                // A lease with a ttl long enough that no clock advance of this test can make it
                // due.
                final long quietLeaseId = 801L;
                final long deadLeaseId = 802L;
                assertEquals(
                        RaftResult.Status.OK,
                        adminSend(
                                        cluster,
                                        RaftCommand.leaseRegister(
                                                quietLeaseId, 10 * ttlMs, now + 10 * ttlMs))
                                .getStatus());
                assertEquals(
                        RaftResult.Status.OK,
                        adminSend(
                                        cluster,
                                        RaftCommand.leaseRegister(deadLeaseId, ttlMs, now + ttlMs))
                                .getStatus());

                final LeaseSession quiet =
                        new LeaseSession(client, quietLeaseId, 10 * ttlMs, cluster.clock);
                quiet.start();
                assertTrue(quiet.isAlive(), "a started session on a live lease is alive");
                quiet.close();
                assertFalse(quiet.isAlive(), "closing the session ends its liveness");

                final LeaseSession paused =
                        new LeaseSession(client, deadLeaseId, ttlMs, cluster.clock);
                assertFalse(
                        paused.isAlive(), "a session that was never started is not alive");
                cluster.clock.advanceTime(55, TimeUnit.SECONDS);
                MetadataCluster.waitThroughTransientFailure(
                        () ->
                                leaseOf(adminSendReadOnly(cluster, RaftCommand.leaseList()), deadLeaseId)
                                        == null,
                        OPERATION_TIMEOUT,
                        "The paused holder's lease was not expired underneath it");
                paused.start();
                assertTrue(
                        paused.isAlive(),
                        "the session is alive until its renewal is rejected");
                waitUntil(
                        () -> !paused.isAlive(),
                        OPERATION_TIMEOUT,
                        "The session did not surface its lease's death through isAlive()");
                paused.close();
            } finally {
                client.close();
            }
        }
    }

    /**
     * End-to-end through the real renewal machinery: once a session's lease dies underneath it,
     * the {@code UNKNOWN_LEASE} renewal detaches the session from the client's registry and the
     * final tick dies with it - and a session opened afterwards ticks again, proving the registry
     * was truly emptied and refilled. The lease's death is forced through the admin client (the
     * exact command the leader's scheduler proposes after a ttl lapse) so the death is
     * deterministic; racing the scheduler's scan cadence against the session's own renewal would
     * let the renewal re-extend the lease instead.
     */
    @Test
    void testExpiredSessionDetachesAndTickingResumesForTheNextSession() throws Exception {
        try (MetadataCluster cluster = newCluster(false)) {
            cluster.start();
            final RaftMetadataClient client = cluster.newMetadataClient();
            try {
                final LeaseSession session = client.openLease(Duration.ofSeconds(2));
                final long leaseId = session.getLeaseId();
                client.create("/det", bytes("dir"));
                assertEquals("/det/a", client.create("/det/a", bytes("a"), session));
                assertTrue(client.sessions.contains(session), "the session is registered");
                assertNotNull(sessionTickOf(client), "the tick is live");

                // The lease dies underneath the session; its bound record cascades away.
                assertEquals(
                        RaftResult.Status.OK,
                        adminSend(cluster, RaftCommand.leaseExpire(leaseId, -1L)).getStatus());
                waitUntil(
                        () -> !client.exists("/det/a"),
                        OPERATION_TIMEOUT,
                        "The dead lease's bound record was not cascaded away");

                // One renewal interval past due: the session's next renewal returns
                // UNKNOWN_LEASE - the terminal path.
                cluster.clock.advanceTime(3, TimeUnit.SECONDS);
                waitUntil(
                        () -> !session.isAlive(),
                        OPERATION_TIMEOUT,
                        "The session did not surface its lease's death");
                assertFalse(
                        client.sessions.contains(session),
                        "the expired session must leave the client's registry");
                assertNull(
                        sessionTickOf(client),
                        "the final tick must be cancelled once the last session is gone");

                // The reopened session re-attaches and ticks again.
                final LeaseSession reopened = client.openLease(Duration.ofSeconds(30));
                assertTrue(client.sessions.contains(reopened));
                waitUntil(
                        () -> sessionTickOf(client) != null,
                        OPERATION_TIMEOUT,
                        "The tick did not restart for the reopened session");
                final long renewIndexAtOpen = lastRenewIndexOf(cluster, reopened.getLeaseId());
                cluster.clock.advanceTime(11, TimeUnit.SECONDS);
                waitUntil(
                        () -> lastRenewIndexOf(cluster, reopened.getLeaseId()) > renewIndexAtOpen,
                        OPERATION_TIMEOUT,
                        "The reopened session did not renew through the recreated tick");
                reopened.close();
            } finally {
                client.close();
            }
        }
    }

    /**
     * Renewal due-ness is monotonic: a client whose wall milliseconds stand still (skewed away
     * from the servers') still renews once its {@link Clock#nanoseconds()} advance - wall-clock
     * gating would starve the lease while the holder is perfectly alive. The leader's expiry
     * decision is unaffected: it uses the replicated leader sample (its own clock never moves
     * here), so the lease never lapses server-side during the test.
     */
    @Test
    void testSkewedWallClockClientStillRenewsByMonotonicTime() throws Exception {
        try (MetadataCluster cluster = newCluster(false)) {
            cluster.start();
            final Clock skewed =
                    new Clock() {
                        @Override
                        public long milliseconds() {
                            return 987_654L; // frozen, and far from the servers' clock
                        }

                        @Override
                        public long nanoseconds() {
                            return System.nanoTime(); // advancing
                        }
                    };
            final RaftMetadataClient client = cluster.newMetadataClient(skewed);
            try {
                final LeaseSession session =
                        client.openLeaseAsync(Duration.ofSeconds(3)).join();
                final long leaseId = session.getLeaseId();
                final long renewIndexAtOpen = lastRenewIndexOf(cluster, leaseId);
                assertTrue(renewIndexAtOpen > 0L, "the registration stamped a renew index");
                // The lease apply is monotone in expiry, and the leader stamps the expiry from
                // its own (here frozen) clock - so a renewal only moves the fence once the
                // server clock stands past the registration. One bounded advance keeps every
                // renewal extending while staying far short of the expiry plus its grace.
                cluster.clock.advanceTime(2, TimeUnit.SECONDS);

                waitUntil(
                        () -> lastRenewIndexOf(cluster, leaseId) > renewIndexAtOpen,
                        Duration.ofSeconds(15),
                        "The session never renewed: due-ness starved on the frozen wall clock");
                assertNotNull(
                        leaseOf(adminSendReadOnly(cluster, RaftCommand.leaseList()), leaseId),
                        "the lease stayed alive server-side throughout");
                session.close();
            } finally {
                client.close();
            }
        }
    }

    /**
     * The typed revocation through the client: it removes exactly the revoked lease's records,
     * leaves every other lease alone, and is idempotent - revoking an unknown or already-expired
     * lease succeeds as a no-op rather than failing.
     */
    @Test
    void testRevokeThroughTheClientIsIdempotentAndScopedToItsOwnLease() throws Exception {
        try (MetadataCluster cluster = newCluster(false)) {
            cluster.start();
            final RaftMetadataClient client = cluster.newMetadataClient();
            try {
                final LeaseSession sessionA =
                        client.openLeaseAsync(Duration.ofSeconds(30)).join();
                final LeaseSession sessionB =
                        client.openLeaseAsync(Duration.ofSeconds(30)).join();
                client.create("/rk", bytes("dir"));
                assertEquals("/rk/of-a", client.create("/rk/of-a", bytes("a"), sessionA));
                assertEquals("/rk/of-b", client.create("/rk/of-b", bytes("b"), sessionB));

                assertEquals(
                        1L,
                        client.revokeLeaseAsync(sessionA.getLeaseId()).join(),
                        "the revocation cascaded exactly session A's record");
                assertFalse(client.exists("/rk/of-a"));
                assertTrue(client.exists("/rk/of-b"), "the sibling lease's record is untouched");

                assertEquals(
                        0L,
                        client.revokeLeaseAsync(sessionA.getLeaseId()).join(),
                        "a repeated revoke of the now-unknown lease is a no-op success");
                assertTrue(client.exists("/rk/of-b"), "the repeat touched nothing");

                assertEquals(
                        1L,
                        client.revokeLeaseAsync(sessionB.getLeaseId()).join(),
                        "the other lease revokes on its own");
                assertFalse(client.exists("/rk/of-b"));
                assertTrue(client.exists("/rk"), "the persistent parent record stays");
            } finally {
                client.close();
            }
        }
    }

    @Test
    void testWatcherCheckpointResumeAndNeedSnapshot() throws Exception {
        try (MetadataCluster cluster = newCluster(true)) {
            cluster.start();
            final RaftMetadataClient client = cluster.newMetadataClient();
            try {
                client.put("/wa", bytes("a"));
                client.put("/wb", bytes("b"));
                client.put("/wc", bytes("c"));

                final Recorder liveRecorder = new Recorder();
                final MetadataWatcher live =
                        MetadataWatcher.newBuilder(client, liveRecorder)
                                .setSinceRevision(0)
                                .setPollInterval(Duration.ofMillis(100))
                                .build();
                live.start();
                waitUntil(
                        () ->
                                createsUnder(liveRecorder.deliveredEvents())
                                        .equals(Arrays.asList("/wa", "/wb", "/wc")),
                        OPERATION_TIMEOUT,
                        "The live watcher did not receive the first three writes in order");
                assertTrue(live.lastDeliveredRevision() > 0L);

                client.put("/wd", bytes("d"));
                client.put("/we", bytes("e"));
                waitUntil(
                        () ->
                                createsUnder(liveRecorder.deliveredEvents())
                                        .equals(
                                                Arrays.asList(
                                                        "/wa", "/wb", "/wc", "/wd", "/we")),
                        OPERATION_TIMEOUT,
                        "The live watcher did not receive the pre-snapshot writes in order");

                // Snapshot + purge: the retention line becomes snapshotIndex + 1, seen via
                // admin first. The probe is addressed to the member that took the snapshot:
                // the retention line is per-member GC state, and under the LINEARIZABLE
                // default any member - including a follower with a stale line - legitimately
                // serves reads, so an untargeted probe can observe (and pin the client's
                // leader cache to) the wrong member.
                final RaftPeerId snapshotTaker = cluster.leaderId();
                final long snapshotIndex = cluster.takeSnapshotOnLeader();
                final long retentionLine = snapshotIndex + 1;
                MetadataCluster.waitThroughTransientFailure(
                        () -> {
                            final RaftResult probe =
                                    adminSendReadOnly(
                                            cluster,
                                            RaftCommand.pollChanges(0, POLL_BUDGET_BYTES),
                                            snapshotTaker);
                            return probe.getStatus() == RaftResult.Status.NEED_SNAPSHOT
                                    && probe.getOldestRetainedRevision() == retentionLine;
                        },
                        OPERATION_TIMEOUT,
                        "The retention line did not advance to " + retentionLine);

                final Recorder resyncRecorder = new Recorder();
                final MetadataWatcher resync =
                        MetadataWatcher.newBuilder(client, resyncRecorder)
                                .setSinceRevision(0)
                                .setPollInterval(Duration.ofMillis(100))
                                .build();
                resync.start();
                waitUntil(
                        () ->
                                resyncRecorder.needSnapshotLine() != null
                                        && !resync.isRunning(),
                        OPERATION_TIMEOUT,
                        "The watcher below the retention line did not stop with NEED_SNAPSHOT");
                assertEquals(
                        Long.valueOf(retentionLine), resyncRecorder.needSnapshotLine());
                assertTrue(
                        resyncRecorder.deliveredEvents().isEmpty(),
                        "a watcher below the line delivers nothing before stopping");

                resync.restartFrom(retentionLine - 1);
                // The restart may land while the report is still completing: the handoff is
                // last-wins, so the new chain leaves within moments - not synchronously.
                waitUntil(
                        resync::isRunning,
                        OPERATION_TIMEOUT,
                        "The watcher did not resume from the retention line");
                client.put("/wf", bytes("f"));
                waitUntil(
                        () -> createsUnder(resyncRecorder.deliveredEvents()).contains("/wf"),
                        OPERATION_TIMEOUT,
                        "The resumed watcher did not receive the post-resume write");
                assertTrue(resync.lastDeliveredRevision() > snapshotIndex);
                assertEquals(
                        Long.valueOf(retentionLine),
                        resyncRecorder.needSnapshotLine(),
                        "resuming at the line does not re-trigger NEED_SNAPSHOT");
                assertStrictlyIncreasing(revisionsOf(resyncRecorder.deliveredEvents()));

                waitUntil(
                        () ->
                                !live.isRunning()
                                        || createsUnder(liveRecorder.deliveredEvents())
                                                .contains("/wf"),
                        OPERATION_TIMEOUT,
                        "The live watcher neither stopped at the line nor kept streaming");
                if (live.isRunning()) {
                    assertTrue(createsUnder(liveRecorder.deliveredEvents()).contains("/wf"));
                } else {
                    assertEquals(
                            Long.valueOf(retentionLine), liveRecorder.needSnapshotLine());
                }
                live.close();
                resync.close();
            } finally {
                client.close();
            }
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /** A lock on a stub client: a ttl far from due, so no renewal muddies the recorded ops. */
    private static MetadataLock newLockOn(RaftMetadataClient client, String path) {
        return MetadataLock.newBuilder()
                .setClient(client)
                .setPath(path)
                .setTtl(Duration.ofSeconds(60))
                .build();
    }

    /**
     * The client's session-tick handle, read reflectively: the harness asserts the final tick is
     * cancelled and later recreated without widening the production surface. Reading without the
     * client's session lock is a benign race - the callers poll until the expected value appears.
     */
    private static Object sessionTickOf(RaftMetadataClient client) {
        try {
            final Field field = RaftMetadataClient.class.getDeclaredField("sessionTick");
            field.setAccessible(true);
            return field.get(client);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Cannot read the client's session tick", e);
        }
    }

    /**
     * The client's current raft client, read reflectively the way {@link #sessionTickOf} is:
     * the recovery suite pins the swap of a poisoned pipeline without widening the production
     * surface. As there, a field rename breaks the probe loudly - the intended failure mode.
     */
    private static RaftClient raftClientOf(RaftMetadataClient client) {
        try {
            final Field field = RaftMetadataClient.class.getDeclaredField("raftClient");
            field.setAccessible(true);
            return (RaftClient) ((AtomicReference<?>) field.get(client)).get();
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Cannot read the client's raft client", e);
        }
    }

    /** The lease's replicated last-renew index, read through the admin listing. */
    private static long lastRenewIndexOf(MetadataCluster cluster, long leaseId) throws IOException {
        final RaftResult.Lease lease =
                leaseOf(adminSendReadOnly(cluster, RaftCommand.leaseList()), leaseId);
        return lease != null ? lease.getLastRenewIndex() : -1L;
    }

    private RaftMetadataClient newStubClient(RaftClientRpc rpc, RetryPolicy retryPolicy) {
        return newStubClient(
                rpc, retryPolicy, RaftClientConfigKeys.Async.OUTSTANDING_REQUESTS_MAX_DEFAULT);
    }

    /**
     * Mirrors the Builder's async wiring, so stub clients behave like built ones. The lowered
     * outstanding-requests limit wedges ratis' pipeline deterministically: as many permits as
     * the test withholds, no more.
     */
    private RaftMetadataClient newStubClient(
            RaftClientRpc rpc, RetryPolicy retryPolicy, int outstandingRequestsMax) {
        // Registered for the @AfterEach belt-and-braces close, so no stub client leaks a sender.
        final RaftMetadataClient client =
                new RaftMetadataClient(
                        stubRaftClientOf(rpc, retryPolicy, outstandingRequestsMax),
                        SystemClock.getInstance());
        stubClients.add(client);
        return client;
    }

    /**
     * A stub client whose raft client the recovery can rebuild: the factory counts its
     * invocations, and the renewals ride whatever the main pipeline currently is - the stub
     * seam of {@link #newStubClient}, kept recoverable.
     */
    private RaftMetadataClient newRecoverableStubClient(
            RaftClientRpc rpc,
            RetryPolicy retryPolicy,
            long quietPeriodMs,
            Clock clock,
            AtomicInteger factoryInvocations) {
        final RaftMetadataClient client =
                new RaftMetadataClient(
                        () -> {
                            factoryInvocations.incrementAndGet();
                            return stubRaftClientOf(rpc, retryPolicy);
                        },
                        clock,
                        quietPeriodMs);
        stubClients.add(client);
        return client;
    }

    /** The poison of the recovery suite: one attempt per send, so one failure kills the window. */
    private static RetryPolicy singleAttemptPolicy() {
        return RetryPolicies.retryUpToMaximumCountWithFixedSleep(
                1, TimeDuration.valueOf(1, TimeUnit.MILLISECONDS));
    }

    /** The stub raft client the renewal seam builds its own pipeline from, with default permits. */
    private static RaftClient stubRaftClientOf(RaftClientRpc rpc, RetryPolicy retryPolicy) {
        return stubRaftClientOf(
                rpc, retryPolicy, RaftClientConfigKeys.Async.OUTSTANDING_REQUESTS_MAX_DEFAULT);
    }

    /** The stub raft client of the given wiring, without the metadata-client wrap. */
    private static RaftClient stubRaftClientOf(
            RaftClientRpc rpc, RetryPolicy retryPolicy, int outstandingRequestsMax) {
        final RaftProperties properties = new RaftProperties();
        RaftClientConfigKeys.Async.Experimental.setSendDummyRequest(properties, false);
        RaftClientConfigKeys.Async.setOutstandingRequestsMax(properties, outstandingRequestsMax);
        final RaftClient.Builder builder =
                RaftClient.newBuilder()
                        .setRaftGroup(
                                RaftGroup.valueOf(
                                        RaftGroupId.valueOf(UUID.fromString(GROUP_UUID)),
                                        RaftPeer.newBuilder()
                                                .setId("n0")
                                                .setAddress("localhost:1")
                                                .build()))
                        .setProperties(properties)
                        .setClientRpc(rpc);
        if (retryPolicy != null) {
            builder.setRetryPolicy(retryPolicy);
        }
        return builder.build();
    }

    /**
     * The failure a metadata future completed with, unwrapped the way a consumer's exception
     * handling would see it; never completing fails the test instead.
     */
    private static <T> Throwable awaitFailureOf(CompletableFuture<T> future, String what) {
        waitUntil(future::isDone, OPERATION_TIMEOUT, what);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        future.whenComplete(
                (reply, e) -> {
                    if (e != null) {
                        failure.set(ExceptionUtils.stripCompletionException(e));
                    }
                });
        assertNotNull(failure.get(), what + ": the future completed normally instead");
        return failure.get();
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Whether the failure's cause chain contains the given type. A manual, depth-bounded walk
     * on purpose - never the production predicate - so the pins are not self-referential.
     */
    private static boolean causedBy(Throwable failure, Class<? extends Throwable> type) {
        for (int depth = 0; failure != null && depth < 32; depth++) {
            if (type.isInstance(failure)) {
                return true;
            }
            failure = failure.getCause();
        }
        return false;
    }

    /** A bound-then-released port: connections are refused within ms, no connect-timeout wait. */
    private static int refusedPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    // ------------------------------------------------------------------
    // Cluster-suite helpers
    // ------------------------------------------------------------------

    /** The 3-member in-process cluster on the stock transport, under the suite's group uuid. */
    private MetadataCluster newCluster(boolean purgeForSnapshot) {
        return MetadataCluster.newBuilder(tempDir)
                .setNumServers(3)
                .setPurgeForSnapshot(purgeForSnapshot)
                .setGroupUuid(UUID.fromString(GROUP_UUID))
                .build();
    }

    private static RaftResult adminSend(MetadataCluster cluster, RaftCommand command)
            throws IOException {
        return decodeReply(cluster.adminClient().io().send(toMessage(command)), command);
    }

    private static RaftResult adminSendReadOnly(MetadataCluster cluster, RaftCommand command)
            throws IOException {
        return decodeReply(cluster.adminClient().io().sendReadOnly(toMessage(command)), command);
    }

    /** Targets one member explicitly - for per-member state (e.g. the retention line) where an
     * untargeted read may legally land on a follower under the LINEARIZABLE default. */
    private static RaftResult adminSendReadOnly(
            MetadataCluster cluster, RaftCommand command, RaftPeerId server) throws IOException {
        return decodeReply(
                cluster.adminClient().io().sendReadOnly(toMessage(command), server), command);
    }

    private static RaftResult.Status expectMetadataFailure(Runnable action) {
        try {
            action.run();
        } catch (RaftMetadataException e) {
            return e.getStatus();
        }
        throw new AssertionError("The action was expected to fail with RaftMetadataException");
    }

    /** The ordered CREATE paths a watcher delivered (all events, in delivery order). */
    private static List<String> createsUnder(List<ChangeEvent> events) {
        final List<String> creates = new ArrayList<>();
        for (ChangeEvent event : events) {
            for (ChangeEvent.Change change : event.getChanges()) {
                if (change.getOp() == ChangeEvent.ChangeOp.CREATE && !change.getPath().isEmpty()) {
                    creates.add(change.getPath());
                }
            }
        }
        return creates;
    }

    private static List<String> pathsOf(List<RaftResult.Entry> entries) {
        final List<String> paths = new ArrayList<>();
        for (RaftResult.Entry entry : entries) {
            paths.add(entry.getPath());
        }
        return paths;
    }

    private static List<Long> revisionsOf(List<ChangeEvent> records) {
        final List<Long> revisions = new ArrayList<>();
        for (ChangeEvent record : records) {
            revisions.add(record.getRevision());
        }
        return revisions;
    }

    /** Adjacent revisions must strictly grow: sorted, with no duplicate checkpoint. */
    private static void assertStrictlyIncreasing(List<Long> values) {
        for (int i = 1; i < values.size(); i++) {
            assertTrue(
                    values.get(i - 1) < values.get(i),
                    "revisions must be strictly increasing: " + values);
        }
    }

    /** The lease entry of the given id, or null when the listing does not contain it. */
    private static RaftResult.Lease leaseOf(RaftResult listing, long leaseId) {
        assertEquals(RaftResult.Status.OK, listing.getStatus());
        for (RaftResult.Lease lease : listing.getLeases()) {
            if (lease.getLeaseId() == leaseId) {
                return lease;
            }
        }
        return null;
    }

    private static RaftResult decodeReply(RaftClientReply reply, RaftCommand command) {
        final RaftResult result =
                RaftCommandCodec.decodeResult(reply.getMessage().getContent().toByteArray());
        assertNotNull(
                result, "reply of " + command.getOp() + " decodes (op echo must match)");
        assertEquals(command.getOp(), result.getOp());
        return result;
    }

    private static Message toMessage(RaftCommand command) {
        return Message.valueOf(ByteBuffer.wrap(RaftCommandCodec.encodeCommand(command)));
    }

    /** Collects a watcher's stream: every record, plus the retention line if hit. */
    private static final class Recorder implements MetadataWatcher.Listener {

        private final List<ChangeEvent> events = new ArrayList<>();
        private Long needSnapshotLine;

        @Override
        public synchronized void onRecord(ChangeEvent event) {
            events.add(event);
        }

        @Override
        public synchronized void onNeedSnapshot(long oldestRetainedRevision) {
            needSnapshotLine = oldestRetainedRevision;
        }

        synchronized List<ChangeEvent> deliveredEvents() {
            return new ArrayList<>(events);
        }

        synchronized Long needSnapshotLine() {
            return needSnapshotLine;
        }
    }

    /** Serves one async poll at a time; the test decides when each reply arrives. */
    private static final class GatedNeedSnapshotRpc implements RaftClientRpc {

        private final byte[] reply =
                RaftCommandCodec.encodeResult(
                        RaftResult.error(
                                RaftCommand.Op.POLL_CHANGES, RaftResult.Status.NEED_SNAPSHOT));
        private RaftClientRequest firstRequest;
        private CompletableFuture<RaftClientReply> firstReply;

        @Override
        public synchronized CompletableFuture<RaftClientReply> sendRequestAsync(
                RaftClientRequest request) {
            final CompletableFuture<RaftClientReply> reply = new CompletableFuture<>();
            if (firstReply == null) {
                firstRequest = request;
                firstReply = reply;
            } else {
                // No second poll is expected while the report never completed; failing the
                // request loudly beats letting an unexpected chain poll on silently.
                reply.completeExceptionally(
                        new AssertionError("A second poll reached the stub: " + request));
            }
            return reply;
        }

        synchronized boolean firstPollInFlight() {
            return firstReply != null;
        }

        synchronized void answerFirstPoll() {
            firstReply.complete(replyOf(firstRequest));
        }

        private RaftClientReply replyOf(RaftClientRequest request) {
            return RaftClientReply.newBuilder()
                    .setRequest(request)
                    .setSuccess()
                    .setMessage(Message.valueOf(ByteBuffer.wrap(reply)))
                    .build();
        }

        @Override
        public RaftClientReply sendRequest(RaftClientRequest request) {
            throw new UnsupportedOperationException("This stub speaks async only");
        }

        @Override
        public void addRaftPeers(Collection<RaftPeer> peers) {}

        @Override
        public void close() {}
    }

    private static final class FailingTransportRpc implements RaftClientRpc {

        final AtomicInteger attempts = new AtomicInteger();

        @Override
        public RaftClientReply sendRequest(RaftClientRequest request) throws IOException {
            attempts.incrementAndGet();
            throw new IOException("stub transport failure");
        }

        @Override
        public CompletableFuture<RaftClientReply> sendRequestAsync(RaftClientRequest request) {
            attempts.incrementAndGet();
            return JavaUtils.completeExceptionally(
                    new IOException("stub transport failure"));
        }

        @Override
        public void addRaftPeers(Collection<RaftPeer> peers) {}

        @Override
        public void close() {}
    }

    /** An executor this test owns: it queues submissions without ever running them. */
    private static final class QueueingExecutor implements Executor {

        final Queue<Runnable> submitted = new ConcurrentLinkedQueue<>();

        @Override
        public void execute(Runnable command) {
            submitted.add(command);
        }
    }

    /** An executor that refuses every unit: the watcher must record the rejection and stop. */
    private static final class RejectingExecutor implements Executor {

        @Override
        public void execute(Runnable command) {
            throw new RejectedExecutionException("scripted executor rejection");
        }
    }

    /** A listener whose first record callback parks until the test releases it. */
    private static final class BlockingListener implements MetadataWatcher.Listener {

        private final List<ChangeEvent> events = new CopyOnWriteArrayList<>();
        private final AtomicInteger inListener = new AtomicInteger();
        private final AtomicBoolean overlap = new AtomicBoolean();
        private final AtomicBoolean firstPending = new AtomicBoolean(true);
        private final CountDownLatch firstEntered = new CountDownLatch(1);
        private final CountDownLatch releaseFirst = new CountDownLatch(1);
        private final CountDownLatch firstReturned = new CountDownLatch(1);
        private volatile String firstThread;
        private volatile Long needSnapshotLine;

        @Override
        public void onRecord(ChangeEvent event) {
            enter();
            try {
                events.add(event);
                if (firstPending.compareAndSet(true, false)) {
                    firstThread = Thread.currentThread().getName();
                    firstEntered.countDown();
                    awaitReleased();
                }
            } finally {
                firstReturned.countDown();
                inListener.decrementAndGet();
            }
        }

        @Override
        public void onNeedSnapshot(long oldestRetainedRevision) {
            enter();
            try {
                needSnapshotLine = oldestRetainedRevision;
            } finally {
                inListener.decrementAndGet();
            }
        }

        /** Un-parks the first callback. */
        void release() {
            releaseFirst.countDown();
        }

        List<ChangeEvent> events() {
            return events;
        }

        boolean overlapped() {
            return overlap.get();
        }

        /** The thread the first callback ran on; set before {@link #firstEntered} trips. */
        String firstCallbackThread() {
            return firstThread;
        }

        private void enter() {
            if (inListener.incrementAndGet() != 1) {
                overlap.set(true);
            }
        }

        /** Parks the first callback until {@link #release()} - bounded, so no test can strand a
         * delivery-pool thread behind a forgotten release. */
        private void awaitReleased() {
            final boolean released;
            try {
                released = releaseFirst.await(60, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while parked in the listener", e);
            }
            if (!released) {
                throw new IllegalStateException("A test listener parked 60s without release");
            }
        }
    }

    /**
     * Serves the watcher's polls; the test decides when each reply arrives and what it carries
     * (records, a watermark-only empty poll, or NEED_SNAPSHOT). Every poll's
     * {@code sinceRevision} is recorded, so a chain that polled again mid-delivery is caught.
     */
    private static final class GatedPollRpc implements RaftClientRpc {

        /** Trips the moment a poll beyond the first arrives - the single-flight probe. */
        final CountDownLatch anotherPollArrived = new CountDownLatch(1);

        private final List<Long> sinceRevisions = new CopyOnWriteArrayList<>();
        private final Queue<PendingPoll> inFlight = new ConcurrentLinkedQueue<>();

        private static final class PendingPoll {
            private final RaftClientRequest request;
            private final CompletableFuture<RaftClientReply> reply;

            PendingPoll(RaftClientRequest request, CompletableFuture<RaftClientReply> reply) {
                this.request = request;
                this.reply = reply;
            }
        }

        @Override
        public synchronized CompletableFuture<RaftClientReply> sendRequestAsync(
                RaftClientRequest request) {
            final RaftCommand command =
                    RaftCommandCodec.decodeCommand(
                            request.getMessage().getContent().toByteArray());
            if (command.getOp() != RaftCommand.Op.POLL_CHANGES) {
                final CompletableFuture<RaftClientReply> reply = new CompletableFuture<>();
                reply.completeExceptionally(
                        new AssertionError("The watcher speaks POLL_CHANGES only: " + command));
                return reply;
            }
            sinceRevisions.add(command.getSinceRevision());
            if (sinceRevisions.size() > 1) {
                anotherPollArrived.countDown();
            }
            final CompletableFuture<RaftClientReply> reply = new CompletableFuture<>();
            inFlight.add(new PendingPoll(request, reply));
            return reply;
        }

        synchronized int pollCount() {
            return sinceRevisions.size();
        }

        /** The {@code sinceRevision} of the n-th poll (1-based), in poll order. */
        synchronized long sinceRevisionOf(int n) {
            return sinceRevisions.get(n - 1);
        }

        synchronized boolean hasInFlightPoll() {
            return !inFlight.isEmpty();
        }

        /** Answers the oldest in-flight poll with one CREATE record per given revision. */
        void answerRecords(long watermark, long... revisions) {
            final List<ChangeEvent> records = new ArrayList<>();
            for (long revision : revisions) {
                records.add(
                        new ChangeEvent(
                                revision,
                                Collections.singletonList(
                                        new ChangeEvent.Change(
                                                ChangeEvent.ChangeOp.CREATE,
                                                "/rev-" + revision,
                                                new byte[0]))));
            }
            final RaftResult result = RaftResult.ok(RaftCommand.Op.POLL_CHANGES);
            setRaftResultField(result, "revision", watermark);
            setRaftResultField(result, "records", records);
            answer(result);
        }

        /** Answers the oldest in-flight poll with NEED_SNAPSHOT of the given retention line. */
        void answerNeedSnapshot(long oldestRetainedRevision) {
            final RaftResult result =
                    RaftResult.error(
                            RaftCommand.Op.POLL_CHANGES, RaftResult.Status.NEED_SNAPSHOT);
            setRaftResultField(result, "oldestRetainedRevision", oldestRetainedRevision);
            answer(result);
        }

        private void answer(RaftResult result) {
            final PendingPoll poll = inFlight.poll();
            assertNotNull(poll, "no poll in flight to answer");
            poll.reply.complete(
                    RaftClientReply.newBuilder()
                            .setRequest(poll.request)
                            .setSuccess()
                            .setMessage(
                                    Message.valueOf(
                                            ByteBuffer.wrap(
                                                    RaftCommandCodec.encodeResult(result))))
                            .build());
        }

        @Override
        public RaftClientReply sendRequest(RaftClientRequest request) {
            throw new UnsupportedOperationException("This stub speaks async only");
        }

        @Override
        public void addRaftPeers(Collection<RaftPeer> peers) {}

        @Override
        public void close() {}
    }

    /**
     * The transport of the poisoned-pipeline watcher suite: every POLL_CHANGES is answered at
     * once - exceptionally while {@code failAll} (so the first poll can exhaust a one-attempt
     * budget and kill the window), else with one fresh CREATE record past the polled revision
     * (so a healed watcher immediately has something to deliver), following {@link
     * GatedPollRpc#answerRecords}'s reply shape.
     */
    private static final class FlakyPollRpc implements RaftClientRpc {

        /** Every request fails at the transport level when set. */
        volatile boolean failAll;

        private final AtomicInteger failedSends = new AtomicInteger();

        /** Whether the one record this stub ever serves went out; every later poll idles. */
        private boolean served;

        @Override
        public synchronized CompletableFuture<RaftClientReply> sendRequestAsync(
                RaftClientRequest request) {
            final RaftCommand command =
                    RaftCommandCodec.decodeCommand(
                            request.getMessage().getContent().toByteArray());
            final CompletableFuture<RaftClientReply> reply = new CompletableFuture<>();
            if (command.getOp() != RaftCommand.Op.POLL_CHANGES) {
                reply.completeExceptionally(
                        new AssertionError("The watcher speaks POLL_CHANGES only: " + command));
                return reply;
            }
            if (failAll) {
                failedSends.incrementAndGet();
                reply.completeExceptionally(new IOException("scripted transport failure"));
                return reply;
            }
            // Exactly one record ever, so the watcher's delivered revision settles at one
            // stable value instead of racing upward poll over poll.
            final RaftResult result = RaftResult.ok(RaftCommand.Op.POLL_CHANGES);
            final long revision = command.getSinceRevision() + (served ? 0 : 1);
            served = true;
            setRaftResultField(result, "revision", revision);
            if (revision > command.getSinceRevision()) {
                setRaftResultField(
                        result,
                        "records",
                        Collections.singletonList(
                                new ChangeEvent(
                                        revision,
                                        Collections.singletonList(
                                                new ChangeEvent.Change(
                                                        ChangeEvent.ChangeOp.CREATE,
                                                        "/rev-" + revision,
                                                        new byte[0])))));
            }
            reply.complete(
                    RaftClientReply.newBuilder()
                            .setRequest(request)
                            .setSuccess()
                            .setMessage(
                                    Message.valueOf(
                                            ByteBuffer.wrap(
                                                    RaftCommandCodec.encodeResult(result))))
                            .build());
            return reply;
        }

        synchronized int failedSends() {
            return failedSends.get();
        }

        @Override
        public RaftClientReply sendRequest(RaftClientRequest request) {
            throw new UnsupportedOperationException("This stub speaks async only");
        }

        @Override
        public void addRaftPeers(Collection<RaftPeer> peers) {}

        @Override
        public void close() {}
    }

    /**
     * The transport of the isolation suite: every POLL_CHANGES is answered at once with one
     * fresh record (each watcher immediately has a delivery to make), every other command is
     * answered OK, and every LEASE_RENEW is counted - so a renewal whose settle never ran is
     * caught as a count that never advances. The first GET is withheld until the test drives
     * it, so the settle probe provably runs on whatever thread settles it.
     */
    private static final class ImmediatePollRpc implements RaftClientRpc {

        private final AtomicInteger renewals = new AtomicInteger();
        private final AtomicLong nextRevision = new AtomicLong();
        private RaftClientRequest heldRequest;
        private CompletableFuture<RaftClientReply> heldReply;

        @Override
        public synchronized CompletableFuture<RaftClientReply> sendRequestAsync(
                RaftClientRequest request) {
            final RaftCommand command =
                    RaftCommandCodec.decodeCommand(
                            request.getMessage().getContent().toByteArray());
            if (command.getOp() == RaftCommand.Op.LEASE_RENEW) {
                renewals.incrementAndGet(); // counted at SEND time - the settle lands later
            }
            final RaftResult result = RaftResult.ok(command.getOp());
            if (command.getOp() == RaftCommand.Op.POLL_CHANGES) {
                final long revision = nextRevision.incrementAndGet();
                setRaftResultField(result, "revision", revision);
                setRaftResultField(result, "records", recordsOf(revision));
            }
            final CompletableFuture<RaftClientReply> reply = new CompletableFuture<>();
            if (command.getOp() == RaftCommand.Op.GET && heldReply == null) {
                heldRequest = request;
                heldReply = reply;
                return reply;
            }
            reply.complete(replyOf(request, result));
            return reply;
        }

        synchronized boolean hasPendingGet() {
            return heldReply != null && !heldReply.isDone();
        }

        /** Answers the withheld GET; its settle then provably runs on the completion executor. */
        synchronized void answerPendingGet() {
            if (heldReply == null || heldReply.isDone()) {
                return;
            }
            heldReply.complete(replyOf(heldRequest, RaftResult.ok(RaftCommand.Op.GET)));
        }

        private static RaftClientReply replyOf(RaftClientRequest request, RaftResult result) {
            return RaftClientReply.newBuilder()
                    .setRequest(request)
                    .setSuccess()
                    .setMessage(
                            Message.valueOf(
                                    ByteBuffer.wrap(RaftCommandCodec.encodeResult(result))))
                    .build();
        }

        private static List<ChangeEvent> recordsOf(long revision) {
            return Collections.singletonList(
                    new ChangeEvent(
                            revision,
                            Collections.singletonList(
                                    new ChangeEvent.Change(
                                            ChangeEvent.ChangeOp.CREATE,
                                            "/rev-" + revision,
                                            new byte[0]))));
        }

        synchronized int renewals() {
            return renewals.get();
        }

        @Override
        public RaftClientReply sendRequest(RaftClientRequest request) {
            throw new UnsupportedOperationException("This stub speaks async only");
        }

        @Override
        public void addRaftPeers(Collection<RaftPeer> peers) {}

        @Override
        public void close() {}
    }

    /**
     * Sets one {@link RaftResult} payload field for a scripted reply: the fluent setters are
     * package-private to the state machine while the watcher stubs live here. A field rename
     * breaks this probe loudly, which is the intended failure mode.
     */
    private static void setRaftResultField(RaftResult result, String name, Object value) {
        try {
            final Field field = RaftResult.class.getDeclaredField(name);
            field.setAccessible(true);
            field.set(result, value);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Cannot script RaftResult." + name, e);
        }
    }

    /**
     * Builds one {@link RaftResult.Entry} for a scripted reply: the constructor is package-private
     * to the state machine. As above, a rename breaks this probe loudly.
     */
    private static RaftResult.Entry listEntryOf(String path) {
        try {
            final Constructor<RaftResult.Entry> constructor =
                    RaftResult.Entry.class.getDeclaredConstructor(String.class, long.class);
            constructor.setAccessible(true);
            return constructor.newInstance(path, 1L);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Cannot construct RaftResult.Entry", e);
        }
    }

    /**
     * Reads one private static field: the probes above pin private constants (the client's page
     * size, the state machine's path cap) without widening production visibility. As above, a
     * rename breaks the probe loudly.
     */
    private static Object staticFieldOf(Class<?> type, String name) {
        try {
            final Field field = type.getDeclaredField(name);
            field.setAccessible(true);
            return field.get(null);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Cannot read " + type.getSimpleName() + "." + name, e);
        }
    }

    /**
     * Requires the latch NOT to trip within the window. The defect each call guards against
     * trips its latch within microseconds of its cause - never near the bound - so the bound
     * only ends the observation once absence is proven beyond any scheduling noise.
     */
    private static void assertNeverTripped(CountDownLatch latch, Duration window, String what) {
        final boolean tripped;
        try {
            tripped = latch.await(window.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while watching: " + what, e);
        }
        assertFalse(tripped, what);
    }

    /** One command the client sent, decoded: the op plus the path and lease it acted on. */
    private static final class SentCommand {
        final RaftCommand.Op op;
        final String path;
        final long leaseId;

        SentCommand(RaftCommand command) {
            this.op = command.getOp();
            this.path = command.getPath();
            this.leaseId = command.getLeaseId();
        }

        boolean matches(RaftCommand.Op anOp, String aPath) {
            return op == anOp && (aPath == null || aPath.equals(path));
        }

        @Override
        public String toString() {
            return op + "(" + path + ", lease " + leaseId + ")";
        }
    }

    /**
     * The scripted transport of the lock suites: every async request is recorded and answered
     * from a small script - ok by default, an error status per op, all failing, or specific
     * replies withheld until the test releases them.
     */
    private static final class ScriptedReplyRpc implements RaftClientRpc {

        /** Per-op error statuses to answer with (business errors, not transport failures). */
        final Map<RaftCommand.Op, RaftResult.Status> errorStatuses = new ConcurrentHashMap<>();

        /** Replies of the CREATE of exactly this path are withheld until {@link #completeHeld}. */
        volatile String holdCreatePath;

        /** Replies of every LEASE_RENEW are withheld until {@link #completeHeld}. */
        volatile boolean holdRenewals;

        /** Every request fails at the transport level when set. */
        volatile boolean failAll;

        /** Every close of this stub, with the thread it ran on - the off-thread-close probe. */
        final CopyOnWriteArrayList<String> closeThreads = new CopyOnWriteArrayList<>();

        /** Trips on the first close - the grace-period observation window's anchor. */
        final CountDownLatch closedOnce = new CountDownLatch(1);

        private final List<SentCommand> sent = new CopyOnWriteArrayList<>();
        private final List<RaftClientRequest> heldRequests = new CopyOnWriteArrayList<>();
        private final List<CompletableFuture<RaftClientReply>> heldReplies =
                new CopyOnWriteArrayList<>();

        @Override
        public synchronized CompletableFuture<RaftClientReply> sendRequestAsync(
                RaftClientRequest request) {
            final RaftCommand command =
                    RaftCommandCodec.decodeCommand(
                            request.getMessage().getContent().toByteArray());
            sent.add(new SentCommand(command));
            final CompletableFuture<RaftClientReply> reply = new CompletableFuture<>();
            final boolean hold =
                    (command.getOp() == RaftCommand.Op.CREATE
                                    && command.getPath() != null
                                    && command.getPath().equals(holdCreatePath))
                            || (command.getOp() == RaftCommand.Op.LEASE_RENEW && holdRenewals);
            if (hold) {
                heldRequests.add(request);
                heldReplies.add(reply);
                return reply;
            }
            settle(request, command, reply);
            return reply;
        }

        private void settle(
                RaftClientRequest request, RaftCommand command,
                CompletableFuture<RaftClientReply> reply) {
            if (failAll) {
                reply.completeExceptionally(new IOException("scripted transport failure"));
                return;
            }
            final RaftResult.Status status = errorStatuses.get(command.getOp());
            final RaftResult result =
                    status != null
                            ? RaftResult.error(command.getOp(), status)
                            : RaftResult.ok(command.getOp());
            reply.complete(
                    RaftClientReply.newBuilder()
                            .setRequest(request)
                            .setSuccess()
                            .setMessage(
                                    Message.valueOf(
                                            ByteBuffer.wrap(
                                                    RaftCommandCodec.encodeResult(result))))
                            .build());
        }

        /** Completes every withheld reply successfully - the delayed outcome finally lands. */
        synchronized void completeHeld() {
            for (int i = 0; i < heldReplies.size(); i++) {
                final CompletableFuture<RaftClientReply> reply = heldReplies.get(i);
                final RaftClientRequest request = heldRequests.get(i);
                if (!reply.isDone()) {
                    settle(request, lastCommandOf(request), reply);
                }
            }
        }

        private static RaftCommand lastCommandOf(RaftClientRequest request) {
            return RaftCommandCodec.decodeCommand(
                    request.getMessage().getContent().toByteArray());
        }

        /** How many commands of this op were sent ({@code path} null = any path). */
        int countOf(RaftCommand.Op op) {
            return countOf(op, null);
        }

        int countOf(RaftCommand.Op op, String path) {
            int count = 0;
            for (SentCommand command : sent) {
                if (command.matches(op, path)) {
                    count++;
                }
            }
            return count;
        }

        /** How many commands of this op carried the given lease id. */
        int countOf(RaftCommand.Op op, long leaseId) {
            int count = 0;
            for (SentCommand command : sent) {
                if (command.op == op && command.leaseId == leaseId) {
                    count++;
                }
            }
            return count;
        }

        /** The lease id of the first command of this op; 0 when none was sent. */
        long sentLeaseIdOf(RaftCommand.Op op) {
            for (SentCommand command : sent) {
                if (command.op == op) {
                    return command.leaseId;
                }
            }
            return 0L;
        }

        long sentLeaseIdOf(RaftCommand.Op op, String path) {
            for (SentCommand command : sent) {
                if (command.matches(op, path)) {
                    return command.leaseId;
                }
            }
            return 0L;
        }

        /** The lease id of the single lease the client ever registered. */
        long singleLeaseIdOf() {
            return sentLeaseIdOf(RaftCommand.Op.LEASE_REGISTER);
        }

        /** Index of the first command of this op in send order; -1 when never sent. */
        int firstIndexOf(RaftCommand.Op op, String path) {
            for (int i = 0; i < sent.size(); i++) {
                if (sent.get(i).matches(op, path)) {
                    return i;
                }
            }
            return -1;
        }

        @Override
        public RaftClientReply sendRequest(RaftClientRequest request) {
            throw new UnsupportedOperationException("This stub speaks async only");
        }

        @Override
        public void addRaftPeers(Collection<RaftPeer> peers) {}

        @Override
        public synchronized void close() {
            closeThreads.add(Thread.currentThread().getName());
            closedOnce.countDown();
        }
    }

    private static final class UndecodableReplyRpc implements RaftClientRpc {

        @Override
        public RaftClientReply sendRequest(RaftClientRequest request) {
            return RaftClientReply.newBuilder()
                    .setRequest(request)
                    .setSuccess()
                    .setMessage(Message.valueOf(ByteBuffer.wrap(new byte[] {0x7f, 0x00, 0x01})))
                    .build();
        }

        @Override
        public CompletableFuture<RaftClientReply> sendRequestAsync(RaftClientRequest request) {
            return CompletableFuture.completedFuture(sendRequest(request));
        }

        @Override
        public void addRaftPeers(Collection<RaftPeer> peers) {}

        @Override
        public void close() {}
    }

    /**
     * Serves LIST the way the state machine's queryList does, on both rpc surfaces: one prepared
     * sorted prefix, sliced strictly after the cursor up to the requested cap, with {@code
     * hasMore} set only when the cap cut the range with a matching key left. Every request's
     * cap, cursor and issuing thread is recorded, so a page chain that is unbounded, never
     * advancing or issued from a completion thread is caught.
     */
    private static final class ListServingRpc implements RaftClientRpc {

        /** One LIST the client issued: its wire shape and the thread that sent it. */
        static final class ServedList {
            final long maxResults;
            final String startAfter;
            final String thread;
            final String lastPath;

            ServedList(long maxResults, String startAfter, String thread, String lastPath) {
                this.maxResults = maxResults;
                this.startAfter = startAfter;
                this.thread = thread;
                this.lastPath = lastPath;
            }
        }

        private final List<String> keys;
        private final List<ServedList> served = new CopyOnWriteArrayList<>();

        ListServingRpc(List<String> sortedKeys) {
            this.keys = sortedKeys;
        }

        @Override
        public synchronized RaftClientReply sendRequest(RaftClientRequest request) {
            return replyOf(request);
        }

        @Override
        public synchronized CompletableFuture<RaftClientReply> sendRequestAsync(
                RaftClientRequest request) {
            return CompletableFuture.completedFuture(replyOf(request));
        }

        void resetRecordings() {
            served.clear();
        }

        synchronized List<ServedList> served() {
            return new ArrayList<>(served);
        }

        private RaftClientReply replyOf(RaftClientRequest request) {
            final RaftCommand command =
                    RaftCommandCodec.decodeCommand(
                            request.getMessage().getContent().toByteArray());
            assertEquals(RaftCommand.Op.LIST, command.getOp(), "this stub serves LIST only");
            final List<RaftResult.Entry> entries = new ArrayList<>();
            boolean more = false;
            // The server's rule: the first cap keys strictly after the cursor, hasMore only when
            // the cap stopped the scan with a matching key left. Sizing by count, never by index
            // arithmetic, keeps an uncapped Long.MAX_VALUE request answerable whole.
            final int cap = (int) Math.max(0L, Math.min(command.getMaxResults(), keys.size()));
            for (String path : keys) {
                if (path.compareTo(command.getStartAfter()) <= 0) {
                    continue;
                }
                if (entries.size() >= cap) {
                    more = true;
                    break;
                }
                entries.add(listEntryOf(path));
            }
            served.add(
                    new ServedList(
                            command.getMaxResults(),
                            command.getStartAfter(),
                            Thread.currentThread().getName(),
                            entries.isEmpty() ? "" : entries.get(entries.size() - 1).getPath()));
            final RaftResult result = RaftResult.ok(RaftCommand.Op.LIST);
            setRaftResultField(result, "entries", entries);
            setRaftResultField(result, "hasMore", more);
            return RaftClientReply.newBuilder()
                    .setRequest(request)
                    .setSuccess()
                    .setMessage(
                            Message.valueOf(
                                    ByteBuffer.wrap(RaftCommandCodec.encodeResult(result))))
                    .build();
        }

        @Override
        public void addRaftPeers(Collection<RaftPeer> peers) {}

        @Override
        public void close() {}
    }

    /** Serves one async request at a time; the test decides when its reply arrives. */
    private static final class ControllableReplyRpc implements RaftClientRpc {
        private RaftClientRequest pendingRequest;
        private CompletableFuture<RaftClientReply> pendingReply;

        @Override
        public RaftClientReply sendRequest(RaftClientRequest request) {
            throw new UnsupportedOperationException("This stub speaks async only");
        }

        @Override
        public synchronized CompletableFuture<RaftClientReply> sendRequestAsync(
                RaftClientRequest request) {
            pendingRequest = request;
            pendingReply = new CompletableFuture<>();
            return pendingReply;
        }

        /** Whether a request has reached the stub and is still unanswered. */
        synchronized boolean hasPending() {
            return pendingReply != null && !pendingReply.isDone();
        }

        synchronized void completePendingGet() {
            if (pendingReply == null || pendingReply.isDone()) {
                return; // no pending get (or already settled): a stray drive must not NPE
            }
            pendingReply.complete(
                    RaftClientReply.newBuilder()
                            .setRequest(pendingRequest)
                            .setSuccess()
                            .setMessage(
                                    Message.valueOf(
                                            ByteBuffer.wrap(
                                                    RaftCommandCodec.encodeResult(
                                                            RaftResult.ok(RaftCommand.Op.GET)))))
                            .build());
        }

        @Override
        public void addRaftPeers(Collection<RaftPeer> peers) {}

        @Override
        public void close() {}
    }

    /**
     * Never answers its first request. With ratis' outstanding-requests limit lowered by the
     * test, the FIRST send reaches this stub and is withheld; every later send is DELAYED
     * inside ratis' sliding window (each still holds a permit but never reaches this stub) until
     * the first settles - so {@link #failAllPending()} on the withheld reply cascades through
     * the window (SlidingWindow.fail) and frees every permit. Both facts matter when wedging a
     * client: the withheld reply is the only lever, and releasing it unwedges everything.
     */
    private static final class WithholdingRpc implements RaftClientRpc {
        private final List<CompletableFuture<RaftClientReply>> pending = new CopyOnWriteArrayList<>();

        @Override
        public RaftClientReply sendRequest(RaftClientRequest request) {
            throw new UnsupportedOperationException("This stub speaks async only");
        }

        @Override
        public synchronized CompletableFuture<RaftClientReply> sendRequestAsync(
                RaftClientRequest request) {
            final CompletableFuture<RaftClientReply> reply = new CompletableFuture<>();
            pending.add(reply);
            return reply;
        }

        /** Fails every withheld reply; the sliding window then fails its delayed sends too. */
        synchronized void failAllPending() {
            for (CompletableFuture<RaftClientReply> reply : pending) {
                if (!reply.isDone()) {
                    reply.completeExceptionally(new IOException("released by the test"));
                }
            }
            pending.clear();
        }

        @Override
        public void addRaftPeers(Collection<RaftPeer> peers) {}

        @Override
        public void close() {}
    }
}
