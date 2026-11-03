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
package org.apache.ratis.metadata.lease;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.metadata.protocol.RaftCommand;
import org.apache.ratis.metadata.protocol.RaftCommandCodec;
import org.apache.ratis.metadata.protocol.RaftResult;
import org.apache.ratis.metadata.statemachine.MetadataStateMachine;
import org.apache.ratis.metadata.util.Clock;
import org.apache.ratis.metadata.util.ManualClock;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.server.storage.StorageImplUtils;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.TransactionContextImpl;
import org.apache.ratis.util.SizeInBytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.apache.ratis.metadata.util.TestWait.waitUntil;

/**
 * Unit tests of {@link LeaseExpiryScheduler}'s loop liveness on a standalone, never-started
 * member: one unexpected failure inside a scan must cost exactly that scan, not the rest of the
 * leadership term's expiry.
 */
public class TestLeaseExpiryScheduler {

    private static final String GROUP_UUID = "5c3d8e1f-9a47-4b62-8e55-1f0d6c2a7b34";
    private static final Duration SCAN_INTERVAL = Duration.ofMillis(20);
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    @TempDir File tempDir;

    /** Delegates to the manual clock but throws once on demand: the injected scan bug. */
    private static final class FlakyClock implements Clock {
        private final ManualClock delegate;
        private volatile boolean failNext;

        FlakyClock(ManualClock delegate) {
            this.delegate = delegate;
        }

        void failNextRead() {
            failNext = true;
        }

        @Override
        public long milliseconds() {
            if (failNext) {
                failNext = false;
                throw new IllegalStateException("injected unexpected scan failure");
            }
            return delegate.milliseconds();
        }

        @Override
        public long nanoseconds() {
            return delegate.nanoseconds();
        }
    }

    /** One never-started member: real RocksDB machine + real built raft server + scheduler. */
    private static final class StandaloneMember implements AutoCloseable {
        final ManualClock clock = new ManualClock(100_000L);
        final FlakyClock schedulerClock = new FlakyClock(clock);
        final LeaseExpiryScheduler scheduler;
        private final RaftServer server;
        private final MetadataStateMachine stateMachine;
        private long index;

        StandaloneMember(File dir) throws Exception {
            final RaftPeerId selfId = RaftPeerId.valueOf("m0");
            final RaftGroupId groupId = RaftGroupId.valueOf(UUID.fromString(GROUP_UUID));
            final RaftGroup group =
                    RaftGroup.valueOf(
                            groupId,
                            RaftPeer.newBuilder().setId(selfId).setAddress("localhost:0").build());
            final RaftProperties properties = new RaftProperties();
            RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.GRPC);
            // the machine is never started; the rpc binds an ephemeral port at build time
            GrpcConfigKeys.Server.setPort(properties, 0);
            RaftServerConfigKeys.setStorageDir(
                    properties, Collections.singletonList(new File(dir, "raft")));

            stateMachine =
                    new MetadataStateMachine(
                            new File(dir, "meta"), clock, Duration.ofMillis(10_000L));
            server =
                    RaftServer.newBuilder()
                            .setServerId(selfId)
                            .setGroup(group)
                            .setStateMachine(stateMachine)
                            .setProperties(properties)
                            .build();
            // build() registers no division (impls are created on start()), so initialize by
            // hand
            final RaftStorage storage =
                    StorageImplUtils.newRaftStorage(
                            new File(dir, "raft"),
                            SizeInBytes.valueOf("0MB"),
                            RaftStorage.StartupOption.RECOVER,
                            null);
            try {
                storage.initialize();
                stateMachine.initialize(server, groupId, storage);
            } finally {
                storage.close();
            }
            scheduler =
                    new LeaseExpiryScheduler(
                            server, stateMachine, groupId, schedulerClock, SCAN_INTERVAL, null);
        }

        RaftResult apply(RaftCommand command) throws Exception {
            final long revision = ++index;
            final RaftClientRequest request =
                    RaftClientRequest.newBuilder()
                            .setClientId(ClientId.randomId())
                            .setServerId(stateMachine.getId())
                            .setGroupId(stateMachine.getGroupId())
                            .setCallId(revision)
                            .setMessage(
                                    Message.valueOf(
                                            ByteBuffer.wrap(
                                                    RaftCommandCodec.encodeCommand(command))))
                            .setType(RaftClientRequest.writeRequestType())
                            .build();
            final TransactionContext context = stateMachine.startTransaction(request);
            ((TransactionContextImpl) context).initLogEntry(1L, revision);
            final Message reply =
                    stateMachine.applyTransaction(context).get(10, TimeUnit.SECONDS);
            return RaftCommandCodec.decodeResult(reply.getContent().toByteArray());
        }

        @Override
        public void close() throws Exception {
            scheduler.close();
            server.close();
            stateMachine.close();
        }
    }

    @Test
    void testScanLoopSurvivesAnUnexpectedScanFailure() throws Exception {
        try (StandaloneMember member = new StandaloneMember(tempDir)) {
            // Exactly what MetadataStateMachine.notifyLeaderReady forwards on a new term.
            member.scheduler.onLeaderReady();
            waitUntil(
                    () -> member.scheduler.scansCompleted() >= 1,
                    TIMEOUT,
                    "The scan loop never completed a scan");

            final long before = member.scheduler.scansCompleted();
            member.schedulerClock.failNextRead();

            // Pre-fix, the injected failure ends the loop for the rest of the term: at most
            // the one in-flight scan can still complete, so before + 3 never holds.
            waitUntil(
                    () -> member.scheduler.scansCompleted() >= before + 3,
                    TIMEOUT,
                    "The scan loop died on an unexpected scan failure");
            assertTrue(
                    member.scheduler.isScanLoopRunning(),
                    "The scan loop still runs after an unexpected scan failure");
        }
    }

    @Test
    void testDueLeaseIsStillProposedAfterAnUnexpectedScanFailure() throws Exception {
        try (StandaloneMember member = new StandaloneMember(tempDir)) {
            final long ttlMs = TimeUnit.SECONDS.toMillis(60);
            assertEquals(
                    RaftResult.Status.OK,
                    member.apply(
                            RaftCommand.leaseRegister(
                                    701L, ttlMs, member.clock.milliseconds() + ttlMs))
                            .getStatus(),
                    "the lease registers");

            member.scheduler.onLeaderReady();
            waitUntil(
                    () -> member.scheduler.scansCompleted() >= 1,
                    TIMEOUT,
                    "The scan loop never completed a scan");
            final long before = member.scheduler.scansCompleted();
            member.schedulerClock.failNextRead();
            waitUntil(
                    () -> member.scheduler.scansCompleted() >= before + 2,
                    TIMEOUT,
                    "The scan loop died on an unexpected scan failure");

            // Past the stored expiry plus the ttl/3 grace plus the ttl/3 skew tolerance: only a
            // still-alive loop proposes.
            member.clock.advanceTime(
                    ttlMs + 2 * (ttlMs / 3) + TimeUnit.SECONDS.toMillis(5),
                    TimeUnit.MILLISECONDS);
            waitUntil(
                    () -> member.scheduler.proposalsSubmitted() >= 1,
                    TIMEOUT,
                    "No expiry was proposed after the unexpected scan failure");
        }
    }
}
