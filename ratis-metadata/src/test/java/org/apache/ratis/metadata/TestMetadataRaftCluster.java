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
package org.apache.ratis.metadata;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.GroupInfoReply;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.apache.ratis.metadata.util.TestWait.waitUntil;
import static org.apache.ratis.metadata.util.TestWait.waitValue;

/**
 * An end-to-end test of the stock transport under the ported coordination layer: a three-node
 * in-process raft group whose entire consensus traffic - votes, log replication, heartbeats,
 * group info and client writes - runs over unmodified ratis grpc.
 */
public class TestMetadataRaftCluster {

    private static final int NUM_SERVERS = 3;
    private static final String GROUP_UUID = "0dc4b816-31a5-4c56-8d1e-7c8b9a6f5d43";
    private static final Duration LEADER_TIMEOUT = Duration.ofSeconds(60);
    private static final Duration APPLY_TIMEOUT = Duration.ofSeconds(30);

    @TempDir File tempDir;

    @Test
    void testRaftClusterElectsReplicatesAndFailsOverOnTheStockTransport() throws Exception {
        List<RaftServer> raftServers = new ArrayList<>();
        List<RecordingStateMachine> stateMachines = new ArrayList<>();
        RaftClient client = null;
        try {
            // The grpc ports are probed up front so the peer list is concrete before any
            // server builds - the one piece of wiring the transport artifact owns.
            int[] ports = new int[NUM_SERVERS];
            for (int i = 0; i < NUM_SERVERS; i++) {
                ports[i] = probedPort();
            }

            RaftGroupId groupId = RaftGroupId.valueOf(UUID.fromString(GROUP_UUID));
            List<RaftPeer> peers = new ArrayList<>();
            List<RaftPeerId> peerIds = new ArrayList<>();
            for (int i = 0; i < NUM_SERVERS; i++) {
                String peerId = "n" + i;
                peers.add(
                        RaftPeer.newBuilder()
                                .setId(peerId)
                                .setAddress("localhost:" + ports[i])
                                .build());
                peerIds.add(RaftPeerId.valueOf(peerId));
            }
            RaftGroup group = RaftGroup.valueOf(groupId, peers);

            for (int i = 0; i < NUM_SERVERS; i++) {
                RaftProperties properties = new RaftProperties();
                RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.GRPC);
                GrpcConfigKeys.Server.setPort(properties, ports[i]);
                RaftServerConfigKeys.setStorageDir(
                        properties, Collections.singletonList(new File(tempDir, "raft-n" + i)));

                RecordingStateMachine stateMachine = new RecordingStateMachine();
                stateMachines.add(stateMachine);
                RaftServer raftServer =
                        RaftServer.newBuilder()
                                .setServerId(peerIds.get(i))
                                .setGroup(group)
                                .setStateMachine(stateMachine)
                                .setProperties(properties)
                                .build();
                raftServers.add(raftServer);
            }
            for (RaftServer raftServer : raftServers) {
                raftServer.start();
            }

            client =
                    RaftClient.newBuilder()
                            .setRaftGroup(group)
                            .setProperties(new RaftProperties())
                            .build();

            // (1) A leader is elected; this already proves votes flow through the transport,
            // since group info itself travels the same grpc endpoints.
            RaftPeerId leader = awaitLeader(client, group, peerIds);
            int leaderIndex = peerIds.indexOf(leader);
            assertTrue(
                    leaderIndex >= 0 && leaderIndex < NUM_SERVERS, "leader is one of the peers");

            // (2) Replicate three writes and verify all replicas apply them, in the same order.
            List<String> firstBatch = new ArrayList<>();
            for (int i = 1; i <= 3; i++) {
                String message = "message-" + i;
                client.io().send(Message.valueOf(message));
                firstBatch.add(message);
            }
            awaitApplied(stateMachines, 3, "after first batch");
            assertApplied(stateMachines, firstBatch);

            // (3) Kill the leader's raft server. The remaining peers must elect a new leader
            // over the same transport; leader queries stay restricted to the survivors, since
            // asking the dead member would block until its request timeout.
            raftServers.get(leaderIndex).close();
            List<RaftPeerId> survivors = new ArrayList<>(peerIds);
            survivors.remove(leader);
            RaftPeerId newLeader = awaitLeader(client, group, survivors);
            assertNotEquals(leader, newLeader, "a different node takes over");

            List<String> secondBatch = new ArrayList<>();
            for (int i = 4; i <= 5; i++) {
                String message = "message-" + i;
                client.io().send(Message.valueOf(message));
                secondBatch.add(message);
            }
            List<RecordingStateMachine> survivorMachines = new ArrayList<>(stateMachines);
            survivorMachines.remove(leaderIndex);
            awaitApplied(survivorMachines, 5, "after failover batch");
            List<String> allMessages = new ArrayList<>(firstBatch);
            allMessages.addAll(secondBatch);
            assertApplied(survivorMachines, allMessages);
        } finally {
            if (client != null) {
                client.close();
            }
            for (int i = raftServers.size() - 1; i >= 0; i--) {
                closeQuietly(raftServers.get(i));
            }
        }
    }

    /** A bind-and-release free port for one grpc member. */
    private static int probedPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void closeQuietly(RaftServer server) {
        try {
            server.close();
        } catch (Exception ignored) {
            // close is best-effort during test teardown
        }
    }

    /**
     * Asks each candidate peer for its group info over the transport and returns the peer
     * that reports itself as leader.
     */
    private static Optional<RaftPeerId> findLeader(
            RaftClient client, RaftGroupId groupId, List<RaftPeerId> candidates)
            throws IOException {
        for (RaftPeerId candidate : candidates) {
            final GroupInfoReply info = client.getGroupManagementApi(candidate).info(groupId);
            if (info.getRoleInfoProto().getRole() == RaftPeerRole.LEADER) {
                return Optional.of(candidate);
            }
        }
        return Optional.empty();
    }

    private static RaftPeerId awaitLeader(
            RaftClient client, RaftGroup group, List<RaftPeerId> candidates) {
        return waitValue(
                () -> findLeader(client, group.getGroupId(), candidates),
                LEADER_TIMEOUT,
                "No leader elected among " + candidates + " within " + LEADER_TIMEOUT);
    }

    private static void awaitApplied(
            List<RecordingStateMachine> stateMachines, int expectedCount, String hint) {
        waitUntil(
                () -> {
                    for (RecordingStateMachine stateMachine : stateMachines) {
                        if (stateMachine.appliedCount() < expectedCount) {
                            return false;
                        }
                    }
                    return true;
                },
                APPLY_TIMEOUT,
                "State machines did not reach " + expectedCount + " applied messages: " + hint);
    }

    private static void assertApplied(
            List<RecordingStateMachine> stateMachines, List<String> expected) {
        for (int i = 0; i < stateMachines.size(); i++) {
            assertEquals(expected, stateMachines.get(i).appliedMessages(),
                    "applied messages of state machine " + i);
        }
    }

    /**
     * The recording state machine: a purely in-memory map from log index to the applied message
     * bytes, so tests can assert replication and ordering by reading each replica directly. No
     * snapshot support is needed; this suite never requests one.
     */
    private static final class RecordingStateMachine extends BaseStateMachine {

        private final ConcurrentSkipListMap<Long, byte[]> applied = new ConcurrentSkipListMap<>();

        @Override
        public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
            final LogEntryProto entry = trx.getLogEntry();
            if (entry.hasStateMachineLogEntry()) {
                applied.put(
                        entry.getIndex(),
                        entry.getStateMachineLogEntry().getLogData().toByteArray());
            }
            updateLastAppliedTermIndex(TermIndex.valueOf(entry));
            return CompletableFuture.completedFuture(Message.EMPTY);
        }

        @Override
        public CompletableFuture<Message> query(Message request) {
            return CompletableFuture.completedFuture(Message.valueOf("count=" + applied.size()));
        }

        int appliedCount() {
            return applied.size();
        }

        List<String> appliedMessages() {
            List<String> messages = new ArrayList<>();
            for (byte[] value : applied.values()) {
                messages.add(new String(value, StandardCharsets.UTF_8));
            }
            return messages;
        }
    }
}
