/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis;

import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.server.impl.PeerChanges;
import org.apache.ratis.RaftTestUtil.SimpleMessage;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientRpc;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.ClientInvocationId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.RaftServerTestUtil;
import org.apache.ratis.server.impl.RetryCacheTestUtil;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.raftlog.RaftLogIOException;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Slf4jUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public abstract class RetryCacheTests<CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  static {
    Slf4jUtils.setLogLevel(RaftServer.Division.LOG, Level.DEBUG);
  }

  public static final int NUM_SERVERS = 3;

  /**
   * make sure the retry cache can correct capture the retry from a client,
   * and returns the result from the previous request
   */
  @Test
  public void testBasicRetry() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::runTestBasicRetry);
  }

  void runTestBasicRetry(CLUSTER cluster) throws Exception {
    RaftTestUtil.waitForLeader(cluster);
    final RaftPeerId leaderId = cluster.getLeaderAndSendFirstMessage(false).getId();
    final long oldLastApplied = cluster.getLeader().getInfo().getLastAppliedIndex();

    try (final RaftClient client = cluster.createClient(leaderId)) {
      final RaftClientRpc rpc = client.getClientRpc();
      final long callId = 999;
      RaftClientRequest r = cluster.newRaftClientRequest(client.getId(), leaderId,
              callId, new SimpleMessage("message"));
      assertReply(rpc.sendRequest(r), client, callId);

      // retry with the same callId
      for (int i = 0; i < 5; i++) {
        assertReply(rpc.sendRequest(r), client, callId);
      }

      assertServer(cluster, client.getId(), callId, oldLastApplied);
    }
  }

  public static void assertReply(RaftClientReply reply, RaftClient client, long callId) {
    Assertions.assertEquals(client.getId(), reply.getClientId());
    Assertions.assertEquals(callId, reply.getCallId());
    Assertions.assertTrue(reply.isSuccess());
  }

  public void assertServer(MiniRaftCluster cluster, ClientId clientId, long callId, long oldLastApplied)
      throws Exception {
    final long leaderApplied = cluster.getLeader().getInfo().getLastAppliedIndex();
    // make sure retry cache has the entry
    for (RaftServer.Division server : cluster.iterateDivisions()) {
      LOG.info("check server " + server.getId());
      if (server.getInfo().getLastAppliedIndex() < leaderApplied) {
        Thread.sleep(1000);
      }
      Assertions.assertEquals(2, server.getRetryCache().getStatistics().size());
      Assertions.assertNotNull(RetryCacheTestUtil.get(server, clientId, callId));
      // make sure there is only one log entry committed
      Assertions.assertEquals(1, count(server.getRaftLog(), oldLastApplied + 1));
    }
  }

  static int count(RaftLog log, long startIndex) throws RaftLogIOException {
    final long nextIndex = log.getNextIndex();
    int count = 0;
    for(long i = startIndex; i < nextIndex; i++) {
      if (log.get(i).hasStateMachineLogEntry()) {
        count++;
      }
    }
    return count;
  }

  /**
   * Test retry while the leader changes to another peer
   */
  @Test
  public void testRetryOnNewLeader() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::runTestRetryOnNewLeader);
  }

  void runTestRetryOnNewLeader(CLUSTER cluster) throws Exception {
    RaftTestUtil.waitForLeader(cluster);
    final RaftPeerId leaderId = cluster.getLeaderAndSendFirstMessage(false).getId();

    try (final RaftClient client = cluster.createClient(leaderId)) {
      RaftClientRpc rpc = client.getClientRpc();
      final long callId = 999;
      RaftClientRequest r = cluster.newRaftClientRequest(client.getId(), leaderId,
              callId, new SimpleMessage("message"));
      assertReply(rpc.sendRequest(r), client, callId);
      final long oldLastApplied = cluster.getLeader().getInfo().getLastAppliedIndex();

      // trigger the reconfiguration, make sure the original leader is kicked out
      final PeerChanges change = cluster.removePeers(2, true, Collections.emptyList());
      final List<RaftPeer> allPeers = change.getPeersInNewConf();
      // trigger setConfiguration
      RaftServerTestUtil.runWithMinorityPeers(cluster, allPeers, cluster::setConfiguration);

      final RaftPeerId newLeaderId = JavaUtils.attemptRepeatedly(() -> {
        final RaftPeerId id = RaftTestUtil.waitForLeader(cluster).getId();
        Assertions.assertNotEquals(leaderId, id);
        return id;
      }, 10, TimeDuration.valueOf(100, TimeUnit.MILLISECONDS), "wait for a leader different than " + leaderId, LOG);
      Assertions.assertNotEquals(leaderId, newLeaderId);
      // same clientId and callId in the request
      r = cluster.newRaftClientRequest(client.getId(), newLeaderId,
              callId, new SimpleMessage("message"));
      rpc.addRaftPeers(change.getAddedPeers());
      for (int i = 0; i < 10; i++) {
        try {
          assertReply(rpc.sendRequest(r), client, callId);
          LOG.info("successfully sent out the retry request_" + i);
        } catch (Exception e) {
          LOG.info("hit exception while retrying the same request: " + r, e);
        }
        Thread.sleep(100);
      }

      // check the new leader and make sure the retry did not get committed
      Assertions.assertEquals(0, count(cluster.getLeader().getRaftLog(), oldLastApplied + 1));
    }
  }

  @Test
  public void testMultiIndexCompletionOrder() throws Exception {
    runTestMultiIndexCompletionOrder(false);
    runTestMultiIndexCompletionOrder(true);
  }

  private void runTestMultiIndexCompletionOrder(boolean newestFirst) throws Exception {
    final RaftProperties properties = new RaftProperties(getProperties());
    properties.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY, GatedStateMachine.class, StateMachine.class);
    RaftServerConfigKeys.RetryCache.setExpiryTime(properties, TimeDuration.valueOf(1, TimeUnit.SECONDS));
    GatedStateMachine.INSTANCE.set(null);

    try (CLUSTER cluster = getFactory().newCluster(1, properties)) {
      cluster.start();
      RaftTestUtil.waitForLeader(cluster);
      final RaftServer.Division leader = cluster.getLeaderAndSendFirstMessage(false);
      final GatedStateMachine stateMachine = GatedStateMachine.INSTANCE.get();
      Assertions.assertNotNull(stateMachine);

      // Netty permits only one outstanding request per callId on a connection.
      // Use an independent connection for the second original RPC with the same invocation.
      try (RaftClient client = cluster.createClient(leader.getId());
          RaftClient retryClient = RaftClient.newBuilder()
              .setClientId(client.getId())
              .setRaftGroup(cluster.getGroup())
              .setLeaderId(leader.getId())
              .setProperties(properties)
              .setParameters(cluster.getParameters())
              .build()) {
        final RaftClientRpc rpc = client.getClientRpc();
        final RaftClientRpc retryRpc = retryClient.getClientRpc();
        final long callId = 999;
        final ClientInvocationId invocationId = ClientInvocationId.valueOf(client.getId(), callId);
        final RaftClientRequest olderRequest = cluster.newRaftClientRequest(client.getId(), leader.getId(),
            callId, new SimpleMessage(GatedStateMachine.TARGET_MESSAGE));
        final RaftClientRequest newerRequest = cluster.newRaftClientRequest(client.getId(), leader.getId(),
            callId, new SimpleMessage(GatedStateMachine.TARGET_MESSAGE));
        final ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
          final Future<RaftClientReply> olderReply = executor.submit(() -> rpc.sendRequest(olderRequest));
          Assertions.assertTrue(stateMachine.awaitFirstApply());
          final long olderIndex = stateMachine.getOnlyApplyIndex();

          JavaUtils.attemptUntilTrue(() -> leader.getRetryCache().getIfPresent(invocationId) == null,
              50, HUNDRED_MILLIS, "first retry cache entry expires", LOG);
          final Future<RaftClientReply> newerReply = executor.submit(() -> retryRpc.sendRequest(newerRequest));
          // Metadata entries may advance getNextIndex before the second target entry is appended.
          final long newerIndex = JavaUtils.attempt(() -> {
            final long index = findTargetLogIndex(leader.getRaftLog(), olderIndex + 1);
            Assertions.assertTrue(index > olderIndex);
            return index;
          }, 50, HUNDRED_MILLIS, () -> "same invocation reaches a second log index", LOG);

          stateMachine.releaseFirstApply();
          Assertions.assertTrue(stateMachine.awaitApplyFutures());
          final List<Long> indices = stateMachine.getApplyIndices();
          Assertions.assertEquals(2, indices.size());
          Assertions.assertEquals(olderIndex, indices.get(0));
          Assertions.assertEquals(newerIndex, indices.get(1));

          if (newestFirst) {
            stateMachine.complete(newerIndex);
            stateMachine.complete(olderIndex);
          } else {
            stateMachine.complete(olderIndex);
            stateMachine.complete(newerIndex);
          }

          final RaftClientReply older = olderReply.get(10, TimeUnit.SECONDS);
          final RaftClientReply newer = newerReply.get(10, TimeUnit.SECONDS);
          Assertions.assertTrue(older.isSuccess());
          Assertions.assertTrue(newer.isSuccess());
          Assertions.assertEquals(olderIndex, older.getLogIndex());
          Assertions.assertEquals(newerIndex, newer.getLogIndex());
        } finally {
          stateMachine.releaseFirstApply();
          stateMachine.completeAll();
          executor.shutdownNow();
        }
      }
    }
  }

  private static long findTargetLogIndex(RaftLog log, long startIndex) throws RaftLogIOException {
    for (long index = startIndex; index < log.getNextIndex(); index++) {
      final LogEntryProto entry = log.get(index);
      if (entry != null && entry.hasStateMachineLogEntry()
          && GatedStateMachine.TARGET_MESSAGE.equals(entry.getStateMachineLogEntry().getLogData().toStringUtf8())) {
        return index;
      }
    }
    return RaftLog.INVALID_LOG_INDEX;
  }

  public static class GatedStateMachine extends BaseStateMachine {
    private static final String TARGET_MESSAGE = "duplicate";
    private static final AtomicReference<GatedStateMachine> INSTANCE = new AtomicReference<>();

    private final CountDownLatch firstApply = new CountDownLatch(1);
    private final CountDownLatch releaseFirstApply = new CountDownLatch(1);
    private final CountDownLatch applyFutures = new CountDownLatch(2);
    private final AtomicInteger targetCalls = new AtomicInteger();
    private final Map<Long, CompletableFuture<Message>> futures = new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
      INSTANCE.compareAndSet(null, this);
      final LogEntryProto entry = trx.getLogEntry();
      if (!TARGET_MESSAGE.equals(entry.getStateMachineLogEntry().getLogData().toStringUtf8())) {
        return super.applyTransaction(trx);
      }

      updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
      final CompletableFuture<Message> future = new CompletableFuture<>();
      futures.put(entry.getIndex(), future);
      if (targetCalls.getAndIncrement() == 0) {
        firstApply.countDown();
        try {
          if (!releaseFirstApply.await(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Timed out waiting to release the first apply");
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(e);
        }
      }
      applyFutures.countDown();
      return future;
    }

    boolean awaitFirstApply() throws InterruptedException {
      return firstApply.await(5, TimeUnit.SECONDS);
    }

    long getOnlyApplyIndex() {
      Assertions.assertEquals(1, futures.size());
      return futures.keySet().iterator().next();
    }

    void releaseFirstApply() {
      releaseFirstApply.countDown();
    }

    boolean awaitApplyFutures() throws InterruptedException {
      return applyFutures.await(5, TimeUnit.SECONDS);
    }

    List<Long> getApplyIndices() {
      final List<Long> indices = new ArrayList<>(futures.keySet());
      Collections.sort(indices);
      return indices;
    }

    void complete(long index) {
      final CompletableFuture<Message> future = futures.get(index);
      Assertions.assertNotNull(future, "Missing apply future for index " + index);
      future.complete(Message.valueOf("applied-" + index));
    }

    void completeAll() {
      futures.keySet().forEach(this::complete);
    }
  }
}
