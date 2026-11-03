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
package org.apache.ratis.server.impl;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.server.metrics.RaftServerMetricsImpl;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestPendingRequests {
  private static PendingRequests newPendingRequests() {
    // RaftServerMetricsImpl is final and the default Mockito mock maker cannot
    // mock final classes - build a real instance, as TestRetryCacheMetrics does.
    final RetryCacheImpl retryCache = new RetryCacheImpl(TimeDuration.valueOf(1, TimeUnit.HOURS), null);
    final RaftServerMetricsImpl metrics = RaftServerMetricsImpl.computeIfAbsentRaftServerMetrics(
        RaftServerTestUtil.TEST_MEMBER_ID, id -> 0L, retryCache::getStatistics);
    return new PendingRequests(RaftServerTestUtil.TEST_MEMBER_ID, new RaftProperties(), metrics);
  }

  private static SetConfigurationRequest newSetConfRequest() {
    return new SetConfigurationRequest(ClientId.randomId(), RaftPeerId.valueOf("s0"),
        RaftGroupId.randomId(), 0,
        Collections.singletonList(RaftPeer.newBuilder().setId("s0").build()));
  }

  @Test
  void testSetConfReplyRacingLeaderStop() {
    final PendingRequests pendingRequests = newPendingRequests();
    final PendingRequest pending = pendingRequests.addConfRequest(newSetConfRequest());
    assertFalse(pending.getFuture().isDone());

    // The (new) configuration commits on the EventProcessor while, inside the
    // same window, the leader state stops on the closer thread and sends
    // NotLeaderException replies.  Neither completer may throw; the future
    // keeps the first reply (first-wins).
    pendingRequests.replySetConfiguration(request -> {
      pendingRequests.sendNotLeaderResponses(
          new NotLeaderException(RaftServerTestUtil.TEST_MEMBER_ID, null, null),
          Collections.emptyList());
      return RaftClientReply.newBuilder().setRequest(request).setSuccess().build();
    });

    assertTrue(pending.getFuture().isDone());
    assertTrue(pending.getFuture().join().getException() instanceof NotLeaderException);
  }
}
