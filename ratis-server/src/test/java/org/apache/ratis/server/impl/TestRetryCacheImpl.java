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

import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.ClientInvocationId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.impl.RetryCacheImpl.CacheEntry;
import org.apache.ratis.server.impl.RetryCacheImpl.CacheQueryResult;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestRetryCacheImpl {
  private static RetryCacheImpl newCache() {
    return new RetryCacheImpl(TimeDuration.valueOf(1, TimeUnit.HOURS), null);
  }

  private static ClientInvocationId newKey() {
    return ClientInvocationId.valueOf(ClientId.randomId(), 7);
  }

  private static RaftClientReply reply(ClientInvocationId key, long logIndex) {
    return RaftClientReply.newBuilder()
        .setClientInvocationId(key)
        .setServerId(RaftPeerId.valueOf("s0"))
        .setGroupId(RaftGroupId.randomId())
        .setLogIndex(logIndex)
        .setSuccess()
        .build();
  }

  private static RaftClientRequest request(ClientInvocationId key) {
    return RaftClientRequest.newBuilder()
        .setClientId(key.getClientId())
        .setServerId(RaftPeerId.valueOf("s0"))
        .setGroupId(RaftGroupId.randomId())
        .setCallId(key.getLongId())
        .setMessage(Message.valueOf("test"))
        .setType(RaftClientRequest.writeRequestType())
        .build();
  }

  @Test
  void testNewestIndexWinsRegardlessOfCompletionOrder() {
    assertNewestIndexWins(true);
    assertNewestIndexWins(false);
  }

  private void assertNewestIndexWins(boolean olderFirst) {
    final RetryCacheImpl cache = newCache();
    final ClientInvocationId key = newKey();
    final CacheEntry older = cache.getOrCreateEntry(key, TermIndex.valueOf(1, 10));
    final CacheEntry newer = cache.getOrCreateEntry(key, TermIndex.valueOf(1, 11));
    assertNotSame(older, newer);

    if (olderFirst) {
      older.updateResult(reply(key, 10));
      assertSame(newer, cache.getIfPresent(key));
      assertFalse(newer.getReplyFuture().isDone());
      newer.updateResult(reply(key, 11));
    } else {
      newer.updateResult(reply(key, 11));
      assertSame(newer, cache.getIfPresent(key));
      assertFalse(older.getReplyFuture().isDone());
      older.updateResult(reply(key, 10));
    }

    assertEquals(10, older.getReplyFuture().join().getLogIndex());
    assertEquals(11, newer.getReplyFuture().join().getLogIndex());
    assertSame(newer, cache.getIfPresent(key));
    assertEquals(11, cache.getIfPresent(key).getReplyFuture().join().getLogIndex());
  }

  @Test
  void testUnboundPendingSurvivesPositionAwareLookup() {
    final RetryCacheImpl cache = newCache();
    final ClientInvocationId key = newKey();
    final CacheEntry pending = cache.getOrCreateEntry(key);

    assertSame(pending, cache.getOrCreateEntry(key, TermIndex.valueOf(1, 10)));
    assertFalse(pending.isDone());
    assertEquals(RaftLog.INVALID_LOG_INDEX, pending.getLogIndex());

    pending.bind(TermIndex.valueOf(1, 10));
    assertEquals(10, pending.getLogIndex());
  }

  @Test
  void testBindAcceptsExactlyOnePosition() {
    final CacheEntry entry = new CacheEntry(newKey());
    assertEquals(RaftLog.INVALID_LOG_INDEX, entry.getLogIndex());

    entry.bind(TermIndex.valueOf(1, 10));
    assertEquals(10, entry.getLogIndex());
    assertThrows(IllegalStateException.class, () -> entry.bind(TermIndex.valueOf(1, 11)));
    assertEquals(10, entry.getLogIndex());
  }

  @Test
  void testFailedEntryReplacementRespectsPosition() {
    final RetryCacheImpl cache = newCache();
    final ClientInvocationId key = newKey();
    final TermIndex olderPosition = TermIndex.valueOf(1, 10);
    final TermIndex currentPosition = TermIndex.valueOf(1, 11);
    final CacheEntry current = cache.getOrCreateEntry(key, currentPosition);
    current.failWithReply(currentPosition, reply(key, 11));

    assertSame(current, cache.getOrCreateEntry(key, olderPosition),
        "a larger current index must survive even when it has failed");

    final CacheEntry sameIndex = cache.getOrCreateEntry(key, currentPosition);
    assertNotSame(current, sameIndex, "a failed entry must be replaceable at the same index");

    sameIndex.failWithReply(currentPosition, reply(key, 11));
    final CacheEntry newer = cache.getOrCreateEntry(key, TermIndex.valueOf(1, 12));
    assertNotSame(sameIndex, newer, "a failed older entry must be replaceable by a newer index");
  }

  @Test
  void testUpdateBeforeBindDoesNotCompleteEntry() {
    final RetryCacheImpl cache = newCache();
    final ClientInvocationId key = newKey();
    final TermIndex olderPosition = TermIndex.valueOf(1, 10);
    final TermIndex boundPosition = TermIndex.valueOf(1, 11);
    final CacheEntry entry = cache.getOrCreateEntry(key);

    assertSame(entry, cache.getOrCreateEntry(key, olderPosition));
    entry.updateResult(reply(key, olderPosition.getIndex()));
    assertFalse(entry.isDone(), "an unbound entry must ignore a replicated reply");

    entry.bind(boundPosition);
    assertFalse(entry.isDone(), "binding must not expose an older replicated reply");
    entry.updateResult(reply(key, olderPosition.getIndex()));
    assertFalse(entry.isDone(), "replaying the older reply must remain ineffective");

    entry.updateResult(reply(key, boundPosition.getIndex()));
    assertEquals(boundPosition.getIndex(), entry.getReplyFuture().join().getLogIndex());
    assertSame(entry, cache.getIfPresent(key));
    assertEquals(boundPosition.getIndex(), cache.getIfPresent(key).getLogIndex());
    assertEquals(boundPosition.getIndex(), cache.getIfPresent(key).getReplyFuture().join().getLogIndex());
    assertThrows(AssertionError.class, () -> entry.updateResult(reply(key, boundPosition.getIndex())));
  }

  @Test
  void testUnboundReplyIsDeferredAndDeliveredOnMatchingBind() {
    final RetryCacheImpl cache = newCache();
    final ClientInvocationId key = newKey();
    final CacheEntry entry = cache.getOrCreateEntry(key);
    final RaftClientReply deferred = reply(key, 10);

    entry.updateResult(deferred);
    assertFalse(entry.getReplyFuture().isDone(), "an unbound entry must defer the reply, not complete");

    entry.bind(TermIndex.valueOf(1, 10));
    assertTrue(entry.getReplyFuture().isDone(), "bind to the deferred index must deliver the deferred reply");
    assertSame(deferred, entry.getReplyFuture().join());
  }

  @Test
  void testUnboundReplyDiscardedWhenBindMovesToAnotherIndex() {
    final RetryCacheImpl cache = newCache();
    final ClientInvocationId key = newKey();
    final CacheEntry entry = cache.getOrCreateEntry(key);

    entry.updateResult(reply(key, 10));
    assertFalse(entry.getReplyFuture().isDone());

    entry.bind(TermIndex.valueOf(1, 11));
    assertFalse(entry.getReplyFuture().isDone(), "a deferred reply for a superseded index must be discarded on bind");

    entry.updateResult(reply(key, 11));
    assertEquals(11, entry.getReplyFuture().join().getLogIndex());
  }

  @Test
  void testBindBeforeUpdateChecksIndexAndSingleCompletion() {
    final RetryCacheImpl cache = newCache();
    final ClientInvocationId key = newKey();
    final CacheEntry entry = cache.getOrCreateEntry(key);
    entry.bind(TermIndex.valueOf(1, 10));

    entry.updateResult(reply(key, 11));
    assertFalse(entry.getReplyFuture().isDone(), "a reply for another index must be ignored");

    entry.updateResult(reply(key, 10));
    assertEquals(10, entry.getReplyFuture().join().getLogIndex());
    assertThrows(AssertionError.class, () -> entry.updateResult(reply(key, 10)));
    assertEquals(10, entry.getReplyFuture().join().getLogIndex());
  }

  @Test
  void testFailWithReplyRequiresExactTermIndex() {
    final RetryCacheImpl cache = newCache();
    final ClientInvocationId key = newKey();
    final TermIndex position = TermIndex.valueOf(1, 10);
    final CacheEntry entry = cache.getOrCreateEntry(key, position);

    entry.failWithReply(TermIndex.valueOf(2, 10), reply(key, 10));
    entry.failWithReply(TermIndex.valueOf(1, 11), reply(key, 11));
    assertFalse(entry.isDone());
    assertFalse(entry.isFailed());

    entry.failWithReply(position, reply(key, 10));
    assertTrue(entry.isFailed());
    assertEquals(10, entry.getReplyFuture().join().getLogIndex());
  }

  @Test
  void testQueryCacheFirstWins() {
    final RetryCacheImpl cache = newCache();
    final ClientInvocationId key = newKey();
    final RaftClientRequest request = request(key);
    final CacheQueryResult first = cache.queryCache(request);
    assertFalse(first.isRetry());

    final CacheQueryResult pending = cache.queryCache(request);
    assertTrue(pending.isRetry());
    assertSame(first.getEntry(), pending.getEntry());

    first.getEntry().bind(TermIndex.valueOf(1, 10));
    first.getEntry().updateResult(reply(key, 10));
    final CacheQueryResult completed = cache.queryCache(request);
    assertTrue(completed.isRetry());
    assertSame(first.getEntry(), completed.getEntry());
  }

  @Test
  void testQueryCacheReplacesFailedEntry() {
    final RetryCacheImpl cache = newCache();
    final ClientInvocationId key = newKey();
    final RaftClientRequest request = request(key);
    final CacheQueryResult failed = cache.queryCache(request);
    failed.getEntry().failWithReply(reply(key, 10));

    final CacheQueryResult replacement = cache.queryCache(request);
    assertFalse(replacement.isRetry());
    assertNotSame(failed.getEntry(), replacement.getEntry());

    final CacheQueryResult pending = cache.queryCache(request);
    assertTrue(pending.isRetry());
    assertSame(replacement.getEntry(), pending.getEntry());
  }

  @Test
  void testPendingRequestRejectsSecondReply() {
    final ClientInvocationId key = newKey();
    final PendingRequest pending = new PendingRequest(request(key), null);
    pending.setReply(reply(key, 10));

    final IllegalStateException exception = assertThrows(IllegalStateException.class,
        () -> pending.setReply(reply(key, 11)));
    assertTrue(exception.getMessage().contains("PendingRequest already completed"));
    assertEquals(10, pending.getFuture().join().getLogIndex());
  }
}
