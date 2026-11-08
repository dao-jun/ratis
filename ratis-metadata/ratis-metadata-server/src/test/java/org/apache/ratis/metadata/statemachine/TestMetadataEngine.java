/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file distributed with
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
package org.apache.ratis.metadata.statemachine;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import org.apache.ratis.metadata.proto.MetadataProtos.AdminWrite;
import org.apache.ratis.metadata.proto.MetadataProtos.Admission;
import org.apache.ratis.metadata.proto.MetadataProtos.BusinessResult;
import org.apache.ratis.metadata.proto.MetadataProtos.ClientWrite;
import org.apache.ratis.metadata.proto.MetadataProtos.Lane;
import org.apache.ratis.metadata.proto.MetadataProtos.Role;
import org.apache.ratis.metadata.proto.MetadataProtos.Status;
import org.apache.ratis.metadata.proto.MetadataProtos.WriteReply;
import org.apache.ratis.metadata.protocol.GroupConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The admission ladder and identity semantics of the engine, as observable
 * trajectories over the fixed contract fixture: writer registration,
 * takeover, replay, expiry, digest conflicts, gaps and the envelope gates.
 */
class TestMetadataEngine {
  private static final byte[] COUNTER = "counter-a".getBytes(StandardCharsets.UTF_8);

  @TempDir
  Path tempDir;

  private MetadataContractFixture fixture;
  private long index;
  private long now;

  @BeforeEach
  void setUp() {
    fixture = MetadataContractFixture.create(Role.COORDINATION,
        GroupConfig.defaultsFor(Role.COORDINATION), tempDir.resolve("store"));
    fixture.initialize(1, 1);
    index = 1;
    now = 1_000;
  }

  private long nextIndex() {
    return ++index;
  }

  private long advanceTime(long delta) {
    now += delta;
    return now;
  }

  private WriteReply register(int seed, Lane lane) {
    return fixture.admin(nextIndex(), now,
        EngineRequests.register(fixture.groupId(), fixtureConfig(),
            EngineRequests.writerId(seed), lane, 60_000));
  }

  private GroupConfig fixtureConfig() {
    return fixture.store().groupConfig();
  }

  private WriteReply accept(int seed, long generation, long sequence,
      org.apache.ratis.metadata.proto.MetadataProtos.Operation operation, long budget) {
    return fixture.apply(nextIndex(), now,
        EngineRequests.write(fixture.groupId(), fixtureConfig(),
            EngineRequests.operationId(EngineRequests.writerId(seed), generation, sequence),
            operation, budget));
  }

  private static BusinessResult business(WriteReply reply) {
    try {
      return BusinessResult.parseFrom(reply.getOriginalBusinessResult());
    } catch (org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException e) {
      throw new AssertionError("the reply carries no parsable business result", e);
    }
  }

  private static void assertRejected(WriteReply reply, Status status) {
    assertEquals(Admission.REJECTED, reply.getAdmission(), () -> "reply=" + reply);
    assertEquals(status, reply.getAdmissionStatus(), () -> "reply=" + reply);
    assertTrue(reply.getOriginalBusinessResult().isEmpty(),
        "a rejection carries no historical business result");
  }

  private static void assertAccepted(WriteReply reply) {
    assertEquals(Admission.ACCEPTED, reply.getAdmission(), () -> "reply=" + reply);
    assertEquals(Status.S_OK, reply.getAdmissionStatus(), () -> "reply=" + reply);
    assertTrue(!reply.getOriginalBusinessResult().isEmpty(),
        "an accepted reply carries the business result");
  }

  // ---- A06: registration never resets ----

  @Test
  void registrationIsIdempotentAndNeverResets() {
    final byte[] writer = EngineRequests.writerId(1);
    final WriteReply first = fixture.admin(nextIndex(), now,
        EngineRequests.register(fixture.groupId(), fixtureConfig(), writer, Lane.BUSINESS, 60_000));
    assertEquals(Admission.ACCEPTED, first.getAdmission());
    assertEquals(1L, first.getRegisteredWriter().getGeneration());
    assertEquals(0L, first.getRegisteredWriter().getHighWater());
    assertEquals(1L, fixture.store().stats().writerCount());

    // same id, same lane: a pure confirmation of the current state
    final WriteReply again = fixture.admin(nextIndex(), now,
        EngineRequests.register(fixture.groupId(), fixtureConfig(), writer, Lane.BUSINESS, 60_000));
    assertEquals(Admission.ACCEPTED, again.getAdmission());
    assertEquals(1L, again.getRegisteredWriter().getGeneration());
    assertEquals(0L, again.getRegisteredWriter().getHighWater());
    assertEquals(1L, fixture.store().stats().writerCount(), "no second writer row");

    // a consumed sequence must survive re-registration
    assertAccepted(accept(1, 1, 1, EngineRequests.increment("c", 5), 60_000));
    final WriteReply afterWork = fixture.admin(nextIndex(), now,
        EngineRequests.register(fixture.groupId(), fixtureConfig(), writer, Lane.BUSINESS, 60_000));
    assertEquals(Admission.ACCEPTED, afterWork.getAdmission());
    assertEquals(1L, afterWork.getRegisteredWriter().getGeneration());
    assertEquals(1L, afterWork.getRegisteredWriter().getHighWater(), "high water is not reset");

    // after takeover the row is spoken for: re-registering cannot pretend success
    fixture.admin(nextIndex(), now, EngineRequests.takeover(fixture.groupId(), fixtureConfig(),
        writer, 1, EngineRequests.nonce(7), 60_000));
    final WriteReply postTakeover = fixture.admin(nextIndex(), now,
        EngineRequests.register(fixture.groupId(), fixtureConfig(), writer, Lane.BUSINESS, 60_000));
    assertRejected(postTakeover, Status.S_WRITER_EXISTS);
    assertEquals(2L, postTakeover.getRegisteredWriter().getGeneration());
    assertEquals(2L, postTakeover.getCurrentGeneration());

    // a different lane under the same id conflicts just the same
    final WriteReply crossLane = fixture.admin(nextIndex(), now,
        EngineRequests.register(fixture.groupId(), fixtureConfig(), writer, Lane.CONTROL, 60_000));
    assertRejected(crossLane, Status.S_WRITER_EXISTS);
  }

  // ---- A07: terminal replies replay byte-for-byte ----

  @Test
  void replayReturnsTheOriginalReplyWithoutReexecution() {
    register(1, Lane.BUSINESS);
    final WriteReply first =
        accept(1, 1, 1, EngineRequests.increment("c", 1), 60_000);
    assertAccepted(first);
    assertEquals(1L, business(first).getMutation().getCounterValue());
    assertEquals(2L, first.getExpectedNextSequence());

    final WriteReply retry = accept(1, 1, 1, EngineRequests.increment("c", 1), 60_000);
    assertEquals(Admission.REPLAY, retry.getAdmission());
    assertArrayEquals(first.getOriginalBusinessResult().toByteArray(),
        retry.getOriginalBusinessResult().toByteArray(), "the original bytes come back");
    assertEquals(2L, retry.getExpectedNextSequence(), "a replay consumes nothing");
    assertTrue(!retry.getStaleGeneration(), "the generation is still current");

    // the counter executed exactly once: seq2 sees value 2, not 3
    final WriteReply second = accept(1, 1, 2, EngineRequests.increment("c", 1), 60_000);
    assertAccepted(second);
    assertEquals(2L, business(second).getMutation().getCounterValue());
  }

  // ---- A08: a late retry past the window never re-executes ----

  @Test
  void expiredResultsNeverReexecute() {
    register(1, Lane.BUSINESS);
    assertAccepted(accept(1, 1, 1, EngineRequests.increment("c", 1), 100));

    advanceTime(1_000); // far beyond the 100ms budget of seq1
    // any later entry carries the bounded collection; the retry lands after it
    register(2, Lane.BUSINESS);

    final WriteReply late = accept(1, 1, 1, EngineRequests.increment("c", 1), 100);
    assertRejected(late, Status.S_RESULT_EXPIRED);
    assertEquals(2L, late.getExpectedNextSequence(), "the ladder sees high water 1");

    // the old sequence stays consumed and the counter untouched
    final WriteReply next = accept(1, 1, 2, EngineRequests.increment("c", 1), 60_000);
    assertAccepted(next);
    assertEquals(2L, business(next).getMutation().getCounterValue());
    final WriteReply stale = accept(1, 1, 1, EngineRequests.increment("c", 1), 100);
    assertRejected(stale, Status.S_RESULT_EXPIRED);
  }

  // ---- A09: the same identity under a different digest ----

  @Test
  void digestConflictIsInvocationMismatch() {
    register(1, Lane.BUSINESS);
    assertAccepted(accept(1, 1, 1, EngineRequests.increment("c", 1), 60_000));

    final WriteReply conflict =
        accept(1, 1, 1, EngineRequests.increment("other", 1), 60_000);
    assertRejected(conflict, Status.S_INVOCATION_MISMATCH);
    assertEquals(2L, conflict.getExpectedNextSequence(), "the conflict consumes nothing");
  }

  // ---- A10: gaps do not consume, stored failures do ----

  @Test
  void gapsRejectWhileStoredFailuresConsume() {
    register(1, Lane.BUSINESS);

    final WriteReply gap = accept(1, 1, 3, EngineRequests.increment("c", 1), 60_000);
    assertRejected(gap, Status.S_SEQUENCE_GAP);
    assertEquals(1L, gap.getExpectedNextSequence());

    // a deterministic business failure is a stored terminal: consumed and replayable
    final WriteReply failure = accept(1, 1, 1, EngineRequests.delete("/missing"), 60_000);
    assertAccepted(failure);
    assertEquals(Status.S_NOT_FOUND, business(failure).getStatus());
    assertEquals(2L, failure.getExpectedNextSequence());

    final WriteReply retryFailure = accept(1, 1, 1, EngineRequests.delete("/missing"), 60_000);
    assertEquals(Admission.REPLAY, retryFailure.getAdmission());
    assertArrayEquals(failure.getOriginalBusinessResult().toByteArray(),
        retryFailure.getOriginalBusinessResult().toByteArray());

    // seq2 runs after the consumed failure; seq4 is still a gap
    assertAccepted(accept(1, 1, 2, EngineRequests.increment("c", 1), 60_000));
    final WriteReply gapAgain = accept(1, 1, 4, EngineRequests.increment("c", 1), 60_000);
    assertRejected(gapAgain, Status.S_SEQUENCE_GAP);
    assertEquals(3L, gapAgain.getExpectedNextSequence());
  }

  // ---- A11/A12: takeover advances exactly once, replies live for the budget ----

  @Test
  void takeoverAdvancesTheGenerationExactlyOnce() {
    register(1, Lane.BUSINESS);
    assertAccepted(accept(1, 1, 1, EngineRequests.increment("c", 1), 60_000));

    final byte[] nonce = EngineRequests.nonce(7);
    final WriteReply takeover = fixture.admin(nextIndex(), now,
        EngineRequests.takeover(fixture.groupId(), fixtureConfig(),
            EngineRequests.writerId(1), 1, nonce, 60_000));
    assertEquals(Admission.ACCEPTED, takeover.getAdmission());
    assertEquals(2L, takeover.getRegisteredWriter().getGeneration());
    assertEquals(0L, takeover.getRegisteredWriter().getHighWater(), "takeover resets the water");
    assertEquals(2L, business(takeover).getWriter().getGeneration());

    // a concurrent takeover of the same expected generation loses deterministically
    final WriteReply loser = fixture.admin(nextIndex(), now,
        EngineRequests.takeover(fixture.groupId(), fixtureConfig(),
            EngineRequests.writerId(1), 1, EngineRequests.nonce(8), 60_000));
    assertRejected(loser, Status.S_STALE_GENERATION);
    assertEquals(2L, loser.getCurrentGeneration());

    // the winner's reply is durably replayable
    final WriteReply winnerReplay = fixture.admin(nextIndex(), now,
        EngineRequests.takeover(fixture.groupId(), fixtureConfig(),
            EngineRequests.writerId(1), 1, nonce, 60_000));
    assertEquals(Admission.REPLAY, winnerReplay.getAdmission());
    assertArrayEquals(takeover.getOriginalBusinessResult().toByteArray(),
        winnerReplay.getOriginalBusinessResult().toByteArray());

    // the old generation is fenced; the new one starts from sequence 1
    final WriteReply oldGeneration =
        accept(1, 1, 2, EngineRequests.increment("c", 1), 60_000);
    assertRejected(oldGeneration, Status.S_STALE_GENERATION);
    assertEquals(2L, oldGeneration.getCurrentGeneration());
    final WriteReply newGeneration =
        accept(1, 2, 1, EngineRequests.increment("c", 1), 60_000);
    assertAccepted(newGeneration);
    assertEquals(2L, business(newGeneration).getMutation().getCounterValue());
  }

  @Test
  void expiredTakeoverRepliesDoNotReexecute() {
    register(1, Lane.BUSINESS);
    final AdminWrite takeover = EngineRequests.takeover(fixture.groupId(), fixtureConfig(),
        EngineRequests.writerId(1), 1, EngineRequests.nonce(7), 100);
    assertEquals(Admission.ACCEPTED,
        fixture.admin(nextIndex(), now, takeover).getAdmission());

    advanceTime(1_000);
    register(2, Lane.BUSINESS); // carries the collection past the budget

    final WriteReply late = fixture.admin(nextIndex(), now, takeover);
    assertRejected(late, Status.S_STALE_GENERATION);
    assertEquals(2L, late.getCurrentGeneration(), "the generation never advances twice");
  }

  // ---- envelope gates: structural rejections never consume ----

  @Test
  void envelopeGatesRejectWithoutConsuming() {
    register(1, Lane.BUSINESS);

    // a foreign group's envelope for our group id
    final GroupConfig foreign = GroupConfig.defaultsFor(Role.KEYSPACE);
    final ClientWrite foreignRole = EngineRequests.foreignGroupWrite(fixture.groupId(),
        Role.KEYSPACE, foreign,
        EngineRequests.operationId(EngineRequests.writerId(1), 1, 1),
        EngineRequests.put("k".getBytes(StandardCharsets.UTF_8), new byte[] {1}), 60_000);
    assertRejected(fixture.apply(nextIndex(), now, foreignRole), Status.S_CONFIG_MISMATCH);

    // a corrupted self-digest is undecodable, not a conflict
    final ClientWrite good = EngineRequests.write(fixture.groupId(), fixtureConfig(),
        EngineRequests.operationId(EngineRequests.writerId(1), 1, 1),
        EngineRequests.increment("c", 1), 60_000);
    assertRejected(fixture.apply(nextIndex(), now,
        EngineRequests.corruptedDigestWrite(good)), Status.S_MALFORMED);

    // a budget beyond the group retention is refused
    final ClientWrite overBudget = good.toBuilder().setRetryBudgetMs(
        fixtureConfig().retentionMs() + 1).build();
    assertRejected(fixture.apply(nextIndex(), now, overBudget), Status.S_BUDGET_MISMATCH);

    // nothing above consumed: seq1 still executes
    final WriteReply first =
        accept(1, 1, 1, EngineRequests.increment("c", 1), 60_000);
    assertAccepted(first);
    assertEquals(1L, business(first).getMutation().getCounterValue());
  }

  // ---- lane discipline: cross-lane commands are stored terminals ----

  @Test
  void crossLaneCommandsAreStoredTerminals() {
    register(1, Lane.BUSINESS);
    register(2, Lane.CONTROL);

    // a business writer cannot open leases
    final WriteReply wrongLane =
        accept(1, 1, 1, EngineRequests.leaseOpen(EngineRequests.writerId(1), 1, 30_000), 60_000);
    assertAccepted(wrongLane);
    assertEquals(Status.S_NOT_ALLOWED, business(wrongLane).getStatus());
    assertEquals(2L, wrongLane.getExpectedNextSequence(), "the stored terminal consumed seq1");

    // a control writer cannot touch nodes; that consumes its own sequence too
    final WriteReply control =
        accept(2, 1, 1, EngineRequests.increment("c", 1), 60_000);
    assertAccepted(control);
    assertEquals(Status.S_NOT_ALLOWED, business(control).getStatus());
    assertEquals(2L, control.getExpectedNextSequence());
    assertEquals(2L, fixture.store().stats().writerCount());
  }
}
