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
package org.apache.ratis.metadata.statemachine;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import org.apache.ratis.metadata.proto.MetadataProtos.Admission;
import org.apache.ratis.metadata.proto.MetadataProtos.BusinessResult;
import org.apache.ratis.metadata.proto.MetadataProtos.Lane;
import org.apache.ratis.metadata.proto.MetadataProtos.Operation;
import org.apache.ratis.metadata.proto.MetadataProtos.QueryReply;
import org.apache.ratis.metadata.proto.MetadataProtos.Role;
import org.apache.ratis.metadata.proto.MetadataProtos.Status;
import org.apache.ratis.metadata.proto.MetadataProtos.WriteReply;
import org.apache.ratis.metadata.protocol.GroupConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A56: the stored-result lanes are separately capped. A full business lane
 * refuses the next sequence without consuming it and without any effect,
 * the control lane keeps working, and once the collection has reclaimed
 * expired business results the very same identity executes exactly once -
 * no reply inside its window is ever dropped early.
 */
class TestResultCapacity {
  private static final byte[] V = "v".getBytes(StandardCharsets.UTF_8);

  @TempDir
  Path tempDir;

  private MetadataContractFixture fixture;
  private long index = 1;
  private long now = 1_000;

  private long nextIndex() {
    return ++index;
  }

  private WriteReply accept(int seed, long generation, long sequence,
      Operation operation, long budget) {
    return fixture.apply(nextIndex(), now,
        EngineRequests.write(fixture.groupId(), fixture.store().groupConfig(),
            EngineRequests.operationId(EngineRequests.writerId(seed), generation, sequence),
            operation, budget));
  }

  private void register(int seed, Lane lane) {
    assertEquals(Admission.ACCEPTED, fixture.admin(nextIndex(), now,
        EngineRequests.register(fixture.groupId(), fixture.store().groupConfig(),
            EngineRequests.writerId(seed), lane, 60_000)).getAdmission());
  }

  private static BusinessResult business(WriteReply reply) {
    try {
      return BusinessResult.parseFrom(reply.getOriginalBusinessResult());
    } catch (Exception e) {
      throw new AssertionError("the reply carries no parsable business result", e);
    }
  }

  /** One 16-sub-operation batch: fifteen creates plus one counter bump. */
  private static Operation batch(String prefix) {
    final Operation[] ops = new Operation[16];
    for (int i = 0; i < 15; i++) {
      // the first child also brings the prefix parent into being
      ops[i] = EngineRequests.create(prefix + "/" + i, V, i == 0);
    }
    ops[15] = EngineRequests.increment("m", 1);
    return EngineRequests.multi(ops);
  }

  private QueryReply get(String path) {
    return fixture.query(EngineRequests.get(fixture.groupId(),
        fixture.store().groupConfig(), path));
  }

  @Test
  void fullLaneRefusesWithoutConsumingThenRetriesOnceAfterGc() {
    // the probe measures the exact charge and reply size of one batch on
    // this very code path, so the derived caps cannot drift
    long charge;
    int replySize;
    try (MetadataContractFixture probe = MetadataContractFixture.create(Role.COORDINATION,
        GroupConfig.defaultsFor(Role.COORDINATION), tempDir.resolve("probe"))) {
      fixture = probe;
      probe.initialize(1, 1);
      register(1, Lane.BUSINESS);
      register(2, Lane.CONTROL);
      final WriteReply measured = accept(1, 1, 1, batch("/probe"), 60_000);
      assertEquals(Admission.ACCEPTED, measured.getAdmission());
      charge = fixture.store().stats().businessResultBytes();
      replySize = measured.getOriginalBusinessResult().size();
      assertNotEquals(0L, charge);
    }

    try (MetadataContractFixture main = MetadataContractFixture.create(Role.COORDINATION,
        GroupConfig.valueOf(GroupConfig.defaultsFor(Role.COORDINATION).toBuilder()
            .setStoredReplyBytes(replySize + 128)
            .setBusinessResultBytes(charge + charge / 2)
            .build()), tempDir.resolve("main"))) {
      fixture = main;
      fixture.initialize(1, 1);
      register(1, Lane.BUSINESS);
      register(2, Lane.CONTROL);

      // a live lease on the control lane, opened before the pressure
      final WriteReply opened = accept(2, 1, 1,
          EngineRequests.leaseOpen(EngineRequests.writerId(1), 1, 60_000), 60_000);
      assertEquals(Status.S_OK, business(opened).getStatus());
      final long leaseId = business(opened).getLease().getLeaseId();

      // one batch fills the lane; its reply stays replayable inside the window
      assertEquals(Status.S_OK, business(accept(1, 1, 1, batch("/f"), 100)).getStatus());
      final WriteReply replay = accept(1, 1, 1, batch("/f"), 100);
      assertEquals(Admission.REPLAY, replay.getAdmission(), "no early deletion inside W");

      // the second batch does not fit: refused, not consumed, no effect
      final WriteReply refused = accept(1, 1, 2, batch("/g"), 100);
      assertEquals(Admission.REJECTED, refused.getAdmission());
      assertEquals(Status.S_RESULT_CAPACITY, refused.getAdmissionStatus());
      assertEquals(2L, refused.getExpectedNextSequence(), "the sequence stays unconsumed");
      assertEquals(Status.S_NOT_FOUND, get("/g/0").getStatus());
      final WriteReply counter = accept(1, 1, 2, EngineRequests.increment("m", 0), 100);
      assertEquals(Status.S_OK, business(counter).getStatus());
      assertEquals(1L, business(counter).getMutation().getCounterValue(),
          "the refused batch bumped nothing");

      // the control lane is untouched by the business pressure
      final WriteReply renewed = accept(2, 1, 2,
          EngineRequests.leaseRenew(EngineRequests.writerId(1), 1, leaseId, 60_000), 60_000);
      assertEquals(Status.S_OK, business(renewed).getStatus());

      // past the window the collection frees the lane; the same identity
      // (the client's new callId changes nothing) now executes exactly once
      now += 1_000;
      fixture.maintenance(nextIndex(), now, null);
      final WriteReply retry = accept(1, 1, 3, batch("/g"), 100);
      assertEquals(Admission.ACCEPTED, retry.getAdmission(),
          () -> "reply=" + retry + " usage=" + fixture.store().stats().businessResultBytes());
      assertEquals(Status.S_OK, business(retry).getStatus());
      assertEquals(Status.S_OK, get("/g/0").getStatus());
      assertEquals(3L, business(accept(1, 1, 4,
          EngineRequests.increment("m", 1), 100)).getMutation().getCounterValue(),
          "f plus the retried batch plus this bump - exactly once each");
      final WriteReply retryAgain = accept(1, 1, 3, batch("/g"), 100);
      assertEquals(Admission.REPLAY, retryAgain.getAdmission());
      assertArrayEquals(retry.getOriginalBusinessResult().toByteArray(),
          retryAgain.getOriginalBusinessResult().toByteArray());
    }
  }
}
