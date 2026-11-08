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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;

import org.apache.ratis.metadata.proto.MetadataProtos.Admission;
import org.apache.ratis.metadata.proto.MetadataProtos.BusinessResult;
import org.apache.ratis.metadata.proto.MetadataProtos.Lane;
import org.apache.ratis.metadata.proto.MetadataProtos.Operation;
import org.apache.ratis.metadata.proto.MetadataProtos.QueryReply;
import org.apache.ratis.metadata.proto.MetadataProtos.Role;
import org.apache.ratis.metadata.proto.MetadataProtos.Status;
import org.apache.ratis.metadata.proto.MetadataProtos.WriteReply;
import org.apache.ratis.metadata.protocol.GroupConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.RocksIterator;

/**
 * The lease reverse index of section 4.2: every binding move keeps the
 * bidirectional index, the binding count and the deletion paths consistent,
 * the per-lease binding cap holds, and failed batches change nothing.
 */
class TestLeaseSemantics {
  private static final byte[] V = "v".getBytes(StandardCharsets.UTF_8);
  private static final byte[] V2 = "v2".getBytes(StandardCharsets.UTF_8);

  @TempDir
  Path tempDir;

  private MetadataContractFixture fixture;
  private long index;
  private long now;
  private long nextLeaseId;

  @BeforeEach
  void setUp() {
    fixture = MetadataContractFixture.create(Role.COORDINATION,
        GroupConfig.defaultsFor(Role.COORDINATION), tempDir.resolve("store"));
    fixture.initialize(1, 1);
    index = 1;
    now = 1_000;
    nextLeaseId = 0;
  }

  private long nextIndex() {
    return ++index;
  }

  private WriteReply accept(int seed, long generation, long sequence,
      Operation operation) {
    return fixture.apply(nextIndex(), now,
        EngineRequests.write(fixture.groupId(), fixture.store().groupConfig(),
            EngineRequests.operationId(EngineRequests.writerId(seed), generation, sequence),
            operation, 60_000));
  }

  private void register(int seed, Lane lane) {
    assertEquals(Admission.ACCEPTED, fixture.admin(nextIndex(), now,
        EngineRequests.register(fixture.groupId(), fixture.store().groupConfig(),
            EngineRequests.writerId(seed), lane, 60_000)).getAdmission());
  }

  /** Opens a lease owned by the business writer; the id is the entry index. */
  private long openLease() {
    final WriteReply reply = accept(2, 1, ++nextLeaseId,
        EngineRequests.leaseOpen(EngineRequests.writerId(1), 1, 60_000));
    assertEquals(Admission.ACCEPTED, reply.getAdmission(), () -> "reply=" + reply);
    return business(reply).getLease().getLeaseId();
  }

  /** The physical reverse-index rows of one lease. */
  private int leasePathCount(long leaseId) {
    final byte[] prefix = new byte[8];
    ByteBuffer.wrap(prefix).putLong(leaseId);
    try (ReadView view = fixture.store().openReadView()) {
      final RocksIterator it = view.iterator(MetadataCf.LEASE_PATHS);
      try {
        int rows = 0;
        for (it.seek(prefix); it.isValid(); it.next()) {
          if (it.key().length < 8
              || !Arrays.equals(Arrays.copyOf(it.key(), 8), prefix)) {
            break;
          }
          rows++;
        }
        return rows;
      } finally {
        it.close();
      }
    }
  }

  private QueryReply get(String path) {
    return fixture.query(EngineRequests.get(fixture.groupId(),
        fixture.store().groupConfig(), path));
  }

  private static BusinessResult business(WriteReply reply) {
    try {
      return BusinessResult.parseFrom(reply.getOriginalBusinessResult());
    } catch (Exception e) {
      throw new AssertionError("the reply carries no parsable business result", e);
    }
  }

  private static void assertStatus(WriteReply reply, Status status) {
    assertEquals(Admission.ACCEPTED, reply.getAdmission(), () -> "reply=" + reply);
    assertEquals(status, business(reply).getStatus(), () -> "reply=" + reply);
  }

  // ---- A53: the index and the count track every binding move ----

  @Test
  void reverseIndexAndCountsTrackEveryMove() {
    register(1, Lane.BUSINESS);
    register(2, Lane.CONTROL);
    final long l1 = openLease();
    final long l2 = openLease();

    assertStatus(accept(1, 1, 1, EngineRequests.createEphemeral("/n1", l1)), Status.S_OK);
    assertStatus(accept(1, 1, 2, EngineRequests.putEphemeral("/n2", V, l1)), Status.S_OK);
    assertEquals(2, leasePathCount(l1));

    // a rewrite of the same binding never counts twice
    assertStatus(accept(1, 1, 3, EngineRequests.putEphemeral("/n1", V2, l1)), Status.S_OK);
    assertEquals(2, leasePathCount(l1));

    // a move to another lease shifts exactly one row
    assertStatus(accept(1, 1, 4, EngineRequests.putEphemeral("/n1", V, l2)), Status.S_OK);
    assertEquals(1, leasePathCount(l1));
    assertEquals(1, leasePathCount(l2));

    // a plain write makes the node persistent again
    assertStatus(accept(1, 1, 5, EngineRequests.put("/n1", V)), Status.S_OK);
    assertEquals(0, leasePathCount(l2));
    assertEquals(1, leasePathCount(l1));

    // revoking L1 deletes its one bound node with it
    assertStatus(accept(2, 1, ++nextLeaseId,
        EngineRequests.leaseRevoke(EngineRequests.writerId(1), 1, l1)), Status.S_OK);
    assertEquals(Status.S_NOT_FOUND, get("/n2").getStatus());
    assertEquals(0, leasePathCount(l1));
    assertEquals(1L, fixture.store().stats().leaseCount(), "only L2 survives");
  }

  // ---- A53: the cap and the childless guard, both without residue ----

  @Test
  void bindingCapAndChildlessGuardLeaveNoResidue() {
    register(1, Lane.BUSINESS);
    register(2, Lane.CONTROL);
    final long lease = openLease();

    assertStatus(accept(1, 1, 1, EngineRequests.create("/p", V)), Status.S_OK);
    assertStatus(accept(1, 1, 2, EngineRequests.create("/p/c", V)), Status.S_OK);

    // an existing node with descendants never binds
    assertStatus(accept(1, 1, 3, EngineRequests.putEphemeral("/p", V, lease)),
        Status.S_HAS_CHILDREN);
    assertEquals(0, leasePathCount(lease));

    // exactly leaseBindings nodes fit; the next one is refused whole
    for (int i = 0; i < 256; i++) {
      final WriteReply bound = accept(1, 1, 4 + i,
          EngineRequests.createEphemeral("/b" + i, lease));
      assertEquals(Admission.ACCEPTED, bound.getAdmission(), "binding " + i);
      assertEquals(Status.S_OK, business(bound).getStatus(), "binding " + i);
    }
    assertEquals(256, leasePathCount(lease));

    final WriteReply over = accept(1, 1, 4 + 256,
        EngineRequests.createEphemeral("/b256", lease));
    assertStatus(over, Status.S_LEASE_LIMIT);
    assertEquals(256, leasePathCount(lease), "the refused binding left nothing");
    assertEquals(Status.S_NOT_FOUND, get("/b256").getStatus());

    // a rewrite of an existing binding stays legal at the cap
    assertStatus(accept(1, 1, 4 + 257,
        EngineRequests.putEphemeral("/b0", V2, lease)), Status.S_OK);
    assertEquals(256, leasePathCount(lease));
  }

  // ---- A53: every deletion path keeps the index consistent ----

  @Test
  void deletionsKeepTheReverseIndexConsistent() {
    register(1, Lane.BUSINESS);
    register(2, Lane.CONTROL);
    final long lease = openLease();

    assertStatus(accept(1, 1, 1, EngineRequests.createEphemeral("/g/1", lease)),
        Status.S_OK);
    assertStatus(accept(1, 1, 2, EngineRequests.createEphemeral("/g/2", lease)),
        Status.S_OK);
    assertEquals(2, leasePathCount(lease));

    // the recursive delete releases both bindings with the nodes
    assertStatus(accept(1, 1, 3, EngineRequests.deleteRecursive("/g")), Status.S_OK);
    assertEquals(0, leasePathCount(lease));

    // a failed MULTI stages no binding change at all
    assertStatus(accept(1, 1, 4, EngineRequests.createEphemeral("/h", lease)),
        Status.S_OK);
    final WriteReply failed = accept(1, 1, 5, EngineRequests.multi(
        EngineRequests.putEphemeral("/x", V, lease),
        EngineRequests.delete("/missing")));
    assertStatus(failed, Status.S_MULTI_FAILED);
    assertEquals(1, business(failed).getMulti().getFailIndex());
    assertEquals(1, leasePathCount(lease), "the discarded sub-operation bound nothing");
    assertEquals(Status.S_NOT_FOUND, get("/x").getStatus());

    // and the revoke still reports exactly its one live binding
    final WriteReply revoked = accept(2, 1, ++nextLeaseId,
        EngineRequests.leaseRevoke(EngineRequests.writerId(1), 1, lease));
    assertStatus(revoked, Status.S_OK);
    assertEquals(1L, business(revoked).getLease().getDeletedCount());
    assertEquals(0, leasePathCount(lease));
    assertTrue(fixture.store().stats().leaseCount() == 0, "the lease row is gone");
  }
}
