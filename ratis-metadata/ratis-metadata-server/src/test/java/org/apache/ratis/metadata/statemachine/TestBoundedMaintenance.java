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

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import org.apache.ratis.metadata.proto.MetadataProtos.Admission;
import org.apache.ratis.metadata.proto.MetadataProtos.BusinessResult;
import org.apache.ratis.metadata.proto.MetadataProtos.LeaseCandidate;
import org.apache.ratis.metadata.proto.MetadataProtos.Lane;
import org.apache.ratis.metadata.proto.MetadataProtos.Operation;
import org.apache.ratis.metadata.proto.MetadataProtos.QueryReply;
import org.apache.ratis.metadata.proto.MetadataProtos.Role;
import org.apache.ratis.metadata.proto.MetadataProtos.Status;
import org.apache.ratis.metadata.proto.MetadataProtos.WriteReply;
import org.apache.ratis.metadata.protocol.GroupConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.RocksIterator;

/**
 * A57: every maintenance pass is bounded per entry - the result collection,
 * the floor advance and the physical change collection each move at most
 * their budget per entry, consecutive entries drain any backlog, and
 * change-byte capacity pressure evicts whole old revisions without ever
 * blocking writes or the lease expiry for good.
 */
class TestBoundedMaintenance {
  private static final byte[] V = "v".getBytes(StandardCharsets.UTF_8);

  @TempDir
  Path tempDir;

  private MetadataContractFixture fixture;
  private long index = 1;
  private long now = 1_000;

  private long nextIndex() {
    return ++index;
  }

  private void register(int seed, Lane lane) {
    assertEquals(Admission.ACCEPTED, fixture.admin(nextIndex(), now,
        EngineRequests.register(fixture.groupId(), fixture.store().groupConfig(),
            EngineRequests.writerId(seed), lane, 60_000)).getAdmission());
  }

  private WriteReply accept(int seed, long generation, long sequence,
      Operation operation) {
    return accept(seed, generation, sequence, operation, 60_000);
  }

  private WriteReply accept(int seed, long generation, long sequence,
      Operation operation, long budget) {
    return fixture.apply(nextIndex(), now,
        EngineRequests.write(fixture.groupId(), fixture.store().groupConfig(),
            EngineRequests.operationId(EngineRequests.writerId(seed), generation, sequence),
            operation, budget));
  }

  private static BusinessResult business(WriteReply reply) {
    try {
      return BusinessResult.parseFrom(reply.getOriginalBusinessResult());
    } catch (Exception e) {
      throw new AssertionError("the reply carries no parsable business result", e);
    }
  }

  private static int rowCount(RocksMetadataStore store, MetadataCf cf) {
    try (ReadView view = store.openReadView()) {
      final RocksIterator it = view.iterator(cf);
      try {
        int rows = 0;
        for (it.seekToFirst(); it.isValid(); it.next()) {
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

  // ---- the result collection moves at most its record budget per entry ----

  @Test
  void resultCollectionIsBoundedPerEntry() {
    fixture = MetadataContractFixture.create(Role.COORDINATION,
        GroupConfig.defaultsFor(Role.COORDINATION), tempDir.resolve("results"));
    fixture.initialize(1, 1);
    register(9, Lane.BUSINESS); // the writer row the collection reads the lane from

    final byte[] digest = new byte[StoredResult.DIGEST_BYTES];
    try (PreparedTransition seeding = fixture.newUncommittedTransition(1, nextIndex(), 900)) {
      for (int seq = 1; seq <= 250; seq++) {
        digest[0] = (byte) seq;
        fixture.stageStoredResult(seeding, EngineRequests.writerId(9), 1, seq,
            StoredResult.newBuilder()
                .setDigest(digest)
                .setFirstTerm(1)
                .setFirstIndex(3)
                .setExecutedAt(800)
                .setExpiresAt(1_000)
                .setReply(new byte[8])
                .build());
      }
      fixture.store().commit(seeding);
    }
    assertEquals(250, rowCount(fixture.store(), MetadataCf.RESULT_EXPIRY));

    // three consecutive entries drain the backlog, one hundred at a time
    fixture.maintenance(nextIndex(), 5_000, null);
    assertEquals(150, rowCount(fixture.store(), MetadataCf.RESULT_EXPIRY));
    fixture.maintenance(nextIndex(), 5_000, null);
    assertEquals(50, rowCount(fixture.store(), MetadataCf.RESULT_EXPIRY));
    fixture.maintenance(nextIndex(), 5_000, null);
    assertEquals(0, rowCount(fixture.store(), MetadataCf.RESULT_EXPIRY));
    assertEquals(0L, fixture.store().stats().businessResultBytes(),
        "every seeded charge came back out");
  }

  // ---- the floor and the physical collection advance in bounded batches ----

  @Test
  void floorAndPhysicalCollectionAdvanceInBoundedBatches() {
    fixture = MetadataContractFixture.create(Role.COORDINATION,
        GroupConfig.valueOf(GroupConfig.defaultsFor(Role.COORDINATION).toBuilder()
            .setChangeRetentionRevisions(10).build()), tempDir.resolve("floor"));
    fixture.initialize(1, 1);
    register(1, Lane.BUSINESS);

    accept(1, 1, 1, EngineRequests.create("/w", V));
    for (int i = 0; i < 250; i++) {
      final WriteReply put = accept(1, 1, i + 2,
          EngineRequests.put("/w/" + i, V));
      assertEquals(Admission.ACCEPTED, put.getAdmission(), "put " + i);
    }
    final long floorBefore = fixture.store().changeFloor();
    final int rowsBefore = rowCount(fixture.store(), MetadataCf.CHANGES);

    // a jump far ahead of the retention window leaves a bounded backlog
    long floor = floorBefore;
    int rows = rowsBefore;
    for (int round = 0; round < 12; round++) {
      final long entry = nextIndex();
      fixture.maintenance(entry, now, null);
      final long floorNow = fixture.store().changeFloor();
      final int rowsNow = rowCount(fixture.store(), MetadataCf.CHANGES);
      assertTrue(floorNow - floor <= 100, "floor jumped " + (floorNow - floor));
      assertTrue(rows - rowsNow <= 100, "collected " + (rows - rowsNow) + " rows");
      floor = floorNow;
      rows = rowsNow;
      if (rows == 0) {
        break;
      }
    }
    assertEquals(0, rows, "the physical backlog drained");
    assertEquals(0L, fixture.store().stats().visibleChangeBytes(),
        "the visible bytes left with the revisions");

    // a watcher far behind the floor must take a snapshot, not scan
    final QueryReply behind = fixture.query(EngineRequests.pollChanges(
        fixture.groupId(), fixture.store().groupConfig(), "/", 1,
        Integer.MAX_VALUE, 1024));
    assertEquals(Status.S_NEED_SNAPSHOT, behind.getStatus());
    assertTrue(fixture.store().changeFloor() > 2,
        "the floor moved far past revision one");
  }

  // ---- capacity pressure evicts old revisions but blocks nothing for good ----

  @Test
  void capacityEvictionKeepsWritesAliveAndNeverBlocksExpiry() {
    fixture = MetadataContractFixture.create(Role.COORDINATION,
        GroupConfig.valueOf(GroupConfig.defaultsFor(Role.COORDINATION).toBuilder()
            .setChangeCapacityBytes(4096).build()), tempDir.resolve("capacity"));
    fixture.initialize(1, 1);
    register(1, Lane.BUSINESS);
    register(2, Lane.CONTROL);

    final WriteReply opened = accept(2, 1, 1,
        EngineRequests.leaseOpen(EngineRequests.writerId(1), 1, 60_000));
    final long leaseId = business(opened).getLease().getLeaseId();
    for (int i = 0; i < 3; i++) {
      assertEquals(Status.S_OK, business(accept(1, 1, i + 1,
          EngineRequests.createEphemeral("/e/" + i, leaseId))).getStatus());
    }

    // far more change bytes than the cap allows: every write still lands
    final byte[] payload = new byte[200];
    accept(1, 1, 4, EngineRequests.create("/w", V));
    for (int i = 0; i < 60; i++) {
      assertEquals(Status.S_OK, business(accept(1, 1, 5 + i,
          EngineRequests.put("/w/" + i, payload))).getStatus(), "write " + i);
    }
    assertTrue(fixture.store().stats().visibleChangeBytes() <= 4096 + payload.length + 128,
        "eviction kept the visible bytes at the cap plus this entry's own events");
    assertEquals(Status.S_OK, get("/w/59").getStatus());
    assertEquals(Status.S_OK, get("/e/1").getStatus());

    // the history the eviction consumed is honestly gone
    final QueryReply behind = fixture.query(EngineRequests.pollChanges(
        fixture.groupId(), fixture.store().groupConfig(), "/", 0,
        Integer.MAX_VALUE, 1024));
    assertEquals(Status.S_NEED_SNAPSHOT, behind.getStatus());

    // and the expiry still proceeds under capacity pressure
    now += 61_000;
    fixture.maintenance(nextIndex(), now, LeaseCandidate.newBuilder()
        .setLeaseId(leaseId).setExpectedLastRenewIndex(0).build());
    assertEquals(0L, fixture.store().stats().leaseCount(), "the lease expired");
    assertEquals(Status.S_NOT_FOUND, get("/e/1").getStatus());
    assertTrue(fixture.store().stats().visibleChangeBytes() <= 4096 + 256,
        "the expiry batch also stayed at the cap plus its own events");
  }
}
