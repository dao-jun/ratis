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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import org.apache.ratis.metadata.proto.MetadataProtos.Admission;
import org.apache.ratis.metadata.proto.MetadataProtos.BusinessResult;
import org.apache.ratis.metadata.proto.MetadataProtos.Lane;
import org.apache.ratis.metadata.proto.MetadataProtos.MultiResult;
import org.apache.ratis.metadata.proto.MetadataProtos.Node;
import org.apache.ratis.metadata.proto.MetadataProtos.NodeKind;
import org.apache.ratis.metadata.proto.MetadataProtos.Operation;
import org.apache.ratis.metadata.proto.MetadataProtos.QueryReply;
import org.apache.ratis.metadata.proto.MetadataProtos.Role;
import org.apache.ratis.metadata.proto.MetadataProtos.Stamp;
import org.apache.ratis.metadata.proto.MetadataProtos.Status;
import org.apache.ratis.metadata.proto.MetadataProtos.WriteReply;
import org.apache.ratis.metadata.protocol.GroupConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.RocksIterator;

/**
 * The node-semantics trajectories of section 4: the permanent writer high
 * water, MULTI overlay atomicity, the single-entry lock token, CAS over
 * delete-recreate, sequential lifecycles and the recursive-delete budget.
 */
class TestNodeSemantics {
  /** The ordinal a fresh continuation carries: past the end of every revision. */
  private static final int END = Integer.MAX_VALUE;
  private static final byte[] V1 = "v1".getBytes(StandardCharsets.UTF_8);
  private static final byte[] V2 = "v2".getBytes(StandardCharsets.UTF_8);
  private static final byte[] V3 = "v3".getBytes(StandardCharsets.UTF_8);

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

  private WriteReply applyAt(long entry, int seed, long generation, long sequence,
      Operation operation, long budget) {
    return fixture.apply(entry, now,
        EngineRequests.write(fixture.groupId(), fixtureConfig(),
            EngineRequests.operationId(EngineRequests.writerId(seed), generation, sequence),
            operation, budget));
  }

  private WriteReply accept(int seed, long generation, long sequence,
      Operation operation, long budget) {
    return applyAt(nextIndex(), seed, generation, sequence, operation, budget);
  }

  private void register(int seed, Lane lane) {
    assertEquals(Admission.ACCEPTED, fixture.admin(nextIndex(), now,
        EngineRequests.register(fixture.groupId(), fixtureConfig(),
            EngineRequests.writerId(seed), lane, 60_000)).getAdmission());
  }

  private GroupConfig fixtureConfig() {
    return fixture.store().groupConfig();
  }

  // ---- observation helpers over the query path ----

  private QueryReply get(String path) {
    return fixture.query(EngineRequests.get(fixture.groupId(), fixtureConfig(), path));
  }

  private Node getPresent(String path) {
    final QueryReply reply = get(path);
    assertEquals(Status.S_OK, reply.getStatus(), () -> "path=" + path + " reply=" + reply);
    return reply.getNode();
  }

  private void assertMissing(String path) {
    assertEquals(Status.S_NOT_FOUND, get(path).getStatus(), () -> "path=" + path);
  }

  /** Pages the whole tree listing under one prefix, following continuations. */
  private int scanTreeCount(String prefix) {
    int total = 0;
    byte[] continuation = null;
    while (true) {
      final QueryReply page = fixture.query(EngineRequests.scan(
          fixture.groupId(), fixtureConfig(), prefix, 1024, continuation));
      assertEquals(Status.S_OK, page.getStatus());
      total += page.getScan().getNodesCount();
      if (!page.getScan().getHasMore()) {
        return total;
      }
      continuation = page.getScan().getContinuation().toByteArray();
    }
  }

  /** The physical sequential-counter rows: the observable old-counter cleanup. */
  private int counterRows() {
    try (ReadView view = fixture.store().openReadView()) {
      final RocksIterator it = view.iterator(MetadataCf.COUNTERS);
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

  private static BusinessResult business(WriteReply reply) {
    try {
      return BusinessResult.parseFrom(reply.getOriginalBusinessResult());
    } catch (Exception e) {
      throw new AssertionError("the reply carries no parsable business result", e);
    }
  }

  private static void assertAccepted(WriteReply reply) {
    assertEquals(Admission.ACCEPTED, reply.getAdmission(), () -> "reply=" + reply);
    assertTrue(!reply.getOriginalBusinessResult().isEmpty(),
        "an accepted reply carries the business result");
  }

  private static void assertStatus(WriteReply reply, Status status) {
    assertAccepted(reply);
    assertEquals(status, business(reply).getStatus(), () -> "reply=" + reply);
  }

  // ---- A13: the writer high water is permanent ----

  @Test
  void writerHighWaterSurvivesResultGcAndRestart() {
    register(1, Lane.BUSINESS);
    for (long seq = 1; seq <= 12; seq++) {
      assertAccepted(applyAt(nextIndex(), 1, 1, seq,
          EngineRequests.increment("perm", 1), 100));
    }

    advanceTime(1_000); // far beyond every 100ms budget issued above
    fixture.maintenance(nextIndex(), now, null); // collects all twelve results
    fixture.reopenStore(); // and a restart changes nothing

    final WriteReply late = accept(1, 1, 1, EngineRequests.increment("perm", 1), 100);
    assertEquals(Admission.REJECTED, late.getAdmission());
    assertEquals(Status.S_RESULT_EXPIRED, late.getAdmissionStatus());
    assertEquals(13L, late.getExpectedNextSequence(), "the ladder sees high water 12");

    final QueryReply row = fixture.query(EngineRequests.readWriter(
        fixture.groupId(), fixtureConfig(), EngineRequests.writerId(1)));
    assertEquals(Status.S_OK, row.getStatus());
    assertEquals(1L, row.getWriter().getGeneration());
    assertEquals(12L, row.getWriter().getHighWater());
    assertEquals(1L, fixture.store().stats().writerCount(), "no writer-row GC path exists");

    // the twelve increments executed exactly once across the restart
    final WriteReply next = accept(1, 1, 13, EngineRequests.increment("perm", 1), 60_000);
    assertAccepted(next);
    assertEquals(13L, business(next).getMutation().getCounterValue());
  }

  private void advanceTime(long delta) {
    now += delta;
  }

  // ---- A14: a failed MULTI leaves nothing behind but its terminal reply ----

  @Test
  void failedMultiLeavesNoResidue() {
    register(1, Lane.BUSINESS);

    // the CAS sub-operation expects the stamp the same batch just created:
    // create /p takes ordinal 1, create /p/c ordinal 2
    final long entry = nextIndex();
    final WriteReply failed = applyAt(entry, 1, 1, 1, EngineRequests.multi(
        EngineRequests.create("/p", V1),
        EngineRequests.create("/p/c", V1),
        EngineRequests.cas("/p/c", V2, entry, 2),
        EngineRequests.create("/p", V1)), 60_000); // the deliberate failure
    assertStatus(failed, Status.S_MULTI_FAILED);
    final MultiResult partial = business(failed).getMulti();
    assertEquals(3, partial.getFailIndex());
    assertEquals(Status.S_NODE_EXISTS, partial.getFailStatus());
    assertEquals(3, partial.getResultsCount(), "three sub-results precede the failure");
    assertEquals(2L, failed.getExpectedNextSequence(), "the failure consumed seq1");

    assertMissing("/p");
    assertMissing("/p/c");
    final QueryReply feed = fixture.query(EngineRequests.pollChanges(
        fixture.groupId(), fixtureConfig(), "/", 0, END, 1024));
    assertEquals(Status.S_OK, feed.getStatus());
    assertEquals(0, feed.getChanges().getEventsCount(), "a discarded batch stages no events");

    // a second batch may build on itself: create then CAS with the batch's own
    // fresh stamp - the first stamped mutation of the entry, ordinal 1
    final long builtEntry = nextIndex();
    final WriteReply built = applyAt(builtEntry, 1, 1, 2, EngineRequests.multi(
        EngineRequests.create("/p", V1),
        EngineRequests.cas("/p", V2, builtEntry, 1)), 60_000);
    assertStatus(built, Status.S_OK);
    final Stamp firstStamp = business(built).getMulti().getResults(0)
        .getMutation().getVersion();
    assertArrayEquals(V2, getPresent("/p").getValue().toByteArray());

    // delete/create/CAS-old inside one batch: the recreate carries a different
    // stamp, the old one fails, and the whole batch - delete included - dies
    final WriteReply aba = accept(1, 1, 3, EngineRequests.multi(
        EngineRequests.delete("/p"),
        EngineRequests.create("/p", V1),
        EngineRequests.cas("/p", V3, firstStamp.getLogIndex(), firstStamp.getOrdinal())),
        60_000);
    assertStatus(aba, Status.S_MULTI_FAILED);
    assertEquals(2, business(aba).getMulti().getFailIndex());
    assertEquals(Status.S_VERSION_MISMATCH, business(aba).getMulti().getFailStatus());
    final Stamp recreated = business(aba).getMulti().getResults(1)
        .getMutation().getVersion();
    assertNotEquals(firstStamp, recreated, "two creates never share a stamp");
    assertArrayEquals(firstStamp.toByteArray(),
        getPresent("/p").getCreateStamp().toByteArray(),
        "the discarded delete leaves the original node in place");
  }

  // ---- A15: parents, lock and token in one entry; no re-execution on replay ----

  @Test
  void singleEntryLockCarriesTokenAndSurvivesReplay() {
    register(1, Lane.BUSINESS);
    register(2, Lane.CONTROL);

    final long leaseEntry = nextIndex();
    final WriteReply opened = applyAt(leaseEntry, 2, 1, 1,
        EngineRequests.leaseOpen(EngineRequests.writerId(1), 1, 30_000), 60_000);
    assertStatus(opened, Status.S_OK);
    final long leaseId = business(opened).getLease().getLeaseId();

    final long lockEntry = nextIndex();
    final WriteReply locked = applyAt(lockEntry, 1, 1, 1,
        EngineRequests.createLock("/d/l", leaseId), 60_000);
    assertStatus(locked, Status.S_OK);
    final Stamp lockVersion = business(locked).getMutation().getVersion();
    assertArrayEquals(fixture.groupId().toByteString().toByteArray(),
        business(locked).getMutation().getLockToken().getGroupId().toByteArray());
    assertEquals(lockEntry, business(locked).getMutation().getLockToken().getLogIndex());
    assertEquals(NodeKind.EPHEMERAL_LOCK, getPresent("/d/l").getKind());
    assertEquals(NodeKind.NORMAL, getPresent("/d").getKind());

    // the identity replays the original token; nothing bumps, nothing recreates
    final WriteReply replay = applyAt(nextIndex(), 1, 1, 1,
        EngineRequests.createLock("/d/l", leaseId), 60_000);
    assertEquals(Admission.REPLAY, replay.getAdmission());
    assertArrayEquals(locked.getOriginalBusinessResult().toByteArray(),
        replay.getOriginalBusinessResult().toByteArray());
    assertEquals(lockVersion, getPresent("/d/l").getModifyStamp());

    // a plain PUT may not reshape a lock; a lock may not ride inside MULTI -
    // the protocol refuses to freeze that envelope at all, so the engine's
    // sub-operation guard only ever sees raw injection
    assertStatus(accept(1, 1, 2, EngineRequests.put("/d/l", V1), 60_000),
        Status.S_NOT_ALLOWED);
    assertThrows(org.apache.ratis.metadata.protocol.MetadataProtocolException.class,
        () -> EngineRequests.write(fixture.groupId(), fixtureConfig(),
            EngineRequests.operationId(EngineRequests.writerId(1), 1, 3),
            EngineRequests.multi(EngineRequests.createLock("/d/x", leaseId)), 60_000),
        "a lock CREATE must be submitted alone, not inside MULTI");
    assertMissing("/d/x");
    assertEquals(NodeKind.EPHEMERAL_LOCK, getPresent("/d/l").getKind());
  }

  // ---- A17: stamps carry lifecycle identity, not a bare counter ----

  @Test
  void casAcrossDeleteRecreateFailsOnBothSides() {
    register(1, Lane.BUSINESS);

    final WriteReply created = accept(1, 1, 1, EngineRequests.put("/p", V1), 60_000);
    assertStatus(created, Status.S_OK);
    final Stamp old = business(created).getMutation().getVersion();

    assertStatus(accept(1, 1, 2, EngineRequests.delete("/p"), 60_000), Status.S_OK);
    final WriteReply recreated = accept(1, 1, 3, EngineRequests.create("/p", V2), 60_000);
    assertStatus(recreated, Status.S_OK);
    final Stamp fresh = business(recreated).getMutation().getVersion();
    assertNotEquals(old, fresh, "a recreate is a new lifecycle, never the bare counter again");

    assertStatus(accept(1, 1, 4,
        EngineRequests.cas("/p", V3, old.getLogIndex(), old.getOrdinal()), 60_000),
        Status.S_VERSION_MISMATCH);

    // the same trick inside one batch: the delete dies with the failed CAS
    final WriteReply batch = accept(1, 1, 5, EngineRequests.multi(
        EngineRequests.delete("/p"),
        EngineRequests.create("/p", V1),
        EngineRequests.cas("/p", V3, old.getLogIndex(), old.getOrdinal())), 60_000);
    assertStatus(batch, Status.S_MULTI_FAILED);
    assertEquals(Status.S_VERSION_MISMATCH, business(batch).getMulti().getFailStatus());
    assertArrayEquals(fresh.toByteArray(), getPresent("/p").getCreateStamp().toByteArray());
    assertArrayEquals(V2, getPresent("/p").getValue().toByteArray());
  }

  // ---- A18: one sequence per parent lifecycle ----

  @Test
  void sequentialCounterIsSharedPerParentLifecycle() {
    register(1, Lane.BUSINESS);
    assertStatus(accept(1, 1, 1, EngineRequests.create("/p", V1), 60_000), Status.S_OK);

    final WriteReply a1 = accept(1, 1, 2,
        EngineRequests.createSequential("/p/a", V1, false), 60_000);
    final WriteReply b2 = accept(1, 1, 3,
        EngineRequests.createSequential("/p/b", V1, false), 60_000);
    final WriteReply a3 = accept(1, 1, 4,
        EngineRequests.createSequential("/p/a", V1, false), 60_000);
    assertEquals("/p/a~00000000000000000001",
        business(a1).getMutation().getKey().toStringUtf8());
    assertEquals("/p/b~00000000000000000002",
        business(b2).getMutation().getKey().toStringUtf8());
    assertEquals("/p/a~00000000000000000003",
        business(a3).getMutation().getKey().toStringUtf8());

    // the reserved suffix cannot even be frozen into an envelope: explicit
    // creates and writes of that namespace are unrepresentable, so the
    // engine's BAD_PATH guard is defense against raw injection only
    assertThrows(org.apache.ratis.metadata.protocol.MetadataProtocolException.class,
        () -> EngineRequests.write(fixture.groupId(), fixtureConfig(),
            EngineRequests.operationId(EngineRequests.writerId(1), 1, 5),
            EngineRequests.create("/p/x~00000000000000000001", V1), 60_000),
        "the leaf name is reserved for sequential creates");
    assertThrows(org.apache.ratis.metadata.protocol.MetadataProtocolException.class,
        () -> EngineRequests.write(fixture.groupId(), fixtureConfig(),
            EngineRequests.operationId(EngineRequests.writerId(1), 1, 6),
            EngineRequests.put("/p/y~00000000000000000099", V1), 60_000));

    // a new parent lifecycle restarts the sequence under a different stamp
    final WriteReply removed = accept(1, 1, 5,
        EngineRequests.deleteRecursive("/p"), 60_000);
    assertStatus(removed, Status.S_OK);
    assertEquals(4L, business(removed).getMutation().getDeletedCount());
    assertEquals(0, counterRows(), "the parent's counter dies with the parent");

    assertStatus(accept(1, 1, 6, EngineRequests.create("/p", V2), 60_000), Status.S_OK);
    final WriteReply restarted = accept(1, 1, 7,
        EngineRequests.createSequential("/p/a", V1, false), 60_000);
    assertEquals("/p/a~00000000000000000001",
        business(restarted).getMutation().getKey().toStringUtf8());
    assertNotEquals(business(a1).getMutation().getVersion(),
        business(restarted).getMutation().getVersion(), "a restarted counter is a new stamp");
    assertMissing("/p/b~00000000000000000002");
    assertEquals(1, counterRows(), "only the new lifecycle's counter exists");
  }

  // ---- A19: the recursive-delete budget is all-or-nothing ----

  @Test
  void recursiveDeleteHonorsTheWholeWorkLimit() {
    register(1, Lane.BUSINESS);

    // 1024 nodes including the root: exactly the budget
    assertStatus(accept(1, 1, 1,
        EngineRequests.multi(subtreeOps("/t", 1023)), 60_000), Status.S_OK);
    assertEquals(1024, scanTreeCount("/t"));
    final WriteReply exact = accept(1, 1, 2,
        EngineRequests.deleteRecursive("/t"), 60_000);
    assertStatus(exact, Status.S_OK);
    assertEquals(1024L, business(exact).getMutation().getDeletedCount());
    assertMissing("/t");
    assertEquals(0, scanTreeCount("/t"));

    // one node over: the whole operation fails, nothing is deleted
    assertStatus(accept(1, 1, 3,
        EngineRequests.multi(subtreeOps("/u", 1023)), 60_000), Status.S_OK);
    assertStatus(accept(1, 1, 4,
        EngineRequests.create("/u/c1023", V1), 60_000), Status.S_OK);
    assertEquals(1025, scanTreeCount("/u"));
    assertStatus(accept(1, 1, 5,
        EngineRequests.deleteRecursive("/u"), 60_000), Status.S_LIMIT_EXCEEDED);
    assertEquals(1025, scanTreeCount("/u"), "no partial deletion happened");
    assertMissing("/t"); // the earlier subtree stays gone too

    // within one batch later writes shadow earlier ones, in order
    assertStatus(accept(1, 1, 6, EngineRequests.multi(
        EngineRequests.create("/m", V1),
        EngineRequests.put("/m", V2)), 60_000), Status.S_OK);
    assertArrayEquals(V2, getPresent("/m").getValue().toByteArray());
    assertStatus(accept(1, 1, 7, EngineRequests.multi(
        EngineRequests.create("/m2", V1),
        EngineRequests.delete("/m2")), 60_000), Status.S_OK);
    assertMissing("/m2");
  }

  @Test
  void recursiveDeleteCoversNodesCreatedEarlierInTheSameEntry() {
    register(1, Lane.BUSINESS);

    assertAccepted(accept(1, 1, 1, EngineRequests.create("/x", V1, true), 60_000));
    assertAccepted(accept(1, 1, 2, EngineRequests.create("/x/m", V1), 60_000));
    assertAccepted(accept(1, 1, 3, EngineRequests.create("/x/z", V1), 60_000));

    // two staged children, one sorting before and one between the committed
    // rows: the merged scan must still walk every one of them
    final WriteReply multi = accept(1, 1, 4, EngineRequests.multi(
        EngineRequests.put("/x/a", V1),
        EngineRequests.put("/x/n", V1),
        EngineRequests.deleteRecursive("/x")), 60_000);
    assertStatus(multi, Status.S_OK);
    assertEquals(5L, business(multi).getMulti().getResults(2).getMutation()
        .getDeletedCount(), "the root plus two committed and two staged children");
    assertMissing("/x");
    assertMissing("/x/m");
    assertMissing("/x/z");
    assertMissing("/x/a");
    assertMissing("/x/n");
    assertEquals(0, scanTreeCount("/x"));
  }

  /** A CREATE for the root plus {@code children} leaves, one operation each. */
  private static Operation[] subtreeOps(String root, int children) {
    final Operation[] ops = new Operation[children + 1];
    ops[0] = EngineRequests.create(root, V1);
    for (int i = 0; i < children; i++) {
      ops[i + 1] = EngineRequests.create(root + "/c" + i, V1);
    }
    return ops;
  }
}
