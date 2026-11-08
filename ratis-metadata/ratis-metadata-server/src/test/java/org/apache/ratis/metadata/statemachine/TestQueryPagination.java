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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.ratis.metadata.proto.MetadataProtos.Admission;
import org.apache.ratis.metadata.proto.MetadataProtos.ChangeCursor;
import org.apache.ratis.metadata.proto.MetadataProtos.ChangeEvent;
import org.apache.ratis.metadata.proto.MetadataProtos.ChangeKind;
import org.apache.ratis.metadata.proto.MetadataProtos.Lane;
import org.apache.ratis.metadata.proto.MetadataProtos.Operation;
import org.apache.ratis.metadata.proto.MetadataProtos.QueryReply;
import org.apache.ratis.metadata.proto.MetadataProtos.Role;
import org.apache.ratis.metadata.proto.MetadataProtos.ScanPage;
import org.apache.ratis.metadata.proto.MetadataProtos.Status;
import org.apache.ratis.metadata.proto.MetadataProtos.WriteReply;
import org.apache.ratis.metadata.protocol.GroupConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A20 and A21: the cursor contracts of the change feed and the scans. Pages
 * carry their revision+ordinal position without losing same-revision events,
 * empty pages still advance, nothing is ever skipped past an unemitted row,
 * and scan continuations bind to the exact query that issued them.
 */
class TestQueryPagination {
  private static final byte[] V = "v".getBytes(StandardCharsets.UTF_8);
  private static final int END = Integer.MAX_VALUE;

  @TempDir
  Path tempDir;

  private MetadataContractFixture fixture;
  private long index = 1;
  private long now = 1_000;

  @BeforeEach
  void setUp() {
    fixture = MetadataContractFixture.create(Role.COORDINATION,
        GroupConfig.defaultsFor(Role.COORDINATION), tempDir.resolve("store"));
    fixture.initialize(1, 1);
  }

  private long nextIndex() {
    return ++index;
  }

  private WriteReply accept(long sequence, Operation operation) {
    final WriteReply reply = fixture.apply(nextIndex(), now,
        EngineRequests.write(fixture.groupId(), fixture.store().groupConfig(),
            EngineRequests.operationId(EngineRequests.writerId(1), 1, sequence),
            operation, 60_000));
    assertEquals(Admission.ACCEPTED, reply.getAdmission(), () -> "reply=" + reply);
    return reply;
  }

  private QueryReply poll(String prefix, long revision, int ordinal, int maxEntries) {
    return fixture.query(EngineRequests.pollChanges(fixture.groupId(),
        fixture.store().groupConfig(), prefix, revision, ordinal, maxEntries));
  }

  // ---- A20: one revision, several events, no same-revision loss ----

  @Test
  void changePagesSplitRevisionsWithoutLoss() {
    assertEquals(Admission.ACCEPTED, fixture.admin(nextIndex(), now,
        EngineRequests.register(fixture.groupId(), fixture.store().groupConfig(),
            EngineRequests.writerId(1), Lane.BUSINESS, 60_000)).getAdmission());

    // one entry, three events - the page boundary falls inside the revision
    accept(1L, EngineRequests.create("/m", V));
    final long afterParent = index;
    final long revision = index + 1;
    accept(2L, EngineRequests.multi(
        EngineRequests.create("/m/a", V),
        EngineRequests.create("/m/b", V),
        EngineRequests.create("/m/c", V)));

    final QueryReply page1 = poll("/m", afterParent, END, 2);
    assertEquals(Status.S_OK, page1.getStatus());
    assertEquals(2, page1.getChanges().getEventsCount());
    assertTrue(page1.getChanges().getHasMore());
    final ChangeCursor mid = page1.getChanges().getNext();
    assertEquals(revision, mid.getRevision(), "the cursor stays inside the revision");
    assertEquals(2, mid.getOrdinal(), "the third event is still ahead of the cursor");

    final QueryReply page2 = poll("/m", mid.getRevision(), mid.getOrdinal(), 2);
    assertEquals(1, page2.getChanges().getEventsCount());
    assertFalse(page2.getChanges().getHasMore());
    assertEquals("/m/c",
        page2.getChanges().getEvents(0).getPath().toStringUtf8());
    assertEquals(revision, page2.getChanges().getNext().getRevision());
    assertEquals(END, page2.getChanges().getNext().getOrdinal());
  }

  // ---- A20: an empty page still advances and never claims the end ----

  @Test
  void emptyPagesAdvancePastNonMatchingEvents() {
    assertEquals(Admission.ACCEPTED, fixture.admin(nextIndex(), now,
        EngineRequests.register(fixture.groupId(), fixture.store().groupConfig(),
            EngineRequests.writerId(1), Lane.BUSINESS, 60_000)).getAdmission());

    // 8193 events under /x: a page of ten may examine all of them, match none
    accept(1L, EngineRequests.create("/w", V));
    final long afterParent = index;
    for (int batch = 0; batch < 64; batch++) {
      accept(batch + 2L, EngineRequests.multi(xOps("b" + batch + "-", 128)));
    }
    // the one matching event lands in the very last revision
    accept(66L, EngineRequests.create("/w/keep", V));
    final long matchRevision = index;

    final QueryReply page1 = poll("/w", afterParent, END, 10);
    assertEquals(Status.S_OK, page1.getStatus());
    assertEquals(0, page1.getChanges().getEventsCount(), "nothing matches yet");
    assertTrue(page1.getChanges().getHasMore(), "an empty page is not the end");
    final ChangeCursor advanced = page1.getChanges().getNext();
    assertTrue(advanced.getRevision() < matchRevision,
        "the last-examined cursor advanced through the noise");

    final QueryReply page2 = poll("/w", advanced.getRevision(), advanced.getOrdinal(), 10);
    assertEquals(1, page2.getChanges().getEventsCount());
    assertEquals(ChangeKind.NODE_CREATE, page2.getChanges().getEvents(0).getKind());
    assertFalse(page2.getChanges().getHasMore());
  }

  private static Operation[] xOps(String tag, int count) {
    final List<Operation> ops = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      // the very first child also brings the /x parent into being
      ops.add(EngineRequests.create("/x/" + tag + i, V, i == 0));
    }
    return ops.toArray(new Operation[0]);
  }

  // ---- A20: a strict-ancestor delete notifies the watched prefix ----

  @Test
  void subtreeDeleteMatchesStrictAncestorPrefixes() {
    assertEquals(Admission.ACCEPTED, fixture.admin(nextIndex(), now,
        EngineRequests.register(fixture.groupId(), fixture.store().groupConfig(),
            EngineRequests.writerId(1), Lane.BUSINESS, 60_000)).getAdmission());

    accept(1L, EngineRequests.create("/p", V));
    accept(2L, EngineRequests.create("/p/c", V));
    accept(3L, EngineRequests.create("/p/c/d", V));
    accept(4L, EngineRequests.deleteRecursive("/p"));

    final QueryReply watched = poll("/p/c", 0, END, 1024);
    assertEquals(Status.S_OK, watched.getStatus());
    // the /p/c and /p/c/d creates reach the watcher, and the ancestor's
    // one delete event covers the whole watched prefix
    assertEquals(3, watched.getChanges().getEventsCount());
    final ChangeEvent last = watched.getChanges().getEvents(2);
    assertEquals(ChangeKind.DELETE_SUBTREE, last.getKind());
    assertEquals("/p", last.getPath().toStringUtf8(),
        "the strict ancestor's delete covers the watched prefix");

    final QueryReply elsewhere = poll("/q", 0, END, 1024);
    assertEquals(0, elsewhere.getChanges().getEventsCount());
    assertFalse(elsewhere.getChanges().getHasMore());
  }

  // ---- A21: per-page consistency and cursor identity ----

  @Test
  void scanContinuationsBindToTheirQueryAndPage() {
    assertEquals(Admission.ACCEPTED, fixture.admin(nextIndex(), now,
        EngineRequests.register(fixture.groupId(), fixture.store().groupConfig(),
            EngineRequests.writerId(1), Lane.BUSINESS, 60_000)).getAdmission());
    accept(1L, EngineRequests.create("/a", V));
    for (int i = 1; i <= 5; i++) {
      accept((long) (i + 1), EngineRequests.create("/a/" + i, V));
    }

    // page one stops after two rows; inserts before and behind follow
    final QueryReply page1 = fixture.query(EngineRequests.scan(
        fixture.groupId(), fixture.store().groupConfig(), "/a", 2, null));
    assertEquals(Status.S_OK, page1.getStatus());
    assertEquals(2, page1.getScan().getNodesCount());
    assertFalse(page1.getScan().getValuesIncluded(), "a tree listing carries no values");
    final byte[] continuation = page1.getScan().getContinuation().toByteArray();

    accept(7L, EngineRequests.create("/a/0", V)); // sorts before the cursor
    accept(8L, EngineRequests.create("/a/9", V)); // sorts after the cursor
    accept(9L, EngineRequests.delete("/a/4")); // a row the next page missed

    final QueryReply page2 = fixture.query(EngineRequests.scan(
        fixture.groupId(), fixture.store().groupConfig(), "/a", 2, continuation));
    assertEquals(Status.S_OK, page2.getStatus());
    final ScanPage scan = page2.getScan();
    assertEquals(2, scan.getNodesCount());
    assertEquals("/a/2", scan.getNodes(0).getKey().toStringUtf8());
    assertEquals("/a/3", scan.getNodes(1).getKey().toStringUtf8(),
        "the deleted row vanished, the pre-cursor insert never appears");

    // the same continuation under another query shape is not a cursor
    final QueryReply wrongQuery = fixture.query(EngineRequests.scan(
        fixture.groupId(), fixture.store().groupConfig(), "/b", 2, continuation));
    assertEquals(Status.S_BAD_CURSOR, wrongQuery.getStatus());

    // a page that cannot hold even one record is refused, never overfilled
    final QueryReply tooSmall = fixture.query(EngineRequests.scan(
        fixture.groupId(), fixture.store().groupConfig(), "/a", 1024, 1, null));
    assertEquals(Status.S_PAGE_TOO_SMALL, tooSmall.getStatus());
  }

  // ---- A21: prefixes, ranges and the unsigned order ----

  @Test
  void treePrefixesFollowTheUnsignedOrder() {
    assertEquals(Admission.ACCEPTED, fixture.admin(nextIndex(), now,
        EngineRequests.register(fixture.groupId(), fixture.store().groupConfig(),
            EngineRequests.writerId(1), Lane.BUSINESS, 60_000)).getAdmission());
    accept(1L, EngineRequests.create("/a", V));
    accept(2L, EngineRequests.create("/a/1", V));
    accept(3L, EngineRequests.create("/ab/1", V));

    // /a lists its subtree and never /ab, the prefix itself included when real
    final QueryReply treeA = fixture.query(EngineRequests.scan(
        fixture.groupId(), fixture.store().groupConfig(), "/a", 0, null));
    assertEquals(Status.S_OK, treeA.getStatus());
    assertEquals(2, treeA.getScan().getNodesCount());
    assertEquals("/a", treeA.getScan().getNodes(0).getKey().toStringUtf8());
    assertEquals("/a/1", treeA.getScan().getNodes(1).getKey().toStringUtf8());
    assertFalse(treeA.getScan().getHasMore(), "no fake end-of-file mid-prefix");
    assertFalse(treeA.getScan().getValuesIncluded(), "a tree listing carries no values");
  }

  @Test
  void keyRangesCarryValuesInUnsignedOrder() {
    // the keyspace model lives in its own role: raw byte keys, no tree
    try (MetadataContractFixture kv = MetadataContractFixture.create(Role.KEYSPACE,
        GroupConfig.defaultsFor(Role.KEYSPACE), tempDir.resolve("kv"))) {
      kv.initialize(1, 1);
      final GroupConfig config = kv.store().groupConfig();
      assertEquals(Admission.ACCEPTED, kv.admin(nextIndex(), now,
          EngineRequests.register(kv.groupId(), config,
              EngineRequests.writerId(1), Lane.BUSINESS, 60_000)).getAdmission());

      final byte[][] keys = {
          new byte[] {0x00}, new byte[] {0x01, (byte) 0xFF}, new byte[] {0x7F},
          new byte[] {(byte) 0x80}, new byte[] {(byte) 0xFF}
      };
      for (int i = 0; i < keys.length; i++) {
        assertEquals(Admission.ACCEPTED, kv.apply(nextIndex(), now,
            EngineRequests.write(kv.groupId(), config,
                EngineRequests.operationId(EngineRequests.writerId(1), 1, i + 1),
                EngineRequests.put(keys[i], keys[i]), 60_000)).getAdmission());
      }
      final QueryReply range = kv.query(EngineRequests.scan(kv.groupId(), config,
          new byte[] {0x00}, new byte[] {(byte) 0xFF, 0x00}, 1024));
      assertEquals(Status.S_OK, range.getStatus());
      assertEquals(5, range.getScan().getNodesCount());
      assertTrue(range.getScan().getValuesIncluded(), "a keyspace scan carries values");
      assertArrayEquals(new byte[] {(byte) 0x80},
          range.getScan().getNodes(3).getKey().toByteArray(),
          "unsigned order: 0x80 sorts between 0x7F and 0xFF");
      assertArrayEquals(new byte[] {(byte) 0x80},
          range.getScan().getNodes(3).getValue().toByteArray());
      assertFalse(range.getScan().getHasMore(), "no fake end-of-file mid-range");
    }
  }
}
