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
package org.apache.ratis.metadata.protocol;

import org.apache.ratis.metadata.proto.MetadataProtos;
import org.apache.ratis.metadata.proto.MetadataProtos.Cas;
import org.apache.ratis.metadata.proto.MetadataProtos.Create;
import org.apache.ratis.metadata.proto.MetadataProtos.ExpectedVersion;
import org.apache.ratis.metadata.proto.MetadataProtos.Increment;
import org.apache.ratis.metadata.proto.MetadataProtos.LeaseOpen;
import org.apache.ratis.metadata.proto.MetadataProtos.LeaseRenew;
import org.apache.ratis.metadata.proto.MetadataProtos.LeaseRevoke;
import org.apache.ratis.metadata.proto.MetadataProtos.LockOwner;
import org.apache.ratis.metadata.proto.MetadataProtos.Multi;
import org.apache.ratis.metadata.proto.MetadataProtos.NodeKind;
import org.apache.ratis.metadata.proto.MetadataProtos.Operation;
import org.apache.ratis.metadata.proto.MetadataProtos.Put;
import org.apache.ratis.metadata.proto.MetadataProtos.Role;
import org.apache.ratis.metadata.proto.MetadataProtos.WriterRef;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.UnknownFieldSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * T01 acceptance tests: canonical golden bytes and digests (A01), frozen
 * requests are immune to caller mutation (A02), structural validation and
 * boundaries (A03), configuration and budget coupling (A04), tree path and
 * binary key rules (A51). Golden constants are hand-reviewed wire hex from
 * docs/metadata-redesign/protocol-vectors.json; they are assertion inputs,
 * never derived from the code under test.
 */
public final class TestMetadataProtocolV1 {
  private static final String GROUP_ID_HEX = "000102030405060708090a0b0c0d0e0f";
  private static final String WRITER_ID_HEX = "101112131415161718191a1b1c1d1e1f";

  private static final String COORD_CONFIG_HEX =
      "08011001188088b2a0022080402880403080013880801040808040488008508008588010608040688080801070800278e807800180b899298801a08d069001a08d069801808010a0018080808007a8018080808001b00164b80180808002c001808004c8018080808010d00180808002d80164e00180808004e8018008f001808040f8018040800264";
  private static final String COORD_FINGERPRINT_HEX =
      "152a09e7b8d1cacf3fc9c3e51aa4ce6e15bf72f441a86dfba71dcd0741f279df";
  private static final String KV_CONFIG_HEX =
      "080110021880b899292080402880403080013880801040808040488008508008588010608040688080801070800278e807800180b899298801a08d069001a08d069801808010a0018080808007a8018080808001b00164b80180808002c001808004c8018080808010d00180808002d80164e00180808004e8018008f001808040f8018040800264";
  private static final String KV_FINGERPRINT_HEX =
      "c2f28d1b77f87aae59e8d465630f3ee7655306f3f3296403e3d6e74b7355338a";

  private static final RaftGroupId GROUP_ID =
      RaftGroupId.valueOf(ByteString.copyFrom(hex(GROUP_ID_HEX)));
  private static final long COORD_W_MS = 604_800_000L;  // 7d
  private static final long KV_W_MS = 86_400_000L;      // 24h

  @Test
  public void goldenConfigCanonicalBytesAndFingerprints() {
    final MetadataProtos.GroupConfig coord = GroupConfig.defaultsFor(Role.COORDINATION).toProto();
    Assertions.assertEquals(COORD_CONFIG_HEX, hex(coord.toByteArray()));
    Assertions.assertEquals(COORD_FINGERPRINT_HEX,
        hex(MetadataProtocol.fingerprint(GroupConfig.defaultsFor(Role.COORDINATION))));

    final MetadataProtos.GroupConfig kv = GroupConfig.defaultsFor(Role.KEYSPACE).toProto();
    Assertions.assertEquals(KV_CONFIG_HEX, hex(kv.toByteArray()));
    Assertions.assertEquals(KV_FINGERPRINT_HEX,
        hex(MetadataProtocol.fingerprint(GroupConfig.defaultsFor(Role.KEYSPACE))));
  }

  @Test
  public void goldenOperationCanonicalBytesAndDigests() {
    // PUT /x value 0102
    assertVector("coord", "52080a022f7812020102",
        "82303fe109b5cf51573fd9b98c508407db359871de398f3fb7caf6c33659a1ce",
        put("/x", new byte[]{1, 2}));
    // CREATE /p/c NORMAL createParents=true (unset kind canonically becomes NORMAL)
    assertVector("coord", "5a0a0a042f702f6320013001",
        "58d66571ef494b30569bcc9d952a7c844dc6611861f72768ef3a4fc2a597d73e",
        create("/p/c", true));
    // CAS /x value B exact Stamp(7,2)
    assertVector("coord", "620f0a022f781201421a06120408071002",
        "3cbbc9849de08ef6632f375df7d3300ffaa4b9c3d20889e83d8245e2f23f045a",
        cas("/x", "B"));
    // MULTI [CREATE /p/c, PUT /x]
    assertVector("coord", "7a1a0a0c5a0a0a042f702f63200130010a0a52080a022f7812020102",
        "e05492728ad78aa0dcd166c471a00a8b6ca134aaa931fb73ea094145759579bb",
        Operation.newBuilder().setMulti(Multi.newBuilder()
            .addOperations(create("/p/c", true))
            .addOperations(put("/x", new byte[]{1, 2}))).build());
    // LEASE_OPEN owner 1011..1f generation 1 ttl 30000, explicit and unset
    // (an omitted TTL canonically becomes the same 30s default)
    assertVector("coord", "82011a0a140a10101112131415161718191a1b1c1d1e1f100110b0ea01",
        "8cf56e9aafea67a7d83927855ac8836a70f6d59cc9ab90c73db5c33462588a13",
        leaseOpen(30_000));
    assertVector("coord", "82011a0a140a10101112131415161718191a1b1c1d1e1f100110b0ea01",
        "8cf56e9aafea67a7d83927855ac8836a70f6d59cc9ab90c73db5c33462588a13",
        leaseOpen(0));
    // INCREMENT counter +7
    assertVector("coord", "720b0a07636f756e7465721007",
        "53ec6134b135b87f76f5a2930f7c90a6e4e761496b9e71a24ec760790665d65b",
        Operation.newBuilder().setIncrement(Increment.newBuilder()
            .setName(bs("counter")).setDelta(7)).build());
    // KV PUT with a binary key whose bytes happen to be "/x"
    assertVector("kv", "52080a022f7812020102",
        "b3f68a4bd5cee098c367125c4d26f7cf4ec92b687c1284195fd0c300d831b5d4",
        put(new byte[]{'/', 'x'}, new byte[]{1, 2}));
    // KV CAS with the same binary key
    assertVector("kv", "620f0a022f781201421a06120408071002",
        "c8b07c3bfbd606ef53b1774bb5d6fe9984c6f8febc0262b741275fe8c898a30e",
        cas(new byte[]{'/', 'x'}, "B"));
  }

  @Test
  public void digestIgnoresRetryBudget() {
    final FrozenRequest first = freeze("coord", put("/x", new byte[]{1}));
    final FrozenRequest second = freeze("coord", put("/x", new byte[]{1}));
    // digest covers group, role, config fingerprint and canonical operation;
    // retry budget and Ratis call identity are not operation semantics
    final FrozenRequest otherBudget = MetadataProtocol.freeze(GROUP_ID,
        GroupConfig.defaultsFor(Role.COORDINATION), first.operationId(),
        put("/x", new byte[]{1}), 1_000L);
    Assertions.assertArrayEquals(first.digest(), second.digest());
    Assertions.assertArrayEquals(first.digest(), otherBudget.digest());
    Assertions.assertArrayEquals(first.canonicalOperationBytes(),
        otherBudget.canonicalOperationBytes());
    // a different operation under the same identity has a different digest
    final FrozenRequest otherValue = freeze("coord", put("/x", new byte[]{9}));
    Assertions.assertFalse(Arrays.equals(first.digest(), otherValue.digest()));
  }

  @Test
  public void frozenPayloadDoesNotTrackCallerArray() {
    byte[] value = new byte[]{1};
    FrozenRequest r = request(value);
    byte[] before = r.canonicalOperationBytes();
    value[0] = 2;
    Assertions.assertArrayEquals(before, r.canonicalOperationBytes());
  }

  @Test
  public void frozenPayloadDoesNotTrackCallerListOrBuilder() {
    final List<Operation> subs = new ArrayList<>();
    subs.add(put("/x", new byte[]{1}));
    final Operation multi = Operation.newBuilder().setMulti(Multi.newBuilder()
        .addAllOperations(subs)).build();
    final FrozenRequest frozen = freeze("coord", multi);
    final byte[] before = frozen.canonicalOperationBytes();

    subs.add(put("/y", new byte[]{2}));
    Assertions.assertArrayEquals(before, frozen.canonicalOperationBytes());

    // mutating a builder after build+freeze cannot change the frozen bytes
    final Operation.Builder builder = put("/x", new byte[]{1}).toBuilder();
    final GroupConfig coord = GroupConfig.defaultsFor(Role.COORDINATION);
    final FrozenRequest fromBuilder = MetadataProtocol.freeze(GROUP_ID, coord,
        frozen.operationId(), builder.build(), 1_000L);
    builder.getPutBuilder().setValue(bs("changed"));
    Assertions.assertArrayEquals(fromBuilder.canonicalOperationBytes(),
        MetadataProtocol.freeze(GROUP_ID, coord, frozen.operationId(),
            put("/x", new byte[]{1}), 1_000L).canonicalOperationBytes());
  }

  @Test
  public void returnedBytesAreDefensiveCopies() {
    final FrozenRequest r = request(new byte[]{1});
    final byte[] canonical = r.canonicalOperationBytes();
    canonical[0] = (byte) 0xff;
    Assertions.assertNotEquals(canonical[0], r.canonicalOperationBytes()[0]);
    final byte[] digest = r.digest();
    digest[0] = (byte) 0xff;
    Assertions.assertNotEquals(digest[0], r.digest()[0]);
  }

  @Test
  public void unknownFieldsAreRejected() {
    final Operation withUnknown = put("/x", new byte[]{1}).toBuilder()
        .setUnknownFields(unknownVarint(99, 42)).build();
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> freeze("coord", withUnknown));

    final MetadataProtos.GroupConfig badEnum =
        GroupConfig.defaultsFor(Role.COORDINATION).toBuilder().setRoleValue(99).build();
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> GroupConfig.valueOf(badEnum));

    final MetadataProtos.GroupConfig withUnknownField =
        GroupConfig.defaultsFor(Role.COORDINATION).toBuilder()
            .setUnknownFields(unknownVarint(100, 1)).build();
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> GroupConfig.valueOf(withUnknownField));
  }

  @Test
  public void missingOneofAndInvalidCombinationsAreRejected() {
    // Operation with no command set
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> freeze("coord", Operation.newBuilder().build()));
    // CAS without an expected version
    final Operation casWithoutExpected = Operation.newBuilder().setCas(
        Cas.newBuilder().setKey(bs("/x")).setValue(bs("B"))).build();
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> freeze("coord", casWithoutExpected));
    // ExpectedVersion with absent=false is invalid
    final Operation absentFalse = Operation.newBuilder().setCas(Cas.newBuilder()
        .setKey(bs("/x")).setValue(bs("B"))
        .setExpected(ExpectedVersion.newBuilder().setAbsent(false))).build();
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> freeze("coord", absentFalse));
    // ExpectedVersion with neither predicate set
    final Operation noPredicate = Operation.newBuilder().setCas(Cas.newBuilder()
        .setKey(bs("/x")).setValue(bs("B"))
        .setExpected(ExpectedVersion.newBuilder())).build();
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> freeze("coord", noPredicate));
  }

  @Test
  public void multiSubOperationsAreRestricted() {
    // nested MULTI
    final Operation nested = Operation.newBuilder().setMulti(Multi.newBuilder()
        .addOperations(Operation.newBuilder().setMulti(Multi.newBuilder()))).build();
    Assertions.assertThrows(MetadataProtocolException.class, () -> freeze("coord", nested));
    // lease operation inside MULTI
    final Operation withLease = Operation.newBuilder().setMulti(Multi.newBuilder()
        .addOperations(leaseOpen(30_000))).build();
    Assertions.assertThrows(MetadataProtocolException.class, () -> freeze("coord", withLease));
    // LOCK CREATE inside MULTI
    final Operation withLock = Operation.newBuilder().setMulti(Multi.newBuilder()
        .addOperations(Operation.newBuilder().setCreate(Create.newBuilder()
            .setPath(bs("/l")).setKindValue(2)))).build();
    Assertions.assertThrows(MetadataProtocolException.class, () -> freeze("coord", withLock));
    // sequential CREATE inside MULTI
    final Operation withSequential = Operation.newBuilder().setMulti(Multi.newBuilder()
        .addOperations(Operation.newBuilder().setCreate(Create.newBuilder()
            .setPath(bs("/s")).setSequential(true)))).build();
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> freeze("coord", withSequential));
  }

  @Test
  public void sizeBoundaries() {
    // largest legal tree path is 8192 UTF-8 bytes at depth 1; largest value 256 KiB
    Assertions.assertDoesNotThrow(
        () -> freeze("coord", put(new String(prefixedPath(8191), StandardCharsets.UTF_8),
            new byte[262144])));
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> freeze("coord", put(new String(prefixedPath(8192), StandardCharsets.UTF_8),
            new byte[1])));
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> freeze("coord", put("/x", new byte[262145])));
    // KV key boundaries: 1..8192 arbitrary bytes
    Assertions.assertDoesNotThrow(() -> freeze("kv", put(new byte[8192], new byte[1])));
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> freeze("kv", put(new byte[8193], new byte[1])));
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> freeze("kv", put(new byte[0], new byte[1])));
  }

  @Test
  public void multiOpBoundaries() {
    final Multi.Builder full = Multi.newBuilder();
    for (int i = 0; i < 1024; i++) {
      full.addOperations(put("/x" + i, new byte[]{1}));
    }
    Assertions.assertDoesNotThrow(() -> freeze("coord",
        Operation.newBuilder().setMulti(full).build()));
    full.addOperations(put("/y", new byte[]{1}));
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> freeze("coord", Operation.newBuilder().setMulti(full).build()));
  }

  @Test
  public void requestBytesBoundary() {
    // 1024 sub-ops x ~1027 canonical bytes push the entry past requestBytes=1MiB
    final Multi.Builder over = Multi.newBuilder();
    final byte[] value = new byte[1024];
    for (int i = 0; i < 1024; i++) {
      over.addOperations(put("/x" + i, value));
    }
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> freeze("coord", Operation.newBuilder().setMulti(over).build()));

    // the same batch shape just under 1 MiB is admitted
    final Multi.Builder under = Multi.newBuilder();
    final byte[] small = new byte[980];
    for (int i = 0; i < 1024; i++) {
      under.addOperations(put("/x" + i, small));
    }
    Assertions.assertDoesNotThrow(
        () -> freeze("coord", Operation.newBuilder().setMulti(under).build()));
  }

  @Test
  public void identityBoundaries() {
    final byte[] id = hex(WRITER_ID_HEX);
    Assertions.assertDoesNotThrow(() -> OperationId.valueOf(id, 1, 1));
    Assertions.assertDoesNotThrow(() -> OperationId.valueOf(id, 1, Long.MAX_VALUE - 1));
    Assertions.assertThrows(MetadataProtocolException.class, () -> OperationId.valueOf(id, 1, 0));
    Assertions.assertThrows(MetadataProtocolException.class, () -> OperationId.valueOf(id, 1, -1));
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> OperationId.valueOf(id, 1, Long.MAX_VALUE));
    Assertions.assertThrows(MetadataProtocolException.class, () -> OperationId.valueOf(id, 0, 1));
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> OperationId.valueOf(id, Long.MIN_VALUE, 1));
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> OperationId.valueOf(new byte[15], 1, 1));
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> OperationId.valueOf(new byte[16], 1, 1));
  }

  @Test
  public void budgetWindow() {
    final GroupConfig coord = GroupConfig.defaultsFor(Role.COORDINATION);
    final Operation op = put("/x", new byte[]{1});
    final OperationId oid = OperationId.valueOf(hex(WRITER_ID_HEX), 1, 1);
    Assertions.assertDoesNotThrow(
        () -> MetadataProtocol.freeze(GROUP_ID, coord, oid, op, COORD_W_MS));
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> MetadataProtocol.freeze(GROUP_ID, coord, oid, op, 0));
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> MetadataProtocol.freeze(GROUP_ID, coord, oid, op, COORD_W_MS + 1));
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> MetadataProtocol.freeze(GROUP_ID, coord, oid, op, -1));
  }

  @Test
  public void crossBudgetConfigsAreRejected() {
    // the default config validates
    Assertions.assertDoesNotThrow(() -> GroupConfig.defaultsFor(Role.COORDINATION).validate());
    // GC single-record budget smaller than the max stored reply
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> GroupConfig.valueOf(GroupConfig.defaultsFor(Role.COORDINATION).toBuilder()
            .setResultGcBytes(100).build()));
    // expiry worst-case batch overflow
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> GroupConfig.valueOf(GroupConfig.defaultsFor(Role.COORDINATION).toBuilder()
            .setBatchBytes(65_536).build()));
    // schema version is pinned to 1 and the role must be specified
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> GroupConfig.valueOf(GroupConfig.defaultsFor(Role.COORDINATION).toBuilder()
            .setSchemaVersion(2).build()));
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> GroupConfig.valueOf(GroupConfig.defaultsFor(Role.COORDINATION).toBuilder()
            .setRoleValue(0).build()));
  }

  @Test
  public void treePathRules() {
    // unpaired surrogate rejected at the String boundary
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> MetadataProtocol.encodeTreePath("/\uD800x"));
    // NUL inside a raw path
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> freeze("coord", rawPut(new byte[]{'/', 0x00}, new byte[]{1})));
    // invalid UTF-8 bytes (a surrogate half) in a raw path
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> freeze("coord", rawPut(new byte[]{0x2f, (byte) 0xed, (byte) 0xa0, (byte) 0x80},
            new byte[]{1})));
    // empty segment, trailing slash, '.', '..'
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> freeze("coord", put("//a", new byte[]{1})));
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> freeze("coord", put("/a/", new byte[]{1})));
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> freeze("coord", put("/a/./b", new byte[]{1})));
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> freeze("coord", put("/a/../b", new byte[]{1})));
    // depth 129
    final StringBuilder deep = new StringBuilder();
    for (int i = 0; i < 129; i++) {
      deep.append("/s");
    }
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> freeze("coord", put(deep.toString(), new byte[]{1})));
    // root is virtual: not writable by put/cas/create/delete
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> freeze("coord", put("/", new byte[]{1})));
    Assertions.assertThrows(MetadataProtocolException.class, () -> freeze("coord", cas("/", "B")));
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> freeze("coord", Operation.newBuilder().setCreate(Create.newBuilder()
            .setPath(bs("/"))).build()));
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> freeze("coord", Operation.newBuilder().setDelete(
            MetadataProtos.Delete.newBuilder().setKey(bs("/"))).build()));
    // depth 128 within path bytes is accepted
    final StringBuilder ok = new StringBuilder();
    for (int i = 0; i < 128; i++) {
      ok.append("/s");
    }
    Assertions.assertDoesNotThrow(() -> freeze("coord", put(ok.toString(), new byte[]{1})));
  }

  @Test
  public void kvKeysAreArbitraryBytesInUnsignedOrder() {
    // 0x00 and 0xff are legal KV key bytes
    Assertions.assertDoesNotThrow(() -> freeze("kv",
        put(new byte[]{0x00, (byte) 0xff}, new byte[]{1})));
    // unsigned comparison order
    Assertions.assertTrue(MetadataProtocol.compareUnsigned(new byte[]{0x00}, new byte[]{0x01}) < 0);
    Assertions.assertTrue(
        MetadataProtocol.compareUnsigned(new byte[]{0x01}, new byte[]{(byte) 0x80}) < 0);
    Assertions.assertTrue(
        MetadataProtocol.compareUnsigned(new byte[]{(byte) 0x80}, new byte[]{(byte) 0xff}) < 0);
    Assertions.assertTrue(MetadataProtocol.compareUnsigned(new byte[]{0x01}, new byte[]{0x01, 0x00}) < 0);
    Assertions.assertEquals(0, MetadataProtocol.compareUnsigned(new byte[]{0x01}, new byte[]{0x01}));
  }

  @Test
  public void lockCreateRecipeIsEnforced() {
    // the typed lock CREATE has exactly one legal form
    Assertions.assertDoesNotThrow(() -> freeze("coord", lockCreate(1, false, true, new byte[0])));
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> freeze("coord", lockCreate(0, false, true, new byte[0])));
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> freeze("coord", lockCreate(1, true, true, new byte[0])));
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> freeze("coord", lockCreate(1, false, false, new byte[0])));
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> freeze("coord", lockCreate(1, false, true, new byte[]{1})));
    // a lock CREATE never rides inside MULTI
    Assertions.assertThrows(MetadataProtocolException.class, () -> freeze("coord",
        Operation.newBuilder().setMulti(Multi.newBuilder().addOperations(lockCreate(1, false, true, new byte[0]))).build()));
  }

  @Test
  public void reservedSequentialSuffixCoversAnyLeafBase() {
    // any leaf ending in "~" + 20 digits is reserved, whatever precedes it
    final String reserved = "/a/b~00000000000000000001";
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> freeze("coord", put(reserved, new byte[]{1})));
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> freeze("coord", create(reserved, true)));
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> freeze("coord", put("/~00000000000000000001", new byte[]{1})));
    // shorter digit runs and non-digit tails are ordinary names
    Assertions.assertDoesNotThrow(() -> freeze("coord", put("/a/b~0000000000000000000", new byte[]{1})));
    Assertions.assertDoesNotThrow(() -> freeze("coord", put("/a/b~x0000000000000000001", new byte[]{1})));
  }

  @Test
  public void semanticRulesHaveNegativeCoverage() {
    // INCREMENT names reject NUL; the keyspace role admits only PUT/CAS/DELETE
    Assertions.assertThrows(MetadataProtocolException.class, () -> freeze("coord",
        Operation.newBuilder().setIncrement(Increment.newBuilder().setName(bs("a\0b")).setDelta(1)).build()));
    Assertions.assertThrows(MetadataProtocolException.class, () -> freeze("kv", create("/x", true)));
    Assertions.assertThrows(MetadataProtocolException.class, () -> freeze("kv",
        Operation.newBuilder().setIncrement(Increment.newBuilder().setName(bs("c")).setDelta(1)).build()));
    Assertions.assertThrows(MetadataProtocolException.class, () -> freeze("kv",
        Operation.newBuilder().setMulti(Multi.newBuilder()
            .addOperations(put(new byte[]{'k'}, new byte[]{1}))).build()));
    // LEASE_RENEW must carry an explicit ttl and lease id; so must LEASE_REVOKE
    Assertions.assertThrows(MetadataProtocolException.class, () -> freeze("coord", leaseRenew(0)));
    Assertions.assertThrows(MetadataProtocolException.class, () -> freeze("coord",
        Operation.newBuilder().setLeaseRenew(LeaseRenew.newBuilder()
            .setOwner(writerRef()).setTtlMs(30_000)).build()));
    Assertions.assertThrows(MetadataProtocolException.class, () -> freeze("coord",
        Operation.newBuilder().setLeaseRevoke(LeaseRevoke.newBuilder()
            .setOwner(writerRef())).build()));
    // a lock DELETE is not recursive
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> freeze("coord", lockDelete(true)));
    Assertions.assertDoesNotThrow(() -> freeze("coord", lockDelete(false)));
    // the sequence exhaustion marker reports its own status
    final MetadataProtocolException exhausted = Assertions.assertThrows(
        MetadataProtocolException.class,
        () -> OperationId.valueOf(hex(WRITER_ID_HEX), 1, Long.MAX_VALUE));
    Assertions.assertEquals(MetadataStatus.S_SEQUENCE_EXHAUSTED, exhausted.getStatus());
  }

  @Test
  public void decodedEnvelopeRoundTripAndRecursiveUnknownRejection() {
    final FrozenRequest frozen = freeze("coord", put("/x", new byte[]{1, 2}));
    final MetadataProtos.ClientWrite envelope = MetadataProtos.ClientWrite.newBuilder()
        .setWireVersion(1)
        .setGroupId(ByteString.copyFrom(hex(GROUP_ID_HEX)))
        .setRole(Role.COORDINATION)
        .setFingerprint(ByteString.copyFrom(frozen.configFingerprint()))
        .setRetryBudgetMs(frozen.retryBudgetMs())
        .setId(MetadataProtos.OperationId.newBuilder()
            .setWriter(writerRef()).setSequence(1))
        .setOperation(frozen.operation())
        .setDigest(ByteString.copyFrom(frozen.digest()))
        .build();
    final ClientWrite decoded = MetadataProtocol.decodeWrite(envelope.toByteArray());
    Assertions.assertArrayEquals(frozen.canonicalOperationBytes(), decoded.canonicalOperationBytes());
    Assertions.assertArrayEquals(frozen.digest(), decoded.digest());
    Assertions.assertEquals(frozen.operationId(), decoded.operationId());

    // unknown fields are rejected recursively, also inside the writer submessage
    final MetadataProtos.ClientWrite withUnknownWriter = envelope.toBuilder()
        .setId(envelope.getId().toBuilder().setWriter(
            envelope.getId().getWriter().toBuilder().setUnknownFields(unknownVarint(99, 7))))
        .build();
    Assertions.assertThrows(MetadataProtocolException.class,
        () -> MetadataProtocol.decodeWrite(withUnknownWriter.toByteArray()));
  }

  private static void assertVector(String configName, String canonicalHex, String digestHex,
      Operation op) {
    final FrozenRequest frozen = freeze(configName, op);
    Assertions.assertEquals(canonicalHex, hex(frozen.canonicalOperationBytes()), "canonical bytes");
    Assertions.assertEquals(digestHex, hex(frozen.digest()), "digest");
  }

  private static FrozenRequest freeze(String configName, Operation op) {
    final boolean coord = "coord".equals(configName);
    final GroupConfig config = GroupConfig.defaultsFor(coord ? Role.COORDINATION : Role.KEYSPACE);
    return MetadataProtocol.freeze(GROUP_ID, config,
        OperationId.valueOf(hex(WRITER_ID_HEX), 1, 1), op, coord ? KV_W_MS : 3_600_000L);
  }

  private static FrozenRequest request(byte[] value) {
    return MetadataProtocol.freeze(GROUP_ID, GroupConfig.defaultsFor(Role.COORDINATION),
        OperationId.valueOf(hex(WRITER_ID_HEX), 1, 1), put("/x", value), KV_W_MS);
  }

  private static Operation put(String path, byte[] value) {
    return rawPut(MetadataProtocol.encodeTreePath(path), value);
  }

  private static Operation put(byte[] key, byte[] value) {
    return rawPut(key, value);
  }

  private static Operation rawPut(byte[] key, byte[] value) {
    return Operation.newBuilder().setPut(Put.newBuilder()
        .setKey(ByteString.copyFrom(key))
        .setValue(ByteString.copyFrom(value))).build();
  }

  private static Operation create(String path, boolean createParents) {
    return Operation.newBuilder().setCreate(Create.newBuilder()
        .setPath(bs(path)).setCreateParents(createParents)).build();
  }

  private static Operation cas(String path, String value) {
    return cas(MetadataProtocol.encodeTreePath(path), value);
  }

  private static Operation cas(byte[] key, String value) {
    return Operation.newBuilder().setCas(Cas.newBuilder()
        .setKey(ByteString.copyFrom(key)).setValue(bs(value))
        .setExpected(ExpectedVersion.newBuilder().setExact(
            MetadataProtos.Stamp.newBuilder().setLogIndex(7).setOrdinal(2)))).build();
  }

  private static Operation leaseOpen(long ttlMs) {
    return Operation.newBuilder().setLeaseOpen(LeaseOpen.newBuilder()
        .setOwner(writerRef()).setTtlMs(ttlMs)).build();
  }

  private static WriterRef writerRef() {
    return WriterRef.newBuilder()
        .setWriterId(ByteString.copyFrom(hex(WRITER_ID_HEX))).setGeneration(1).build();
  }

  private static Operation leaseRenew(long ttlMs) {
    return Operation.newBuilder().setLeaseRenew(LeaseRenew.newBuilder()
        .setOwner(writerRef()).setLeaseId(1).setTtlMs(ttlMs)).build();
  }

  private static Operation lockCreate(long leaseId, boolean sequential, boolean createParents, byte[] value) {
    return Operation.newBuilder().setCreate(Create.newBuilder()
        .setPath(bs("/l")).setKind(NodeKind.EPHEMERAL_LOCK)
        .setLeaseId(leaseId).setSequential(sequential).setCreateParents(createParents)
        .setValue(ByteString.copyFrom(value))).build();
  }

  private static Operation lockDelete(boolean recursive) {
    return Operation.newBuilder().setDelete(MetadataProtos.Delete.newBuilder()
        .setKey(bs("/l")).setRecursive(recursive)
        .setLockOwner(LockOwner.newBuilder().setOwner(writerRef()).setLeaseId(1)
            .setToken(MetadataProtos.LockToken.newBuilder()
                .setGroupId(ByteString.copyFrom(hex(GROUP_ID_HEX))).setLogIndex(5)))).build();
  }

  private static UnknownFieldSet unknownVarint(int fieldNumber, long value) {
    return UnknownFieldSet.newBuilder().addField(fieldNumber,
        UnknownFieldSet.Field.newBuilder().addVarint(value).build()).build();
  }

  /** @return "/" followed by {@code n} 'a' characters: exactly n+1 UTF-8 bytes. */
  private static byte[] prefixedPath(int n) {
    final byte[] path = new byte[n + 1];
    path[0] = '/';
    Arrays.fill(path, 1, path.length, (byte) 'a');
    return path;
  }

  private static ByteString bs(String s) {
    return ByteString.copyFrom(s.getBytes(StandardCharsets.UTF_8));
  }

  private static byte[] hex(String hex) {
    final byte[] out = new byte[hex.length() / 2];
    for (int i = 0; i < out.length; i++) {
      out[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
    }
    return out;
  }

  private static String hex(byte[] bytes) {
    final StringBuilder sb = new StringBuilder(bytes.length * 2);
    for (byte b : bytes) {
      sb.append(Character.forDigit((b >> 4) & 0xf, 16)).append(Character.forDigit(b & 0xf, 16));
    }
    return sb.toString();
  }
}
