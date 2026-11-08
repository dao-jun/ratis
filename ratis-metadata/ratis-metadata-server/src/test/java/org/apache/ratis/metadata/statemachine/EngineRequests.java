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

import java.nio.charset.StandardCharsets;

import org.apache.ratis.metadata.proto.MetadataProtos.AdminWrite;
import org.apache.ratis.metadata.proto.MetadataProtos.ClientWrite;
import org.apache.ratis.metadata.proto.MetadataProtos.Create;
import org.apache.ratis.metadata.proto.MetadataProtos.Delete;
import org.apache.ratis.metadata.proto.MetadataProtos.ExpectedVersion;
import org.apache.ratis.metadata.proto.MetadataProtos.Increment;
import org.apache.ratis.metadata.proto.MetadataProtos.Lane;
import org.apache.ratis.metadata.proto.MetadataProtos.LeaseOpen;
import org.apache.ratis.metadata.proto.MetadataProtos.LeaseRenew;
import org.apache.ratis.metadata.proto.MetadataProtos.LeaseRevoke;
import org.apache.ratis.metadata.proto.MetadataProtos.NodeKind;
import org.apache.ratis.metadata.proto.MetadataProtos.Operation;
import org.apache.ratis.metadata.proto.MetadataProtos.Put;
import org.apache.ratis.metadata.proto.MetadataProtos.RegisterWriter;
import org.apache.ratis.metadata.proto.MetadataProtos.Role;
import org.apache.ratis.metadata.proto.MetadataProtos.TakeoverId;
import org.apache.ratis.metadata.proto.MetadataProtos.TakeoverWriter;
import org.apache.ratis.metadata.proto.MetadataProtos.WriterRef;
import org.apache.ratis.metadata.protocol.FrozenRequest;
import org.apache.ratis.metadata.protocol.GroupConfig;
import org.apache.ratis.metadata.protocol.MetadataProtocol;
import org.apache.ratis.metadata.protocol.OperationId;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/**
 * Deterministic request builders for the engine tests: every envelope goes
 * through the real freeze path, so the digest under test is the digest
 * production computes.
 */
final class EngineRequests {
  private EngineRequests() {
  }

  /** A 16-byte writer id that is never all-zero and stable per seed. */
  static byte[] writerId(int seed) {
    final byte[] id = new byte[16];
    for (int i = 0; i < 16; i++) {
      id[i] = (byte) (seed * 16 + i + 1);
    }
    return id;
  }

  /** A 16-byte takeover nonce, stable per seed and never equal to a writer id. */
  static byte[] nonce(int seed) {
    final byte[] id = new byte[16];
    for (int i = 0; i < 16; i++) {
      id[i] = (byte) (0xA0 + ((seed * 16 + i) % 0x5F));
    }
    return id;
  }

  static OperationId operationId(byte[] writerId, long generation, long sequence) {
    return OperationId.valueOf(writerId, generation, sequence);
  }

  // ---- operations ----

  static Operation put(byte[] key, byte[] value) {
    return Operation.newBuilder()
        .setPut(Put.newBuilder().setKey(ByteString.copyFrom(key))
            .setValue(ByteString.copyFrom(value)))
        .build();
  }

  static Operation put(String path, byte[] value) {
    return put(MetadataProtocol.encodeTreePath(path), value);
  }

  static Operation create(String path, byte[] value) {
    return create(path, value, false, 0, NodeKind.NORMAL, false);
  }

  static Operation create(String path, byte[] value, boolean createParents) {
    return create(path, value, createParents, 0, NodeKind.NORMAL, false);
  }

  static Operation createSequential(String base, byte[] value, boolean createParents) {
    return create(base, value, createParents, 0, NodeKind.NORMAL, true);
  }

  /** The typed lock: parents always created, the lease decides the owner. */
  static Operation createLock(String path, long leaseId) {
    return create(path, new byte[0], true, leaseId, NodeKind.EPHEMERAL_LOCK, false);
  }

  private static Operation create(String path, byte[] value, boolean createParents,
      long leaseId, NodeKind kind, boolean sequential) {
    return Operation.newBuilder()
        .setCreate(Create.newBuilder()
            .setPath(ByteString.copyFrom(MetadataProtocol.encodeTreePath(path)))
            .setValue(ByteString.copyFrom(value))
            .setLeaseId(leaseId)
            .setKind(kind)
            .setSequential(sequential)
            .setCreateParents(createParents))
        .build();
  }

  /** A CREATE whose explicit lease binding turns the node ephemeral. */
  static Operation createEphemeral(String path, long leaseId) {
    return create(path, new byte[0], true, leaseId, NodeKind.NORMAL, false);
  }

  static Operation putEphemeral(String path, byte[] value, long leaseId) {
    return Operation.newBuilder()
        .setPut(Put.newBuilder()
            .setKey(ByteString.copyFrom(MetadataProtocol.encodeTreePath(path)))
            .setValue(ByteString.copyFrom(value))
            .setLeaseId(leaseId))
        .build();
  }

  static Operation cas(String path, byte[] value, long logIndex, int ordinal) {
    return Operation.newBuilder()
        .setCas(org.apache.ratis.metadata.proto.MetadataProtos.Cas.newBuilder()
            .setKey(ByteString.copyFrom(MetadataProtocol.encodeTreePath(path)))
            .setValue(ByteString.copyFrom(value))
            .setExpected(ExpectedVersion.newBuilder()
                .setExact(org.apache.ratis.metadata.proto.MetadataProtos.Stamp.newBuilder()
                    .setLogIndex(logIndex).setOrdinal(ordinal))))
        .build();
  }

  static Operation delete(String path) {
    return Operation.newBuilder()
        .setDelete(Delete.newBuilder()
            .setKey(ByteString.copyFrom(MetadataProtocol.encodeTreePath(path))))
        .build();
  }

  static Operation deleteRecursive(String path) {
    return Operation.newBuilder()
        .setDelete(Delete.newBuilder()
            .setKey(ByteString.copyFrom(MetadataProtocol.encodeTreePath(path)))
            .setRecursive(true))
        .build();
  }

  static Operation multi(Operation... operations) {
    final org.apache.ratis.metadata.proto.MetadataProtos.Multi.Builder multi =
        org.apache.ratis.metadata.proto.MetadataProtos.Multi.newBuilder();
    for (Operation operation : operations) {
      multi.addOperations(operation);
    }
    return Operation.newBuilder().setMulti(multi).build();
  }

  static Operation increment(String name, long delta) {
    return Operation.newBuilder()
        .setIncrement(Increment.newBuilder()
            .setName(ByteString.copyFrom(name.getBytes(StandardCharsets.UTF_8)))
            .setDelta(delta))
        .build();
  }

  static Operation leaseOpen(byte[] ownerWriterId, long ownerGeneration, long ttlMs) {
    return Operation.newBuilder()
        .setLeaseOpen(LeaseOpen.newBuilder().setOwner(writerRef(ownerWriterId, ownerGeneration))
            .setTtlMs(ttlMs))
        .build();
  }

  static Operation leaseRenew(byte[] ownerWriterId, long ownerGeneration, long leaseId, long ttlMs) {
    return Operation.newBuilder()
        .setLeaseRenew(LeaseRenew.newBuilder().setOwner(writerRef(ownerWriterId, ownerGeneration))
            .setLeaseId(leaseId).setTtlMs(ttlMs))
        .build();
  }

  static Operation leaseRevoke(byte[] ownerWriterId, long ownerGeneration, long leaseId) {
    return Operation.newBuilder()
        .setLeaseRevoke(LeaseRevoke.newBuilder().setOwner(writerRef(ownerWriterId, ownerGeneration))
            .setLeaseId(leaseId))
        .build();
  }

  private static WriterRef writerRef(byte[] writerId, long generation) {
    return WriterRef.newBuilder().setWriterId(ByteString.copyFrom(writerId))
        .setGeneration(generation).build();
  }

  // ---- envelopes ----

  /**
   * Freezes the operation and renders the exact envelope a client sends.
   */
  static ClientWrite write(RaftGroupId groupId, GroupConfig config, OperationId id,
      Operation operation, long retryBudgetMs) {
    final FrozenRequest frozen =
        MetadataProtocol.freeze(groupId, config, id, operation, retryBudgetMs);
    return ClientWrite.newBuilder()
        .setWireVersion(MetadataProtocol.WIRE_VERSION)
        .setGroupId(groupId.toByteString())
        .setRole(config.role())
        .setFingerprint(ByteString.copyFrom(frozen.configFingerprint()))
        .setRetryBudgetMs(retryBudgetMs)
        .setId(protoId(id))
        .setOperation(frozen.operation())
        .setDigest(ByteString.copyFrom(frozen.digest()))
        .build();
  }

  /** The envelope of a foreign group: same identity, wrong group and role. */
  static ClientWrite foreignGroupWrite(RaftGroupId groupId, Role foreignRole,
      GroupConfig foreignConfig, OperationId id, Operation operation, long retryBudgetMs) {
    final ClientWrite correct = write(groupId, foreignConfig, id, operation, retryBudgetMs);
    return correct.toBuilder().setRole(foreignRole).build();
  }

  /** The same envelope with one flipped digest byte - structurally undecodable. */
  static ClientWrite corruptedDigestWrite(ClientWrite envelope) {
    final byte[] digest = envelope.getDigest().toByteArray();
    digest[0] ^= 0x55;
    return envelope.toBuilder().setDigest(ByteString.copyFrom(digest)).build();
  }

  static AdminWrite register(RaftGroupId groupId, GroupConfig config,
      byte[] writerId, Lane lane, long retryBudgetMs) {
    return AdminWrite.newBuilder()
        .setWireVersion(MetadataProtocol.WIRE_VERSION)
        .setGroupId(groupId.toByteString())
        .setFingerprint(ByteString.copyFrom(MetadataProtocol.fingerprint(config)))
        .setRetryBudgetMs(retryBudgetMs)
        .setRegisterWriter(RegisterWriter.newBuilder()
            .setWriterId(ByteString.copyFrom(writerId)).setLane(lane))
        .build();
  }

  static AdminWrite takeover(RaftGroupId groupId, GroupConfig config,
      byte[] writerId, long expectedGeneration, byte[] nonce, long retryBudgetMs) {
    return AdminWrite.newBuilder()
        .setWireVersion(MetadataProtocol.WIRE_VERSION)
        .setGroupId(groupId.toByteString())
        .setFingerprint(ByteString.copyFrom(MetadataProtocol.fingerprint(config)))
        .setRetryBudgetMs(retryBudgetMs)
        .setTakeoverWriter(TakeoverWriter.newBuilder().setId(TakeoverId.newBuilder()
            .setWriterId(ByteString.copyFrom(writerId))
            .setExpectedGeneration(expectedGeneration)
            .setNonce(ByteString.copyFrom(nonce))))
        .build();
  }

  private static org.apache.ratis.metadata.proto.MetadataProtos.OperationId protoId(OperationId id) {
    return org.apache.ratis.metadata.proto.MetadataProtos.OperationId.newBuilder()
        .setWriter(writerRef(id.writerId(), id.generation()))
        .setSequence(id.sequence())
        .build();
  }

  // ---- queries ----

  static org.apache.ratis.metadata.proto.MetadataProtos.Query get(
      RaftGroupId groupId, GroupConfig config, byte[] key) {
    return queryBase(groupId, config)
        .setGet(org.apache.ratis.metadata.proto.MetadataProtos.Get.newBuilder()
            .setKey(ByteString.copyFrom(key)))
        .build();
  }

  static org.apache.ratis.metadata.proto.MetadataProtos.Query get(
      RaftGroupId groupId, GroupConfig config, String path) {
    return get(groupId, config, MetadataProtocol.encodeTreePath(path));
  }

  static org.apache.ratis.metadata.proto.MetadataProtos.Query readWriter(
      RaftGroupId groupId, GroupConfig config, byte[] writerId) {
    return queryBase(groupId, config)
        .setReadWriter(org.apache.ratis.metadata.proto.MetadataProtos.ReadWriter.newBuilder()
            .setWriterId(ByteString.copyFrom(writerId)))
        .build();
  }

  static org.apache.ratis.metadata.proto.MetadataProtos.Query scan(
      RaftGroupId groupId, GroupConfig config, String treePrefix) {
    return scan(groupId, config, treePrefix, 0, null);
  }

  static org.apache.ratis.metadata.proto.MetadataProtos.Query scan(
      RaftGroupId groupId, GroupConfig config, String treePrefix, int maxEntries,
      byte[] continuation) {
    return scan(groupId, config, treePrefix, maxEntries, 0, continuation);
  }

  static org.apache.ratis.metadata.proto.MetadataProtos.Query scan(
      RaftGroupId groupId, GroupConfig config, String treePrefix, int maxEntries,
      int maxBytes, byte[] continuation) {
    final org.apache.ratis.metadata.proto.MetadataProtos.PageRequest.Builder page =
        org.apache.ratis.metadata.proto.MetadataProtos.PageRequest.newBuilder();
    if (maxEntries > 0) {
      page.setMaxEntries(maxEntries);
    }
    if (maxBytes > 0) {
      page.setMaxBytes(maxBytes);
    }
    if (continuation != null) {
      page.setContinuation(ByteString.copyFrom(continuation));
    }
    return queryBase(groupId, config)
        .setScan(org.apache.ratis.metadata.proto.MetadataProtos.ScanQuery.newBuilder()
            .setTreePrefix(ByteString.copyFrom(MetadataProtocol.encodeTreePath(treePrefix)))
            .setPage(page))
        .build();
  }

  /** A keyspace range scan: raw byte bounds, values included. */
  static org.apache.ratis.metadata.proto.MetadataProtos.Query scan(
      RaftGroupId groupId, GroupConfig config, byte[] start, byte[] end, int maxEntries) {
    final org.apache.ratis.metadata.proto.MetadataProtos.PageRequest.Builder page =
        org.apache.ratis.metadata.proto.MetadataProtos.PageRequest.newBuilder();
    if (maxEntries > 0) {
      page.setMaxEntries(maxEntries);
    }
    return queryBase(groupId, config)
        .setScan(org.apache.ratis.metadata.proto.MetadataProtos.ScanQuery.newBuilder()
            .setStart(ByteString.copyFrom(start))
            .setEnd(ByteString.copyFrom(end))
            .setPage(page))
        .build();
  }

  static org.apache.ratis.metadata.proto.MetadataProtos.Query pollChanges(
      RaftGroupId groupId, GroupConfig config, String prefix, long afterRevision,
      int afterOrdinal, int maxEntries) {
    return queryBase(groupId, config)
        .setPollChanges(org.apache.ratis.metadata.proto.MetadataProtos.PollChanges.newBuilder()
            .setPrefix(ByteString.copyFrom(MetadataProtocol.encodeTreePath(prefix)))
            .setAfter(org.apache.ratis.metadata.proto.MetadataProtos.ChangeCursor.newBuilder()
                .setGroupId(groupId.toByteString())
                .setRevision(afterRevision)
                .setOrdinal(afterOrdinal))
            .setPage(org.apache.ratis.metadata.proto.MetadataProtos.PageRequest.newBuilder()
                .setMaxEntries(maxEntries)))
        .build();
  }

  private static org.apache.ratis.metadata.proto.MetadataProtos.Query.Builder queryBase(
      RaftGroupId groupId, GroupConfig config) {
    return org.apache.ratis.metadata.proto.MetadataProtos.Query.newBuilder()
        .setWireVersion(MetadataProtocol.WIRE_VERSION)
        .setGroupId(groupId.toByteString())
        .setRole(config.role())
        .setFingerprint(ByteString.copyFrom(MetadataProtocol.fingerprint(config)));
  }
}
