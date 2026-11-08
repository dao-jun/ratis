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

import java.util.Arrays;

import org.apache.ratis.metadata.proto.MetadataProtos.AdminWrite;
import org.apache.ratis.metadata.proto.MetadataProtos.BusinessResult;
import org.apache.ratis.metadata.proto.MetadataProtos.ClientWrite;
import org.apache.ratis.metadata.proto.MetadataProtos.LeaseCandidate;
import org.apache.ratis.metadata.proto.MetadataProtos.Lane;
import org.apache.ratis.metadata.proto.MetadataProtos.Operation;
import org.apache.ratis.metadata.proto.MetadataProtos.OperationId;
import org.apache.ratis.metadata.proto.MetadataProtos.Query;
import org.apache.ratis.metadata.proto.MetadataProtos.QueryReply;
import org.apache.ratis.metadata.proto.MetadataProtos.Status;
import org.apache.ratis.metadata.proto.MetadataProtos.WriteReply;
import org.apache.ratis.metadata.proto.MetadataProtos.WriterRef;
import org.apache.ratis.metadata.protocol.FrozenRequest;
import org.apache.ratis.metadata.protocol.GroupConfig;
import org.apache.ratis.metadata.protocol.MetadataProtocol;
import org.apache.ratis.metadata.protocol.MetadataProtocolException;
import org.apache.ratis.metadata.protocol.MetadataStatus;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/**
 * The deterministic core every replicated entry passes through: the
 * admission ladder of section 3.3 over the overlay of section 4, with the
 * bounded maintenance of section 4.1/4.2 staged at the start of every
 * entry whatever its outcome. One entry builds one
 * {@link PreparedTransition}; the reply is staged on it and published only
 * by the commit - the engine never touches the store outside a transition.
 */
final class MetadataEngine {
  private final RaftGroupId groupId;

  MetadataEngine(RaftGroupId groupId) {
    this.groupId = java.util.Objects.requireNonNull(groupId, "groupId");
  }

  RaftGroupId groupId() {
    return groupId;
  }

  // ---- the three entry kinds ----

  /** Applies and commits one client write entry; the reply is the committed one. */
  WriteReply apply(long term, long index, long proposedTime, ClientWrite proto,
      GroupConfig config, RocksMetadataStore store) {
    final long now = Math.max(store.logicalTime(), proposedTime);
    try (PreparedTransition transition = store.newTransition(term, index, now)) {
      final ApplyContext context =
          new ApplyContext(groupId, term, index, now, config);
      final WriteReply reply = admitWrite(proto, context, transition, store);
      transition.stageReply(reply);
      store.commit(transition);
      return reply;
    }
  }

  /** Applies and commits one admin write entry: registration or takeover. */
  WriteReply admin(long term, long index, long proposedTime, AdminWrite proto,
      GroupConfig config, RocksMetadataStore store) {
    final long now = Math.max(store.logicalTime(), proposedTime);
    try (PreparedTransition transition = store.newTransition(term, index, now)) {
      try (ReadView view = store.openReadView()) {
        stageMaintenance(transition, view, config, index);
      }
      final MetadataStatus gate = WriterRegistry.gate(groupId, config, proto);
      final WriteReply reply = gate != null
          ? WriteReplies.rejected(Status.forNumber(gate.wireCode()), 0, 0, null)
          : dispatchAdmin(proto,
              new ApplyContext(groupId, term, index, now, config), transition, store);
      transition.stageReply(reply);
      store.commit(transition);
      return reply;
    }
  }

  /**
   * Applies and commits one maintenance entry: the bounded passes plus at
   * most one lease-expiry candidate. The candidate stages into the entry's
   * business buffer - a maintenance entry has no other business content, so
   * that buffer is its selective-discard scope when change-byte pressure
   * cannot be resolved and the expiry defers to a later entry.
   */
  void maintenance(long term, long index, long proposedTime, LeaseCandidate candidate,
      GroupConfig config, RocksMetadataStore store) {
    final long now = Math.max(store.logicalTime(), proposedTime);
    try (PreparedTransition transition = store.newTransition(term, index, now)) {
      final int[] summaryBudget = {config.changeFloorRevisionsPerEntry()};
      try (ReadView view = store.openReadView()) {
        stageMaintenance(transition, view, config, index, summaryBudget);
        if (candidate != null) {
          stageLeaseExpiry(candidate,
              new ApplyContext(groupId, term, index, now, config),
              transition, view, store, config, summaryBudget);
        }
      }
      store.commit(transition);
    }
  }

  /** Runs one read-only query under a short read view. */
  QueryReply query(Query proto, GroupConfig config, RocksMetadataStore store) {
    if (!proto.getUnknownFields().asMap().isEmpty()
        || proto.getActionCase() == Query.ActionCase.ACTION_NOT_SET) {
      return QueryReply.newBuilder().setStatus(Status.S_MALFORMED).build();
    }
    if (proto.getWireVersion() != MetadataProtocol.WIRE_VERSION
        || !Arrays.equals(proto.getGroupId().toByteArray(),
            groupId.toByteString().toByteArray())
        || !Arrays.equals(proto.getFingerprint().toByteArray(),
            MetadataProtocol.fingerprint(config))) {
      return QueryReply.newBuilder().setStatus(Status.S_CONFIG_MISMATCH).build();
    }
    if (proto.getRole() != config.role()) {
      return QueryReply.newBuilder().setStatus(Status.S_WRONG_ROLE).build();
    }
    try (ReadView view = store.openReadView()) {
      return QueryOps.run(proto, groupId, config, store, view);
    }
  }

  // ---- the write admission ladder ----

  private WriteReply admitWrite(ClientWrite proto, ApplyContext context,
      PreparedTransition transition, RocksMetadataStore store) {
    final GroupConfig config = context.config();
    final int[] summaryBudget = {config.changeFloorRevisionsPerEntry()};
    try (ReadView view = store.openReadView()) {
      stageMaintenance(transition, view, config, context.index(), summaryBudget);
    }
    final FrozenRequest request;
    try {
      MetadataProtocol.decodeWrite(proto.toByteArray()); // structural self-check
    } catch (MetadataProtocolException e) {
      return WriteReplies.rejected(Status.forNumber(e.getStatus().wireCode()), 0, 0, null);
    }
    // the deterministic gates run before any canonicalization: a foreign
    // group or stale fingerprint is never a path error
    if (!Arrays.equals(proto.getGroupId().toByteArray(),
        groupId.toByteString().toByteArray())
        || !Arrays.equals(proto.getFingerprint().toByteArray(),
            MetadataProtocol.fingerprint(config))) {
      return WriteReplies.rejected(Status.S_CONFIG_MISMATCH, 0, 0, null);
    }
    if (proto.getRole() != config.role()) {
      return WriteReplies.rejected(Status.S_WRONG_ROLE, 0, 0, null);
    }
    if (proto.getRetryBudgetMs() > config.retentionMs()) {
      return WriteReplies.rejected(Status.S_BUDGET_MISMATCH, 0, 0, null);
    }
    try {
      request = MetadataProtocol.freeze(groupId, config, protocolId(proto.getId()),
          proto.getOperation(), proto.getRetryBudgetMs());
    } catch (MetadataProtocolException e) {
      return WriteReplies.rejected(Status.forNumber(e.getStatus().wireCode()), 0, 0, null);
    }
    if (!Arrays.equals(proto.getDigest().toByteArray(), request.digest())) {
      return WriteReplies.rejected(Status.S_MALFORMED, 0, 0, null); // non-canonical send
    }
    return ladder(request, context, transition, store, summaryBudget);
  }

  private WriteReply ladder(FrozenRequest request, ApplyContext context,
      PreparedTransition transition, RocksMetadataStore store, int[] summaryBudget) {
    final org.apache.ratis.metadata.protocol.OperationId id = request.operationId();
    final byte[] digest = request.digest();
    try (ReadView view = store.openReadView()) {
      final OverlayState overlay = new OverlayState(view, transition, false);
      final ResultLookup lookup = InvocationResultStore.INSTANCE.lookup(
          id, digest, context.logicalNow(), view);
      final WriterRow row = overlay.writer(id.writerId());
      switch (lookup.kind()) {
        case REPLAY:
          return WriteReplies.replay(lookup.originalReply(),
              row != null && id.generation() < row.currentGeneration(),
              row != null ? row.highWater() + 1 : 0,
              row != null ? row.currentGeneration() : 0, digest);
        case RESULT_EXPIRED:
          return WriteReplies.rejected(Status.S_RESULT_EXPIRED,
              row != null ? row.highWater() + 1 : 0,
              row != null ? row.currentGeneration() : 0, digest);
        case INVOCATION_MISMATCH:
          return WriteReplies.rejected(Status.S_INVOCATION_MISMATCH,
              row != null ? row.highWater() + 1 : 0,
              row != null ? row.currentGeneration() : 0, digest);
        default:
          break; // a miss falls through to the writer ladder
      }
      if (row == null) {
        return WriteReplies.rejected(Status.S_UNKNOWN_WRITER, 0, 0, digest);
      }
      final long next = row.highWater() + 1;
      if (row.currentGeneration() > id.generation()) {
        return WriteReplies.rejected(Status.S_STALE_GENERATION, next,
            row.currentGeneration(), digest);
      }
      if (row.currentGeneration() < id.generation()) {
        return WriteReplies.rejected(Status.S_FUTURE_GENERATION, next,
            row.currentGeneration(), digest);
      }
      if (id.sequence() == Long.MAX_VALUE) {
        return WriteReplies.rejected(Status.S_SEQUENCE_EXHAUSTED, next,
            row.currentGeneration(), digest);
      }
      if (id.sequence() <= row.highWater()) {
        // the result existed and is gone: this identity can no longer execute
        return WriteReplies.rejected(Status.S_RESULT_EXPIRED, next,
            row.currentGeneration(), digest);
      }
      if (id.sequence() > next) {
        return WriteReplies.rejected(Status.S_SEQUENCE_GAP, next,
            row.currentGeneration(), digest);
      }
      return execute(request, context, transition, overlay, row, digest, summaryBudget);
    }
  }

  private WriteReply execute(FrozenRequest request, ApplyContext context,
      PreparedTransition transition, OverlayState overlay, WriterRow row,
      byte[] digest, int[] summaryBudget) {
    final GroupConfig config = context.config();
    final org.apache.ratis.metadata.protocol.OperationId id = request.operationId();
    final byte[] resultKey = StoredRecordCodec.businessResultKey(
        id.writerId(), id.generation(), id.sequence());
    // the worst deterministic failure reply must fit before anything executes
    final byte[] worstFailure = identityResult(Status.S_MULTI_FAILED, id, context.index())
        .build().toByteArray();
    if (!WriterRegistry.laneFits(transition, config, row.lane(),
        InvocationResultStore.charge(resultKey,
            storedResult(digest, context, request, worstFailure)))) {
      return WriteReplies.rejected(Status.S_RESULT_CAPACITY,
          row.highWater() + 1, row.currentGeneration(), digest);
    }
    final ChangeLog changes = new ChangeLog(context.index(), false);
    final OpExecution exec = new OpExecution(context, overlay, changes);
    BusinessResult.Builder business = dispatch(exec, request.operation(), row.lane());
    boolean discarded = false;
    if (business.getStatus() != Status.S_OK) {
      transition.discardBusiness(); // node effects die; the failure reply survives
      discarded = true;
    } else if (business.build().getSerializedSize() > config.storedReplyBytes()) {
      transition.discardBusiness();
      discarded = true;
      business = identityResult(Status.S_REPLY_TOO_LARGE, id, context.index());
    }
    final byte[] replyBytes = business
        .setFirstRevision(context.index())
        .setOperationId(protoId(id))
        .build().toByteArray();
    final StoredResult result = storedResult(digest, context, request, replyBytes);
    if (!WriterRegistry.laneFits(transition, config, row.lane(),
        InvocationResultStore.charge(resultKey, result))) {
      if (!discarded) {
        transition.discardBusiness();
        discarded = true;
      }
      return WriteReplies.rejected(Status.S_RESULT_CAPACITY,
          row.highWater() + 1, row.currentGeneration(), digest);
    }
    if (ChangeLog.visibleBytes(transition) > config.changeCapacityBytes()) {
      ChangeLog.evictForCapacity(transition, overlay.view(), config, summaryBudget);
      if (ChangeLog.visibleBytes(transition) > config.changeCapacityBytes()) {
        if (!discarded) {
          transition.discardBusiness();
          discarded = true;
        }
        return WriteReplies.rejected(Status.S_CHANGE_CAPACITY,
            row.highWater() + 1, row.currentGeneration(), digest);
      }
    }
    if (!discarded) {
      changes.finish(transition); // publish the feed summary with the effects
    }
    overlay.stageWriter(id.writerId(),
        new WriterRow(row.lane(), row.currentGeneration(), id.sequence()));
    InvocationResultStore.INSTANCE.stageResult(
        transition, id, row.lane(), result);
    return WriteReplies.accepted(replyBytes, id.sequence() + 1,
        row.currentGeneration(), digest);
  }

  /** The lane matrix: business writes carry node mutations, control carries leases. */
  private BusinessResult.Builder dispatch(OpExecution exec, Operation operation, Lane lane) {
    final boolean leaseCommand;
    switch (operation.getCommandCase()) {
      case LEASE_OPEN: case LEASE_RENEW: case LEASE_REVOKE:
        leaseCommand = true;
        break;
      default:
        leaseCommand = false;
        break;
    }
    if (leaseCommand != (lane == Lane.CONTROL)) {
      // the identity and revision are stamped uniformly on the reply
      return BusinessResult.newBuilder().setStatus(Status.S_NOT_ALLOWED);
    }
    return lane == Lane.CONTROL
        ? LeaseOps.apply(exec, operation)
        : NodeOps.apply(exec, operation);
  }

  // ---- admin and maintenance helpers ----

  private WriteReply dispatchAdmin(AdminWrite proto, ApplyContext context,
      PreparedTransition transition, RocksMetadataStore store) {
    try (ReadView view = store.openReadView()) {
      final OverlayState overlay = new OverlayState(view, transition, false);
      return proto.getActionCase() == AdminWrite.ActionCase.REGISTER_WRITER
          ? WriterRegistry.INSTANCE.register(overlay, context.config(), proto)
          : WriterRegistry.INSTANCE.takeover(overlay, context,
              InvocationResultStore.INSTANCE, proto);
    }
  }

  /**
   * The expiry candidate: validated against the current state, staged on the
   * entry's business side so unresolvable change-byte pressure can defer it
   * cleanly - the maintenance passes below it always commit.
   */
  private void stageLeaseExpiry(LeaseCandidate candidate, ApplyContext context,
      PreparedTransition transition, ReadView view, RocksMetadataStore store,
      GroupConfig config, int[] summaryBudget) {
    final OverlayState overlay = new OverlayState(view, transition, false);
    final ChangeLog changes = new ChangeLog(context.index(), false);
    final OpExecution exec = new OpExecution(context, overlay, changes);
    if (!LeaseOps.expire(exec, candidate.getLeaseId(),
        candidate.getExpectedLastRenewIndex())) {
      return; // the state moved on; a no-op entry
    }
    if (ChangeLog.visibleBytes(transition) > config.changeCapacityBytes()) {
      ChangeLog.evictForCapacity(transition, view, config, summaryBudget);
      if (ChangeLog.visibleBytes(transition) > config.changeCapacityBytes()) {
        transition.discardBusiness(); // defer the expiry; GC and floor survive
        return;
      }
    }
    changes.finish(transition);
  }

  /** The bounded passes every entry stages first, whatever its outcome. */
  private void stageMaintenance(PreparedTransition transition, ReadView view,
      GroupConfig config, long index, int[] summaryBudget) {
    InvocationResultStore.INSTANCE.stageGc(transition, GcBudget.forResults(config),
        transition.logicalNowAfter(), view);
    final long floor =
        ChangeLog.advanceFloor(transition, view, config, index, summaryBudget);
    ChangeLog.collectPhysical(transition, view, config, floor);
  }

  private void stageMaintenance(PreparedTransition transition, ReadView view,
      GroupConfig config, long index) {
    stageMaintenance(transition, view, config, index,
        new int[] {config.changeFloorRevisionsPerEntry()});
  }

  // ---- small shared builders ----

  private static StoredResult storedResult(byte[] digest, ApplyContext context,
      FrozenRequest request, byte[] reply) {
    return StoredResult.newBuilder()
        .setDigest(digest)
        .setFirstTerm(context.term())
        .setFirstIndex(context.index())
        .setExecutedAt(context.logicalNow())
        .setExpiresAt(Math.addExact(context.logicalNow(), request.retryBudgetMs()))
        .setReply(reply)
        .build();
  }

  private static BusinessResult.Builder identityResult(Status status,
      org.apache.ratis.metadata.protocol.OperationId id, long index) {
    return BusinessResult.newBuilder()
        .setStatus(status)
        .setFirstRevision(index)
        .setOperationId(protoId(id));
  }

  private static OperationId protoId(org.apache.ratis.metadata.protocol.OperationId id) {
    return OperationId.newBuilder()
        .setWriter(WriterRef.newBuilder()
            .setWriterId(ByteString.copyFrom(id.writerId()))
            .setGeneration(id.generation()))
        .setSequence(id.sequence())
        .build();
  }

  private static org.apache.ratis.metadata.protocol.OperationId protocolId(
      OperationId proto) {
    return org.apache.ratis.metadata.protocol.OperationId.valueOf(
        proto.getWriter().getWriterId().toByteArray(),
        proto.getWriter().getGeneration(),
        proto.getSequence());
  }
}
