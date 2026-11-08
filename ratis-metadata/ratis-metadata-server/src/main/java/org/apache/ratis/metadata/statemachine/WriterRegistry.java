/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this License except in compliance with
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
import org.apache.ratis.metadata.proto.MetadataProtos.Lane;
import org.apache.ratis.metadata.proto.MetadataProtos.RegisterWriter;
import org.apache.ratis.metadata.proto.MetadataProtos.Role;
import org.apache.ratis.metadata.proto.MetadataProtos.Status;
import org.apache.ratis.metadata.proto.MetadataProtos.TakeoverId;
import org.apache.ratis.metadata.proto.MetadataProtos.TakeoverWriter;
import org.apache.ratis.metadata.proto.MetadataProtos.WriterState;
import org.apache.ratis.metadata.proto.MetadataProtos.WriteReply;
import org.apache.ratis.metadata.protocol.GroupConfig;
import org.apache.ratis.metadata.protocol.MetadataProtocol;
import org.apache.ratis.metadata.protocol.MetadataStatus;
import org.apache.ratis.protocol.RaftGroupId;

/**
 * The two admin writes of the writer lifecycle. Registration is idempotent
 * over the current row state and never resets anything; takeover is a CAS on
 * the writer generation whose success reply is itself a durable result of
 * the takeover lane, so a retried winner replays while every loser observes
 * a stale generation.
 */
final class WriterRegistry {
  /** The digest domain separating takeover results from operation digests. */
  private static final byte[] TAKEOVER_DOMAIN = "RMD-TAKEOVER1".getBytes();

  static final WriterRegistry INSTANCE = new WriterRegistry();

  private WriterRegistry() {
  }

  /**
   * The takeover result digest: SHA-256 over the takeover domain, the group,
   * the config fingerprint and the canonical action bytes - identity is
   * {@code (writerId, expectedGeneration, nonce)} in full.
   */
  static byte[] takeoverDigest(RaftGroupId groupId, GroupConfig config, TakeoverWriter action) {
    final TakeoverId id = action.getId();
    final TakeoverWriter canonical = TakeoverWriter.newBuilder()
        .setId(TakeoverId.newBuilder()
            .setWriterId(id.getWriterId())
            .setExpectedGeneration(id.getExpectedGeneration())
            .setNonce(id.getNonce()))
        .build();
    return StoredRecordCodec.sha256(concat(TAKEOVER_DOMAIN, new byte[] {0},
        groupId.toByteString().toByteArray(),
        MetadataProtocol.fingerprint(config), canonical.toByteArray()));
  }

  /**
   * The deterministic admin gates: identity extraction, group, fingerprint
   * and budget. {@code null} means the envelope is admissible.
   */
  static MetadataStatus gate(RaftGroupId groupId, GroupConfig config, AdminWrite proto) {
    if (!proto.getUnknownFields().asMap().isEmpty()
        || proto.getActionCase() == AdminWrite.ActionCase.ACTION_NOT_SET) {
      return MetadataStatus.S_MALFORMED;
    }
    if (proto.getWireVersion() != MetadataProtocol.WIRE_VERSION
        || !Arrays.equals(proto.getGroupId().toByteArray(),
            groupId.toByteString().toByteArray())
        || !Arrays.equals(proto.getFingerprint().toByteArray(),
            MetadataProtocol.fingerprint(config))) {
      return MetadataStatus.S_CONFIG_MISMATCH;
    }
    if (proto.getRetryBudgetMs() < 1 || proto.getRetryBudgetMs() > config.retentionMs()) {
      return MetadataStatus.S_BUDGET_MISMATCH;
    }
    return null;
  }

  /**
   * Registers one writer: the first registration fixes the lane and starts
   * at generation 1; every later registration only reports the row's state.
   */
  WriteReply register(OverlayState overlay, GroupConfig config, AdminWrite envelope) {
    final RegisterWriter action = envelope.getRegisterWriter();
    final byte[] writerId = action.getWriterId().toByteArray();
    if (writerId.length != 16 || allZero(writerId)
        || action.getLane() == Lane.LANE_UNSPECIFIED
        || action.getLane() == Lane.UNRECOGNIZED) {
      return WriteReplies.rejected(Status.S_MALFORMED, 0, 0, null);
    }
    if (config.role() == Role.KEYSPACE && action.getLane() == Lane.CONTROL) {
      return WriteReplies.rejected(Status.S_WRONG_ROLE, 0, 0, null);
    }
    final WriterRow row = overlay.writer(writerId);
    if (row != null && row.lane() != action.getLane()) {
      return rejectedWithWriter(Status.S_WRITER_EXISTS, row);
    }
    if (row != null) {
      // still at generation 1 an idempotent confirmation; after a takeover
      // the row can never again be reached by registration
      return row.currentGeneration() > 1
          ? rejectedWithWriter(Status.S_WRITER_EXISTS, row)
          : stateReply(row, envelope);
    }
    if (writerCount(overlay.transition()) >= config.writerRows()) {
      return WriteReplies.rejected(Status.S_WRITER_LIMIT, 0, 0, null);
    }
    final WriterRow fresh = new WriterRow(action.getLane(), 1, 0);
    overlay.stageWriter(writerId, fresh);
    overlay.transition().stageStatsDelta(new StoreStats(0, 0, 0, 0, 1, 0));
    return stateReply(fresh, envelope);
  }

  private static long writerCount(PreparedTransition transition) {
    return transition.statsDelta().plus(transition.owner().stats()).writerCount();
  }

  private WriteReply stateReply(WriterRow row, AdminWrite envelope) {
    return WriteReplies.accepted(null, 0, row.currentGeneration(), null)
        .toBuilder()
        .setRegisteredWriter(WriterState.newBuilder()
            .setWriterId(envelope.getRegisterWriter().getWriterId())
            .setLane(row.lane())
            .setGeneration(row.currentGeneration())
            .setHighWater(row.highWater()))
        .build();
  }

  private WriteReply rejectedWithWriter(Status status, WriterRow row) {
    return WriteReplies.rejected(status, row.highWater() + 1, row.currentGeneration(), null)
        .toBuilder()
        .setRegisteredWriter(WriterState.newBuilder()
            .setLane(row.lane())
            .setGeneration(row.currentGeneration())
            .setHighWater(row.highWater()))
        .build();
  }

  /**
   * Takes one writer over: a CAS from exactly the expected generation to the
   * next, resetting the high water. The winner's reply is a durable lane
   * result; every late or losing attempt observes the moved generation.
   */
  WriteReply takeover(OverlayState overlay, ApplyContext context,
      InvocationResultStore results, AdminWrite envelope) {
    final TakeoverId id = envelope.getTakeoverWriter().getId();
    final byte[] writerId = id.getWriterId().toByteArray();
    final byte[] nonce = id.getNonce().toByteArray();
    if (writerId.length != 16 || allZero(writerId) || nonce.length != 16 || allZero(nonce)
        || id.getExpectedGeneration() < 1) {
      return WriteReplies.rejected(Status.S_MALFORMED, 0, 0, null);
    }
    final GroupConfig config = context.config();
    final byte[] digest =
        takeoverDigest(context.groupId(), config, envelope.getTakeoverWriter());
    final ResultLookup lookup =
        results.lookupTakeover(id, digest, context.logicalNow(), overlay.view());
    final WriterRow row = overlay.writer(writerId);
    if (lookup.kind() == ResultLookup.Kind.REPLAY) {
      return WriteReplies.replay(lookup.originalReply(),
          row != null && row.currentGeneration() != id.getExpectedGeneration(),
          row != null ? row.highWater() + 1 : 1,
          row != null ? row.currentGeneration() : 0, digest);
    }
    if (lookup.kind() == ResultLookup.Kind.INVOCATION_MISMATCH) {
      return WriteReplies.rejected(Status.S_INVOCATION_MISMATCH,
          row != null ? row.highWater() + 1 : 0,
          row != null ? row.currentGeneration() : 0, digest);
    }
    // an expired takeover result can never re-execute: the generation moved
    if (row == null) {
      return WriteReplies.rejected(Status.S_UNKNOWN_WRITER, 0, 0, digest);
    }
    if (row.currentGeneration() != id.getExpectedGeneration()) {
      return WriteReplies.rejected(Status.S_STALE_GENERATION,
          row.highWater() + 1, row.currentGeneration(), digest);
    }
    if (id.getExpectedGeneration() == Long.MAX_VALUE) {
      return WriteReplies.rejected(Status.S_GENERATION_EXHAUSTED,
          row.highWater() + 1, row.currentGeneration(), digest);
    }
    final long newGeneration = id.getExpectedGeneration() + 1;
    final byte[] reply = BusinessResult.newBuilder()
        .setStatus(Status.S_OK)
        .setFirstRevision(context.index())
        .setTakeoverId(id)
        .setWriter(WriterState.newBuilder()
            .setWriterId(id.getWriterId())
            .setLane(row.lane())
            .setGeneration(newGeneration)
            .setHighWater(0))
        .build().toByteArray();
    final StoredResult result = StoredResult.newBuilder()
        .setDigest(digest)
        .setFirstTerm(context.term())
        .setFirstIndex(context.index())
        .setExecutedAt(context.logicalNow())
        .setExpiresAt(checkedAdd(context.logicalNow(), envelope.getRetryBudgetMs()))
        .setReply(reply)
        .build();
    final byte[] resultKey = StoredRecordCodec.takeoverResultKey(
        writerId, id.getExpectedGeneration(), nonce);
    if (!laneFits(overlay.transition(), config, row.lane(),
        InvocationResultStore.charge(resultKey, result))) {
      return WriteReplies.rejected(Status.S_RESULT_CAPACITY,
          row.highWater() + 1, row.currentGeneration(), digest);
    }
    final WriterRow advanced = new WriterRow(row.lane(), newGeneration, 0);
    overlay.stageWriter(writerId, advanced);
    results.stageTakeoverResult(overlay.transition(), id, row.lane(), result);
    return WriteReplies.accepted(reply, 1, newGeneration, digest)
        .toBuilder()
        .setRegisteredWriter(WriterState.newBuilder()
            .setWriterId(id.getWriterId())
            .setLane(row.lane())
            .setGeneration(newGeneration)
            .setHighWater(0))
        .build();
  }

  /**
   * @return whether one more lane charge fits the persisted usage plus
   *         everything this transition already staged
   */
  static boolean laneFits(PreparedTransition transition, GroupConfig config, Lane lane,
      long charge) {
    final StoreStats stats = transition.statsDelta().plus(transition.owner().stats());
    final long usage = lane == Lane.CONTROL
        ? stats.controlResultBytes() : stats.businessResultBytes();
    final long cap = lane == Lane.CONTROL
        ? config.controlResultBytes() : config.businessResultBytes();
    return charge <= cap - usage;
  }

  private static long checkedAdd(long left, long right) {
    final long sum = left + right;
    if (((left ^ sum) & (right ^ sum)) < 0) {
      throw new ArithmeticException("long overflow: " + left + " + " + right);
    }
    return sum;
  }

  private static boolean allZero(byte[] bytes) {
    for (byte value : bytes) {
      if (value != 0) {
        return false;
      }
    }
    return true;
  }

  private static byte[] concat(byte[]... parts) {
    int length = 0;
    for (byte[] part : parts) {
      length += part.length;
    }
    final byte[] out = new byte[length];
    int offset = 0;
    for (byte[] part : parts) {
      System.arraycopy(part, 0, out, offset, part.length);
      offset += part.length;
    }
    return out;
  }
}
