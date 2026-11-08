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
import java.util.Objects;
import java.util.Optional;

import org.apache.ratis.metadata.proto.MetadataProtos.BusinessResult;
import org.apache.ratis.metadata.proto.MetadataProtos.Lane;
import org.apache.ratis.metadata.proto.MetadataProtos.TakeoverId;
import org.apache.ratis.metadata.protocol.OperationId;
import org.rocksdb.RocksIterator;

/**
 * The layout-3 result table: durable original replies keyed by writer
 * identity, one expiry-index row per result, and the bounded collection of
 * logically expired rows. Stateless - every method is a pure function over
 * the passed view and transition, so one shared instance serves every group.
 *
 * <p>The lookup order is fixed: a missing row is a miss; a row past its
 * logical expiry is {@link ResultLookup.Kind#RESULT_EXPIRED} even though it
 * may still sit on disk (an expired digest cannot promise conflict
 * detection); only a live row compares digests - equal replays the original
 * bytes, unequal is {@link ResultLookup.Kind#INVOCATION_MISMATCH}.</p>
 */
final class InvocationResultStore {
  static final InvocationResultStore INSTANCE = new InvocationResultStore();

  private InvocationResultStore() {
  }

  // ---- lookup ----

  /**
   * Looks up the stored result of one business operation.
   */
  ResultLookup lookup(OperationId id, byte[] digest, long logicalNow, ReadView view) {
    Objects.requireNonNull(id, "id");
    final byte[] resultKey = StoredRecordCodec.businessResultKey(
        id.writerId(), id.generation(), id.sequence());
    final ResultLookup lookup = lookupAt(resultKey, digest, logicalNow, view);
    if (lookup.kind() == ResultLookup.Kind.REPLAY) {
      verifyOperationReply(lookup, id);
    }
    return lookup;
  }

  /**
   * Looks up the stored result of one takeover: identity is
   * {@code (writerId, expectedGeneration, nonce)}.
   */
  ResultLookup lookupTakeover(TakeoverId id, byte[] digest, long logicalNow, ReadView view) {
    Objects.requireNonNull(id, "id");
    final byte[] resultKey = StoredRecordCodec.takeoverResultKey(
        id.getWriterId().toByteArray(), id.getExpectedGeneration(), id.getNonce().toByteArray());
    final ResultLookup lookup = lookupAt(resultKey, digest, logicalNow, view);
    if (lookup.kind() == ResultLookup.Kind.REPLAY) {
      verifyTakeoverReply(lookup, id);
    }
    return lookup;
  }

  private ResultLookup lookupAt(byte[] resultKey, byte[] digest, long logicalNow, ReadView view) {
    final byte[] encoded = view.get(MetadataCf.COMMAND_RESULTS, resultKey);
    if (encoded == null) {
      return ResultLookup.miss();
    }
    final StoredResult result = StoredRecordCodec.decodeStoredResult(encoded);
    if (result.expiresAt() <= logicalNow) {
      return ResultLookup.expired();
    }
    if (!Arrays.equals(result.digest(), digest)) {
      return ResultLookup.mismatch();
    }
    return ResultLookup.replay(result);
  }

  /**
   * The replay path verifies the stored reply still carries the identity of
   * the key it is filed under and the revision it claims to come from; a
   * checksum-valid but identity-inconsistent row is storage corruption.
   */
  private void verifyOperationReply(ResultLookup lookup, OperationId id) {
    final BusinessResult reply = parseReply(lookup, "operation");
    if (!reply.hasOperationId()
        || !Arrays.equals(reply.getOperationId().getWriter().getWriterId().toByteArray(),
            id.writerId())
        || reply.getOperationId().getWriter().getGeneration() != id.generation()
        || reply.getOperationId().getSequence() != id.sequence()) {
      throw identityCorrupt(lookup, "operation");
    }
    requireClaimedRevision(lookup, reply, "operation");
  }

  private void verifyTakeoverReply(ResultLookup lookup, TakeoverId id) {
    final BusinessResult reply = parseReply(lookup, "takeover");
    if (!reply.hasTakeoverId()
        || !Arrays.equals(reply.getTakeoverId().getWriterId().toByteArray(),
            id.getWriterId().toByteArray())
        || reply.getTakeoverId().getExpectedGeneration() != id.getExpectedGeneration()
        || !Arrays.equals(reply.getTakeoverId().getNonce().toByteArray(),
            id.getNonce().toByteArray())) {
      throw identityCorrupt(lookup, "takeover");
    }
    requireClaimedRevision(lookup, reply, "takeover");
  }

  private BusinessResult parseReply(ResultLookup lookup, String what) {
    try {
      return BusinessResult.parseFrom(lookup.originalReply());
    } catch (org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException e) {
      throw MetadataStoreException.corrupt(
          "the stored " + what + " reply is not a parsable BusinessResult", e);
    }
  }

  private MetadataStoreException identityCorrupt(ResultLookup lookup, String what) {
    return MetadataStoreException.corrupt(
        "the stored " + what + " reply does not carry the identity of its key");
  }

  private void requireClaimedRevision(ResultLookup lookup, BusinessResult reply, String what) {
    if (reply.getFirstRevision() != lookup.firstIndex()) {
      throw MetadataStoreException.corrupt("the stored " + what
          + " reply claims revision " + reply.getFirstRevision()
          + " but its record was first executed at index " + lookup.firstIndex());
    }
  }

  // ---- staging ----

  /**
   * Stages one stored business result: the result row, its exactly-one
   * expiry-index row and the lane byte charge, all into the transition's
   * business buffer so a business discard drops them with it.
   */
  void stageResult(PreparedTransition transition, OperationId id, Lane lane, StoredResult result) {
    final byte[] resultKey = StoredRecordCodec.businessResultKey(
        id.writerId(), id.generation(), id.sequence());
    stageAt(transition, resultKey, lane, result);
  }

  /** The takeover twin of {@link #stageResult}. */
  void stageTakeoverResult(PreparedTransition transition, TakeoverId id, Lane lane,
      StoredResult result) {
    final byte[] resultKey = StoredRecordCodec.takeoverResultKey(
        id.getWriterId().toByteArray(), id.getExpectedGeneration(), id.getNonce().toByteArray());
    stageAt(transition, resultKey, lane, result);
  }

  private void stageAt(PreparedTransition transition, byte[] resultKey, Lane lane,
      StoredResult result) {
    transition.business().put(MetadataCf.COMMAND_RESULTS, resultKey,
        StoredRecordCodec.storedResultValue(result));
    transition.business().put(MetadataCf.RESULT_EXPIRY,
        StoredRecordCodec.resultExpiryKey(result.expiresAt(), resultKey),
        StoredRecordCodec.resultExpiryValue(result.firstIndex()));
    transition.stageStatsDelta(laneCharge(lane, charge(resultKey, result)));
  }

  /** The exact byte charge of one result plus its index row, per lane. */
  static long charge(byte[] resultKey, StoredResult result) {
    return StoredRecordCodec.storedResultValue(result).length + resultKey.length
        + (8 + resultKey.length) + 9;
  }

  private static StoreStats laneCharge(Lane lane, long bytes) {
    if (lane == Lane.CONTROL) {
      return new StoreStats(0, bytes, 0, 0, 0, 0);
    }
    return new StoreStats(bytes, 0, 0, 0, 0, 0);
  }

  // ---- bounded collection ----

  /**
   * Stages the bounded collection of logically expired results: from the
   * smallest expiry key while the budget lasts, each row is verified against
   * its result and the writer's lane before both rows are deleted and the
   * lane bytes returned. Whatever does not fit stays for a later entry; no
   * persistent "fully collected" watermark exists - the next pass resumes at
   * the smallest remaining key.
   */
  void stageGc(PreparedTransition transition, GcBudget budget, long logicalNow, ReadView view) {
    final RocksIterator iterator = view.iterator(MetadataCf.RESULT_EXPIRY);
    try {
      iterator.seekToFirst();
      while (iterator.isValid()) {
        final byte[] expiryKey = iterator.key();
        final long expiresAt = expiryAt(expiryKey);
        if (expiresAt > logicalNow) {
          break; // index order: every later row expires later
        }
        final byte[] resultKey = resultKeyOf(expiryKey);
        final long visitBytes = expiryKey.length
            + Optional.ofNullable(iterator.value()).map(v -> v.length).orElse(0)
            + resultKey.length;
        if (!budget.tryCharge(1, visitBytes)) {
          break;
        }
        final StoredResult result = StoredRecordCodec.decodeStoredResult(
            view.get(MetadataCf.COMMAND_RESULTS, resultKey));
        if (result.expiresAt() != expiresAt) {
          throw MetadataStoreException.corrupt("the expiry index says " + expiresAt
              + " but the result row expires at " + result.expiresAt());
        }
        final Lane lane = laneOf(resultKey, view);
        final byte[] encoded = StoredRecordCodec.storedResultValue(result);
        transition.maintenance().delete(MetadataCf.COMMAND_RESULTS, resultKey);
        transition.maintenance().delete(MetadataCf.RESULT_EXPIRY, expiryKey);
        transition.stageMaintenanceStatsDelta(
            laneCharge(lane, -(encoded.length + resultKey.length + expiryKey.length + 9)));
        iterator.next();
      }
      try {
        iterator.status();
      } catch (org.rocksdb.RocksDBException e) {
        throw MetadataStoreException.io("result-expiry scan ended in error", e);
      }
    } finally {
      iterator.close();
    }
  }

  private long expiryAt(byte[] expiryKey) {
    return ((long) (expiryKey[0] & 0xff) << 56) | ((long) (expiryKey[1] & 0xff) << 48)
        | ((long) (expiryKey[2] & 0xff) << 40) | ((long) (expiryKey[3] & 0xff) << 32)
        | ((long) (expiryKey[4] & 0xff) << 24) | ((long) (expiryKey[5] & 0xff) << 16)
        | ((long) (expiryKey[6] & 0xff) << 8) | (expiryKey[7] & 0xff);
  }

  private byte[] resultKeyOf(byte[] expiryKey) {
    return Arrays.copyOfRange(expiryKey, 8, expiryKey.length);
  }

  /**
   * @return the lane of the writer the result key names; the writer row
   *         never goes away, so a missing row is corruption
   */
  private Lane laneOf(byte[] resultKey, ReadView view) {
    final byte[] writerId = Arrays.copyOfRange(resultKey, 1, 17);
    final byte[] encoded = view.get(MetadataCf.WRITERS, StoredRecordCodec.writerKey(writerId));
    if (encoded == null) {
      throw MetadataStoreException.corrupt(
          "the writer of an expired result row has no writer row");
    }
    return StoredRecordCodec.decodeWriter(encoded).lane();
  }
}
