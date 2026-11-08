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

import org.apache.ratis.metadata.proto.MetadataProtos.ChangeCursor;
import org.apache.ratis.metadata.proto.MetadataProtos.ChangeEvent;
import org.apache.ratis.metadata.protocol.GroupConfig;
import org.rocksdb.RocksIterator;

/**
 * The change feed of one entry: events staged under {@code (revision,
 * ordinal)} with their per-revision summary, plus the two bounded maintenance
 * passes of section 4.2 - the floor advance that subtracts already-retired
 * visible bytes one whole revision at a time, and the physical collection
 * below the floor. The floor is pure replicated logic: it moves only over
 * revisions whose summary has been accounted for, whatever the disk still
 * holds.
 */
final class ChangeLog {
  /** The ordinal that marks a revision as fully consumed by a reader. */
  static final int END_REVISION = Integer.MAX_VALUE;
  /** The encoded width of one event row's key: revision plus ordinal. */
  private static final int EVENT_KEY_BYTES = 12;

  private final long revision;
  private final boolean maintenanceSide;
  private int eventCount;
  private long encodedBytes;

  ChangeLog(long revision, boolean maintenanceSide) {
    this.revision = revision;
    this.maintenanceSide = maintenanceSide;
  }

  /**
   * Stages one event under the entry's event ordinal - its own domain,
   * contiguous from 1, independent of the stamp ordinals.
   */
  void stageEvent(PreparedTransition transition, int ordinal, ChangeEvent event) {
    final byte[] encoded = event.toBuilder()
        .setCursor(ChangeCursor.newBuilder()
            .setRevision(revision).setOrdinal(ordinal))
        .build().toByteArray();
    buffer(transition).put(MetadataCf.CHANGES,
        StoredRecordCodec.changeKey(revision, ordinal), encoded);
    eventCount++;
    encodedBytes += encoded.length;
  }

  /** Stages the revision summary and the stats charge; a no-op without events. */
  void finish(PreparedTransition transition) {
    if (eventCount == 0) {
      return;
    }
    final byte[] summary = StoredRecordCodec.changeRevisionValue(eventCount, encodedBytes);
    buffer(transition).put(MetadataCf.CHANGE_REVISIONS,
        StoredRecordCodec.changeRevisionKey(revision), summary);
    // visible counts the events; physical also counts every event row's key
    // and the summary row with its own - the collection subtracts full rows
    final StoreStats delta = new StoreStats(0, 0, encodedBytes,
        encodedBytes + EVENT_KEY_BYTES * eventCount + summary.length + 8, 0, 0);
    if (maintenanceSide) {
      transition.stageMaintenanceStatsDelta(delta);
    } else {
      transition.stageStatsDelta(delta);
    }
  }

  private MutationBuffer buffer(PreparedTransition transition) {
    return maintenanceSide ? transition.maintenance() : transition.business();
  }

  /**
   * Advances the change floor toward the retention target
   * {@code max(oldFloor, index + 1 - retentionRevisions)}, subtracting each
   * revision's visible bytes before the floor may pass it. The per-entry
   * summary budget is shared with {@link #evictForCapacity} through
   * {@code summaryBudget} (one cell, counted down); when it runs out the
   * floor stops at the processed boundary and the rest waits for a later
   * entry.
   *
   * @return the floor this entry commits
   */
  static long advanceFloor(PreparedTransition transition, ReadView view, GroupConfig config,
      long index, int[] summaryBudget) {
    final long oldFloor = floorOf(transition);
    final long target = Math.max(oldFloor, index + 1 - config.changeRetentionRevisions());
    if (target <= oldFloor) {
      return oldFloor;
    }
    long floor = oldFloor;
    final RocksIterator summaries = view.iterator(MetadataCf.CHANGE_REVISIONS);
    try {
      summaries.seek(StoredRecordCodec.changeRevisionKey(oldFloor));
      while (summaries.isValid() && summaryBudget[0] > 0) {
        final long revisionOfRow = revisionOf(summaries.key());
        if (revisionOfRow >= target) {
          floor = target; // nothing in between holds visible bytes
          break;
        }
        final long[] summary =
            StoredRecordCodec.decodeChangeRevision(summaries.value());
        transition.stageMaintenanceStatsDelta(
            new StoreStats(0, 0, -summary[1], 0, 0, 0));
        floor = revisionOfRow + 1;
        summaryBudget[0]--;
        summaries.next();
      }
      if (!summaries.isValid() && floor < target) {
        // no summary anywhere below the target: the jump is free
        floor = target;
      }
      summaries.status();
    } catch (org.rocksdb.RocksDBException e) {
      throw MetadataStoreException.io("change-revisions scan failed", e);
    } finally {
      summaries.close();
    }
    transition.stageChangeFloor(floor);
    return floor;
  }

  /**
   * The byte-pressure escape of section 4.2: evicts the oldest
   * still-summarized revisions - whole revisions only, visible bytes
   * subtracted before each pass - until the visible change bytes fit the
   * capacity or the remaining summary budget runs out. Lease expiry's event
   * space must always be obtainable, so this runs before giving up on a
   * candidate; whether it moved far enough is the caller's check.
   */
  static void evictForCapacity(PreparedTransition transition, ReadView view,
      GroupConfig config, int[] summaryBudget) {
    final long capacity = config.changeCapacityBytes();
    long floor = floorOf(transition);
    final RocksIterator summaries = view.iterator(MetadataCf.CHANGE_REVISIONS);
    try {
      summaries.seek(StoredRecordCodec.changeRevisionKey(floor));
      while (visibleBytes(transition) > capacity && summaryBudget[0] > 0
          && summaries.isValid()) {
        final long revisionOfRow = revisionOf(summaries.key());
        final long[] summary =
            StoredRecordCodec.decodeChangeRevision(summaries.value());
        transition.stageMaintenanceStatsDelta(
            new StoreStats(0, 0, -summary[1], 0, 0, 0));
        floor = revisionOfRow + 1;
        transition.stageChangeFloor(floor);
        summaryBudget[0]--;
        summaries.next();
      }
      summaries.status();
    } catch (org.rocksdb.RocksDBException e) {
      throw MetadataStoreException.io("change-revisions pressure scan failed", e);
    } finally {
      summaries.close();
    }
  }

  /** @return the visible change bytes after this transition's deltas. */
  static long visibleBytes(PreparedTransition transition) {
    return transition.statsDelta().plus(transition.owner().stats()).visibleChangeBytes();
  }

  private static long floorOf(PreparedTransition transition) {
    return transition.changeFloor() >= 0
        ? transition.changeFloor() : transition.owner().changeFloor();
  }

  /**
   * Collects the physical event rows and summaries below the floor, at most
   * {@code changeGcRecords} rows / {@code changeGcBytes} bytes per entry.
   */
  static void collectPhysical(PreparedTransition transition, ReadView view, GroupConfig config,
      long floor) {
    final GcBudget budget = GcBudget.forChanges(config);
    collectBelow(transition, view, budget, floor, MetadataCf.CHANGES, true);
    collectBelow(transition, view, budget, floor, MetadataCf.CHANGE_REVISIONS, false);
  }

  private static void collectBelow(PreparedTransition transition, ReadView view,
      GcBudget budget, long floor, MetadataCf cf, boolean events) {
    final RocksIterator iterator = view.iterator(cf);
    try {
      iterator.seekToFirst();
      while (iterator.isValid()) {
        final byte[] key = iterator.key();
        if (revisionOf(key) >= floor) {
          break; // keys sort by revision first
        }
        final byte[] value = iterator.value();
        if (!budget.tryCharge(1, key.length + value.length)) {
          break;
        }
        transition.maintenance().delete(cf, key);
        transition.stageMaintenanceStatsDelta(
            new StoreStats(0, 0, 0, -(key.length + value.length), 0, 0));
        iterator.next();
      }
      iterator.status();
    } catch (org.rocksdb.RocksDBException e) {
      throw MetadataStoreException.io("change collection of " + cf + " failed", e);
    } finally {
      iterator.close();
    }
  }

  private static long revisionOf(byte[] key) {
    long revision = 0;
    for (int i = 0; i < 8; i++) {
      revision = (revision << 8) | (key[i] & 0xff);
    }
    return revision;
  }
}
