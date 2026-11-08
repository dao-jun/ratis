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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.rocksdb.RocksIterator;

/**
 * The state one entry executes against: reads pass through the business
 * buffer, then the maintenance buffer, then the committed {@link ReadView};
 * tombstones shadow the lower layers. Every staging method preserves the
 * store's atomic invariants - node bindings, the lease reverse index, lease
 * binding counts, the lease expiry index and the stats deltas - so business
 * and maintenance can never grow two divergent copies of that logic. The
 * {@code maintenanceSide} flag picks the buffer staged rows die with: a
 * business execution discards on deterministic failure, a lease expiry must
 * survive one.
 */
final class OverlayState {
  private final ReadView view;
  private final PreparedTransition transition;
  private final boolean maintenanceSide;

  OverlayState(ReadView view, PreparedTransition transition, boolean maintenanceSide) {
    this.view = view;
    this.transition = transition;
    this.maintenanceSide = maintenanceSide;
  }

  ReadView view() {
    return view;
  }

  PreparedTransition transition() {
    return transition;
  }

  // ---- read-through ----

  /** @return the raw value bytes after the overlay, or {@code null} when absent. */
  byte[] raw(MetadataCf cf, byte[] key) {
    final Optional<byte[]> business = transition.business().staged(cf, key);
    if (business != null) {
      return business.orElse(null);
    }
    final Optional<byte[]> maintenance = transition.maintenance().staged(cf, key);
    if (maintenance != null) {
      return maintenance.orElse(null);
    }
    return view.get(cf, key);
  }

  /** @return the node record after the overlay, or {@code null} when absent. */
  NodeRecord node(byte[] key) {
    final byte[] encoded = raw(MetadataCf.NODES, key);
    return encoded == null ? null : StoredRecordCodec.decodeNode(encoded);
  }

  /** @return the counter value after the overlay, or {@code null} when absent. */
  Long counter(byte[] counterKey) {
    final byte[] encoded = raw(MetadataCf.COUNTERS, counterKey);
    return encoded == null ? null : StoredRecordCodec.decodeCounter(encoded);
  }

  /** @return the writer row after the overlay, or {@code null} when absent. */
  WriterRow writer(byte[] writerId) {
    final byte[] key = StoredRecordCodec.writerKey(writerId);
    final byte[] encoded = raw(MetadataCf.WRITERS, key);
    return encoded == null ? null : StoredRecordCodec.decodeWriter(encoded);
  }

  /** @return the lease row after the overlay, or {@code null} when absent. */
  LeaseRow lease(long leaseId) {
    final byte[] key = StoredRecordCodec.leaseKey(leaseId);
    final byte[] encoded = raw(MetadataCf.LEASES, key);
    return encoded == null ? null : StoredRecordCodec.decodeLease(encoded);
  }

  /** @return whether the lease reverse index names this node. */
  boolean leasePathExists(long leaseId, byte[] nodeKey) {
    return raw(MetadataCf.LEASE_PATHS,
        StoredRecordCodec.leasePathKey(leaseId, nodeKey)) != null;
  }

  /**
   * Visits the merged rows of {@code [from, toExclusive)} in unsigned key
   * order: staged rows shadow committed rows of the same key - a business
   * row beats a maintenance row beats the committed state - and tombstones
   * hide the lower layers entirely. An infinite upper bound is a
   * {@code null} {@code toExclusive}.
   *
   * @return whether the walk ran to its natural end ({@code true}) or the
   *         visitor stopped it early ({@code false})
   */
  boolean scan(MetadataCf cf, byte[] from, byte[] toExclusive, RowVisitor visitor) {
    final RocksIterator committed = view.iterator(cf);
    try {
      committed.seek(from);
      final java.util.Iterator<Map.Entry<byte[], Optional<byte[]>>> overlayIt =
          transition.business().stagedRows(cf, from, toExclusive).entrySet().iterator();
      final java.util.Iterator<Map.Entry<byte[], Optional<byte[]>>> maintenanceIt =
          transition.maintenance().stagedRows(cf, from, toExclusive).entrySet().iterator();
      Map.Entry<byte[], Optional<byte[]>> overlayRow = nextOf(overlayIt);
      Map.Entry<byte[], Optional<byte[]>> maintenanceRow = nextOf(maintenanceIt);
      while (true) {
        // the next staged candidate is only peeked at, never consumed: a
        // committed row sorting ahead of it advances the committed iterator
        // alone, so the staged row stays a candidate for its own turn.
        // business wins a tie with maintenance.
        final Map.Entry<byte[], Optional<byte[]>> staged;
        final boolean stagedIsBusiness;
        if (overlayRow != null && maintenanceRow != null) {
          final int order = MutationBuffer.UNSIGNED_ORDER.compare(
              overlayRow.getKey(), maintenanceRow.getKey());
          stagedIsBusiness = order <= 0;
          staged = stagedIsBusiness ? overlayRow : maintenanceRow;
        } else if (overlayRow != null) {
          stagedIsBusiness = true;
          staged = overlayRow;
        } else if (maintenanceRow != null) {
          stagedIsBusiness = false;
          staged = maintenanceRow;
        } else {
          stagedIsBusiness = false;
          staged = null;
        }
        final byte[] committedKey;
        if (committed.isValid()
            && (toExclusive == null || MutationBuffer.UNSIGNED_ORDER.compare(
                committed.key(), toExclusive) < 0)) {
          committedKey = committed.key();
        } else {
          committedKey = null;
        }
        if (staged == null && committedKey == null) {
          break;
        }
        final int order = staged == null ? 1
            : committedKey == null ? -1
                : MutationBuffer.UNSIGNED_ORDER.compare(staged.getKey(), committedKey);
        if (order > 0) {
          if (!visitor.visit(committedKey, committed.value())) {
            return false;
          }
          committed.next();
          continue;
        }
        // the staged row is visited or skipped here, so it is consumed now
        if (stagedIsBusiness) {
          overlayRow = nextOf(overlayIt);
          if (maintenanceRow != null && MutationBuffer.UNSIGNED_ORDER.compare(
              staged.getKey(), maintenanceRow.getKey()) == 0) {
            maintenanceRow = nextOf(maintenanceIt); // business shadows its tie
          }
        } else {
          maintenanceRow = nextOf(maintenanceIt);
        }
        if (order == 0) {
          // the staged state replaces the committed row of the same key
          committed.next();
        }
        if (staged.getValue().isPresent()
            && !visitor.visit(staged.getKey(), staged.getValue().get())) {
          return false;
        }
      }
      committed.status();
      return true;
    } catch (org.rocksdb.RocksDBException e) {
      throw MetadataStoreException.io("merged scan of " + cf + " failed", e);
    } finally {
      committed.close();
    }
  }

  private static Map.Entry<byte[], Optional<byte[]>> nextOf(
      java.util.Iterator<Map.Entry<byte[], Optional<byte[]>>> iterator) {
    return iterator.hasNext() ? iterator.next() : null;
  }

  /** One merged row; returning {@code false} stops the walk. */
  interface RowVisitor {
    boolean visit(byte[] key, byte[] value);
  }

  // ---- staging: nodes ----

  /**
   * Stages one node record and rebinds its lease binding: the reverse index
   * rows and both binding counts move with the node, whatever the prior
   * state was.
   */
  void stageNode(byte[] key, NodeRecord record) {
    unbind(key);
    if (record.leaseId() != 0) {
      final LeaseRow lease = requireLease(record.leaseId());
      stageLease(record.leaseId(),
          new LeaseRow(lease.ownerWriterId(), lease.ownerGeneration(), lease.ttlMs(),
              lease.expireAt(), lease.lastRenewIndex(), lease.bindingCount() + 1));
      buffer().put(MetadataCf.LEASE_PATHS,
          StoredRecordCodec.leasePathKey(record.leaseId(), key),
          StoredRecordCodec.leasePathValue());
    }
    buffer().put(MetadataCf.NODES, key, StoredRecordCodec.nodeValue(record));
  }

  /**
   * Stages the deletion of one node, releasing its lease binding and count.
   */
  void stageNodeDelete(byte[] key) {
    unbind(key);
    buffer().delete(MetadataCf.NODES, key);
  }

  private void unbind(byte[] key) {
    final NodeRecord prior = node(key);
    if (prior == null || prior.leaseId() == 0) {
      return;
    }
    buffer().delete(MetadataCf.LEASE_PATHS,
        StoredRecordCodec.leasePathKey(prior.leaseId(), key));
    final LeaseRow lease = requireLease(prior.leaseId());
    if (lease.bindingCount() < 1) {
      throw MetadataStoreException.corrupt("lease " + prior.leaseId()
          + " has a binding at " + Arrays.toString(key) + " but count "
          + lease.bindingCount());
    }
    stageLease(prior.leaseId(),
        new LeaseRow(lease.ownerWriterId(), lease.ownerGeneration(), lease.ttlMs(),
            lease.expireAt(), lease.lastRenewIndex(), lease.bindingCount() - 1));
  }

  // ---- staging: counters ----

  void stageCounter(byte[] counterKey, long value) {
    buffer().put(MetadataCf.COUNTERS, counterKey, StoredRecordCodec.counterValue(value));
  }

  void stageCounterDelete(byte[] counterKey) {
    if (counter(counterKey) != null) {
      buffer().delete(MetadataCf.COUNTERS, counterKey);
    }
  }

  // ---- staging: writers ----

  void stageWriter(byte[] writerId, WriterRow row) {
    buffer().put(MetadataCf.WRITERS,
        StoredRecordCodec.writerKey(writerId), StoredRecordCodec.writerValue(row));
  }

  // ---- staging: leases ----

  /**
   * Stages one lease row and moves its expiry-index row when - and only when
   * - the expiry moved: a renew without extension touches neither the index
   * nor {@code lastRenewIndex}.
   */
  void stageLease(long leaseId, LeaseRow row) {
    final LeaseRow prior = lease(leaseId);
    if (prior != null && prior.equals(row)) {
      return; // nothing moves, not even the index value
    }
    final boolean expiryMoved = prior == null || prior.expireAt() != row.expireAt();
    if (prior == null) {
      stageLeaseCount(1);
    } else if (expiryMoved) {
      buffer().delete(MetadataCf.LEASE_EXPIRY,
          StoredRecordCodec.leaseExpiryKey(prior.expireAt(), leaseId));
    }
    if (expiryMoved) {
      buffer().put(MetadataCf.LEASE_EXPIRY,
          StoredRecordCodec.leaseExpiryKey(row.expireAt(), leaseId),
          StoredRecordCodec.leaseExpiryValue(row.lastRenewIndex()));
    }
    buffer().put(MetadataCf.LEASES,
        StoredRecordCodec.leaseKey(leaseId), StoredRecordCodec.leaseValue(row));
  }

  /**
   * Stages the deletion of one lease row and its expiry-index row. The
   * caller owns the bound nodes: it collects them first via
   * {@link #leaseBoundNodes(long)} and deletes each with its own event.
   */
  void stageLeaseDelete(long leaseId) {
    final LeaseRow prior = lease(leaseId);
    if (prior == null) {
      return;
    }
    buffer().delete(MetadataCf.LEASES, StoredRecordCodec.leaseKey(leaseId));
    buffer().delete(MetadataCf.LEASE_EXPIRY,
        StoredRecordCodec.leaseExpiryKey(prior.expireAt(), leaseId));
    stageLeaseCount(-1);
  }

  /**
   * @return every node the lease reverse index names, node-key sorted - the
   *         deletion order bindings must take so events and digests never
   *         depend on hash iteration
   */
  List<byte[]> leaseBoundNodes(long leaseId) {
    final List<byte[]> nodes = new ArrayList<>();
    final byte[] prefix = StoredRecordCodec.leaseKey(leaseId);
    scan(MetadataCf.LEASE_PATHS, prefix, upperExclusive(prefix), (key, value) -> {
      final byte[] nodeKey = Arrays.copyOfRange(key, 8, key.length);
      if (node(nodeKey) == null) {
        throw MetadataStoreException.corrupt("the reverse index of lease " + leaseId
            + " names " + Arrays.toString(nodeKey) + " but no node exists");
      }
      nodes.add(nodeKey);
      return true;
    });
    return nodes;
  }

  private LeaseRow requireLease(long leaseId) {
    final LeaseRow lease = lease(leaseId);
    if (lease == null) {
      throw MetadataStoreException.corrupt("node binding names lease " + leaseId
          + " but the lease row is missing");
    }
    return lease;
  }

  private MutationBuffer buffer() {
    return maintenanceSide ? transition.maintenance() : transition.business();
  }

  private void stageLeaseCount(long delta) {
    final StoreStats stats = new StoreStats(0, 0, 0, 0, 0, delta);
    if (maintenanceSide) {
      transition.stageMaintenanceStatsDelta(stats);
    } else {
      transition.stageStatsDelta(stats);
    }
  }

  /** The exclusive upper bound covering every key with this prefix. */
  static byte[] upperExclusive(byte[] prefix) {
    for (int i = prefix.length - 1; i >= 0; i--) {
      if ((prefix[i] & 0xff) != 0xff) {
        final byte[] upper = prefix.clone();
        upper[i] = (byte) (upper[i] + 1);
        return Arrays.copyOf(upper, i + 1);
      }
    }
    return null; // all-0xff prefix runs to the end of the keyspace
  }
}
