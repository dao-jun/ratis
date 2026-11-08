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
import java.util.List;

import org.apache.ratis.metadata.proto.MetadataProtos.BusinessResult;
import org.apache.ratis.metadata.proto.MetadataProtos.LeaseOpen;
import org.apache.ratis.metadata.proto.MetadataProtos.LeaseRenew;
import org.apache.ratis.metadata.proto.MetadataProtos.LeaseResult;
import org.apache.ratis.metadata.proto.MetadataProtos.LeaseRevoke;
import org.apache.ratis.metadata.proto.MetadataProtos.Lane;
import org.apache.ratis.metadata.proto.MetadataProtos.Operation;
import org.apache.ratis.metadata.proto.MetadataProtos.Status;
import org.apache.ratis.metadata.proto.MetadataProtos.WriterRef;
import org.apache.ratis.metadata.protocol.GroupConfig;

/**
 * The control lane's three lease commands. The lease id is the opening
 * entry's index, the owner is a business writer fenced by its generation,
 * and a revocation - explicit or by expiry - deletes every bound node with
 * its own feed event. The expiry candidate runs on a maintenance-side
 * {@link OpExecution}, so what it stages survives a concurrent business
 * failure in the same entry.
 */
final class LeaseOps {
  private LeaseOps() {
  }

  /** Applies one lease command; the caller is an authenticated control writer. */
  static BusinessResult.Builder apply(OpExecution exec, Operation op) {
    switch (op.getCommandCase()) {
      case LEASE_OPEN:
        return open(exec, op.getLeaseOpen());
      case LEASE_RENEW:
        return renew(exec, op.getLeaseRenew());
      case LEASE_REVOKE:
        return revoke(exec, op.getLeaseRevoke());
      default:
        return BusinessResult.newBuilder().setStatus(Status.S_NOT_ALLOWED);
    }
  }

  private static BusinessResult.Builder open(OpExecution exec, LeaseOpen op) {
    final GroupConfig config = exec.entry().config();
    if (op.getTtlMs() < config.leaseMinTtlMs() || op.getTtlMs() > config.leaseMaxTtlMs()) {
      return failed(Status.S_BAD_TTL);
    }
    final byte[] owner = op.getOwner().getWriterId().toByteArray();
    final WriterRow row = owner.length == LeaseRow.WRITER_ID_BYTES
        ? exec.overlay().writer(owner) : null;
    if (row == null) {
      return failed(Status.S_UNKNOWN_WRITER);
    }
    if (row.lane() != Lane.BUSINESS
        || row.currentGeneration() != op.getOwner().getGeneration()) {
      return failed(Status.S_STALE_GENERATION); // a stale owner opens nothing
    }
    final long leaseId = exec.entry().index();
    if (exec.overlay().lease(leaseId) != null) {
      return failed(Status.S_NODE_EXISTS); // defensive: ids are entry indices
    }
    if (leaseCount(exec.overlay()) >= config.activeLeases()) {
      return failed(Status.S_LEASE_LIMIT);
    }
    final long expireAt;
    try {
      expireAt = Math.addExact(exec.entry().logicalNow(), op.getTtlMs());
    } catch (ArithmeticException e) {
      return failed(Status.S_BAD_TTL);
    }
    final LeaseRow lease = new LeaseRow(owner, row.currentGeneration(), op.getTtlMs(),
        expireAt, 0, 0);
    exec.overlay().stageLease(leaseId, lease);
    return leaseResult(leaseId, op.getOwner(), lease, 0);
  }

  private static BusinessResult.Builder renew(OpExecution exec, LeaseRenew op) {
    final GroupConfig config = exec.entry().config();
    if (op.getTtlMs() < config.leaseMinTtlMs() || op.getTtlMs() > config.leaseMaxTtlMs()) {
      return failed(Status.S_BAD_TTL);
    }
    final LeaseRow lease = exec.overlay().lease(op.getLeaseId());
    if (lease == null) {
      return failed(Status.S_UNKNOWN_LEASE);
    }
    if (!Arrays.equals(op.getOwner().getWriterId().toByteArray(), lease.ownerWriterId())
        || op.getOwner().getGeneration() != lease.ownerGeneration()) {
      return failed(Status.S_NOT_OWNER);
    }
    // the owner must still be the current generation - a takeover fences the lease
    final WriterRow row = exec.overlay().writer(lease.ownerWriterId());
    if (row == null || row.currentGeneration() != lease.ownerGeneration()) {
      return failed(Status.S_NOT_OWNER);
    }
    if (exec.entry().logicalNow() >= lease.expireAt()) {
      return failed(Status.S_LEASE_EXPIRED); // an expired lease never revives
    }
    final long extended;
    try {
      extended = Math.addExact(exec.entry().logicalNow(), op.getTtlMs());
    } catch (ArithmeticException e) {
      return failed(Status.S_BAD_TTL);
    }
    final long newExpiry = Math.max(lease.expireAt(), extended);
    if (newExpiry == lease.expireAt()) {
      // a shorter ttl never shortens the lease and touches nothing at all
      return leaseResult(op.getLeaseId(), op.getOwner(), lease, lease.bindingCount());
    }
    final LeaseRow renewed = new LeaseRow(lease.ownerWriterId(), lease.ownerGeneration(),
        op.getTtlMs(), newExpiry, exec.entry().index(), lease.bindingCount());
    exec.overlay().stageLease(op.getLeaseId(), renewed);
    return leaseResult(op.getLeaseId(), op.getOwner(), renewed, renewed.bindingCount());
  }

  private static BusinessResult.Builder revoke(OpExecution exec, LeaseRevoke op) {
    final LeaseRow lease = exec.overlay().lease(op.getLeaseId());
    if (lease == null) {
      // idempotent: nothing to release, nothing deleted
      return BusinessResult.newBuilder().setStatus(Status.S_OK)
          .setLease(LeaseResult.newBuilder()
              .setLeaseId(op.getLeaseId())
              .setOwner(op.getOwner())
              .setDeletedCount(0));
    }
    if (!Arrays.equals(op.getOwner().getWriterId().toByteArray(), lease.ownerWriterId())) {
      return failed(Status.S_NOT_OWNER); // an old generation may revoke, a stranger may not
    }
    return deleteLease(exec, op.getLeaseId(), lease, op.getOwner());
  }

  /**
   * The expiry candidate: revoked with the current state, never the
   * scheduler's stale view. Any validation failure is a no-op, not an error.
   *
   * @return whether the expiry was staged
   */
  static boolean expire(OpExecution exec, long leaseId, long expectedLastRenewIndex) {
    final LeaseRow lease = exec.overlay().lease(leaseId);
    if (lease == null || lease.lastRenewIndex() != expectedLastRenewIndex
        || exec.entry().logicalNow() < lease.expireAt()) {
      return false;
    }
    deleteLease(exec, leaseId, lease, ownerRef(lease));
    return true;
  }

  /** Deletes the lease and every bound node, each with its own NODE_DELETE. */
  private static BusinessResult.Builder deleteLease(OpExecution exec, long leaseId,
      LeaseRow lease, WriterRef owner) {
    final List<byte[]> bound = exec.overlay().leaseBoundNodes(leaseId);
    for (byte[] nodeKey : bound) {
      NodeOps.deleteOne(exec, nodeKey, exec.overlay().node(nodeKey), true);
    }
    exec.overlay().stageLeaseDelete(leaseId);
    return leaseResult(leaseId, owner, lease, bound.size());
  }

  private static WriterRef ownerRef(LeaseRow lease) {
    return WriterRef.newBuilder()
        .setWriterId(org.apache.ratis.thirdparty.com.google.protobuf.ByteString
            .copyFrom(lease.ownerWriterId()))
        .setGeneration(lease.ownerGeneration())
        .build();
  }

  private static BusinessResult.Builder leaseResult(long leaseId, WriterRef owner,
      LeaseRow lease, long deletedCount) {
    return BusinessResult.newBuilder().setStatus(Status.S_OK)
        .setLease(LeaseResult.newBuilder()
            .setLeaseId(leaseId)
            .setOwner(owner)
            .setTtlMs(lease.ttlMs())
            .setExpireAt(lease.expireAt())
            .setLastRenewIndex(lease.lastRenewIndex())
            .setDeletedCount(deletedCount));
  }

  private static BusinessResult.Builder failed(Status status) {
    return BusinessResult.newBuilder().setStatus(status);
  }

  private static long leaseCount(OverlayState overlay) {
    return overlay.transition().statsDelta()
        .plus(overlay.transition().owner().stats()).leaseCount();
  }
}
