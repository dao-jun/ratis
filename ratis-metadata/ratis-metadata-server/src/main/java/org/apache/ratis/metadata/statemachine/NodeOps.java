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

import org.apache.ratis.metadata.proto.MetadataProtos.BusinessResult;
import org.apache.ratis.metadata.proto.MetadataProtos.Cas;
import org.apache.ratis.metadata.proto.MetadataProtos.ChangeEvent;
import org.apache.ratis.metadata.proto.MetadataProtos.ChangeKind;
import org.apache.ratis.metadata.proto.MetadataProtos.Create;
import org.apache.ratis.metadata.proto.MetadataProtos.Delete;
import org.apache.ratis.metadata.proto.MetadataProtos.ExpectedVersion;
import org.apache.ratis.metadata.proto.MetadataProtos.Increment;
import org.apache.ratis.metadata.proto.MetadataProtos.LockToken;
import org.apache.ratis.metadata.proto.MetadataProtos.Multi;
import org.apache.ratis.metadata.proto.MetadataProtos.MultiResult;
import org.apache.ratis.metadata.proto.MetadataProtos.MutationResult;
import org.apache.ratis.metadata.proto.MetadataProtos.NodeKind;
import org.apache.ratis.metadata.proto.MetadataProtos.Operation;
import org.apache.ratis.metadata.proto.MetadataProtos.Put;
import org.apache.ratis.metadata.proto.MetadataProtos.Role;
import org.apache.ratis.metadata.proto.MetadataProtos.Status;
import org.apache.ratis.metadata.protocol.Stamp;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/**
 * The node and counter semantics of section 4: the COORDINATION tree with
 * parents, stamps, lease bindings, sequential children, typed locks and the
 * recursive delete, and the flat KEYSPACE value store. Every effect goes
 * through the overlay's invariant-preserving staging; every check runs
 * against the overlay, so MULTI sub-operations see their predecessors.
 */
final class NodeOps {
  private static final byte[] ROOT = new byte[] {'/'};
  private static final int SEQUENTIAL_WIDTH = 20;

  private NodeOps() {
  }

  /** Applies one node-side operation; MULTI included. */
  static BusinessResult.Builder apply(OpExecution exec, Operation operation) {
    switch (operation.getCommandCase()) {
      case PUT:
        return put(exec, operation.getPut());
      case CREATE:
        return create(exec, operation.getCreate());
      case CAS:
        return cas(exec, operation.getCas());
      case DELETE:
        return delete(exec, operation.getDelete());
      case INCREMENT:
        return increment(exec, operation.getIncrement());
      case MULTI:
        return multi(exec, operation.getMulti());
      default:
        return failure(Status.S_NOT_ALLOWED);
    }
  }

  // ---- PUT / CAS ----

  private static BusinessResult.Builder put(OpExecution exec, Put op) {
    return mutate(exec, op.getKey().toByteArray(), op.getValue().toByteArray(),
        op.getLeaseId(), null);
  }

  private static BusinessResult.Builder cas(OpExecution exec, Cas op) {
    final ExpectedVersion expected = op.getExpected();
    if (expected.getPredicateCase() == ExpectedVersion.PredicateCase.PREDICATE_NOT_SET) {
      return failure(Status.S_MALFORMED);
    }
    final Stamp exact = expected.getPredicateCase() == ExpectedVersion.PredicateCase.EXACT
        ? Stamp.valueOf(expected.getExact().getLogIndex(), expected.getExact().getOrdinal())
        : null;
    return mutate(exec, op.getKey().toByteArray(), op.getValue().toByteArray(),
        op.getLeaseId(), exact);
  }

  /**
   * The shared upsert of PUT and CAS: the CAS precondition runs before any
   * effect is staged, the lease binding moves with the node, and the modify
   * stamp takes this entry's next ordinal.
   */
  private static BusinessResult.Builder mutate(OpExecution exec, byte[] key, byte[] value,
      long leaseId, Stamp expectedModifyStamp) {
    final OverlayState overlay = exec.overlay();
    final boolean tree = exec.entry().config().role() == Role.COORDINATION;
    final NodeRecord prior = overlay.node(key);
    if (expectedModifyStamp != null
        && (prior == null || !prior.modifyStamp().equals(expectedModifyStamp))) {
      return failure(Status.S_VERSION_MISMATCH); // a missing node with EXACT mismatches too
    }
    if (tree && Arrays.equals(key, ROOT)) {
      return failure(Status.S_NOT_ALLOWED);
    }
    if (tree && reservedSequentialLeaf(key)) {
      return failure(Status.S_BAD_PATH); // the "~"+20-digit namespace is the counter's
    }
    if (prior != null && prior.kind() == NodeKind.EPHEMERAL_LOCK) {
      return failure(Status.S_NOT_ALLOWED);
    }
    if (prior == null && tree) {
      final Status parent = checkParent(overlay, key);
      if (parent != null) {
        return failure(parent);
      }
    }
    if (leaseId != 0) {
      if (!tree) {
        return failure(Status.S_NOT_ALLOWED); // leases are a coordination concept
      }
      final LeaseRow lease = overlay.lease(leaseId);
      if (lease == null) {
        return failure(Status.S_UNKNOWN_LEASE);
      }
      if (exec.entry().logicalNow() >= lease.expireAt()) {
        return failure(Status.S_LEASE_EXPIRED); // an expired lease binds nothing new
      }
      if (prior != null && hasDescendants(overlay, key)) {
        return failure(Status.S_HAS_CHILDREN); // an existing node binds only childless
      }
      if ((prior == null || prior.leaseId() != leaseId)
          && lease.bindingCount() >= exec.entry().config().leaseBindings()) {
        return failure(Status.S_LEASE_LIMIT); // the binding slots of this lease are full
      }
    }
    final Stamp stamp = Stamp.valueOf(exec.entry().index(), exec.nextStampOrdinal());
    final NodeRecord.Builder record = NodeRecord.newBuilder()
        .setKind(NodeKind.NORMAL)
        .setCreateStamp(prior != null ? prior.createStamp() : stamp)
        .setModifyStamp(stamp)
        .setCreatedAt(prior != null ? prior.createdAt() : exec.entry().logicalNow())
        .setModifiedAt(exec.entry().logicalNow())
        .setValue(value);
    if (leaseId != 0) {
      final LeaseRow lease = overlay.lease(leaseId);
      record.setLeaseOwnership(leaseId, lease.ownerWriterId(), lease.ownerGeneration());
    }
    overlay.stageNode(key, record.build());
    stageNodeEvent(exec, exec.nextEventOrdinal(),
        prior != null ? ChangeKind.NODE_UPDATE : ChangeKind.NODE_CREATE,
        key, value, stamp, leaseId);
    return mutation(key, stamp, 0, null);
  }

  /** @return the parent rejection, or {@code null} when the parent is usable. */
  private static Status checkParent(OverlayState overlay, byte[] key) {
    final byte[] parentKey = parentOf(key);
    if (parentKey == null) {
      return Status.S_BAD_PATH; // the root itself is never created through here
    }
    if (Arrays.equals(parentKey, ROOT)) {
      return null; // the virtual root parents every top-level node
    }
    final NodeRecord parent = overlay.node(parentKey);
    if (parent == null) {
      return Status.S_NO_PARENT;
    }
    if (parent.kind() != NodeKind.NORMAL || parent.leaseId() != 0) {
      return Status.S_NO_PARENT; // ephemeral and lock parents hold no children
    }
    return null;
  }

  // ---- CREATE ----

  private static BusinessResult.Builder create(OpExecution exec, Create op) {
    final byte[] path = op.getPath().toByteArray();
    if (exec.entry().config().role() != Role.COORDINATION) {
      return failure(Status.S_NOT_ALLOWED);
    }
    if (Arrays.equals(path, ROOT)) {
      return failure(Status.S_NOT_ALLOWED);
    }
    if (op.getSequential()) {
      return createSequential(exec, path, op);
    }
    if (reservedSequentialLeaf(path)) {
      return failure(Status.S_BAD_PATH); // the "~"+20-digit namespace is the counter's
    }
    if (exec.overlay().node(path) != null) {
      return failure(Status.S_NODE_EXISTS);
    }
    if (op.getKind() == NodeKind.EPHEMERAL_LOCK) {
      return createLock(exec, path, op);
    }
    final Status lease = checkBindableLease(exec, path, op.getLeaseId());
    if (lease != null) {
      return failure(lease);
    }
    final BusinessResult.Builder chain = createParents(exec, path, op.getCreateParents());
    if (chain != null) {
      return chain;
    }
    return stageCreate(exec, path, op.getValue().toByteArray(), op.getLeaseId(),
        NodeKind.NORMAL, 0);
  }

  /** The typed lock: one entry, one token, the lease decides the owner. */
  private static BusinessResult.Builder createLock(OpExecution exec, byte[] path, Create op) {
    if (op.getLeaseId() < 1) {
      return failure(Status.S_NOT_ALLOWED);
    }
    final LeaseRow lease = exec.overlay().lease(op.getLeaseId());
    if (lease == null) {
      return failure(Status.S_UNKNOWN_LEASE);
    }
    if (exec.entry().logicalNow() >= lease.expireAt()) {
      return failure(Status.S_LEASE_EXPIRED);
    }
    if (lease.bindingCount() >= exec.entry().config().leaseBindings()) {
      return failure(Status.S_LEASE_LIMIT); // a lock occupies one binding slot too
    }
    final BusinessResult.Builder chain = createParents(exec, path, true);
    if (chain != null) {
      return chain;
    }
    final BusinessResult.Builder result = stageCreate(exec, path, new byte[0],
        op.getLeaseId(), NodeKind.EPHEMERAL_LOCK, exec.entry().index());
    if (result.getStatus() == Status.S_OK) {
      result.getMutationBuilder().setLockToken(LockToken.newBuilder()
          .setGroupId(exec.entry().groupId().toByteString())
          .setLogIndex(exec.entry().index()));
    }
    return result;
  }

  private static BusinessResult.Builder createSequential(OpExecution exec, byte[] base,
      Create op) {
    if (op.getKind() == NodeKind.EPHEMERAL_LOCK) {
      return failure(Status.S_NOT_ALLOWED); // locks never join a sequence
    }
    final byte[] parentKey = parentOf(base);
    if (parentKey == null) {
      return failure(Status.S_BAD_PATH);
    }
    if (!Arrays.equals(parentKey, ROOT)) {
      final BusinessResult.Builder chain =
          createParents(exec, base, op.getCreateParents());
      if (chain != null) {
        return chain;
      }
    }
    final Stamp parentStamp;
    if (Arrays.equals(parentKey, ROOT)) {
      parentStamp = exec.overlay().transition().owner().rootStamp();
    } else {
      final NodeRecord parent = exec.overlay().node(parentKey);
      if (parent == null) {
        return failure(Status.S_NO_PARENT);
      }
      if (parent.kind() != NodeKind.NORMAL || parent.leaseId() != 0) {
        return failure(Status.S_NO_PARENT);
      }
      parentStamp = parent.createStamp();
    }
    final byte[] counterKey = StoredRecordCodec.seqCounterKey(parentKey, parentStamp);
    final Long current = exec.overlay().counter(counterKey);
    if (current != null && current == Long.MAX_VALUE) {
      return failure(Status.S_NUMERIC_OVERFLOW); // the parent lifecycle is exhausted
    }
    final long sequence = (current == null ? 0 : current) + 1;
    final byte[] path = sequentialPath(base, sequence);
    if (exec.overlay().node(path) != null) {
      return failure(Status.S_NODE_EXISTS); // the reserved suffix should prevent this
    }
    final Status lease = checkBindableLease(exec, path, op.getLeaseId());
    if (lease != null) {
      return failure(lease);
    }
    exec.overlay().stageCounter(counterKey, sequence);
    final BusinessResult.Builder result = stageCreate(exec, path,
        op.getValue().toByteArray(), op.getLeaseId(), NodeKind.NORMAL, 0);
    // the caller learns the generated path through the mutation key
    result.getMutationBuilder().setKey(ByteString.copyFrom(path));
    return result;
  }

  /**
   * Builds the missing ancestors of the path top-down as empty persistent
   * nodes; a failure anywhere discards the whole business buffer, so no
   * partial chain can survive.
   *
   * @return the failure reply, or {@code null} when the parent chain is ready
   */
  private static BusinessResult.Builder createParents(OpExecution exec, byte[] path,
      boolean createParents) {
    final List<byte[]> missing = new ArrayList<>();
    byte[] parent = parentOf(path);
    while (parent != null && !Arrays.equals(parent, ROOT)
        && exec.overlay().node(parent) == null) {
      missing.add(0, parent);
      parent = parentOf(parent);
    }
    if (!missing.isEmpty() && !createParents) {
      return failure(Status.S_NO_PARENT);
    }
    for (byte[] ancestor : missing) {
      final Status rejection = checkParent(exec.overlay(), ancestor);
      if (rejection != null) {
        return failure(rejection);
      }
      stageCreate(exec, ancestor, new byte[0], 0, NodeKind.NORMAL, 0);
    }
    return null;
  }

  private static BusinessResult.Builder stageCreate(OpExecution exec, byte[] path,
      byte[] value, long leaseId, NodeKind kind, long lockIndex) {
    byte[] ownerWriterId = new byte[16];
    long ownerGeneration = 0;
    if (leaseId != 0) {
      // a bound create's owner is its lease row's owner, resolved at stage time
      final LeaseRow lease = exec.overlay().lease(leaseId);
      ownerWriterId = lease.ownerWriterId();
      ownerGeneration = lease.ownerGeneration();
    }
    final Stamp stamp = Stamp.valueOf(exec.entry().index(), exec.nextStampOrdinal());
    final NodeRecord.Builder record = NodeRecord.newBuilder()
        .setKind(kind)
        .setCreateStamp(stamp)
        .setModifyStamp(stamp)
        .setLeaseOwnership(leaseId, ownerWriterId, ownerGeneration)
        .setLockIndex(lockIndex)
        .setCreatedAt(exec.entry().logicalNow())
        .setModifiedAt(exec.entry().logicalNow())
        .setValue(value);
    exec.overlay().stageNode(path, record.build());
    stageNodeEvent(exec, exec.nextEventOrdinal(), ChangeKind.NODE_CREATE, path, value, stamp,
        leaseId);
    return mutation(path, stamp, 0, null);
  }

  // ---- DELETE ----

  private static BusinessResult.Builder delete(OpExecution exec, Delete op) {
    final byte[] key = op.getKey().toByteArray();
    if (op.getRecursive()) {
      return deleteSubtree(exec, key);
    }
    final OverlayState overlay = exec.overlay();
    final boolean tree = exec.entry().config().role() == Role.COORDINATION;
    if (tree && Arrays.equals(key, ROOT)) {
      return failure(Status.S_NOT_ALLOWED);
    }
    final NodeRecord prior = overlay.node(key);
    if (prior == null) {
      return failure(Status.S_NOT_FOUND); // a late lock release completes idempotently
    }
    if (prior.kind() == NodeKind.EPHEMERAL_LOCK) {
      if (!op.hasLockOwner()) {
        return failure(Status.S_NOT_ALLOWED); // a lock dies only by owner proof
      }
      final Status owner = checkLockOwner(prior, op.getLockOwner());
      if (owner != null) {
        return failure(owner);
      }
    } else if (op.hasLockOwner()) {
      return failure(Status.S_NOT_OWNER);
    }
    if (tree && hasDescendants(overlay, key)) {
      return failure(Status.S_HAS_CHILDREN);
    }
    deleteOne(exec, key, prior, true);
    return mutation(key, prior.modifyStamp(), 1, null);
  }

  /** @return the owner rejection, or {@code null} when the proof matches. */
  private static Status checkLockOwner(NodeRecord lock,
      org.apache.ratis.metadata.proto.MetadataProtos.LockOwner owner) {
    if (!Arrays.equals(owner.getOwner().getWriterId().toByteArray(), lock.ownerWriterId())
        || owner.getOwner().getGeneration() != lock.ownerGeneration()
        || owner.getLeaseId() != lock.leaseId()
        || !owner.hasToken()
        || owner.getToken().getLogIndex() != lock.lockIndex()) {
      return Status.S_NOT_OWNER; // an old release never deletes a new lock
    }
    return null;
  }

  private static BusinessResult.Builder deleteSubtree(OpExecution exec, byte[] key) {
    if (exec.entry().config().role() != Role.COORDINATION) {
      return failure(Status.S_NOT_ALLOWED);
    }
    if (Arrays.equals(key, ROOT)) {
      return failure(Status.S_NOT_ALLOWED);
    }
    final OverlayState overlay = exec.overlay();
    final NodeRecord subtreeRoot = overlay.node(key);
    if (subtreeRoot == null) {
      return failure(Status.S_NOT_FOUND);
    }
    final int budget = exec.entry().config().recursiveNodes();
    final List<byte[]> victims = new ArrayList<>();
    victims.add(key);
    final byte[] children = childPrefix(key);
    final boolean complete = overlay.scan(MetadataCf.NODES, children,
        OverlayState.upperExclusive(children), (rowKey, rowValue) -> {
          if (victims.size() >= budget) {
            return false;
          }
          victims.add(rowKey);
          return true;
        });
    if (!complete) {
      return failure(Status.S_LIMIT_EXCEEDED); // the whole operation fails, nothing staged
    }
    for (byte[] victim : victims) {
      deleteOne(exec, victim, overlay.node(victim), false);
    }
    exec.changes().stageEvent(exec.overlay().transition(), exec.nextEventOrdinal(),
        ChangeEvent.newBuilder()
            .setKind(ChangeKind.DELETE_SUBTREE)
            .setPath(ByteString.copyFrom(key))
            .build());
    return mutation(key, subtreeRoot.modifyStamp(), victims.size(), null);
  }

  /**
   * Deletes one node and its lifecycle counter. A plain delete emits the
   * node event; the subtree delete replaces its descendants' events with
   * one prefix event, so {@code withEvent} is false for its victims. The
   * lease paths stage the same deletion, event included.
   */
  static void deleteOne(OpExecution exec, byte[] key, NodeRecord prior, boolean withEvent) {
    exec.overlay().stageNodeDelete(key);
    exec.overlay().stageCounterDelete(
        StoredRecordCodec.seqCounterKey(key, prior.createStamp()));
    if (withEvent) {
      exec.changes().stageEvent(exec.overlay().transition(), exec.nextEventOrdinal(),
          ChangeEvent.newBuilder()
              .setKind(ChangeKind.NODE_DELETE)
              .setPath(ByteString.copyFrom(key))
              .setVersion(toProto(prior.modifyStamp()))
              .setLeaseId(prior.leaseId())
              .build());
    }
  }

  // ---- INCREMENT ----

  private static BusinessResult.Builder increment(OpExecution exec, Increment op) {
    if (exec.entry().config().role() != Role.COORDINATION) {
      return failure(Status.S_NOT_ALLOWED); // named counters are a coordination concept
    }
    final byte[] name = op.getName().toByteArray();
    final byte[] counterKey = StoredRecordCodec.userCounterKey(name);
    final Long current = exec.overlay().counter(counterKey);
    final long next;
    try {
      next = Math.addExact(current == null ? 0L : current, op.getDelta());
    } catch (ArithmeticException e) {
      return failure(Status.S_NUMERIC_OVERFLOW);
    }
    exec.overlay().stageCounter(counterKey, next);
    return BusinessResult.newBuilder()
        .setStatus(Status.S_OK)
        .setMutation(MutationResult.newBuilder()
            .setKey(ByteString.copyFrom(name))
            .setCounterValue(next));
  }

  // ---- MULTI ----

  private static BusinessResult.Builder multi(OpExecution exec, Multi op) {
    final MultiResult.Builder results = MultiResult.newBuilder();
    final long mutationBudget = exec.entry().config().mutationNodes();
    long mutations = 0;
    for (int i = 0; i < op.getOperationsCount(); i++) {
      if (mutations >= mutationBudget) {
        return multiFailed(results, i, Status.S_LIMIT_EXCEEDED);
      }
      final Operation sub = op.getOperations(i);
      switch (sub.getCommandCase()) {
        case PUT: case CREATE: case CAS: case DELETE: case INCREMENT:
          break; // the five node mutations MULTI may carry
        default:
          // no nested MULTI, no lease commands, and below: no LOCK
          return multiFailed(results, i, Status.S_NOT_ALLOWED);
      }
      if (sub.getCommandCase() == Operation.CommandCase.CREATE
          && sub.getCreate().getKind() == NodeKind.EPHEMERAL_LOCK) {
        return multiFailed(results, i, Status.S_NOT_ALLOWED);
      }
      final BusinessResult.Builder outcome = apply(exec, sub);
      if (outcome.getStatus() != Status.S_OK) {
        return multiFailed(results, i, outcome.getStatus());
      }
      results.addResults(BusinessResult.newBuilder()
          .setStatus(Status.S_OK)
          .setMutation(outcome.getMutationBuilder()));
      mutations++;
    }
    return BusinessResult.newBuilder().setStatus(Status.S_OK).setMulti(results);
  }

  private static BusinessResult.Builder multiFailed(MultiResult.Builder partial, int index,
      Status status) {
    return BusinessResult.newBuilder()
        .setStatus(Status.S_MULTI_FAILED)
        .setMulti(partial.setFailIndex(index).setFailStatus(status));
  }

  // ---- shared helpers ----

  /** @return the lease rejection, or {@code null} when the lease may bind. */
  private static Status checkBindableLease(OpExecution exec, byte[] key, long leaseId) {
    if (leaseId == 0) {
      return null;
    }
    final LeaseRow lease = exec.overlay().lease(leaseId);
    if (lease == null) {
      return Status.S_UNKNOWN_LEASE;
    }
    if (exec.entry().logicalNow() >= lease.expireAt()) {
      return Status.S_LEASE_EXPIRED;
    }
    final NodeRecord prior = exec.overlay().node(key);
    if (prior != null && prior.leaseId() == leaseId) {
      return null; // a rewrite of the same binding keeps its own slot
    }
    if (lease.bindingCount() >= exec.entry().config().leaseBindings()) {
      return Status.S_LEASE_LIMIT;
    }
    return null;
  }

  private static void stageNodeEvent(OpExecution exec, int ordinal, ChangeKind kind,
      byte[] key, byte[] value, Stamp stamp, long leaseId) {
    exec.changes().stageEvent(exec.overlay().transition(), ordinal, ChangeEvent.newBuilder()
        .setKind(kind)
        .setPath(ByteString.copyFrom(key))
        .setNewValue(ByteString.copyFrom(value))
        .setVersion(toProto(stamp))
        .setLeaseId(leaseId)
        .build());
  }

  private static BusinessResult.Builder mutation(byte[] key, Stamp version, long deletedCount,
      LockToken token) {
    final MutationResult.Builder result = MutationResult.newBuilder()
        .setKey(ByteString.copyFrom(key))
        .setVersion(toProto(version))
        .setDeletedCount(deletedCount);
    if (token != null) {
      result.setLockToken(token);
    }
    return BusinessResult.newBuilder().setStatus(Status.S_OK).setMutation(result);
  }

  private static BusinessResult.Builder failure(Status status) {
    return BusinessResult.newBuilder().setStatus(status);
  }

  private static org.apache.ratis.metadata.proto.MetadataProtos.Stamp toProto(Stamp stamp) {
    return org.apache.ratis.metadata.proto.MetadataProtos.Stamp.newBuilder()
        .setLogIndex(stamp.logIndex()).setOrdinal(stamp.ordinal()).build();
  }

  /** @return whether any node lives strictly below the path. */
  private static boolean hasDescendants(OverlayState overlay, byte[] key) {
    final byte[] children = childPrefix(key);
    return !overlay.scan(MetadataCf.NODES, children, OverlayState.upperExclusive(children),
        (rowKey, rowValue) -> false);
  }

  /** @return the parent path, or {@code null} for the root itself. */
  private static byte[] parentOf(byte[] path) {
    if (path.length == 0 || path[0] != '/' || Arrays.equals(path, ROOT)) {
      return null;
    }
    for (int i = path.length - 1; i >= 0; i--) {
      if (path[i] == '/') {
        // the leading slash at 0 (and a degenerate one at 1) roots the path
        return i <= 1 ? ROOT.clone() : Arrays.copyOf(path, i);
      }
    }
    return null;
  }

  /**
   * @return whether the leaf name is a reserved sequential suffix
   *         ({@code ~} followed by exactly 20 ASCII digits) - the namespace
   *         only the sequential counter may allocate from
   */
  private static boolean reservedSequentialLeaf(byte[] path) {
    int slash = -1;
    for (int i = path.length - 1; i >= 0; i--) {
      if (path[i] == '/') {
        slash = i;
        break;
      }
    }
    if (path.length - slash - 1 != SEQUENTIAL_WIDTH + 1 || path[slash + 1] != '~') {
      return false;
    }
    for (int i = slash + 2; i < path.length; i++) {
      if (path[i] < '0' || path[i] > '9') {
        return false;
      }
    }
    return true;
  }

  private static byte[] childPrefix(byte[] path) {
    final byte[] out = Arrays.copyOf(path, path.length + 1);
    out[path.length] = '/';
    return out;
  }

  private static byte[] sequentialPath(byte[] base, long sequence) {
    final byte[] digits = new byte[SEQUENTIAL_WIDTH];
    long rest = sequence;
    for (int i = SEQUENTIAL_WIDTH - 1; i >= 0; i--) {
      digits[i] = (byte) ('0' + (int) (rest % 10));
      rest /= 10;
    }
    final byte[] out = Arrays.copyOf(base, base.length + 1 + SEQUENTIAL_WIDTH);
    out[base.length] = '~';
    System.arraycopy(digits, 0, out, base.length + 1, SEQUENTIAL_WIDTH);
    return out;
  }
}
