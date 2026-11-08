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

import org.apache.ratis.metadata.proto.MetadataProtos.ChangeCursor;
import org.apache.ratis.metadata.proto.MetadataProtos.ChangeEvent;
import org.apache.ratis.metadata.proto.MetadataProtos.ChangeKind;
import org.apache.ratis.metadata.proto.MetadataProtos.ChangePage;
import org.apache.ratis.metadata.proto.MetadataProtos.CursorKind;
import org.apache.ratis.metadata.proto.MetadataProtos.Get;
import org.apache.ratis.metadata.proto.MetadataProtos.LeasePage;
import org.apache.ratis.metadata.proto.MetadataProtos.LeaseResult;
import org.apache.ratis.metadata.proto.MetadataProtos.LockOwner;
import org.apache.ratis.metadata.proto.MetadataProtos.LockToken;
import org.apache.ratis.metadata.proto.MetadataProtos.Node;
import org.apache.ratis.metadata.proto.MetadataProtos.NodeKind;
import org.apache.ratis.metadata.proto.MetadataProtos.PageCursor;
import org.apache.ratis.metadata.proto.MetadataProtos.PageRequest;
import org.apache.ratis.metadata.proto.MetadataProtos.PollChanges;
import org.apache.ratis.metadata.proto.MetadataProtos.Query;
import org.apache.ratis.metadata.proto.MetadataProtos.QueryReply;
import org.apache.ratis.metadata.proto.MetadataProtos.ReadHold;
import org.apache.ratis.metadata.proto.MetadataProtos.ReadResult;
import org.apache.ratis.metadata.proto.MetadataProtos.ReadWriter;
import org.apache.ratis.metadata.proto.MetadataProtos.Role;
import org.apache.ratis.metadata.proto.MetadataProtos.ScanPage;
import org.apache.ratis.metadata.proto.MetadataProtos.ScanQuery;
import org.apache.ratis.metadata.proto.MetadataProtos.Status;
import org.apache.ratis.metadata.proto.MetadataProtos.SubtreeSnapshot;
import org.apache.ratis.metadata.proto.MetadataProtos.SubtreeSnapshotQuery;
import org.apache.ratis.metadata.proto.MetadataProtos.WriteReply;
import org.apache.ratis.metadata.proto.MetadataProtos.WriterRef;
import org.apache.ratis.metadata.proto.MetadataProtos.WriterState;
import org.apache.ratis.metadata.protocol.GroupConfig;
import org.apache.ratis.metadata.protocol.MetadataProtocol;
import org.apache.ratis.metadata.protocol.Stamp;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.rocksdb.RocksIterator;

/**
 * The read-only queries of section 6. Every multi-record query walks one
 * short {@link ReadView}; every iterator's {@code status()} is checked
 * whatever ended its walk. Pages continue from an exclusive last-examined
 * key inside a versioned cursor that names the group, the query digest and
 * the kind, so a continuation can never leak into another query's range.
 */
final class QueryOps {
  private static final int CURSOR_VERSION = 1;
  private static final int MAX_CURSOR_BYTES = 16384;
  private static final int SNAPSHOT_MAX_NODES = 1024;
  private static final int SNAPSHOT_MAX_BYTES = 1 << 20;
  private static final int MIN_PAGE_ENTRIES = 256;
  private static final int MIN_PAGE_BYTES = 1 << 20;
  /** The digest domain separating page cursors from every other digest. */
  private static final byte[] PAGE_DOMAIN = "RMD-PAGE1".getBytes();
  private static final byte[] LEASES_QUERY = "leases".getBytes();
  private static final byte[] SCAN_QUERY = "scan".getBytes();

  private QueryOps() {
  }

  /** Runs one gated query; the caller owns the read view's lifecycle. */
  static QueryReply run(Query query, RaftGroupId groupId, GroupConfig config,
      RocksMetadataStore store, ReadView view) {
    final QueryReply.Builder reply = QueryReply.newBuilder()
        .setObservedAtLeastIndex(store.appliedIndex())
        .setSnapshotIndex(store.appliedIndex());
    switch (query.getActionCase()) {
      case GET:
        return get(query.getGet(), groupId, store, reply).build();
      case SCAN:
        return scan(query.getScan(), groupId, config, store, view, reply).build();
      case READ_HOLD:
        return readHold(query.getReadHold(), groupId, store, view, reply).build();
      case POLL_CHANGES:
        return pollChanges(query.getPollChanges(), groupId, config, store, view, reply)
            .build();
      case READ_WRITER:
        return readWriter(query.getReadWriter(), store, view, reply).build();
      case READ_RESULT:
        return readResult(query.getReadResult(), groupId, store, view, reply).build();
      case SUBTREE_SNAPSHOT:
        return subtreeSnapshot(query.getSubtreeSnapshot(), groupId, store, view, reply).build();
      case REVISION:
        return reply.setStatus(Status.S_OK).build(); // snapshot_index already set
      case LEASES:
        return leases(query.getLeases(), groupId, config, store, view, reply).build();
      default:
        return reply.setStatus(Status.S_MALFORMED).build();
    }
  }

  // ---- GET ----

  private static QueryReply.Builder get(Get query, RaftGroupId groupId,
      RocksMetadataStore store, QueryReply.Builder reply) {
    final byte[] key = query.getKey().toByteArray();
    final byte[] encoded = store.getNode(key);
    if (encoded == null) {
      return reply.setStatus(Status.S_NOT_FOUND);
    }
    return reply.setStatus(Status.S_OK)
        .setNode(nodeProto(key, StoredRecordCodec.decodeNode(encoded), true, groupId));
  }

  // ---- SCAN ----

  private static QueryReply.Builder scan(ScanQuery query, RaftGroupId groupId,
      GroupConfig config, RocksMetadataStore store, ReadView view,
      QueryReply.Builder reply) {
    final boolean tree = config.role() == Role.COORDINATION;
    if (tree) {
      // a coordination list carries only the prefix; the range fields stay empty
      if (query.getTreePrefix().isEmpty() || !query.getStart().isEmpty()
          || !query.getEnd().isEmpty()) {
        return reply.setStatus(Status.S_MALFORMED);
      }
    } else if (!query.getTreePrefix().isEmpty()) {
      return reply.setStatus(Status.S_MALFORMED); // keyspace scans carry only a range
    }
    final byte[] from = tree ? query.getTreePrefix().toByteArray()
        : query.getStart().toByteArray();
    final byte[] to = tree ? OverlayState.upperExclusive(from)
        : query.getEnd().isEmpty() ? null : query.getEnd().toByteArray();
    final byte[] digest = pageDigest(groupId, config, SCAN_QUERY,
        query.getStart().toByteArray(), query.getEnd().toByteArray(),
        query.getTreePrefix().toByteArray());
    final PageCursor resume = resumeCursor(query.getPage(), digest, groupId, config,
        CursorKind.SCAN, reply);
    if (resume != null && reply.getStatus() != Status.S_UNSPECIFIED) {
      return reply; // the cursor itself was rejected
    }
    final PageBudget budget = PageBudget.of(query.getPage(), config, reply);
    if (budget == null) {
      return reply; // the page request was rejected
    }
    final ScanPage.Builder page = ScanPage.newBuilder().setValuesIncluded(!tree);
    final RocksIterator iterator = view.iterator(MetadataCf.NODES);
    try {
      if (resume != null) {
        iterator.seek(resume.getLastExaminedKey().toByteArray());
        if (iterator.isValid() && Arrays.equals(iterator.key(),
            resume.getLastExaminedKey().toByteArray())) {
          iterator.next(); // the continuation is exclusive
        }
      } else {
        iterator.seek(from);
      }
      byte[] lastEmitted = null;
      while (iterator.isValid()) {
        final byte[] key = iterator.key();
        if (to != null && MutationBuffer.UNSIGNED_ORDER.compare(key, to) >= 0) {
          break;
        }
        if (!budget.accepts()) {
          if (lastEmitted != null) { // nothing emitted cannot advance: refuse, never skip
            page.setHasMore(true).setContinuation(
                continuation(groupId, config, digest, lastEmitted, CursorKind.SCAN));
          }
          break;
        }
        final NodeRecord record = StoredRecordCodec.decodeNode(iterator.value());
        final int valueBytes = tree ? 0 : record.value().length;
        if (!budget.fits(key, valueBytes)) {
          if (page.getNodesCount() == 0) {
            return reply.setStatus(Status.S_PAGE_TOO_SMALL); // holds not one record
          }
          page.setHasMore(true).setContinuation(
              continuation(groupId, config, digest, lastEmitted, CursorKind.SCAN));
          break;
        }
        page.addNodes(nodeProto(key, record, !tree, groupId));
        budget.charge(key, valueBytes);
        lastEmitted = key;
        iterator.next();
      }
      iterator.status();
    } catch (org.rocksdb.RocksDBException e) {
      throw MetadataStoreException.io("scan failed", e);
    } finally {
      iterator.close();
    }
    return reply.setStatus(Status.S_OK).setScan(page);
  }

  // ---- READ_HOLD ----

  private static QueryReply.Builder readHold(ReadHold query, RaftGroupId groupId,
      RocksMetadataStore store, ReadView view, QueryReply.Builder reply) {
    final byte[] path = query.getPath().toByteArray();
    final byte[] encoded = view.get(MetadataCf.NODES, path);
    if (encoded == null) {
      return reply.setStatus(Status.S_NOT_HELD).setHeld(false);
    }
    final NodeRecord lock = StoredRecordCodec.decodeNode(encoded);
    final LeaseRow lease = leaseAt(view, lock.leaseId());
    final WriterRow writer = lease == null ? null : writerAt(view, lease.ownerWriterId());
    final boolean held = lock.kind() == NodeKind.EPHEMERAL_LOCK
        && lease != null
        && writer != null
        && writer.currentGeneration() == lease.ownerGeneration()
        && lockMatches(lock, query.getExpected(), groupId)
        && store.logicalTime() < lease.expireAt();
    return held
        ? reply.setStatus(Status.S_OK).setHeld(true)
        : reply.setStatus(Status.S_NOT_HELD).setHeld(false);
  }

  private static LeaseRow leaseAt(ReadView view, long leaseId) {
    final byte[] encoded = view.get(MetadataCf.LEASES, StoredRecordCodec.leaseKey(leaseId));
    return encoded == null ? null : StoredRecordCodec.decodeLease(encoded);
  }

  private static WriterRow writerAt(ReadView view, byte[] writerId) {
    final byte[] encoded = view.get(MetadataCf.WRITERS,
        StoredRecordCodec.writerKey(writerId));
    return encoded == null ? null : StoredRecordCodec.decodeWriter(encoded);
  }

  private static boolean lockMatches(NodeRecord lock, LockOwner expected,
      RaftGroupId groupId) {
    return expected.hasOwner()
        && Arrays.equals(expected.getOwner().getWriterId().toByteArray(),
            lock.ownerWriterId())
        && expected.getOwner().getGeneration() == lock.ownerGeneration()
        && expected.getLeaseId() == lock.leaseId()
        && expected.hasToken()
        && Arrays.equals(expected.getToken().getGroupId().toByteArray(),
            groupId.toByteString().toByteArray())
        && expected.getToken().getLogIndex() == lock.lockIndex();
  }

  // ---- POLL_CHANGES ----

  private static QueryReply.Builder pollChanges(PollChanges query, RaftGroupId groupId,
      GroupConfig viewConfig, RocksMetadataStore store, ReadView view,
      QueryReply.Builder reply) {
    final long snapshotIndex = store.appliedIndex();
    final long floor = store.changeFloor();
    final ChangeCursor after = query.getAfter();
    if (!Arrays.equals(after.getGroupId().toByteArray(),
        groupId.toByteString().toByteArray())
        || after.getRevision() < 0 || after.getRevision() > snapshotIndex
        || after.getOrdinal() < 0 || after.getOrdinal() > ChangeLog.END_REVISION) {
      return reply.setStatus(Status.S_BAD_CURSOR);
    }
    final boolean behind = after.getRevision() < floor - 1
        || (after.getRevision() == floor - 1
            && after.getOrdinal() < ChangeLog.END_REVISION);
    if (behind) {
      return reply.setStatus(Status.S_NEED_SNAPSHOT);
    }
    final PageBudget budget = PageBudget.of(query.getPage(), viewConfig, reply);
    if (budget == null) {
      return reply; // the page request was rejected
    }
    final byte[] prefix = query.getPrefix().toByteArray();
    final ChangePage.Builder page = ChangePage.newBuilder().setFloorRevision(floor);
    final RocksIterator iterator = view.iterator(MetadataCf.CHANGES);
    try {
      iterator.seek(after.getOrdinal() == ChangeLog.END_REVISION
          ? StoredRecordCodec.changeKey(after.getRevision() + 1, 1)
          : StoredRecordCodec.changeKey(after.getRevision(), after.getOrdinal() + 1));
      ChangeCursor.Builder lastExamined = null;
      while (iterator.isValid()) {
        final ChangeEvent event = ChangeEvent.parseFrom(iterator.value());
        final boolean matches = matchesPrefix(event, prefix);
        if (matches ? !budget.mayExamine() || !budget.accepts() : !budget.mayExamine()) {
          // the cursor never moves past an event the page did not carry
          page.setHasMore(true).setNext(lastExamined == null
              ? after : lastExamined.build());
          break;
        }
        if (matches) {
          page.addEvents(event);
          budget.charge(iterator.key(), event.getSerializedSize());
        } else {
          budget.examine(); // a filtered event still costs its visit
        }
        lastExamined = event.getCursor().toBuilder();
        iterator.next();
      }
      if (!page.getHasMore()) {
        page.setHasMore(false)
            .setNext(ChangeCursor.newBuilder()
                .setGroupId(groupId.toByteString())
                .setRevision(snapshotIndex)
                .setOrdinal(ChangeLog.END_REVISION));
      }
      iterator.status();
    } catch (org.rocksdb.RocksDBException e) {
      throw MetadataStoreException.io("change poll failed", e);
    } catch (InvalidProtocolBufferException e) {
      throw MetadataStoreException.corrupt("change event decode failed", e);
    } finally {
      iterator.close();
    }
    return reply.setStatus(Status.S_OK).setChanges(page);
  }

  private static boolean matchesPrefix(ChangeEvent event, byte[] prefix) {
    final byte[] path = event.getPath().toByteArray();
    if (event.getKind() == ChangeKind.DELETE_SUBTREE) {
      return startsWith(prefix, path); // an ancestor deletion covers the prefix
    }
    return startsWith(path, prefix);
  }

  private static boolean startsWith(byte[] value, byte[] prefix) {
    if (value.length < prefix.length) {
      return false;
    }
    for (int i = 0; i < prefix.length; i++) {
      if (value[i] != prefix[i]) {
        return false;
      }
    }
    return true;
  }

  // ---- READ_WRITER / READ_RESULT ----

  private static QueryReply.Builder readWriter(ReadWriter query, RocksMetadataStore store,
      ReadView view, QueryReply.Builder reply) {
    final byte[] writerId = query.getWriterId().toByteArray();
    final byte[] encoded = view.get(MetadataCf.WRITERS,
        StoredRecordCodec.writerKey(writerId));
    if (encoded == null) {
      return reply.setStatus(Status.S_NOT_FOUND);
    }
    final WriterRow row = StoredRecordCodec.decodeWriter(encoded);
    return reply.setStatus(Status.S_OK).setWriter(WriterState.newBuilder()
        .setWriterId(query.getWriterId())
        .setLane(row.lane())
        .setGeneration(row.currentGeneration())
        .setHighWater(row.highWater()));
  }

  private static QueryReply.Builder readResult(ReadResult query, RaftGroupId groupId,
      RocksMetadataStore store, ReadView view, QueryReply.Builder reply) {
    final byte[] digest = query.getDigest().toByteArray();
    final org.apache.ratis.metadata.protocol.OperationId id =
        org.apache.ratis.metadata.protocol.OperationId.valueOf(
            query.getId().getWriter().getWriterId().toByteArray(),
            query.getId().getWriter().getGeneration(),
            query.getId().getSequence());
    final ResultLookup lookup = InvocationResultStore.INSTANCE.lookup(
        id, digest, store.logicalTime(), view);
    switch (lookup.kind()) {
      case REPLAY:
        final WriterRow row = writerAt(view,
            query.getId().getWriter().getWriterId().toByteArray());
        return reply.setStatus(Status.S_OK).setStoredResult(WriteReply.newBuilder()
            .setAdmission(org.apache.ratis.metadata.proto.MetadataProtos.Admission.REPLAY)
            .setOriginalBusinessResult(ByteString.copyFrom(lookup.originalReply()))
            .setStaleGeneration(row != null
                && query.getId().getWriter().getGeneration() < row.currentGeneration())
            .setCurrentGeneration(row != null ? row.currentGeneration() : 0)
            .setExpectedNextSequence(row != null ? row.highWater() + 1 : 0)
            .setOperationDigest(query.getDigest()));
      case RESULT_EXPIRED:
        return reply.setStatus(Status.S_RESULT_EXPIRED);
      case INVOCATION_MISMATCH:
        return reply.setStatus(Status.S_INVOCATION_MISMATCH);
      default:
        return reply.setStatus(Status.S_REPLAY_UNAVAILABLE); // collected beyond promise
    }
  }

  // ---- SUBTREE_SNAPSHOT ----

  private static QueryReply.Builder subtreeSnapshot(SubtreeSnapshotQuery query,
      RaftGroupId groupId, RocksMetadataStore store, ReadView view,
      QueryReply.Builder reply) {
    final byte[] prefix = query.getPrefix().toByteArray();
    final boolean root = prefix.length == 0;
    if (!root) {
      if (view.get(MetadataCf.NODES, prefix) == null) {
        return reply.setStatus(Status.S_NOT_FOUND);
      }
    }
    final SubtreeSnapshot.Builder snapshot = SubtreeSnapshot.newBuilder();
    long bytes = 0;
    final RocksIterator iterator = view.iterator(MetadataCf.NODES);
    try {
      iterator.seek(prefix);
      final byte[] to = OverlayState.upperExclusive(prefix);
      while (iterator.isValid()) {
        final byte[] key = iterator.key();
        if (to != null && MutationBuffer.UNSIGNED_ORDER.compare(key, to) >= 0) {
          break;
        }
        final NodeRecord record = StoredRecordCodec.decodeNode(iterator.value());
        if (snapshot.getNodesCount() + 1 > SNAPSHOT_MAX_NODES
            || bytes + key.length + record.value().length > SNAPSHOT_MAX_BYTES) {
          return reply.setStatus(Status.S_SNAPSHOT_TOO_LARGE);
        }
        snapshot.addNodes(nodeProto(key, record, true, groupId));
        bytes += key.length + record.value().length;
        iterator.next();
      }
      iterator.status();
    } catch (org.rocksdb.RocksDBException e) {
      throw MetadataStoreException.io("subtree snapshot failed", e);
    } finally {
      iterator.close();
    }
    snapshot.setEnd(ChangeCursor.newBuilder()
        .setGroupId(groupId.toByteString())
        .setRevision(store.appliedIndex())
        .setOrdinal(ChangeLog.END_REVISION));
    return reply.setStatus(Status.S_OK).setSubtreeSnapshot(snapshot);
  }

  // ---- LEASES ----

  private static QueryReply.Builder leases(PageRequest pageRequest, RaftGroupId groupId,
      GroupConfig config, RocksMetadataStore store, ReadView view,
      QueryReply.Builder reply) {
    final byte[] digest = pageDigest(groupId, config, LEASES_QUERY, new byte[0],
        new byte[0], new byte[0]);
    final PageCursor resume = resumeCursor(pageRequest, digest, groupId, config,
        CursorKind.LEASE_SCAN, reply);
    if (resume != null && !reply.getStatus().equals(Status.S_UNSPECIFIED)) {
      return reply;
    }
    final PageBudget budget = PageBudget.of(pageRequest, config, reply);
    if (budget == null) {
      return reply;
    }
    final LeasePage.Builder page = LeasePage.newBuilder();
    final RocksIterator iterator = view.iterator(MetadataCf.LEASES);
    try {
      if (resume != null) {
        iterator.seek(resume.getLastExaminedKey().toByteArray());
        if (iterator.isValid() && Arrays.equals(iterator.key(),
            resume.getLastExaminedKey().toByteArray())) {
          iterator.next();
        }
      } else {
        iterator.seekToFirst();
      }
      byte[] lastEmitted = null;
      while (iterator.isValid()) {
        final byte[] key = iterator.key();
        if (!budget.accepts()) {
          if (lastEmitted != null) {
            page.setHasMore(true).setContinuation(
                continuation(groupId, config, digest, lastEmitted, CursorKind.LEASE_SCAN));
          }
          break;
        }
        final LeaseRow lease = StoredRecordCodec.decodeLease(iterator.value());
        if (!budget.fits(key, 40)) {
          if (page.getLeasesCount() == 0) {
            return reply.setStatus(Status.S_PAGE_TOO_SMALL); // holds not one row
          }
          page.setHasMore(true).setContinuation(
              continuation(groupId, config, digest, lastEmitted, CursorKind.LEASE_SCAN));
          break;
        }
        page.addLeases(LeaseResult.newBuilder()
            .setLeaseId(leaseIdOf(key))
            .setOwner(WriterRef.newBuilder()
                .setWriterId(ByteString.copyFrom(lease.ownerWriterId()))
                .setGeneration(lease.ownerGeneration()))
            .setTtlMs(lease.ttlMs())
            .setExpireAt(lease.expireAt())
            .setLastRenewIndex(lease.lastRenewIndex()));
        budget.charge(key, 40);
        lastEmitted = key;
        iterator.next();
      }
      iterator.status();
    } catch (org.rocksdb.RocksDBException e) {
      throw MetadataStoreException.io("lease scan failed", e);
    } finally {
      iterator.close();
    }
    return reply.setStatus(Status.S_OK).setLeases(page);
  }

  private static long leaseIdOf(byte[] leaseKey) {
    long id = 0;
    for (int i = 0; i < 8; i++) {
      id = (id << 8) | (leaseKey[i] & 0xff);
    }
    return id;
  }

  // ---- page cursors and budgets ----

  /**
   * @return the parsed continuation, {@code null} when absent; a rejected
   *         cursor sets the reply's status and returns non-null
   */
  private static PageCursor resumeCursor(PageRequest page, byte[] digest,
      RaftGroupId groupId, GroupConfig config, CursorKind kind,
      QueryReply.Builder reply) {
    if (page.getContinuation().isEmpty()) {
      return null;
    }
    if (page.getContinuation().size() > MAX_CURSOR_BYTES) {
      reply.setStatus(Status.S_BAD_CURSOR);
      return PageCursor.getDefaultInstance();
    }
    final PageCursor cursor;
    try {
      cursor = PageCursor.parseFrom(page.getContinuation());
    } catch (InvalidProtocolBufferException e) {
      reply.setStatus(Status.S_BAD_CURSOR);
      return PageCursor.getDefaultInstance();
    }
    if (cursor.getVersion() != CURSOR_VERSION
        || !Arrays.equals(cursor.getGroupId().toByteArray(),
            groupId.toByteString().toByteArray())
        || cursor.getRole() != config.role()
        || !Arrays.equals(cursor.getFingerprint().toByteArray(),
            MetadataProtocol.fingerprint(config))
        || !Arrays.equals(cursor.getQueryDigest().toByteArray(), digest)
        || cursor.getKind() != kind) {
      reply.setStatus(Status.S_BAD_CURSOR);
      return PageCursor.getDefaultInstance();
    }
    return cursor;
  }

  private static ByteString continuation(RaftGroupId groupId, GroupConfig config,
      byte[] digest, byte[] lastExaminedKey, CursorKind kind) {
    return PageCursor.newBuilder()
        .setVersion(CURSOR_VERSION)
        .setGroupId(groupId.toByteString())
        .setRole(config.role())
        .setFingerprint(ByteString.copyFrom(MetadataProtocol.fingerprint(config)))
        .setLastExaminedKey(ByteString.copyFrom(lastExaminedKey))
        .setQueryDigest(ByteString.copyFrom(digest))
        .setKind(kind)
        .build().toByteString();
  }

  private static byte[] pageDigest(RaftGroupId groupId, GroupConfig config, byte[] kind,
      byte[] start, byte[] end, byte[] treePrefix) {
    final byte[] fingerprint = MetadataProtocol.fingerprint(config);
    final byte[] out = new byte[PAGE_DOMAIN.length + 1 + 16 + fingerprint.length
        + kind.length + 4 + start.length + 4 + end.length + 4 + treePrefix.length];
    int offset = write(out, 0, PAGE_DOMAIN);
    out[offset++] = (byte) config.role().getNumber();
    System.arraycopy(groupId.toByteString().toByteArray(), 0, out, offset, 16);
    offset += 16;
    offset = write(out, offset, fingerprint);
    offset = write(out, offset, kind);
    offset = writeLengthPrefixed(out, offset, start);
    offset = writeLengthPrefixed(out, offset, end);
    writeLengthPrefixed(out, offset, treePrefix);
    return StoredRecordCodec.sha256(out);
  }

  private static int write(byte[] out, int offset, byte[] bytes) {
    System.arraycopy(bytes, 0, out, offset, bytes.length);
    return offset + bytes.length;
  }

  private static int writeLengthPrefixed(byte[] out, int offset, byte[] bytes) {
    out[offset++] = (byte) (bytes.length >>> 24);
    out[offset++] = (byte) (bytes.length >>> 16);
    out[offset++] = (byte) (bytes.length >>> 8);
    out[offset++] = (byte) bytes.length;
    return write(out, offset, bytes);
  }

  // ---- shared mapping ----

  private static Node nodeProto(byte[] key, NodeRecord record, boolean includeValue,
      RaftGroupId groupId) {
    final Node.Builder node = Node.newBuilder()
        .setKey(ByteString.copyFrom(key))
        .setKind(record.kind())
        .setCreateStamp(stampProto(record.createStamp()))
        .setModifyStamp(stampProto(record.modifyStamp()))
        .setLeaseId(record.leaseId())
        .setCreatedAt(record.createdAt())
        .setModifiedAt(record.modifiedAt());
    if (includeValue) {
      node.setValue(ByteString.copyFrom(record.value()));
    }
    if (record.leaseId() != 0) {
      node.setOwner(WriterRef.newBuilder()
          .setWriterId(ByteString.copyFrom(record.ownerWriterId()))
          .setGeneration(record.ownerGeneration()));
    }
    if (record.kind() == NodeKind.EPHEMERAL_LOCK) {
      node.setLockToken(LockToken.newBuilder()
          .setGroupId(groupId.toByteString())
          .setLogIndex(record.lockIndex()));
    }
    return node.build();
  }

  private static org.apache.ratis.metadata.proto.MetadataProtos.Stamp stampProto(Stamp stamp) {
    return org.apache.ratis.metadata.proto.MetadataProtos.Stamp.newBuilder()
        .setLogIndex(stamp.logIndex()).setOrdinal(stamp.ordinal()).build();
  }

  /** One page's entry, byte and visit budget with the zero-filling defaults. */
  private static final class PageBudget {
    private final int maxEntries;
    private final long maxBytes;
    private final int maxVisits;
    private int entries;
    private long bytes;
    private int visits;

    private PageBudget(int maxEntries, long maxBytes, int maxVisits) {
      this.maxEntries = maxEntries;
      this.maxBytes = maxBytes;
      this.maxVisits = maxVisits;
    }

    /**
     * @return the budget, or {@code null} after rejecting an explicit
     *         over-cap request through the reply
     */
    static PageBudget of(PageRequest page, GroupConfig config, QueryReply.Builder reply) {
      if (page == null) {
        return new PageBudget(Math.min(MIN_PAGE_ENTRIES, config.pageEntries()),
            Math.min(MIN_PAGE_BYTES, (long) config.pageBytes()), config.pageVisits());
      }
      final int entries = page.getMaxEntries() == 0
          ? Math.min(MIN_PAGE_ENTRIES, config.pageEntries()) : page.getMaxEntries();
      final long entryBytes = page.getMaxBytes() == 0
          ? Math.min(MIN_PAGE_BYTES, (long) config.pageBytes()) : page.getMaxBytes();
      if (entries > config.pageEntries() || entryBytes > config.pageBytes()) {
        reply.setStatus(Status.S_PAGE_TOO_SMALL); // explicit over-cap is refused, never cut
        return null;
      }
      return new PageBudget(entries, entryBytes, config.pageVisits());
    }

    /** @return whether one more row may be examined and emitted. */
    boolean accepts() {
      return entries < maxEntries && bytes < maxBytes && visits < maxVisits;
    }

    /** @return whether the visit budget holds for one more examined row. */
    boolean mayExamine() {
      return visits < maxVisits;
    }

    /** One examined row that will not be emitted: a visit, nothing else. */
    void examine() {
      visits++;
    }

    /**
     * @return whether one row of this size fits without crossing the byte
     *         cap - a page that cannot hold even its first record is
     *         refused, never silently overfilled
     */
    boolean fits(byte[] key, int valueBytes) {
      return bytes + key.length + 64 + valueBytes <= maxBytes;
    }

    void charge(byte[] key, int valueBytes) {
      entries++;
      visits++;
      bytes += key.length + 64 + valueBytes;
    }
  }
}
