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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.TreeSet;

import org.apache.ratis.metadata.proto.MetadataProtos;
import org.apache.ratis.metadata.proto.MetadataProtos.Cas;
import org.apache.ratis.metadata.proto.MetadataProtos.Create;
import org.apache.ratis.metadata.proto.MetadataProtos.Delete;
import org.apache.ratis.metadata.proto.MetadataProtos.ExpectedVersion;
import org.apache.ratis.metadata.proto.MetadataProtos.Increment;
import org.apache.ratis.metadata.proto.MetadataProtos.LeaseOpen;
import org.apache.ratis.metadata.proto.MetadataProtos.LeaseRenew;
import org.apache.ratis.metadata.proto.MetadataProtos.LeaseRevoke;
import org.apache.ratis.metadata.proto.MetadataProtos.Multi;
import org.apache.ratis.metadata.proto.MetadataProtos.NodeKind;
import org.apache.ratis.metadata.proto.MetadataProtos.Operation;
import org.apache.ratis.metadata.proto.MetadataProtos.Put;
import org.apache.ratis.metadata.proto.MetadataProtos.Role;
import org.apache.ratis.metadata.proto.MetadataProtos.WriterRef;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.thirdparty.com.google.protobuf.Message;

/**
 * The wire boundary of the metadata protocol: freezing an operation into
 * canonical bytes with a digest, decoding received writes, fingerprinting
 * configurations, and validating tree paths and binary keys. All rules are
 * deterministic functions of the frozen inputs, so every replica and client
 * computes identical bytes and digests for the same request.
 *
 * <p>The canonical form of an operation is a rebuilt protobuf message in
 * which semantic defaults are filled in explicitly: an unset CREATE kind
 * becomes NORMAL and an unset lease TTL becomes the default TTL. The
 * operation digest is SHA-256 over the domain tag {@code "RMD1"}, a zero
 * byte, the 16-byte group id, the 32-byte config fingerprint, the role
 * number and the canonical operation bytes; it deliberately does not cover
 * the retry budget or the transport identity.
 */
public final class MetadataProtocol {
  /** The only wire/schema version of this implementation. */
  public static final int WIRE_VERSION = 1;
  /** The only configuration schema version of this implementation. */
  public static final int SCHEMA_VERSION = 1;
  /** Lease TTL filled into a canonical LEASE_OPEN when the caller omits it. */
  public static final long DEFAULT_LEASE_TTL_MS = 30_000L;
  /** Per-record key/header cost of one stored node key used in budget checks. */
  public static final int NODE_KEY_OVERHEAD = 64;
  /** Per-record key/header cost of one stored reply used in budget checks. */
  public static final int REPLY_OVERHEAD = 128;
  /**
   * Worst-case envelope overhead admitted on top of the canonical bytes:
   * wireVersion(3) + groupId(18) + role(3) + fingerprint(34) + retryBudget(12)
   * + OperationId with a maximal WriterRef(44) + digest(34) + two wrapping
   * message headers(8), rounded up to a fixed deterministic constant.
   */
  public static final int ENVELOPE_OVERHEAD = 160;

  private static final byte[] DIGEST_DOMAIN = "RMD1".getBytes(StandardCharsets.US_ASCII);
  private static final byte[] DIGEST_SEPARATOR = {0};
  private static final int GROUP_ID_BYTES = 16;
  private static final int FINGERPRINT_BYTES = 32;

  private MetadataProtocol() {
    throw new IllegalStateException("No instances of this utility class");
  }

  /**
   * Freezes one business operation for submission: validates it against the
   * group configuration, computes the canonical bytes and the digest and
   * fixes the retry budget. Retries must reuse the returned object.
   *
   * @param groupId       the single target group
   * @param config        the validated group configuration
   * @param operationId   durable writer identity of this operation
   * @param operation     the operation protobuf to validate and freeze
   * @param retryBudgetMs first-freeze deadline budget, in
   *                      {@code [1, config.retentionMs()]}
   * @return the immutable frozen request
   * @throws MetadataProtocolException on any deterministic rule violation
   */
  public static FrozenRequest freeze(RaftGroupId groupId, GroupConfig config,
      OperationId operationId, Operation operation, long retryBudgetMs) {
    Objects.requireNonNull(groupId, "groupId");
    Objects.requireNonNull(config, "config");
    Objects.requireNonNull(operationId, "operationId");
    Objects.requireNonNull(operation, "operation");
    if (retryBudgetMs < 1 || retryBudgetMs > config.retentionMs()) {
      throw new MetadataProtocolException(MetadataStatus.S_BUDGET_MISMATCH,
          "retryBudgetMs must be in [1, " + config.retentionMs() + "]: " + retryBudgetMs);
    }
    final Operation canonical = canonicalOperation(operation, config.role(), config, false);
    final byte[] canonicalBytes = canonical.toByteArray();
    if (canonicalBytes.length + ENVELOPE_OVERHEAD > config.requestBytes()) {
      throw limit("canonical operation bytes plus envelope exceed clientRequestBytes");
    }
    final byte[] fingerprint = fingerprint(config);
    final byte[] digest = operationDigest(groupId, config.role(), fingerprint, canonicalBytes);
    final FrozenOperation frozen = new FrozenOperation(groupId, config.role(), fingerprint,
        operationId, canonical, digest, retryBudgetMs);
    return new FrozenRequest(frozen, config);
  }

  /**
   * @param config a validated configuration
   * @return SHA-256 over its canonical protobuf bytes, 32 bytes
   */
  public static byte[] fingerprint(GroupConfig config) {
    Objects.requireNonNull(config, "config");
    return sha256(config.toProto().toByteArray());
  }

  /**
   * Decodes and self-verifies one received write envelope: the wire version
   * must match, no unknown fields may ride along, and the carried digest
   * must equal the digest recomputed from the envelope itself. Budget
   * admission against the group's actual configuration happens separately.
   *
   * @param bytes a serialized ClientWrite message
   * @return the decoded envelope
   * @throws MetadataProtocolException on any deterministic rule violation
   */
  public static ClientWrite decodeWrite(byte[] bytes) {
    Objects.requireNonNull(bytes, "bytes");
    final MetadataProtos.ClientWrite proto;
    try {
      proto = MetadataProtos.ClientWrite.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      throw malformed("undecodable ClientWrite envelope: " + e.getMessage());
    }
    requireNoUnknown(proto, "ClientWrite");
    if (proto.getWireVersion() != WIRE_VERSION) {
      throw new MetadataProtocolException(MetadataStatus.S_CONFIG_MISMATCH,
          "wireVersion must be " + WIRE_VERSION + ": " + proto.getWireVersion());
    }
    final Role role = proto.getRole();
    if (role == Role.ROLE_UNSPECIFIED || role == Role.UNRECOGNIZED) {
      throw malformed("role must be specified");
    }
    final byte[] groupIdBytes = proto.getGroupId().toByteArray();
    if (groupIdBytes.length != GROUP_ID_BYTES) {
      throw malformed("groupId must be " + GROUP_ID_BYTES + " bytes");
    }
    if (proto.getFingerprint().size() != FINGERPRINT_BYTES) {
      throw malformed("fingerprint must be " + FINGERPRINT_BYTES + " bytes");
    }
    if (proto.getDigest().size() != FINGERPRINT_BYTES) {
      throw malformed("digest must be " + FINGERPRINT_BYTES + " bytes");
    }
    if (proto.getRetryBudgetMs() < 1) {
      throw new MetadataProtocolException(MetadataStatus.S_BUDGET_MISMATCH,
          "retryBudgetMs must be at least 1: " + proto.getRetryBudgetMs());
    }
    final OperationId operationId = operationId(proto.getId());
    final Operation canonical = canonicalOperation(proto.getOperation(), role, null, false);
    final byte[] canonicalBytes = canonical.toByteArray();
    final RaftGroupId groupId = RaftGroupId.valueOf(proto.getGroupId());
    final byte[] fingerprint = proto.getFingerprint().toByteArray();
    final byte[] digest = operationDigest(groupId, role, fingerprint, canonicalBytes);
    if (!java.util.Arrays.equals(digest, proto.getDigest().toByteArray())) {
      throw malformed("envelope digest does not match its operation");
    }
    final FrozenOperation frozen = new FrozenOperation(groupId, role, fingerprint,
        operationId, canonical, digest, proto.getRetryBudgetMs());
    return new ClientWrite(frozen);
  }

  /**
   * Encodes one tree path from its Java form. The string must be strictly
   * valid UTF-8 (no unpaired surrogate is silently replaced), start at the
   * root, contain no NUL, no empty segment, no trailing slash except the
   * root itself, and no {@code .} or {@code ..} segment. Length and depth
   * limits are configuration values and checked when freezing.
   *
   * @param path the Java string form of a tree path
   * @return the UTF-8 bytes of the validated path
   * @throws MetadataProtocolException on any structural violation
   */
  public static byte[] encodeTreePath(String path) {
    Objects.requireNonNull(path, "path");
    if (!hasNoUnpairedSurrogate(path)) {
      throw badPath("path contains an unpaired surrogate");
    }
    final byte[] bytes = path.getBytes(StandardCharsets.UTF_8);
    treePathDepth(bytes);
    return bytes;
  }

  /**
   * Compares two byte arrays as unsigned octet sequences, lexicographically;
   * a strict prefix sorts before its extension. This is the only ordering
   * used for keys and paths.
   *
   * @return a negative int, zero or a positive int as {@code left} sorts
   *         before, equal to or after {@code right}
   */
  public static int compareUnsigned(byte[] left, byte[] right) {
    Objects.requireNonNull(left, "left");
    Objects.requireNonNull(right, "right");
    final int shared = Math.min(left.length, right.length);
    for (int i = 0; i < shared; i++) {
      final int diff = (left[i] & 0xff) - (right[i] & 0xff);
      if (diff != 0) {
        return diff;
      }
    }
    return left.length - right.length;
  }

  /**
   * Validates the operation and returns its canonical form. With a
   * {@code null} config only structural rules apply; otherwise every
   * configured limit is enforced. Sub-operations of MULTI are restricted to
   * plain node mutations: no nested MULTI, no lease commands, no lock or
   * sequential CREATE, no lock-owner DELETE.
   */
  static Operation canonicalOperation(Operation op, Role role, GroupConfig config,
      boolean inMulti) {
    Objects.requireNonNull(op, "operation");
    requireNoUnknown(op, "Operation");
    switch (op.getCommandCase()) {
      case PUT:
        return canonicalPut(op.getPut(), role, config, inMulti);
      case CREATE:
        return canonicalCreate(op.getCreate(), role, config, inMulti);
      case CAS:
        return canonicalCas(op.getCas(), role, config, inMulti);
      case DELETE:
        return canonicalDelete(op.getDelete(), role, config, inMulti);
      case INCREMENT:
        return canonicalIncrement(op.getIncrement(), role, config, inMulti);
      case MULTI:
        return canonicalMulti(op.getMulti(), role, config, inMulti);
      case LEASE_OPEN:
        return canonicalLeaseOpen(op.getLeaseOpen(), role, config, inMulti);
      case LEASE_RENEW:
        return canonicalLeaseRenew(op.getLeaseRenew(), role, config, inMulti);
      case LEASE_REVOKE:
        return canonicalLeaseRevoke(op.getLeaseRevoke(), role, config, inMulti);
      case COMMAND_NOT_SET:
      default:
        throw malformed("operation carries no command");
    }
  }

  private static Operation canonicalPut(Put put, Role role, GroupConfig config, boolean inMulti) {
    requireNoUnknown(put, "Put");
    final byte[] key = put.getKey().toByteArray();
    validateWritableKey(key, role, config);
    validateValue(put.getValue().size(), config);
    validateLeaseId(put.getLeaseId());
    return Operation.newBuilder().setPut(put).build();
  }

  private static Operation canonicalCreate(Create create, Role role, GroupConfig config,
      boolean inMulti) {
    if (role != Role.COORDINATION) {
      throw wrongRole("CREATE is a coordination operation");
    }
    requireNoUnknown(create, "Create");
    final byte[] path = create.getPath().toByteArray();
    validateWritableKey(path, role, config);
    validateValue(create.getValue().size(), config);
    validateLeaseId(create.getLeaseId());
    if (create.getKind() == NodeKind.UNRECOGNIZED) {
      throw malformed("Create kind is not a known NodeKind");
    }
    if (create.getKind() == NodeKind.EPHEMERAL_LOCK) {
      if (inMulti) {
        throw malformed("a lock CREATE must be submitted alone, not inside MULTI");
      }
      if (create.getSequential()) {
        throw malformed("a lock CREATE must not be sequential");
      }
      if (create.getLeaseId() < 1) {
        throw new MetadataProtocolException(MetadataStatus.S_NOT_ALLOWED,
            "a lock CREATE must carry the lease it belongs to");
      }
      if (create.getValue().size() > 0) {
        throw malformed("a lock CREATE carries no value");
      }
      if (!create.getCreateParents()) {
        throw malformed("a lock CREATE creates its parent chain");
      }
    } else if (inMulti && create.getSequential()) {
      throw malformed("MULTI allows only plain CREATE sub-operations");
    }
    final Create.Builder canonical = create.toBuilder();
    if (canonical.getKind() == NodeKind.NODE_UNSPECIFIED) {
      canonical.setKind(NodeKind.NORMAL);
    }
    return Operation.newBuilder().setCreate(canonical).build();
  }

  private static Operation canonicalCas(Cas cas, Role role, GroupConfig config, boolean inMulti) {
    requireNoUnknown(cas, "Cas");
    final byte[] key = cas.getKey().toByteArray();
    validateWritableKey(key, role, config);
    validateValue(cas.getValue().size(), config);
    validateLeaseId(cas.getLeaseId());
    final ExpectedVersion expected = cas.getExpected();
    requireNoUnknown(expected, "ExpectedVersion");
    switch (expected.getPredicateCase()) {
      case ABSENT:
        if (!expected.getAbsent()) {
          throw malformed("ExpectedVersion.absent must be true when present");
        }
        break;
      case EXACT:
        requireNoUnknown(expected.getExact(), "Stamp");
        if (expected.getExact().getLogIndex() < 0 || expected.getExact().getOrdinal() < 1) {
          throw malformed("ExpectedVersion.exact carries an invalid Stamp");
        }
        break;
      case PREDICATE_NOT_SET:
      default:
        throw malformed("CAS must specify ABSENT or an exact Stamp");
    }
    return Operation.newBuilder().setCas(cas).build();
  }

  private static Operation canonicalDelete(Delete delete, Role role, GroupConfig config,
      boolean inMulti) {
    requireNoUnknown(delete, "Delete");
    final byte[] key = delete.getKey().toByteArray();
    validateDeleteKey(key, role, config);
    if (delete.hasLockOwner()) {
      if (inMulti) {
        throw malformed("a lock DELETE must be submitted alone, not inside MULTI");
      }
      if (role != Role.COORDINATION) {
        throw wrongRole("a lock DELETE is a coordination operation");
      }
      if (delete.getRecursive()) {
        throw malformed("a lock DELETE is not recursive");
      }
      requireNoUnknown(delete.getLockOwner(), "LockOwner");
      final MetadataProtos.LockOwner owner = delete.getLockOwner();
      validateWriterRef(owner.getOwner());
      if (owner.getLeaseId() < 1) {
        throw malformed("a lock DELETE must identify the lease");
      }
      requireNoUnknown(owner.getToken(), "LockToken");
      if (owner.getToken().getGroupId().size() != GROUP_ID_BYTES
          || owner.getToken().getLogIndex() < 0) {
        throw malformed("a lock DELETE must carry a well-formed token");
      }
    } else if (role == Role.KEYSPACE && delete.getRecursive()) {
      throw wrongRole("a keyspace DELETE is a single-key delete");
    }
    return Operation.newBuilder().setDelete(delete).build();
  }

  private static Operation canonicalIncrement(Increment increment, Role role, GroupConfig config,
      boolean inMulti) {
    if (role != Role.COORDINATION) {
      throw wrongRole("INCREMENT is a coordination operation");
    }
    requireNoUnknown(increment, "Increment");
    final byte[] name = increment.getName().toByteArray();
    final int limit = config == null ? Integer.MAX_VALUE : config.pathBytes();
    if (name.length < 1 || name.length > limit) {
      throw limit("INCREMENT name must be 1.." + limit + " bytes: " + name.length);
    }
    if (!isStrictUtf8(name, 0, name.length)) {
      throw malformed("INCREMENT name must be strict UTF-8");
    }
    for (byte b : name) {
      if (b == 0) {
        throw malformed("INCREMENT name must not contain a NUL byte");
      }
    }
    return Operation.newBuilder().setIncrement(increment).build();
  }

  private static Operation canonicalMulti(Multi multi, Role role, GroupConfig config,
      boolean inMulti) {
    if (role != Role.COORDINATION) {
      throw wrongRole("MULTI is a coordination operation");
    }
    if (inMulti) {
      throw malformed("MULTI must not nest");
    }
    requireNoUnknown(multi, "Multi");
    final int count = multi.getOperationsCount();
    final int limit = config == null ? Integer.MAX_VALUE : config.multiOps();
    if (count < 1 || count > limit) {
      throw limit("MULTI must carry 1.." + limit + " sub-operations: " + count);
    }
    final Multi.Builder canonical = Multi.newBuilder();
    final TreeSet<byte[]> distinctKeys = new TreeSet<>(MetadataProtocol::compareUnsigned);
    for (Operation sub : multi.getOperationsList()) {
      final Operation canonicalSub = canonicalOperation(sub, role, config, true);
      canonical.addOperations(canonicalSub);
      collectMutationKeys(canonicalSub, distinctKeys);
    }
    if (config != null && distinctKeys.size() > config.mutationNodes()) {
      throw limit("MULTI touches " + distinctKeys.size()
          + " distinct keys, above mutationNodes=" + config.mutationNodes());
    }
    return Operation.newBuilder().setMulti(canonical).build();
  }

  private static void collectMutationKeys(Operation sub, TreeSet<byte[]> distinctKeys) {
    final byte[] key;
    switch (sub.getCommandCase()) {
      case PUT:
        key = sub.getPut().getKey().toByteArray();
        break;
      case CREATE:
        key = sub.getCreate().getPath().toByteArray();
        break;
      case CAS:
        key = sub.getCas().getKey().toByteArray();
        break;
      case DELETE:
        key = sub.getDelete().getKey().toByteArray();
        break;
      case INCREMENT:
        key = sub.getIncrement().getName().toByteArray();
        break;
      default:
        throw malformed("MULTI allows only PUT, plain CREATE, CAS, DELETE and INCREMENT");
    }
    distinctKeys.add(key);
  }

  private static Operation canonicalLeaseOpen(LeaseOpen open, Role role, GroupConfig config,
      boolean inMulti) {
    if (role != Role.COORDINATION) {
      throw wrongRole("LEASE_OPEN is a coordination operation");
    }
    if (inMulti) {
      throw malformed("lease commands must be submitted outside MULTI");
    }
    requireNoUnknown(open, "LeaseOpen");
    validateWriterRef(open.getOwner());
    final LeaseOpen.Builder canonical = open.toBuilder();
    if (canonical.getTtlMs() == 0) {
      canonical.setTtlMs(DEFAULT_LEASE_TTL_MS);
    }
    validateLeaseTtl(canonical.getTtlMs(), config);
    return Operation.newBuilder().setLeaseOpen(canonical).build();
  }

  private static Operation canonicalLeaseRenew(LeaseRenew renew, Role role,
      GroupConfig config, boolean inMulti) {
    if (role != Role.COORDINATION) {
      throw wrongRole("LEASE_RENEW is a coordination operation");
    }
    if (inMulti) {
      throw malformed("lease commands must be submitted outside MULTI");
    }
    requireNoUnknown(renew, "LeaseRenew");
    validateWriterRef(renew.getOwner());
    if (renew.getLeaseId() < 1) {
      throw malformed("LEASE_RENEW must identify the lease");
    }
    if (renew.getTtlMs() < 1) {
      throw new MetadataProtocolException(MetadataStatus.S_BAD_TTL,
          "LEASE_RENEW must carry an explicit ttl: " + renew.getTtlMs());
    }
    validateLeaseTtl(renew.getTtlMs(), config);
    return Operation.newBuilder().setLeaseRenew(renew).build();
  }

  private static Operation canonicalLeaseRevoke(LeaseRevoke revoke, Role role,
      GroupConfig config, boolean inMulti) {
    if (role != Role.COORDINATION) {
      throw wrongRole("LEASE_REVOKE is a coordination operation");
    }
    if (inMulti) {
      throw malformed("lease commands must be submitted outside MULTI");
    }
    requireNoUnknown(revoke, "LeaseRevoke");
    validateWriterRef(revoke.getOwner());
    if (revoke.getLeaseId() < 1) {
      throw malformed("LEASE_REVOKE must identify the lease");
    }
    return Operation.newBuilder().setLeaseRevoke(revoke).build();
  }

  private static void validateWritableKey(byte[] key, Role role, GroupConfig config) {
    if (role == Role.COORDINATION) {
      final int depth = treePathDepth(key);
      if (key.length == 1) {
        throw badPath("the root is virtual and not writable");
      }
      if (hasReservedSequentialSuffix(key)) {
        throw badPath("the leaf name is reserved for sequential creates");
      }
      if (config != null) {
        if (key.length > config.pathBytes()) {
          throw limit("tree path exceeds pathUtf8Bytes=" + config.pathBytes());
        }
        if (depth > config.pathDepth()) {
          throw limit("tree path depth exceeds pathDepth=" + config.pathDepth());
        }
      }
    } else {
      validateBinaryKey(key, config);
    }
  }

  private static void validateDeleteKey(byte[] key, Role role, GroupConfig config) {
    if (role == Role.COORDINATION) {
      final int depth = treePathDepth(key);
      if (key.length == 1) {
        throw new MetadataProtocolException(MetadataStatus.S_NOT_ALLOWED,
            "the root is virtual and cannot be deleted");
      }
      if (config != null) {
        if (key.length > config.pathBytes()) {
          throw limit("tree path exceeds pathUtf8Bytes=" + config.pathBytes());
        }
        if (depth > config.pathDepth()) {
          throw limit("tree path depth exceeds pathDepth=" + config.pathDepth());
        }
      }
    } else {
      validateBinaryKey(key, config);
    }
  }

  private static void validateBinaryKey(byte[] key, GroupConfig config) {
    final int limit = config == null ? Integer.MAX_VALUE : config.keyBytes();
    if (key.length < 1 || key.length > limit) {
      throw limit("binary key must be 1.." + limit + " bytes: " + key.length);
    }
  }

  private static void validateValue(int size, GroupConfig config) {
    if (config != null && size > config.valueBytes()) {
      throw limit("value exceeds valueBytes=" + config.valueBytes());
    }
  }

  private static void validateLeaseId(long leaseId) {
    if (leaseId < 0) {
      throw malformed("leaseId must not be negative");
    }
  }

  private static void validateLeaseTtl(long ttlMs, GroupConfig config) {
    if (ttlMs < 0) {
      throw new MetadataProtocolException(MetadataStatus.S_BAD_TTL,
          "lease TTL must not be negative: " + ttlMs);
    }
    if (ttlMs == 0 || config == null) {
      // zero reaches here only as an omitted LEASE_OPEN TTL; the canonical
      // form fills the default, and the range needs the configuration
      return;
    }
    if (ttlMs < config.leaseMinTtlMs() || ttlMs > config.leaseMaxTtlMs()) {
      throw new MetadataProtocolException(MetadataStatus.S_BAD_TTL,
          "lease TTL must be in [" + config.leaseMinTtlMs() + ", "
              + config.leaseMaxTtlMs() + "]: " + ttlMs);
    }
  }

  private static void validateWriterRef(WriterRef owner) {
    requireNoUnknown(owner, "WriterRef");
    OperationId.valueOf(owner.getWriterId().toByteArray(), owner.getGeneration(), 1);
  }

  private static OperationId operationId(MetadataProtos.OperationId proto) {
    requireNoUnknown(proto, "OperationId");
    if (!proto.hasWriter()) {
      throw malformed("OperationId carries no writer");
    }
    requireNoUnknown(proto.getWriter(), "WriterRef");
    return OperationId.valueOf(proto.getWriter().getWriterId().toByteArray(),
        proto.getWriter().getGeneration(), proto.getSequence());
  }

  private static void requireNoUnknown(Message message, String what) {
    if (!message.getUnknownFields().asMap().isEmpty()) {
      throw malformed(what + " carries unknown fields");
    }
  }

  private static MetadataProtocolException wrongRole(String message) {
    return new MetadataProtocolException(MetadataStatus.S_WRONG_ROLE, message);
  }

  private static byte[] operationDigest(RaftGroupId groupId, Role role, byte[] fingerprint,
      byte[] canonicalBytes) {
    final byte[] groupIdBytes = groupId.toByteString().toByteArray();
    if (groupIdBytes.length != GROUP_ID_BYTES) {
      throw malformed("groupId must be " + GROUP_ID_BYTES + " bytes");
    }
    return sha256(DIGEST_DOMAIN, DIGEST_SEPARATOR, groupIdBytes, fingerprint,
        new byte[]{(byte) role.getNumber()}, canonicalBytes);
  }

  /** @return {@code true} if the string encodes to UTF-8 without loss. */
  static boolean hasNoUnpairedSurrogate(String s) {
    for (int i = 0; i < s.length(); ) {
      final char c = s.charAt(i);
      if (Character.isHighSurrogate(c)) {
        if (i + 1 >= s.length() || !Character.isLowSurrogate(s.charAt(i + 1))) {
          return false;
        }
        i += 2;
      } else if (Character.isLowSurrogate(c)) {
        return false;
      } else {
        i++;
      }
    }
    return true;
  }

  /**
   * Validates the structure of a tree path byte form and returns its depth
   * (root is depth 0). Length and depth limits are enforced by the caller.
   */
  static int treePathDepth(byte[] bytes) {
    if (bytes.length < 1 || bytes[0] != '/') {
      throw badPath("a tree path must start at the root");
    }
    if (!isStrictUtf8(bytes, 0, bytes.length)) {
      throw badPath("tree path bytes are not strict UTF-8");
    }
    if (bytes.length == 1) {
      return 0;
    }
    if (bytes[bytes.length - 1] == '/') {
      throw badPath("a tree path must not end with a slash");
    }
    int depth = 0;
    int segmentStart = 1;
    for (int i = 1; i <= bytes.length; i++) {
      if (i < bytes.length && bytes[i] != '/') {
        continue;
      }
      final int length = i - segmentStart;
      if (length == 0) {
        throw badPath("a tree path must not contain an empty segment");
      }
      if (length == 1 && bytes[segmentStart] == '.') {
        throw badPath("a tree path must not contain a '.' segment");
      }
      if (length == 2 && bytes[segmentStart] == '.' && bytes[segmentStart + 1] == '.') {
        throw badPath("a tree path must not contain a '..' segment");
      }
      for (int j = segmentStart; j < i; j++) {
        if (bytes[j] == 0) {
          throw badPath("a tree path must not contain a NUL byte");
        }
      }
      depth++;
      segmentStart = i + 1;
    }
    return depth;
  }

  /**
   * @return {@code true} if the range is valid UTF-8 with no surrogate code
   *         point, no overlong form and no code point above U+10FFFF.
   */
  static boolean isStrictUtf8(byte[] bytes, int offset, int length) {
    final int end = offset + length;
    int i = offset;
    while (i < end) {
      final int b0 = bytes[i] & 0xff;
      if (b0 < 0x80) {
        i += 1;
      } else if (b0 >= 0xc2 && b0 <= 0xdf) {
        if (i + 1 >= end || !isContinuation(bytes[i + 1])) {
          return false;
        }
        i += 2;
      } else if (b0 == 0xe0) {
        if (i + 2 >= end || (bytes[i + 1] & 0xff) < 0xa0 || (bytes[i + 1] & 0xff) > 0xbf
            || !isContinuation(bytes[i + 2])) {
          return false;
        }
        i += 3;
      } else if (b0 >= 0xe1 && b0 <= 0xec || b0 >= 0xee && b0 <= 0xef) {
        if (i + 2 >= end || !isContinuation(bytes[i + 1]) || !isContinuation(bytes[i + 2])) {
          return false;
        }
        i += 3;
      } else if (b0 == 0xed) {
        if (i + 2 >= end || (bytes[i + 1] & 0xff) > 0x9f || !isContinuation(bytes[i + 1])
            || !isContinuation(bytes[i + 2])) {
          return false;
        }
        i += 3;
      } else if (b0 == 0xf0) {
        if (i + 3 >= end || (bytes[i + 1] & 0xff) < 0x90 || (bytes[i + 1] & 0xff) > 0xbf
            || !isContinuation(bytes[i + 2]) || !isContinuation(bytes[i + 3])) {
          return false;
        }
        i += 4;
      } else if (b0 >= 0xf1 && b0 <= 0xf3) {
        if (i + 3 >= end || !isContinuation(bytes[i + 1]) || !isContinuation(bytes[i + 2])
            || !isContinuation(bytes[i + 3])) {
          return false;
        }
        i += 4;
      } else if (b0 == 0xf4) {
        if (i + 3 >= end || (bytes[i + 1] & 0xff) > 0x8f || !isContinuation(bytes[i + 1])
            || !isContinuation(bytes[i + 2]) || !isContinuation(bytes[i + 3])) {
          return false;
        }
        i += 4;
      } else {
        return false;
      }
    }
    return true;
  }

  private static boolean isContinuation(byte b) {
    return (b & 0xc0) == 0x80;
  }

  /**
   * @return {@code true} if the leaf name ends with the reserved sequential
   *         suffix {@code ~[0-9]{20}}, whatever precedes it. Explicit
   *         writes may not create such names, so sequential numbering can
   *         never collide with them and skip ahead.
   */
  static boolean hasReservedSequentialSuffix(byte[] path) {
    int lastSlash = path.length - 1;
    while (lastSlash >= 0 && path[lastSlash] != '/') {
      lastSlash--;
    }
    final int length = path.length - lastSlash - 1;
    if (length < 21) {
      return false;
    }
    final int suffix = path.length - 21;
    if (path[suffix] != '~') {
      return false;
    }
    for (int i = suffix + 1; i < path.length; i++) {
      final int digit = path[i] - '0';
      if (digit < 0 || digit > 9) {
        return false;
      }
    }
    return true;
  }

  static MetadataProtocolException badPath(String message) {
    return new MetadataProtocolException(MetadataStatus.S_BAD_PATH, message);
  }

  static MetadataProtocolException malformed(String message) {
    return new MetadataProtocolException(MetadataStatus.S_MALFORMED, message);
  }

  static MetadataProtocolException limit(String message) {
    return new MetadataProtocolException(MetadataStatus.S_LIMIT_EXCEEDED, message);
  }

  /** Shared SHA-256 helper; the module never uses another digest. */
  static byte[] sha256(byte[]... parts) {
    final MessageDigest digest;
    try {
      digest = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 must be available", e);
    }
    for (byte[] part : parts) {
      digest.update(part);
    }
    return digest.digest();
  }

  static MetadataProtos.OperationId toProto(OperationId id) {
    return MetadataProtos.OperationId.newBuilder()
        .setWriter(MetadataProtos.WriterRef.newBuilder()
            .setWriterId(ByteString.copyFrom(id.writerId()))
            .setGeneration(id.generation()))
        .setSequence(id.sequence())
        .build();
  }

  /** @return the lowercase hex form used in log-friendly rendering. */
  static String toHexString(byte[] bytes) {
    final StringBuilder sb = new StringBuilder(bytes.length * 2);
    for (byte b : bytes) {
      sb.append(Character.forDigit((b >> 4) & 0xf, 16)).append(Character.forDigit(b & 0xf, 16));
    }
    return sb.toString();
  }
}
