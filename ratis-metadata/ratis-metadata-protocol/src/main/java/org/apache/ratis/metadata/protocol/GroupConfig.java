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

import java.util.Objects;

import org.apache.ratis.metadata.proto.MetadataProtos;
import org.apache.ratis.metadata.proto.MetadataProtos.Role;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

/**
 * The immutable, per-group parameter set persisted by the first
 * INITIALIZE_GROUP entry. Every field is a deterministic limit: replicas and
 * clients must agree on the exact values, checked through the fingerprint.
 * Only a fully valid configuration can exist as an instance, and its
 * serialized form is canonical, so two agreeing parties always compute the
 * same fingerprint.
 */
public final class GroupConfig {
  private final MetadataProtos.GroupConfig proto;

  private GroupConfig(MetadataProtos.GroupConfig canonical) {
    this.proto = canonical;
  }

  /**
   * Development-phase defaults for one role: identical limits except the
   * result retention window (7 days coordination, 24 hours keyspace).
   *
   * @param role COORDINATION or KEYSPACE
   * @return the default configuration of that role
   * @throws MetadataProtocolException if the role is missing or unspecified
   */
  public static GroupConfig defaultsFor(Role role) {
    Objects.requireNonNull(role, "role");
    if (role == Role.ROLE_UNSPECIFIED || role == Role.UNRECOGNIZED) {
      throw new MetadataProtocolException(MetadataStatus.S_MALFORMED, "role must be specified");
    }
    final long retentionMs = role == Role.COORDINATION ? 604_800_000L : 86_400_000L;
    return valueOf(newDefaults(role, retentionMs));
  }

  private static MetadataProtos.GroupConfig newDefaults(Role role, long retentionMs) {
    return MetadataProtos.GroupConfig.newBuilder()
        .setSchemaVersion(MetadataProtocol.SCHEMA_VERSION)
        .setRole(role)
        .setRetentionMs(retentionMs)
        .setPathBytes(8192)
        .setKeyBytes(8192)
        .setPathDepth(128)
        .setValueBytes(262144)
        .setRequestBytes(1048576)
        .setMultiOps(1024)
        .setRecursiveNodes(1024)
        .setMutationNodes(2048)
        .setScannedKeys(8192)
        .setBatchBytes(33554432)
        .setLeaseBindings(256)
        .setLeaseMinTtlMs(1000)
        .setLeaseMaxTtlMs(86_400_000L)
        .setActiveLeases(100_000L)
        .setWriterRows(100_000L)
        .setStoredReplyBytes(262144)
        .setBusinessResultBytes(1_879_048_192L)
        .setControlResultBytes(268_435_456L)
        .setResultGcRecords(100)
        .setResultGcBytes(4_194_304)
        .setChangeRetentionRevisions(65_536L)
        .setChangeCapacityBytes(4_294_967_296L)
        .setChangeRecordBytes(4_194_304)
        .setChangeGcRecords(100)
        .setChangeGcBytes(8_388_608)
        .setPageEntries(1024)
        .setPageBytes(1_048_576)
        .setPageVisits(8192)
        .setChangeFloorRevisionsPerEntry(100)
        .build();
  }

  /**
   * @param proto a decoded configuration message
   * @return a validated config with the same values
   * @throws MetadataProtocolException on unknown fields, bad role or any
   *                                   limit that fails the cross checks
   */
  public static GroupConfig valueOf(MetadataProtos.GroupConfig proto) {
    Objects.requireNonNull(proto, "proto");
    if (!proto.getUnknownFields().asMap().isEmpty()) {
      throw new MetadataProtocolException(MetadataStatus.S_MALFORMED,
          "GroupConfig carries unknown fields");
    }
    if (proto.getRole() == Role.ROLE_UNSPECIFIED || proto.getRole() == Role.UNRECOGNIZED) {
      throw new MetadataProtocolException(MetadataStatus.S_MALFORMED, "role must be specified");
    }
    final GroupConfig config = new GroupConfig(proto);
    config.validate();
    return config;
  }

  /**
   * A convenience parse-and-validate for a serialized configuration.
   *
   * @throws MetadataProtocolException       if validation fails
   * @throws InvalidProtocolBufferException  if the bytes are not a message
   */
  public static GroupConfig parseFrom(byte[] bytes) throws InvalidProtocolBufferException {
    return valueOf(MetadataProtos.GroupConfig.parseFrom(bytes));
  }

  /**
   * Re-checks every deterministic limit and cross-budget invariant. Any
   * failure means this parameter set cannot govern a group, because some
   * bounded entry could not be processed in the worst case.
   *
   * @throws MetadataProtocolException naming the first violated invariant
   */
  public void validate() {
    require(schemaVersion() == MetadataProtocol.SCHEMA_VERSION, "schemaVersion must be "
        + MetadataProtocol.SCHEMA_VERSION);
    require(retentionMs() >= 1, "retentionMs must be at least 1");
    requirePositive("pathBytes", pathBytes());
    requirePositive("keyBytes", keyBytes());
    requirePositive("pathDepth", pathDepth());
    requirePositive("valueBytes", valueBytes());
    requirePositive("requestBytes", requestBytes());
    require(multiOps() >= 1, "multiOps must be at least 1");
    requirePositive("recursiveNodes", recursiveNodes());
    requirePositive("mutationNodes", mutationNodes());
    requirePositive("scannedKeys", scannedKeys());
    requirePositive("batchBytes", batchBytes());
    requirePositive("leaseBindings", leaseBindings());
    require(leaseMinTtlMs() >= 1, "leaseMinTtlMs must be at least 1");
    require(leaseMaxTtlMs() >= leaseMinTtlMs(), "leaseMaxTtlMs must not be below leaseMinTtlMs");
    require(leaseMinTtlMs() <= MetadataProtocol.DEFAULT_LEASE_TTL_MS
        && MetadataProtocol.DEFAULT_LEASE_TTL_MS <= leaseMaxTtlMs(),
        "the canonical default lease TTL must stay inside the configured range");
    requirePositive("activeLeases", activeLeases());
    requirePositive("writerRows", writerRows());
    requirePositive("storedReplyBytes", storedReplyBytes());
    require(businessResultBytes() >= storedReplyBytes() + MetadataProtocol.REPLY_OVERHEAD,
        "businessResultBytes must hold one max reply");
    require(controlResultBytes() >= storedReplyBytes() + MetadataProtocol.REPLY_OVERHEAD,
        "controlResultBytes must hold one max reply");
    requirePositive("resultGcRecords", resultGcRecords());
    require(resultGcBytes() >= storedReplyBytes() + MetadataProtocol.NODE_KEY_OVERHEAD,
        "resultGcBytes must hold one max reply record");
    requirePositive("changeRetentionRevisions", changeRetentionRevisions());
    requirePositive("changeCapacityBytes", changeCapacityBytes());
    requirePositive("changeRecordBytes", changeRecordBytes());
    requirePositive("changeGcRecords", changeGcRecords());
    require(changeGcBytes() >= changeRecordBytes() + MetadataProtocol.NODE_KEY_OVERHEAD,
        "changeGcBytes must hold one max change record");
    require(changeRecordBytes() >= pathBytes() + MetadataProtocol.NODE_KEY_OVERHEAD,
        "changeRecordBytes must hold the worst expiry event of a max-length path");
    requirePositive("pageEntries", pageEntries());
    requirePositive("pageBytes", pageBytes());
    requirePositive("pageVisits", pageVisits());
    requirePositive("changeFloorRevisionsPerEntry", changeFloorRevisionsPerEntry());
    require(leaseBindings() <= scannedKeys() && leaseBindings() <= mutationNodes(),
        "leaseBindings must fit inside the per-entry scan and mutation budgets");
    require((long) leaseBindings() * (pathBytes() + MetadataProtocol.NODE_KEY_OVERHEAD)
        <= batchBytes(),
        "the worst-case lease expiry batch must fit stagedBatchBytes");
    require((long) pathBytes() + valueBytes() + MetadataProtocol.ENVELOPE_OVERHEAD
        <= requestBytes(),
        "clientRequestBytes must admit one max path plus max value operation");
  }

  private static void requirePositive(String name, long value) {
    require(value >= 1, name + " must be at least 1");
  }

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new MetadataProtocolException(MetadataStatus.S_CONFIG_MISMATCH, message);
    }
  }

  public Role role() {
    return proto.getRole();
  }

  public int schemaVersion() {
    return proto.getSchemaVersion();
  }

  public long retentionMs() {
    return proto.getRetentionMs();
  }

  public int pathBytes() {
    return proto.getPathBytes();
  }

  public int keyBytes() {
    return proto.getKeyBytes();
  }

  public int pathDepth() {
    return proto.getPathDepth();
  }

  public int valueBytes() {
    return proto.getValueBytes();
  }

  public int requestBytes() {
    return proto.getRequestBytes();
  }

  public int multiOps() {
    return proto.getMultiOps();
  }

  public int recursiveNodes() {
    return proto.getRecursiveNodes();
  }

  public int mutationNodes() {
    return proto.getMutationNodes();
  }

  public int scannedKeys() {
    return proto.getScannedKeys();
  }

  public int batchBytes() {
    return proto.getBatchBytes();
  }

  public int leaseBindings() {
    return proto.getLeaseBindings();
  }

  public long leaseMinTtlMs() {
    return proto.getLeaseMinTtlMs();
  }

  public long leaseMaxTtlMs() {
    return proto.getLeaseMaxTtlMs();
  }

  public long activeLeases() {
    return proto.getActiveLeases();
  }

  public long writerRows() {
    return proto.getWriterRows();
  }

  public int storedReplyBytes() {
    return proto.getStoredReplyBytes();
  }

  public long businessResultBytes() {
    return proto.getBusinessResultBytes();
  }

  public long controlResultBytes() {
    return proto.getControlResultBytes();
  }

  public int resultGcRecords() {
    return proto.getResultGcRecords();
  }

  public int resultGcBytes() {
    return proto.getResultGcBytes();
  }

  public long changeRetentionRevisions() {
    return proto.getChangeRetentionRevisions();
  }

  public long changeCapacityBytes() {
    return proto.getChangeCapacityBytes();
  }

  public int changeRecordBytes() {
    return proto.getChangeRecordBytes();
  }

  public int changeGcRecords() {
    return proto.getChangeGcRecords();
  }

  public int changeGcBytes() {
    return proto.getChangeGcBytes();
  }

  /** The maximum entries one page may return; the smaller request page size
   *  is a client-side choice, not a group parameter. */
  public int pageEntries() {
    return proto.getPageEntries();
  }

  public int pageBytes() {
    return proto.getPageBytes();
  }

  public int pageVisits() {
    return proto.getPageVisits();
  }

  public int changeFloorRevisionsPerEntry() {
    return proto.getChangeFloorRevisionsPerEntry();
  }

  /**
   * @return the canonical protobuf form: every field is valid and non-zero,
   *         hence serialized, and the bytes are fingerprint-stable.
   */
  public MetadataProtos.GroupConfig toProto() {
    return proto;
  }

  /** @return a builder pre-filled with the canonical values of this config. */
  public MetadataProtos.GroupConfig.Builder toBuilder() {
    return proto.toBuilder();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof GroupConfig)) {
      return false;
    }
    return proto.equals(((GroupConfig) obj).proto);
  }

  @Override
  public int hashCode() {
    return proto.hashCode();
  }

  @Override
  public String toString() {
    return "GroupConfig(role=" + role() + ", retentionMs=" + retentionMs() + ")";
  }
}
