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

import java.util.Arrays;
import java.util.Objects;

import org.apache.ratis.metadata.proto.MetadataProtos.Operation;
import org.apache.ratis.metadata.proto.MetadataProtos.Role;
import org.apache.ratis.protocol.RaftGroupId;

/**
 * The frozen core shared by {@link FrozenRequest} and {@link ClientWrite}:
 * the target group and role, the config fingerprint the digest was computed
 * with, the operation identity, the canonical operation and its digest, and
 * the first-freeze retry budget. Canonical bytes derive from the immutable
 * canonical message, so they cannot drift.
 */
final class FrozenOperation {
  private final RaftGroupId groupId;
  private final Role role;
  private final byte[] configFingerprint;
  private final OperationId operationId;
  private final Operation canonical;
  private final byte[] digest;
  private final long retryBudgetMs;

  FrozenOperation(RaftGroupId groupId, Role role, byte[] configFingerprint,
      OperationId operationId, Operation canonical, byte[] digest, long retryBudgetMs) {
    this.groupId = Objects.requireNonNull(groupId, "groupId");
    this.role = Objects.requireNonNull(role, "role");
    this.configFingerprint = Objects.requireNonNull(configFingerprint, "configFingerprint");
    this.operationId = Objects.requireNonNull(operationId, "operationId");
    this.canonical = Objects.requireNonNull(canonical, "canonical");
    this.digest = Objects.requireNonNull(digest, "digest");
    this.retryBudgetMs = retryBudgetMs;
  }

  RaftGroupId groupId() {
    return groupId;
  }

  Role role() {
    return role;
  }

  byte[] configFingerprintCopy() {
    return configFingerprint.clone();
  }

  OperationId operationId() {
    return operationId;
  }

  Operation canonical() {
    return canonical;
  }

  byte[] canonicalBytes() {
    return canonical.toByteArray();
  }

  byte[] digestCopy() {
    return digest.clone();
  }

  byte[] digest() {
    return digest;
  }

  long retryBudgetMs() {
    return retryBudgetMs;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof FrozenOperation)) {
      return false;
    }
    final FrozenOperation other = (FrozenOperation) obj;
    return groupId.equals(other.groupId) && operationId.equals(other.operationId)
        && retryBudgetMs == other.retryBudgetMs && Arrays.equals(digest, other.digest);
  }

  @Override
  public int hashCode() {
    return Objects.hash(groupId, operationId, Arrays.hashCode(digest));
  }

  @Override
  public String toString() {
    return "(" + operationId + ",digest=" + MetadataProtocol.toHexString(digest) + ")";
  }
}
