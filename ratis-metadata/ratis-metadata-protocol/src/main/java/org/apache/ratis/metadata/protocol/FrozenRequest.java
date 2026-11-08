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

import org.apache.ratis.metadata.proto.MetadataProtos.Operation;
import org.apache.ratis.metadata.proto.MetadataProtos.Role;
import org.apache.ratis.protocol.RaftGroupId;

/**
 * One business operation frozen for submission: the operation has passed
 * deterministic validation against the group configuration, its canonical
 * bytes and digest are immutable snapshots, and the retry budget is fixed
 * at the first freeze. Retries must reuse this object so neither the
 * payload nor the budget can drift.
 */
public final class FrozenRequest {
  private final FrozenOperation frozen;
  private final GroupConfig config;

  FrozenRequest(FrozenOperation frozen, GroupConfig config) {
    this.frozen = Objects.requireNonNull(frozen, "frozen");
    this.config = Objects.requireNonNull(config, "config");
  }

  public RaftGroupId groupId() {
    return frozen.groupId();
  }

  public Role role() {
    return frozen.role();
  }

  /** @return the configuration the operation was validated against. */
  public GroupConfig config() {
    return config;
  }

  /** @return a defensive copy of the 32-byte config fingerprint. */
  public byte[] configFingerprint() {
    return frozen.configFingerprintCopy();
  }

  public OperationId operationId() {
    return frozen.operationId();
  }

  /** @return the canonical operation message the digest was computed over. */
  public Operation operation() {
    return frozen.canonical();
  }

  /** @return a defensive copy of the canonical operation bytes. */
  public byte[] canonicalOperationBytes() {
    return frozen.canonicalBytes();
  }

  /** @return a defensive copy of the 32-byte operation digest. */
  public byte[] digest() {
    return frozen.digestCopy();
  }

  public long retryBudgetMs() {
    return frozen.retryBudgetMs();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof FrozenRequest)) {
      return false;
    }
    final FrozenRequest other = (FrozenRequest) obj;
    return frozen.equals(other.frozen) && config.equals(other.config);
  }

  @Override
  public int hashCode() {
    return Objects.hash(frozen, config);
  }

  @Override
  public String toString() {
    return "FrozenRequest" + frozen;
  }
}
