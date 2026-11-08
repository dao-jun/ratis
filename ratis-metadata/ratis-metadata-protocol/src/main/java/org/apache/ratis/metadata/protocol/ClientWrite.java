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
import org.apache.ratis.metadata.proto.MetadataProtos.Operation;
import org.apache.ratis.metadata.proto.MetadataProtos.Role;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/**
 * The write envelope as it travels over the wire and inside the replicated
 * log: wire version, group, role, config fingerprint, frozen retry budget,
 * operation identity, the operation itself and its digest. Produced from a
 * {@link FrozenRequest} when sending and by
 * {@link MetadataProtocol#decodeWrite(byte[])} when receiving.
 */
public final class ClientWrite {
  private final FrozenOperation frozen;

  ClientWrite(FrozenOperation frozen) {
    this.frozen = Objects.requireNonNull(frozen, "frozen");
  }

  /** @return the envelope message form of this write. */
  public MetadataProtos.ClientWrite toProto() {
    return MetadataProtos.ClientWrite.newBuilder()
        .setWireVersion(MetadataProtocol.WIRE_VERSION)
        .setGroupId(ByteString.copyFrom(frozen.groupId().toByteString().toByteArray()))
        .setRole(frozen.role())
        .setFingerprint(ByteString.copyFrom(frozen.configFingerprintCopy()))
        .setRetryBudgetMs(frozen.retryBudgetMs())
        .setId(MetadataProtocol.toProto(frozen.operationId()))
        .setOperation(frozen.canonical())
        .setDigest(ByteString.copyFrom(frozen.digestCopy()))
        .build();
  }

  public RaftGroupId groupId() {
    return frozen.groupId();
  }

  public Role role() {
    return frozen.role();
  }

  /** @return a defensive copy of the 32-byte config fingerprint. */
  public byte[] configFingerprint() {
    return frozen.configFingerprintCopy();
  }

  public OperationId operationId() {
    return frozen.operationId();
  }

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
}
