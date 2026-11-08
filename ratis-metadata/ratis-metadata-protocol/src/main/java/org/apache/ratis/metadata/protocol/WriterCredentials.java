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
import java.util.Optional;
import org.apache.ratis.protocol.RaftGroupId;

/**
 * The durable writer identity a client presents when opening a facade: the
 * business writer id with its known generation, plus the control writer on
 * the coordination role. A KeyValue facade must not carry a control
 * credential. Credentials contain no lease and promise no payload storage;
 * resuming undecided results requires the caller-supplied recovery plan.
 */
public final class WriterCredentials {
  private final RaftGroupId groupId;
  private final byte[] businessWriterId;
  private final long businessGeneration;
  private final ControlCredential control;

  private WriterCredentials(RaftGroupId groupId, byte[] businessWriterId,
      long businessGeneration, ControlCredential control) {
    this.groupId = groupId;
    this.businessWriterId = businessWriterId;
    this.businessGeneration = businessGeneration;
    this.control = control;
  }

  /**
   * @param groupId          the single group this writer belongs to
   * @param businessWriterId 16 non-zero bytes, persisted by the caller
   * @param businessGeneration at least 1
   * @param control          coordination control writer, or {@code null}
   * @return validated credentials
   * @throws MetadataProtocolException on any malformed field
   */
  public static WriterCredentials valueOf(RaftGroupId groupId, byte[] businessWriterId,
      long businessGeneration, ControlCredential control) {
    Objects.requireNonNull(groupId, "groupId");
    Objects.requireNonNull(businessWriterId, "businessWriterId");
    if (businessWriterId.length != 16) {
      throw new MetadataProtocolException(MetadataStatus.S_MALFORMED,
          "businessWriterId must be 16 bytes, got " + businessWriterId.length);
    }
    if (businessGeneration < 1) {
      throw new MetadataProtocolException(MetadataStatus.S_MALFORMED,
          "businessGeneration must be at least 1: " + businessGeneration);
    }
    final OperationId shape = OperationId.valueOf(businessWriterId, businessGeneration, 1);
    return new WriterCredentials(groupId, shape.writerId(), businessGeneration, control);
  }

  public RaftGroupId groupId() {
    return groupId;
  }

  /** @return a defensive copy of the 16-byte business writer id. */
  public byte[] businessWriterId() {
    return businessWriterId.clone();
  }

  public long businessGeneration() {
    return businessGeneration;
  }

  /** @return the control writer of a coordination facade, if present. */
  public Optional<ControlCredential> control() {
    return Optional.ofNullable(control);
  }

  /** The control-lane writer of a coordination facade. */
  public static final class ControlCredential {
    private final byte[] writerId;
    private final long generation;

    private ControlCredential(byte[] writerId, long generation) {
      this.writerId = writerId;
      this.generation = generation;
    }

    /**
     * @param writerId   16 non-zero bytes, different from the business id
     * @param generation at least 1
     * @return a validated control credential
     * @throws MetadataProtocolException on any malformed field
     */
    public static ControlCredential valueOf(byte[] writerId, long generation) {
      Objects.requireNonNull(writerId, "writerId");
      return new ControlCredential(OperationId.valueOf(writerId, generation, 1).writerId(),
          generation);
    }

    /** @return a defensive copy of the 16-byte control writer id. */
    public byte[] writerId() {
      return writerId.clone();
    }

    public long generation() {
      return generation;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof ControlCredential)) {
        return false;
      }
      final ControlCredential other = (ControlCredential) obj;
      return generation == other.generation && Arrays.equals(writerId, other.writerId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(Arrays.hashCode(writerId), generation);
    }

    @Override
    public String toString() {
      return "(writer=" + MetadataProtocol.toHexString(writerId) + ",gen=" + generation + ")";
    }
  }
}
