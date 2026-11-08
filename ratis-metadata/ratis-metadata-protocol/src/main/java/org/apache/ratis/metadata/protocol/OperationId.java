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

/**
 * The durable identity of one business operation: writer id, generation and
 * the per-writer sequence number. Sequences are strictly consecutive per
 * writer and generation; {@link Long#MAX_VALUE} is reserved as the
 * exhaustion marker, so the last usable sequence is {@code MAX_VALUE - 1}.
 */
public final class OperationId {
  private static final int WRITER_ID_BYTES = 16;

  private final byte[] writerId;
  private final long generation;
  private final long sequence;

  private OperationId(byte[] writerId, long generation, long sequence) {
    this.writerId = writerId;
    this.generation = generation;
    this.sequence = sequence;
  }

  /**
   * @param writerId   16 non-zero bytes identifying the writer
   * @param generation at least 1; older generations cannot produce new effects
   * @param sequence   in {@code [1, Long.MAX_VALUE - 1]}
   * @return a validated operation identity
   * @throws MetadataProtocolException on any malformed field
   */
  public static OperationId valueOf(byte[] writerId, long generation, long sequence) {
    Objects.requireNonNull(writerId, "writerId");
    if (writerId.length != WRITER_ID_BYTES) {
      throw new MetadataProtocolException(MetadataStatus.S_MALFORMED,
          "writerId must be " + WRITER_ID_BYTES + " bytes, got " + writerId.length);
    }
    boolean allZero = true;
    for (byte b : writerId) {
      if (b != 0) {
        allZero = false;
        break;
      }
    }
    if (allZero) {
      throw new MetadataProtocolException(MetadataStatus.S_MALFORMED, "writerId must not be all zero");
    }
    if (generation < 1) {
      throw new MetadataProtocolException(MetadataStatus.S_MALFORMED,
          "generation must be at least 1: " + generation);
    }
    if (sequence < 1) {
      throw new MetadataProtocolException(MetadataStatus.S_MALFORMED,
          "sequence must be at least 1: " + sequence);
    }
    if (sequence == Long.MAX_VALUE) {
      throw new MetadataProtocolException(MetadataStatus.S_SEQUENCE_EXHAUSTED,
          "Long.MAX_VALUE is reserved as the sequence exhaustion marker");
    }
    return new OperationId(writerId.clone(), generation, sequence);
  }

  /** @return a defensive copy of the 16-byte writer id. */
  public byte[] writerId() {
    return writerId.clone();
  }

  public long generation() {
    return generation;
  }

  public long sequence() {
    return sequence;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof OperationId)) {
      return false;
    }
    final OperationId other = (OperationId) obj;
    return generation == other.generation && sequence == other.sequence
        && Arrays.equals(writerId, other.writerId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(Arrays.hashCode(writerId), generation, sequence);
  }

  @Override
  public String toString() {
    return "(writer=" + MetadataProtocol.toHexString(writerId)
        + ",gen=" + generation + ",seq=" + sequence + ")";
  }
}
