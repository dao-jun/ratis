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

/**
 * The version of one node creation or modification: the log index of the
 * entry that produced it plus the ordinal of the node mutation within that
 * entry. Ordinals start at 1 and increase with the actual mutation order of
 * the entry, which prevents delete-then-recreate CAS ABA: a recreated node
 * always carries a different stamp.
 */
public final class Stamp {
  private final long logIndex;
  private final int ordinal;

  private Stamp(long logIndex, int ordinal) {
    this.logIndex = logIndex;
    this.ordinal = ordinal;
  }

  /**
   * @param logIndex log index of the entry that produced the mutation
   * @param ordinal  1-based position of the node mutation within the entry
   * @return a stamp for one concrete node mutation
   * @throws MetadataProtocolException if the ordinal is not positive
   */
  public static Stamp valueOf(long logIndex, int ordinal) {
    if (logIndex < 0) {
      throw new MetadataProtocolException(MetadataStatus.S_MALFORMED,
          "logIndex must not be negative: " + logIndex);
    }
    if (ordinal < 1) {
      throw new MetadataProtocolException(MetadataStatus.S_MALFORMED,
          "ordinal must start at 1: " + ordinal);
    }
    return new Stamp(logIndex, ordinal);
  }

  public long logIndex() {
    return logIndex;
  }

  public int ordinal() {
    return ordinal;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof Stamp)) {
      return false;
    }
    final Stamp other = (Stamp) obj;
    return logIndex == other.logIndex && ordinal == other.ordinal;
  }

  @Override
  public int hashCode() {
    return Objects.hash(logIndex, ordinal);
  }

  @Override
  public String toString() {
    return "(" + logIndex + "," + ordinal + ")";
  }
}
