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
import org.apache.ratis.protocol.RaftGroupId;

/**
 * The unforgeable proof of one lock acquisition. Only a standalone typed
 * lock CREATE produces a token, and its log index is the index of that very
 * entry, so a group's log index alone identifies the acquisition.
 */
public final class LockToken {
  private final RaftGroupId groupId;
  private final long logIndex;

  private LockToken(RaftGroupId groupId, long logIndex) {
    this.groupId = groupId;
    this.logIndex = logIndex;
  }

  /**
   * @param groupId group in which the lock was acquired
   * @param logIndex log index of the lock CREATE entry
   * @return a token for one lock acquisition
   * @throws MetadataProtocolException if the log index is not positive
   */
  public static LockToken valueOf(RaftGroupId groupId, long logIndex) {
    Objects.requireNonNull(groupId, "groupId");
    if (logIndex < 0) {
      throw new MetadataProtocolException(MetadataStatus.S_MALFORMED,
          "logIndex must not be negative: " + logIndex);
    }
    return new LockToken(groupId, logIndex);
  }

  public RaftGroupId groupId() {
    return groupId;
  }

  public long logIndex() {
    return logIndex;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof LockToken)) {
      return false;
    }
    final LockToken other = (LockToken) obj;
    return logIndex == other.logIndex && groupId.equals(other.groupId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(groupId, logIndex);
  }

  @Override
  public String toString() {
    return "(" + groupId + ",#" + logIndex + ")";
  }
}
