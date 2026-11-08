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

import java.util.Objects;

import org.apache.ratis.metadata.protocol.GroupConfig;
import org.apache.ratis.protocol.RaftGroupId;

/**
 * The apply-time context of exactly one replicated entry: the group, the
 * entry position, the post-apply logical time ({@code max} of the persisted
 * watermark and the entry's proposed time - D12) and the group configuration
 * that entry must run under. Constructed by the adapter, never client input.
 */
final class ApplyContext {
  private final RaftGroupId groupId;
  private final long term;
  private final long index;
  private final long logicalNow;
  private final GroupConfig config;

  ApplyContext(RaftGroupId groupId, long term, long index, long logicalNow, GroupConfig config) {
    this.groupId = Objects.requireNonNull(groupId, "groupId");
    if (term < 0 || index < 0 || logicalNow < 0) {
      throw new IllegalArgumentException("apply positions must not be negative: term="
          + term + ",index=" + index + ",logicalNow=" + logicalNow);
    }
    this.term = term;
    this.index = index;
    this.logicalNow = logicalNow;
    this.config = Objects.requireNonNull(config, "config");
  }

  RaftGroupId groupId() {
    return groupId;
  }

  long term() {
    return term;
  }

  long index() {
    return index;
  }

  long logicalNow() {
    return logicalNow;
  }

  GroupConfig config() {
    return config;
  }

  @Override
  public String toString() {
    return "(group=" + groupId + ",term=" + term + ",index=" + index
        + ",logicalNow=" + logicalNow + ")";
  }
}
