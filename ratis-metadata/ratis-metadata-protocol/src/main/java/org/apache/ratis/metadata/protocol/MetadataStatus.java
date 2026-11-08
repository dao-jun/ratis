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

/**
 * Stable status vocabulary shared by write replies and queries. Constant
 * names and wire codes match the {@code Status} enum of the protocol schema
 * one-to-one; the codes are part of the wire contract and never reused.
 */
public enum MetadataStatus {
  S_UNSPECIFIED(0),
  S_OK(1),
  S_NOT_FOUND(2),
  S_NODE_EXISTS(3),
  S_NO_PARENT(4),
  S_HAS_CHILDREN(5),
  S_VERSION_MISMATCH(6),
  S_NOT_OWNER(7),
  S_NOT_ALLOWED(8),
  S_UNKNOWN_LEASE(9),
  S_LEASE_EXPIRED(10),
  S_BAD_TTL(11),
  S_LIMIT_EXCEEDED(12),
  S_REPLY_TOO_LARGE(13),
  S_NUMERIC_OVERFLOW(14),
  S_MULTI_FAILED(15),
  S_MALFORMED(16),
  S_BAD_PATH(17),
  S_WRONG_ROLE(18),
  S_CONFIG_MISMATCH(19),
  S_BUDGET_MISMATCH(20),
  S_GROUP_NOT_INITIALIZED(21),
  S_SEQUENCE_GAP(22),
  S_RESULT_CAPACITY(23),
  S_CHANGE_CAPACITY(24),
  S_UNKNOWN_WRITER(25),
  S_STALE_GENERATION(26),
  S_FUTURE_GENERATION(27),
  S_INVOCATION_MISMATCH(28),
  S_RESULT_EXPIRED(29),
  S_WRITER_LIMIT(30),
  S_WRITER_EXISTS(31),
  S_GENERATION_EXHAUSTED(32),
  S_LEASE_LIMIT(33),
  S_NOT_HELD(34),
  S_NEED_SNAPSHOT(35),
  S_PAGE_TOO_SMALL(36),
  S_SNAPSHOT_TOO_LARGE(37),
  S_BAD_CURSOR(38),
  S_REPLAY_UNAVAILABLE(39),
  S_SEQUENCE_EXHAUSTED(40);

  private final int wireCode;

  MetadataStatus(int wireCode) {
    this.wireCode = wireCode;
  }

  /** @return the wire code of this status, matching the protocol schema. */
  public int wireCode() {
    return wireCode;
  }

  /**
   * @param code a decoded wire code
   * @return the status constant carrying that code
   * @throws IllegalArgumentException if the code is not part of this version
   */
  public static MetadataStatus valueOfWireCode(int code) {
    for (MetadataStatus status : values()) {
      if (status.wireCode == code) {
        return status;
      }
    }
    throw new IllegalArgumentException("Unknown status wire code: " + code);
  }
}
