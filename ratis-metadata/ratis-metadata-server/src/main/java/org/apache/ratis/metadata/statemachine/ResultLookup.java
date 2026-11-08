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

/**
 * The outcome of one result-table lookup: the original reply bytes for a
 * live digest match, or one of the three deterministic alternatives. The
 * reply is only present on {@link Kind#REPLAY}.
 */
final class ResultLookup {
  /** The deterministic lookup outcomes, in ladder order. */
  enum Kind {
    /** No row under this identity: the operation never executed here. */
    MISS,
    /** A row exists but is past its logical expiry; its digest means nothing. */
    RESULT_EXPIRED,
    /** A live row under a different digest: the identity was reused. */
    INVOCATION_MISMATCH,
    /** A live row with this exact digest: replay the original bytes. */
    REPLAY
  }

  private final Kind kind;
  private final byte[] originalReply;
  private final long firstTerm;
  private final long firstIndex;

  private ResultLookup(Kind kind, byte[] originalReply, long firstTerm, long firstIndex) {
    this.kind = kind;
    this.originalReply = originalReply;
    this.firstTerm = firstTerm;
    this.firstIndex = firstIndex;
  }

  static ResultLookup miss() {
    return new ResultLookup(Kind.MISS, null, -1, -1);
  }

  static ResultLookup expired() {
    return new ResultLookup(Kind.RESULT_EXPIRED, null, -1, -1);
  }

  static ResultLookup mismatch() {
    return new ResultLookup(Kind.INVOCATION_MISMATCH, null, -1, -1);
  }

  static ResultLookup replay(StoredResult result) {
    return new ResultLookup(Kind.REPLAY, result.reply(), result.firstTerm(), result.firstIndex());
  }

  Kind kind() {
    return kind;
  }

  /** @return a defensive copy of the original BusinessResult bytes; REPLAY only. */
  byte[] originalReply() {
    if (originalReply == null) {
      throw new IllegalStateException("no reply carried on " + kind);
    }
    return originalReply.clone();
  }

  long firstTerm() {
    return firstTerm;
  }

  long firstIndex() {
    return firstIndex;
  }

  @Override
  public String toString() {
    return kind + (originalReply == null ? "" : "(" + originalReply.length + " bytes)");
  }
}
