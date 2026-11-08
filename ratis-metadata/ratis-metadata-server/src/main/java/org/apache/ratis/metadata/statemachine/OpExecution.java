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
 * The toolkit one entry's operation appliers share: the entry context, the
 * overlay they stage into, the change feed, and the two ordinal counters.
 * Stamps count the node mutations of the entry; events count the feed rows -
 * separate domains, each contiguous from 1, since a mutation without a stamp
 * (a delete) and a stamp without an event (a counter move) must not shift
 * the other domain's sequence.
 */
final class OpExecution {
  private final ApplyContext entry;
  private final OverlayState overlay;
  private final ChangeLog changes;
  private int stampOrdinals;
  private int eventOrdinals;

  OpExecution(ApplyContext entry, OverlayState overlay, ChangeLog changes) {
    this.entry = entry;
    this.overlay = overlay;
    this.changes = changes;
  }

  ApplyContext entry() {
    return entry;
  }

  OverlayState overlay() {
    return overlay;
  }

  ChangeLog changes() {
    return changes;
  }

  /** @return the next node-mutation stamp ordinal of this entry, from 1. */
  int nextStampOrdinal() {
    return ++stampOrdinals;
  }

  /** @return the next change-feed event ordinal of this entry, from 1. */
  int nextEventOrdinal() {
    return ++eventOrdinals;
  }
}
