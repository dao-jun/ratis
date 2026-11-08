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

import org.apache.ratis.metadata.protocol.GroupConfig;

/**
 * The per-entry collection budget of one bounded maintenance pass: how many
 * records and how many bytes one entry may visit while collecting expired
 * results or floor-below change rows. One entry never scans unbounded
 * history; whatever does not fit stays for the next entry.
 */
final class GcBudget {
  private final WorkBudget budget;

  private GcBudget(WorkBudget budget) {
    this.budget = budget;
  }

  /** The result-collection budget: expired results and their expiry rows. */
  static GcBudget forResults(GroupConfig config) {
    return new GcBudget(new WorkBudget(config.resultGcRecords(), config.resultGcBytes()));
  }

  /** The change-collection budget: floor-below event rows and summaries. */
  static GcBudget forChanges(GroupConfig config) {
    return new GcBudget(new WorkBudget(config.changeGcRecords(), config.changeGcBytes()));
  }

  /**
   * @return whether the visit fits; nothing is charged when it does not
   * @see WorkBudget#tryCharge(long, long)
   */
  boolean tryCharge(long records, long bytes) {
    return budget.tryCharge(records, bytes);
  }

  long chargedRecords() {
    return budget.chargedItems();
  }

  long chargedBytes() {
    return budget.chargedBytes();
  }

  @Override
  public String toString() {
    return "GcBudget" + budget;
  }
}
