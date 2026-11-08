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

import org.rocksdb.RocksDB;

/**
 * The column families of layout version 3, in declaration (and batch
 * materialization) order. Every group directory carries exactly these
 * families; an unexpected family on open is a layout mismatch, not data
 * to interpret.
 */
enum MetadataCf {
    DEFAULT(RocksDB.DEFAULT_COLUMN_FAMILY),
    NODES("nodes"),
    COUNTERS("counters"),
    WRITERS("writers"),
    LEASES("leases"),
    LEASE_PATHS("lease-paths"),
    LEASE_EXPIRY("lease-expiry"),
    COMMAND_RESULTS("command-results"),
    RESULT_EXPIRY("result-expiry"),
    CHANGES("changes"),
    CHANGE_REVISIONS("change-revisions");

    private final byte[] name;

    MetadataCf(String name) {
        this.name = name.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    MetadataCf(byte[] name) {
        this.name = name;
    }

    byte[] cfName() {
        return name.clone();
    }

    String displayName() {
        return new String(name, java.nio.charset.StandardCharsets.UTF_8);
    }
}
