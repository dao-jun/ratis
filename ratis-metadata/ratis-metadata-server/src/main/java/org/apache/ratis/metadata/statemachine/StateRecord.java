/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import static org.apache.ratis.metadata.statemachine.MetadataStateMachine.bytesToInt;
import static org.apache.ratis.metadata.statemachine.MetadataStateMachine.bytesToLong;
import static org.apache.ratis.metadata.statemachine.MetadataStateMachine.putInt;
import static org.apache.ratis.metadata.statemachine.MetadataStateMachine.putLong;

/** The record stored in the {@code state} CF; fixed-width header, data appended. */
final class StateRecord {
    private static final int HEADER_LENGTH = 4 + 8 + 8 + 8;

    private final int version;
    private final long leaseId;
    private final long createTime;
    private final long modifyTime;
    private final byte[] data;

    int version() {
        return version;
    }

    long leaseId() {
        return leaseId;
    }

    long createTime() {
        return createTime;
    }

    long modifyTime() {
        return modifyTime;
    }

    byte[] data() {
        return data;
    }

    StateRecord(int version, long leaseId, long createTime, long modifyTime, byte[] data) {
        this.version = version;
        this.leaseId = leaseId;
        this.createTime = createTime;
        this.modifyTime = modifyTime;
        this.data = data;
    }

    byte[] encode() {
        final byte[] encoded = new byte[HEADER_LENGTH + data.length];
        putInt(encoded, 0, version);
        putLong(encoded, 4, leaseId);
        putLong(encoded, 12, createTime);
        putLong(encoded, 20, modifyTime);
        System.arraycopy(data, 0, encoded, HEADER_LENGTH, data.length);
        return encoded;
    }

    /** A record at birth: version 1, lease-bound, one timestamp for create and modify. */
    static StateRecord fresh(long leaseId, long timestamp, byte[] data) {
        return new StateRecord(1, leaseId, timestamp, timestamp, data);
    }

    static StateRecord decode(byte[] encoded) {
        final int version = bytesToInt(encoded, 0);
        final long leaseId = bytesToLong(encoded, 4);
        final long createTime = bytesToLong(encoded, 12);
        final long modifyTime = bytesToLong(encoded, 20);
        final byte[] data = new byte[encoded.length - HEADER_LENGTH];
        System.arraycopy(encoded, HEADER_LENGTH, data, 0, data.length);
        return new StateRecord(version, leaseId, createTime, modifyTime, data);
    }

    /** The version of an encoded record without decoding it; version sits at offset 0. */
    static int versionOf(byte[] encoded) {
        return bytesToInt(encoded, 0);
    }
}
