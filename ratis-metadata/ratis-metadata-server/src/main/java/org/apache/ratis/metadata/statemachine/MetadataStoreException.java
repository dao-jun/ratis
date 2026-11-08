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
 * A storage-layer failure that must never be translated into a business
 * status: corrupted records, RocksDB errors, and lifecycle violations all
 * fail the enclosing group instead. Carrying a {@link Reason} lets callers
 * distinguish "the data is wrong" from "the handle is closed" without
 * parsing messages.
 */
class MetadataStoreException extends RuntimeException {
    /** The failure class; drives metrics and the fail-group decision. */
    enum Reason {
        /** A stored record or marker violated the codec contract. */
        CORRUPT,
        /** RocksDB raised an error reading or writing. */
        IO,
        /** The store lifecycle rejected the call (closed, closing, timeout). */
        LIFECYCLE
    }

    private final Reason reason;

    MetadataStoreException(Reason reason, String message) {
        super(message);
        this.reason = reason;
    }

    MetadataStoreException(Reason reason, String message, Throwable cause) {
        super(message, cause);
        this.reason = reason;
    }

    Reason reason() {
        return reason;
    }

    static MetadataStoreException corrupt(String message) {
        return new MetadataStoreException(Reason.CORRUPT, message);
    }

    static MetadataStoreException corrupt(String message, Throwable cause) {
        return new MetadataStoreException(Reason.CORRUPT, message, cause);
    }

    static MetadataStoreException io(String message, Throwable cause) {
        return new MetadataStoreException(Reason.IO, message, cause);
    }
}
