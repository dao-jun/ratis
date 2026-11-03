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
package org.apache.ratis.metadata.client;

import org.apache.ratis.metadata.protocol.RaftResult;

/**
 * The user-visible failure of a {@link RaftMetadataClient} operation: the committed command was
 * rejected.
 */
public class RaftMetadataException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final RaftResult.Status status;

    /** The failing sub-command index of a {@link RaftResult.Status#MULTI_FAILED}; else -1. */
    private final int failIndex;

    /** The failing sub-command status of a {@link RaftResult.Status#MULTI_FAILED}; else null. */
    private final RaftResult.Status failStatus;

    RaftMetadataException(String message, RaftResult.Status status, int failIndex, RaftResult.Status failStatus) {
        super(message);
        this.status = status;
        this.failIndex = failIndex;
        this.failStatus = failStatus;
    }

    /** The status the state machine answered with. */
    public RaftResult.Status getStatus() {
        return status;
    }

    /** The failing sub-command index when {@link #getStatus()} is {@code MULTI_FAILED}, else -1. */
    public int getFailIndex() {
        return failIndex;
    }

    /**
     * The failing sub-command status when {@link #getStatus()} is {@code MULTI_FAILED}, else null.
     */
    public RaftResult.Status getFailStatus() {
        return failStatus;
    }
}
