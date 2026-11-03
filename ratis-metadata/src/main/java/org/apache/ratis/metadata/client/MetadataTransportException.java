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

/**
 * The transport-level failure of a {@link RaftMetadataClient} operation: the request never went
 * through - it exhausted its retries, the transport broke underneath it, or the reply could not
 * be trusted. Contrast {@link RaftMetadataException}, the user-visible rejection of a committed
 * command.
 */
public class MetadataTransportException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public MetadataTransportException(String message) {
        super(message);
    }

    public MetadataTransportException(String message, Throwable cause) {
        super(message, cause);
    }
}
