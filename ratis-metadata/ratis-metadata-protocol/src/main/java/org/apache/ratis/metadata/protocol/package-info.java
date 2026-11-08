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

/**
 * Immutable public value types and wire boundary of the metadata protocol.
 *
 * <p>The generated protobuf types under {@code org.apache.ratis.metadata.proto}
 * are an internal encoding detail; clients and servers exchange the immutable
 * values defined in this package. {@link org.apache.ratis.metadata.protocol.MetadataProtocol}
 * freezes an operation into a {@link org.apache.ratis.metadata.protocol.FrozenRequest}
 * with canonical bytes and a digest, decodes a received write envelope, and
 * validates tree paths and binary keys. All failures are reported as
 * {@link org.apache.ratis.metadata.protocol.MetadataProtocolException} with a
 * {@link org.apache.ratis.metadata.protocol.MetadataStatus}.
 */
package org.apache.ratis.metadata.protocol;
