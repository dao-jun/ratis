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
package org.apache.ratis.metadata;

import org.apache.ratis.metadata.client.RaftMetadataClient;
import org.apache.ratis.rpc.SupportedRpcType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.apache.ratis.metadata.util.TestWait.waitUntil;

/**
 * A smoke test of the coordination layer on the alternative stock transport: the same three-node
 * grpc main-line flow (elect, replicate, read back, fail over) over unmodified ratis netty. The
 * grpc suite carries the thorough assertions; this one only proves the layer is transport-blind.
 */
public class TestMetadataNettyCluster {

    @TempDir File tempDir;

    @Test
    void testElectReplicateReadBackAndFailOverOnNetty() throws Exception {
        try (MetadataCluster cluster =
                MetadataCluster.newBuilder(tempDir)
                        .setNumServers(3)
                        .setRpcType(SupportedRpcType.NETTY)
                        .build()) {
            cluster.start();
            final RaftMetadataClient client = cluster.newMetadataClient();
            try {
                final String leader0 = cluster.awaitLeader().toString();
                client.put("/smoke", "root".getBytes(StandardCharsets.UTF_8));
                client.put("/smoke/a", "one".getBytes(StandardCharsets.UTF_8));
                assertArrayEquals(
                        "one".getBytes(StandardCharsets.UTF_8),
                        client.get("/smoke/a").getData(),
                        "the written record reads back");

                cluster.killServer(cluster.indexOf(cluster.awaitLeader()));
                final String leader1 = cluster.awaitLeader().toString();
                assertNotEquals(leader0, leader1, "a surviving node takes over");

                // The client addresses every member, so its retries carry the failover; a fresh
                // write confirms the new leader commits again.
                waitUntil(
                        () -> {
                            client.put("/smoke/b", "two".getBytes(StandardCharsets.UTF_8));
                            return "two".equals(
                                    new String(client.get("/smoke/b").getData(),
                                            StandardCharsets.UTF_8));
                        },
                        Duration.ofSeconds(30),
                        "Writes did not resume on the new netty leader");
            } finally {
                client.close();
            }
        }
    }
}
