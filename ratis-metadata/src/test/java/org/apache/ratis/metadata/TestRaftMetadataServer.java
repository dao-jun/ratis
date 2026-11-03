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

import org.apache.ratis.metadata.util.FileUtils;
import org.apache.ratis.metadata.util.SystemClock;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.NetUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Configuration wiring of {@link MetadataServer}: quorum parsing and ratis properties. */
public class TestRaftMetadataServer {

    @TempDir File tempDir;

    @Test
    void testQuorumPeersParseIpv6AddressesToBracketedForm() {
        final List<RaftPeer> peers =
                MetadataServer.parseQuorumPeers(
                        Arrays.asList("n1@[::1]:9126", "n2@::1:9127", "n3@host3:9128"),
                        RaftPeerId.valueOf("n1"),
                        0);

        // ratis parses addresses through java.net.URI, which rejects the bare ::1:9126 form
        assertEquals("[::1]:9126", peers.get(0).getAddress());
        assertEquals("[::1]:9127", peers.get(1).getAddress());
        assertEquals("host3:9128", peers.get(2).getAddress());
        assertEquals(9126, NetUtils.createSocketAddr(peers.get(0).getAddress()).getPort());
        assertEquals(9127, NetUtils.createSocketAddr(peers.get(1).getAddress()).getPort());
        assertEquals(9128, NetUtils.createSocketAddr(peers.get(2).getAddress()).getPort());
    }

    @Test
    void testQuorumPeersSubstituteOwnPortOnlyForSelf() {
        final List<RaftPeer> peers =
                MetadataServer.parseQuorumPeers(
                        Arrays.asList("n1@host1:0", "n2@host2:9126"),
                        RaftPeerId.valueOf("n1"),
                        4242);
        assertEquals("host1:4242", peers.get(0).getAddress());
        assertEquals("host2:9126", peers.get(1).getAddress());

        final IllegalArgumentException refusal =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                MetadataServer.parseQuorumPeers(
                                        Arrays.asList("n1@host1:9126", "n2@host2:0"),
                                        RaftPeerId.valueOf("n1"),
                                        4242));
        assertTrue(refusal.getMessage().contains("n2@host2:0"));
    }

    @Test
    void testQuorumPeersRequireTheSelfId() {
        final IllegalArgumentException refusal =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                MetadataServer.parseQuorumPeers(
                                        Arrays.asList("n1@host1:9126", "n2@host2:9127"),
                                        RaftPeerId.valueOf("n3"),
                                        0));
        assertTrue(refusal.getMessage().contains("n3"));
    }

    /** The endpoints the metadata client connects to must round-trip through ratis as well. */
    @Test
    void testPeerEndpointKeepsTheBracketedHostForTheClientRoundTrip() {
        assertEquals("[::1]", MetadataServer.peerEndpoint("[::1]:9126").getHostString());
        assertEquals("[::1]", MetadataServer.peerEndpoint("::1:9126").getHostString());
        assertEquals("host1", MetadataServer.peerEndpoint("host1:9126").getHostString());

        assertEquals("[::1]:9126", MetadataServer.peerAddress(MetadataServer.peerEndpoint("::1:9126")));
        assertEquals(
                9126,
                NetUtils.createSocketAddr(
                                MetadataServer.peerAddress(MetadataServer.peerEndpoint("::1:9126")))
                        .getPort());
    }

    @Test
    void testRaftPropertiesWireTransportTimeoutAndSnapshotPolicy() {
        final MetadataServer server =
                MetadataServer.newBuilder()
                        .setSelfId("n1")
                        .setQuorum("n1@localhost:9126")
                        .setDataDir(new File(tempDir, "data"))
                        .setRequestTimeout(Duration.ofSeconds(30))
                        .setSnapshotAutoTrigger(false, 1234L)
                        .setSnapshotRetentionFileNum(5)
                        .build();

        final RaftProperties properties = server.raftProperties();

        // server-side peer proxies derive their send deadline from this key alone
        assertEquals(
                30, RaftClientConfigKeys.Rpc.requestTimeout(properties).toLong(TimeUnit.SECONDS));
        assertFalse(RaftServerConfigKeys.Snapshot.autoTriggerEnabled(properties));
        assertEquals(1234L, RaftServerConfigKeys.Snapshot.autoTriggerThreshold(properties));
        assertEquals(5, RaftServerConfigKeys.Snapshot.retentionFileNum(properties));
        assertEquals(
                Collections.singletonList(new File(new File(tempDir, "data"), "raft")),
                RaftServerConfigKeys.storageDir(properties));
        assertEquals("GRPC", properties.get(RaftConfigKeys.Rpc.TYPE_KEY));
        // the transport port key stays unset - only the customizer may set transport keys
        assertNull(properties.get(GrpcConfigKeys.Server.PORT_KEY));
    }

    @Test
    void testRaftPropertiesDefaultsRunABoundedSnapshotLifecycle() {
        final MetadataServer server =
                MetadataServer.newBuilder()
                        .setSelfId("n1")
                        .setQuorum("n1@localhost:9126")
                        .setDataDir(new File(tempDir, "data"))
                        .build();

        final RaftProperties properties = server.raftProperties();

        assertEquals(
                3, RaftClientConfigKeys.Rpc.requestTimeout(properties).toLong(TimeUnit.SECONDS));
        assertTrue(RaftServerConfigKeys.Snapshot.autoTriggerEnabled(properties));
        assertEquals(400000L, RaftServerConfigKeys.Snapshot.autoTriggerThreshold(properties));
        assertEquals(2, RaftServerConfigKeys.Snapshot.retentionFileNum(properties));
        assertTrue(RaftServerConfigKeys.Snapshot.triggerWhenStopEnabled(properties));
        // commit still means majority-fsync with async flush, and no METADATA entry per commit
        assertTrue(RaftServerConfigKeys.Log.asyncFlushEnabled(properties));
        assertFalse(RaftServerConfigKeys.Log.logMetadataEnabled(properties));
        // the retry cache must outlive the client's total retry window (exactly-once)
        assertEquals(
                5,
                RaftServerConfigKeys.RetryCache.expiryTime(properties)
                        .toLong(TimeUnit.MINUTES));
    }

    @Test
    void testStartRefusesAKeptWorkingDatabaseWithoutARaftDirectory() throws Exception {
        final File dataDir = new File(tempDir, "data");
        final File rocksdbDir = new File(dataDir, "rocksdb");
        // A non-empty working database (an empty or absent one is a legitimate fresh start).
        Files.write(
                Files.createDirectories(rocksdbDir.toPath()).resolve("CURRENT"),
                "MANIFEST-000001\n".getBytes(StandardCharsets.UTF_8));

        final MetadataServer server =
                MetadataServer.newBuilder()
                        .setSelfId("n1")
                        .setQuorum("n1@localhost:0")
                        .setDataDir(dataDir)
                        .setClock(SystemClock.getInstance())
                        .build();
        try {
            final IOException refusal = assertThrows(IOException.class, server::start);
            assertTrue(refusal.getMessage().contains(rocksdbDir.getPath()));
            assertTrue(refusal.getMessage().contains(new File(dataDir, "raft").getPath()));
        } finally {
            server.close();
        }
        assertFalse(
                new File(dataDir, "raft").exists(),
                "the refusal happens before the raft storage formats anything");
    }

    @Test
    void testStartRefusesAWipedRaftDirectoryOverAKeptWorkingDatabase() throws Exception {
        final File dataDir = new File(tempDir, "data");
        final File raftDir = new File(dataDir, "raft");
        final File rocksdbDir = new File(dataDir, "rocksdb");
        final int port = probedPort();

        // The legitimate fresh start: neither directory exists yet, and a write round-trips
        // through the single-member group (the client's retries carry it past the election).
        final MetadataServer server =
                MetadataServer.newBuilder()
                        .setSelfId("n1")
                        .setQuorum("n1@localhost:" + port)
                        .setDataDir(dataDir)
                        .setClock(SystemClock.getInstance())
                        .setPropertiesCustomizer(
                                properties -> GrpcConfigKeys.Server.setPort(properties, port))
                        .build();
        try {
            server.start();
            assertEquals("/residue/a", server.getClient().create("/residue/a", new byte[] {1}, true));

            // The residue: wipe the raft directory's CONTENTS but keep the directory itself.
            final File[] children = raftDir.listFiles();
            assertNotNull(children);
            for (File child : children) {
                FileUtils.deleteDirectoryQuietly(child);
            }
            assertTrue(raftDir.exists());
            assertTrue(FileUtils.isDirectoryEmpty(raftDir));
            assertFalse(
                    FileUtils.isDirectoryEmpty(rocksdbDir),
                    "the working database survived the wipe");

            final MetadataServer restarted =
                    MetadataServer.newBuilder()
                            .setSelfId("n1")
                            .setQuorum("n1@localhost:" + port)
                            .setDataDir(dataDir)
                            .setClock(SystemClock.getInstance())
                            .setPropertiesCustomizer(
                                    properties -> GrpcConfigKeys.Server.setPort(properties, port))
                            .build();
            try {
                final IOException refusal = assertThrows(IOException.class, restarted::start);
                assertTrue(refusal.getMessage().contains(rocksdbDir.getPath()));
                assertTrue(refusal.getMessage().contains(raftDir.getPath()));
            } finally {
                restarted.close();
            }
        } finally {
            server.close();
        }
    }

    /** A bind-and-release free port for the single-member grpc group of this test. */
    private static int probedPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
