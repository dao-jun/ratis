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

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.metadata.protocol.RaftCommand;
import org.apache.ratis.metadata.protocol.RaftCommandCodec;
import org.apache.ratis.metadata.statemachine.MetadataLogEnvelope;
import org.apache.ratis.metadata.statemachine.MetadataStateMachine;
import org.apache.ratis.metadata.util.ManualClock;
import org.apache.ratis.metadata.util.SystemClock;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.TransactionContextImpl;
import org.apache.ratis.util.ExitUtils;
import org.apache.ratis.util.NetUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

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
        // this self entry carries a concrete port - nothing was substituted, so the transport
        // port key stays unset and only the customizer may set transport keys; a port-0 entry
        // pins its substituted port (testRaftPropertiesPinTheSubstitutedSelfPort...)
        assertNull(properties.get(GrpcConfigKeys.Server.PORT_KEY));
    }

    @Test
    void testRaftPropertiesPinTheSubstitutedSelfPortAsTheTransportListenPort() {
        final MetadataServer set =
                MetadataServer.newBuilder()
                        .setSelfId("n1")
                        .setQuorum("n1@localhost:0")
                        .setDataDir(new File(tempDir, "data-set"))
                        .setSelfPort(4242)
                        .build();
        assertEquals(4242, set.getSelfEndpoint().getPort());
        assertEquals(4242, GrpcConfigKeys.Server.port(set.raftProperties()));
        // only the active transport's key is pinned
        assertNull(set.raftProperties().get(NettyConfigKeys.Server.PORT_KEY));

        final MetadataServer netty =
                MetadataServer.newBuilder()
                        .setSelfId("n1")
                        .setQuorum("n1@localhost:0")
                        .setDataDir(new File(tempDir, "data-netty"))
                        .setRpcType(SupportedRpcType.NETTY)
                        .setSelfPort(4242)
                        .build();
        assertEquals(4242, NettyConfigKeys.Server.port(netty.raftProperties()));
        assertNull(netty.raftProperties().get(GrpcConfigKeys.Server.PORT_KEY));

        // no setSelfPort: the probed ephemeral port is pinned and exposed
        final MetadataServer probed =
                MetadataServer.newBuilder()
                        .setSelfId("n1")
                        .setQuorum("n1@localhost:0")
                        .setDataDir(new File(tempDir, "data-probed"))
                        .build();
        assertTrue(probed.getSelfEndpoint().getPort() > 0);
        assertEquals(
                probed.getSelfEndpoint().getPort(),
                GrpcConfigKeys.Server.port(probed.raftProperties()));

        // the customizer runs after the pinning and keeps the last word
        final MetadataServer overridden =
                MetadataServer.newBuilder()
                        .setSelfId("n1")
                        .setQuorum("n1@localhost:0")
                        .setDataDir(new File(tempDir, "data-overridden"))
                        .setSelfPort(4242)
                        .setPropertiesCustomizer(
                                properties -> GrpcConfigKeys.Server.setPort(properties, 5555))
                        .build();
        assertEquals(5555, GrpcConfigKeys.Server.port(overridden.raftProperties()));
    }

    /**
     * The port-0 self entry mode must serve, not merely start: the probed port is pinned as
     * the transport listen port, so the address the group and the built-in client advertise
     * has a listener behind it. Without the pinning the transport binds an arbitrary
     * ephemeral port and the create exhausts its retries - the preemptive timeout fails the
     * test.
     */
    @Test
    void testPortZeroSelfEntryServesWithoutACustomizer() throws Exception {
        final MetadataServer server =
                MetadataServer.newBuilder()
                        .setSelfId("n1")
                        .setQuorum("n1@localhost:0")
                        .setDataDir(new File(tempDir, "data-served"))
                        .build();
        try {
            server.start();
            assertEquals(
                    server.getSelfEndpoint().getPort(),
                    GrpcConfigKeys.Server.port(server.raftProperties()));
            assertTimeoutPreemptively(
                    Duration.ofSeconds(60),
                    () -> assertEquals(
                            "/served/a",
                            server.getClient().create("/served/a", new byte[] {1}, true)));
        } finally {
            server.close();
        }
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

        assertEquals(Duration.ofDays(7), MetadataServer.DEFAULT_RESULT_RETENTION);
        assertEquals(
                3, RaftClientConfigKeys.Rpc.requestTimeout(properties).toLong(TimeUnit.SECONDS));
        assertTrue(RaftServerConfigKeys.Snapshot.autoTriggerEnabled(properties));
        assertEquals(400000L, RaftServerConfigKeys.Snapshot.autoTriggerThreshold(properties));
        assertEquals(2, RaftServerConfigKeys.Snapshot.retentionFileNum(properties));
        assertTrue(RaftServerConfigKeys.Snapshot.triggerWhenStopEnabled(properties));
        // ratis' default synchronous segment flush (the async path never coalesces queued
        // appends and measures ten times slower), and no METADATA entry per commit
        assertFalse(RaftServerConfigKeys.Log.asyncFlushEnabled(properties));
        assertFalse(RaftServerConfigKeys.Log.logMetadataEnabled(properties));
        // a five-minute memory fast path; the durable result store is the fallback
        assertEquals(
                5,
                RaftServerConfigKeys.RetryCache.expiryTime(properties)
                        .toLong(TimeUnit.MINUTES));
    }

    @Test
    void testReadOptionDefaultsToLinearizableBeforeTheCustomizer() {
        final MetadataServer plain =
                MetadataServer.newBuilder()
                        .setSelfId("n1")
                        .setQuorum("n1@localhost:9126")
                        .setDataDir(new File(tempDir, "data-default"))
                        .build();
        assertEquals(
                RaftServerConfigKeys.Read.Option.LINEARIZABLE,
                RaftServerConfigKeys.Read.option(plain.raftProperties()),
                "reads must default to the read-index path, not local state");

        // the customizer runs after the default, so an embedder may trade the guarantee away
        final MetadataServer customized =
                MetadataServer.newBuilder()
                        .setSelfId("n1")
                        .setQuorum("n1@localhost:9126")
                        .setDataDir(new File(tempDir, "data-customized"))
                        .setPropertiesCustomizer(
                                properties ->
                                        RaftServerConfigKeys.Read.setOption(
                                                properties,
                                                RaftServerConfigKeys.Read.Option.DEFAULT))
                        .build();
        assertEquals(
                RaftServerConfigKeys.Read.Option.DEFAULT,
                RaftServerConfigKeys.Read.option(customized.raftProperties()),
                "the customizer has the last word on the read option");
    }

    @Test
    void testResultRetentionMustBeFiniteAndPositive() {
        final MetadataServer.Builder builder = MetadataServer.newBuilder();
        assertEquals(builder, builder.setResultRetention(Duration.ofMillis(1)));
        assertThrows(
                IllegalArgumentException.class, () -> builder.setResultRetention(Duration.ZERO));
        assertThrows(
                IllegalArgumentException.class,
                () -> builder.setResultRetention(Duration.ofMillis(-1)));
    }

    @Test
    void testLeaseClockSkewToleranceMustNotBeNegative() {
        final MetadataServer.Builder builder = MetadataServer.newBuilder();
        // zero is the NTP-fleet opt-out: a legal explicit value, unlike a negative one
        assertEquals(builder, builder.setLeaseClockSkewTolerance(Duration.ZERO));
        assertThrows(
                IllegalArgumentException.class,
                () -> builder.setLeaseClockSkewTolerance(Duration.ofMillis(-1)));
    }

    @Test
    void testStartWiresClockAndRetentionIntoBothStateMachinePaths() throws Exception {
        for (int path = 0; path < 2; path++) {
            final boolean injected = path == 1;
            final File dataDir = new File(tempDir, "configured-" + path);
            final int port = probedPort();
            final ManualClock clock = new ManualClock(10_000L);
            final MetadataStateMachine stateMachine =
                    injected
                            ? new MetadataStateMachine(new File(dataDir, "injected-rocksdb"))
                            : null;
            final MetadataServer.Builder builder =
                    MetadataServer.newBuilder()
                            .setSelfId("n1")
                            .setQuorum("n1@localhost:" + port)
                            .setDataDir(dataDir)
                            .setClock(clock)
                            .setResultRetention(Duration.ofMillis(500L))
                            .setPropertiesCustomizer(
                                    properties -> GrpcConfigKeys.Server.setPort(properties, port));
            if (injected) {
                builder.setStateMachine(stateMachine);
            }
            final MetadataServer server = builder.build();
            try {
                server.start();
                final MetadataStateMachine configured = server.getStateMachine();
                final byte[] raw =
                        RaftCommandCodec.encodeCommand(RaftCommand.leaseRegister(77L, 100L, 1L));
                final RaftClientRequest request =
                        RaftClientRequest.newBuilder()
                                .setClientId(
                                        ClientId.valueOf(
                                                UUID.nameUUIDFromBytes(
                                                        ("configuration-" + path)
                                                                .getBytes(StandardCharsets.UTF_8))))
                                .setServerId(RaftPeerId.valueOf("n1"))
                                .setGroupId(server.getGroupId())
                                .setCallId(91L)
                                .setMessage(Message.valueOf(ByteBuffer.wrap(raw)))
                                .setType(RaftClientRequest.writeRequestType())
                                .build();

                final TransactionContext context = configured.startTransaction(request);
                ((TransactionContextImpl) context).initLogEntry(1L, 100L + path);
                final MetadataLogEnvelope envelope =
                        MetadataLogEnvelope.decode(
                                context.getLogEntry()
                                        .getStateMachineLogEntry()
                                        .getLogData()
                                        .toByteArray());
                assertNotNull(envelope);
                assertEquals(10_000L, envelope.getServerTimeMillis());
                assertEquals(9_500L, envelope.getProposedCutoffMillis());
                assertEquals(10_100L, envelope.getCommand().getExpireAtMs());
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                configured.configureBeforeOpen(
                                        new ManualClock(0L), Duration.ofMillis(1L)));
            } finally {
                server.close();
            }
        }
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
                IOUtils.deleteDirectoryQuietly(child);
            }
            assertTrue(raftDir.exists());
            assertTrue(IOUtils.isDirectoryEmpty(raftDir));
            assertFalse(
                    IOUtils.isDirectoryEmpty(rocksdbDir),
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

    /**
     * A live listener on the advertised port must fail start() before any ratis resource
     * exists - the raft storage below the data dir is never even formatted.
     */
    @Test
    void testStartRefusesATakenPortBeforeAnyRaftResource() throws Exception {
        final File dataDir = new File(tempDir, "data");
        final int port = probedPort();
        try (ServerSocket taken = new ServerSocket(port)) {
            final MetadataServer server = newServer(dataDir, port);
            try {
                final IOException refusal = assertThrows(IOException.class, server::start);
                assertTrue(refusal.getMessage().contains("localhost:" + port));
            } finally {
                server.close();
            }
        }
        assertFalse(
                new File(dataDir, "raft").exists(),
                "the refusal happens before the raft storage formats anything");
    }

    /**
     * A transport bind conflict inside the raft start - here the customizer points the grpc
     * port at an occupied port while the advertised one is free - must surface as this
     * call's IOException (the disarmed grpc terminate lands as its cause), never as the
     * grpc transport's System.exit, and the failed start must leave no client behind.
     */
    @Test
    void testStartConvertsATransportBindFailureIntoAnIOException() throws Exception {
        final File dataDir = new File(tempDir, "data");
        final int advertised = probedPort();
        final int occupied = probedPort();
        try (ServerSocket taken = new ServerSocket(occupied)) {
            final MetadataServer server =
                    MetadataServer.newBuilder()
                            .setSelfId("n1")
                            .setQuorum("n1@localhost:" + advertised)
                            .setDataDir(dataDir)
                            .setPropertiesCustomizer(
                                    properties ->
                                            GrpcConfigKeys.Server.setPort(properties, occupied))
                            .build();
            try {
                final long begin = System.nanoTime();
                final IOException failure = assertThrows(IOException.class, server::start);
                assertTrue(
                        Duration.ofNanos(System.nanoTime() - begin)
                                        .compareTo(Duration.ofSeconds(10))
                                < 0,
                        "the failed start returns promptly (the catch-path close does not block)");
                assertTrue(
                        failure.getCause() instanceof ExitUtils.ExitException,
                        "the grpc bind failure routed through ratis' terminate path");
                assertNotNull(ExitUtils.getFirstExitException());
                assertNull(server.getClient(), "a failed start leaves no client");
            } finally {
                ExitUtils.clear();
                server.close();
            }
        }
    }

    /**
     * The port probe must never be stricter than the transport's own listener: after a stop,
     * the member's port is typically still draining (sockets in TIME_WAIT whose local port
     * is the member port), and the transport itself rebinds over them - so must the probe.
     */
    @Test
    void testStartAllowsASamePortRestartOverDrainingSockets() throws Exception {
        final File dataDir = new File(tempDir, "data");
        final int port = probedPort();
        final MetadataServer first = newServer(dataDir, port);
        try {
            first.start();
            assertEquals(
                    "/restart/a", first.getClient().create("/restart/a", new byte[] {1}, true));

            // Connections the way peer proxies hold them: close() force-closes them
            // server-side, leaving draining sockets whose local port is the member port.
            final List<Socket> held = new ArrayList<>();
            try {
                for (int i = 0; i < 3; i++) {
                    held.add(new Socket("localhost", port));
                }
                first.close();
            } finally {
                for (Socket socket : held) {
                    socket.close();
                }
            }

            final MetadataServer second = newServer(dataDir, port);
            try {
                second.start();
                assertEquals(
                        "/restart/b",
                        second.getClient().create("/restart/b", new byte[] {2}, true));
            } finally {
                second.close();
            }
        } finally {
            first.close();
        }
    }

    /**
     * The production hop of the storage refusal: a real raft-1 member whose only snapshot
     * dir cannot even be verified (no stat possible) must refuse start() with the declared
     * IOException - unwrapped from the proxy's CompletionException - carrying the snapshot
     * path in its cause chain, and the failed start must really have closed everything it
     * opened: a third server on the same data dir and port starts and serves again.
     */
    @Test
    void testStartRefusesAnUnverifiableSnapshotAreaAndCleansUpAfterwards() throws Exception {
        assumeTrue(
                FileSystems.getDefault().supportedFileAttributeViews().contains("posix"),
                "the failure is produced by a permission flip (POSIX only)");
        assumeFalse("root".equals(System.getProperty("user.name")), "root ignores permission bits");
        final File dataDir = new File(tempDir, "data");
        final int port = probedPort();
        final MetadataServer first = newServer(dataDir, port);
        first.start();
        assertEquals("/residue/a", first.getClient().create("/residue/a", new byte[] {1}, true));
        first.close(); // snapshot-on-stop publishes exactly one snapshot.<t>_<i>

        final File snapshotDir = theOnlySnapshotDir(dataDir);
        final Set<PosixFilePermission> original =
                Files.getPosixFilePermissions(snapshotDir.toPath());
        Files.setPosixFilePermissions(
                snapshotDir.toPath(), EnumSet.noneOf(PosixFilePermission.class));
        try {
            final MetadataServer second = newServer(dataDir, port);
            try {
                // Pre-fix this start SUCCEEDS (the snapshot is invisible, no exception is
                // thrown); post-fix it refuses with the declared IOException and the failed
                // start has closed everything it opened.
                final IOException refusal = assertThrows(IOException.class, second::start);
                assertTrue(refusal.getMessage().contains(dataDir.getPath()));
                assertTrue(String.valueOf(refusal.getCause()).contains(snapshotDir.getPath()));
            } finally {
                second.close(); // idempotent after the guard's close
            }

            Files.setPosixFilePermissions(snapshotDir.toPath(), original);
            final MetadataServer third = newServer(dataDir, port);
            try {
                third.start(); // same dir, same port: the lock and scheduler were really released
                assertEquals("/residue/b",
                        third.getClient().create("/residue/b", new byte[] {2}, true));
            } finally {
                third.close();
            }
        } finally {
            Files.setPosixFilePermissions(snapshotDir.toPath(), original);
        }
    }

    /** A bind-and-release free port for the single-member grpc group of this test. */
    private static int probedPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    /** A raft-1 member on the concrete port: quorum entry and grpc listen port both carry it. */
    private static MetadataServer newServer(File dataDir, int port) {
        return MetadataServer.newBuilder()
                .setSelfId("n1")
                .setQuorum("n1@localhost:" + port)
                .setDataDir(dataDir)
                .setPropertiesCustomizer(
                        properties -> GrpcConfigKeys.Server.setPort(properties, port))
                .build();
    }

    /**
     * Locates the single published snapshot dir below the data dir, wherever the group-uuid
     * storage layout puts the sm dir. Mirrors the state machine storage's snapshot-dir
     * pattern, which stays package-private to the statemachine package.
     */
    private static File theOnlySnapshotDir(File dataDir) throws IOException {
        try (Stream<Path> files = Files.walk(dataDir.toPath())) {
            final List<Path> matches =
                    files.filter(
                                    path ->
                                            path.getFileName() != null
                                                    && SNAPSHOT_DIR_NAME
                                                            .matcher(path.getFileName().toString())
                                                            .matches())
                            .collect(Collectors.toList());
            assertEquals(1, matches.size(), "exactly one stop-snapshot is published");
            return matches.get(0).toFile();
        }
    }

    /** {@code snapshot.<term>_<index>}, the state machine's snapshot directory name. */
    private static final Pattern SNAPSHOT_DIR_NAME = Pattern.compile("snapshot\\.\\d+_\\d+");
}
