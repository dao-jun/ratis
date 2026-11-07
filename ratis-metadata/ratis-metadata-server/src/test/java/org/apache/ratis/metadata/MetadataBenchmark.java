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
import org.apache.ratis.metadata.protocol.RaftResult;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manual performance harness of the raft metadata path on the stock grpc transport; run from the
 * test classpath, never picked up by surefire (no test methods).
 *
 * <p>Measured stack: {@code raft-N} - N in-process {@link MetadataServer}s, each with the
 * production ratis properties, RocksDB state machine and lease scheduler; operations go through
 * the production {@link RaftMetadataClient} addressing every member (so killing the leader is
 * carried by the client's retries, exactly as production would).
 *
 * <p>Scenarios: sequential write at three payload sizes, sequential read (linearizable, the
 * production servers' configured read option - ratis' read-index protocol, not a leader-local
 * state-machine read),
 * full-prefix list over 1000 children, pipelined write at fixed in-flight windows, and - raft-3
 * only - leader failover under a write stream. Every scenario runs a warmup segment that is
 * discarded. The pipelined scenario's per-op latency is observed at poll time, so an op that
 * completes between polls is charged a little extra; the throughput figure is exact.
 *
 * <p>Usage: {@code MetadataBenchmark [--quick] [--csv=&lt;path&gt;]}. {@code --quick} shrinks all
 * segments to ~20% for iteration; the design document wants the default scale on a quiet machine.
 */
public class MetadataBenchmark {

    private static final int[] PAYLOAD_BYTES = {64, 1024, 8192};
    private static final int[] PIPE_WINDOWS = {16, 64, 256};
    private static final int LIST_CHILDREN = 1000;

    private static final byte[] FILLER = payloadOf(64);

    private final int seqWarmup;
    private final int seqMeasured;
    private final int listWarmup;
    private final int listMeasured;
    private final int pipeOps;
    private final int failoverWrites;
    private final int killAfter;

    private final List<String> csvRows = new ArrayList<>();

    private MetadataBenchmark(boolean quick) {
        final double scale = quick ? 0.2 : 1.0;
        this.seqWarmup = scaled(500, scale);
        this.seqMeasured = scaled(3000, scale);
        this.listWarmup = scaled(50, scale);
        this.listMeasured = scaled(300, scale);
        this.pipeOps = scaled(10000, scale);
        this.failoverWrites = scaled(300, scale);
        this.killAfter = scaled(25, scale);
    }

    private static int scaled(int full, double scale) {
        return Math.max(1, (int) Math.round(full * scale));
    }

    // --------------------------------------------------------------------------------------------
    // entry point and scenario driver
    // --------------------------------------------------------------------------------------------

    /** Runs the full matrix; see the class javadoc for the scenarios and the flags. */
    public static void main(String[] args) throws Exception {
        boolean quick = false;
        String csvPath = "/tmp/ratis-metadata-benchmark-" + System.currentTimeMillis() + ".csv";
        for (String arg : args) {
            if ("--quick".equals(arg)) {
                quick = true;
            } else if (arg.startsWith("--csv=")) {
                csvPath = arg.substring("--csv=".length());
            } else {
                throw new IllegalArgumentException("Unknown argument: " + arg);
            }
        }
        new MetadataBenchmark(quick).run(csvPath);
    }

    private void run(String csvPath) throws Exception {
        printHeader();
        final File base = Files.createTempDirectory("ratis-metadata-benchmark").toFile();
        System.out.println("cluster state kept under " + base + " for post-mortem");

        try (RaftCluster raft3 = new RaftCluster(3, new File(base, "raft3"))) {
            final MetadataOps ops = new RaftOps(raft3.client());
            runScenarios("raft-3", ops);
            failover("raft-3", ops, raft3);
        }
        try (RaftCluster raft1 = new RaftCluster(1, new File(base, "raft1"))) {
            runScenarios("raft-1", new RaftOps(raft1.client()));
        }

        final List<String> lines = new ArrayList<>();
        lines.add(
                "backend,scenario,detail,ops,wall_ms,ops_per_s,p50_ms,p95_ms,p99_ms,max_ms,failover_gap_ms");
        lines.addAll(csvRows);
        Files.write(new File(csvPath).toPath(), lines, StandardCharsets.UTF_8);
        System.out.println("csv written to " + csvPath);
    }

    private void runScenarios(String backend, MetadataOps ops) throws Exception {
        System.out.println();
        System.out.println("== " + backend + " ==");
        for (int payload : PAYLOAD_BYTES) {
            report(
                    backend,
                    "seq-write",
                    payload + "B",
                    seqWrite(ops, payloadOf(payload), "/bw-" + payload));
        }
        report(backend, "seq-read", "1KiB", seqRead(ops, payloadOf(1024), "/br"));
        report(backend, "seq-list", LIST_CHILDREN + " children", seqList(ops, "/bl"));
        for (int window : PIPE_WINDOWS) {
            report(
                    backend,
                    "pipe-write",
                    window + " in-flight",
                    pipeWrite(ops, FILLER, window, "/bp-" + window));
        }
    }

    // --------------------------------------------------------------------------------------------
    // scenarios
    // --------------------------------------------------------------------------------------------

    private Stats seqWrite(MetadataOps ops, byte[] payload, String root) throws Exception {
        ops.ensure(root, payload);
        for (int i = 0; i < seqWarmup; i++) {
            ops.put(root + "/w" + i, payload);
        }
        final long[] latencies = new long[seqMeasured];
        final long wallStart = System.nanoTime();
        for (int i = 0; i < seqMeasured; i++) {
            final long t0 = System.nanoTime();
            ops.put(root + "/m" + i, payload);
            latencies[i] = System.nanoTime() - t0;
        }
        final Stats stats = new Stats(latencies, System.nanoTime() - wallStart);
        verifyPayload(ops.get(root + "/m" + (seqMeasured - 1)), payload);
        return stats;
    }

    private Stats seqRead(MetadataOps ops, byte[] payload, String root) throws Exception {
        ops.ensure(root, payload);
        for (int i = 0; i < seqWarmup + seqMeasured; i++) {
            ops.put(root + "/d" + i, payload);
        }
        for (int i = 0; i < seqWarmup; i++) {
            ops.get(root + "/d" + i);
        }
        final long[] latencies = new long[seqMeasured];
        final long wallStart = System.nanoTime();
        for (int i = 0; i < seqMeasured; i++) {
            final long t0 = System.nanoTime();
            ops.get(root + "/d" + i);
            latencies[i] = System.nanoTime() - t0;
        }
        final Stats stats = new Stats(latencies, System.nanoTime() - wallStart);
        verifyPayload(ops.get(root + "/d0"), payload);
        return stats;
    }

    private Stats seqList(MetadataOps ops, String root) throws Exception {
        ops.ensure(root, FILLER);
        for (int i = 0; i < LIST_CHILDREN; i++) {
            ops.put(root + "/c" + i, FILLER);
        }
        for (int i = 0; i < listWarmup; i++) {
            ops.children(root);
        }
        final long[] latencies = new long[listMeasured];
        final long wallStart = System.nanoTime();
        for (int i = 0; i < listMeasured; i++) {
            final long t0 = System.nanoTime();
            final List<String> children = ops.children(root);
            latencies[i] = System.nanoTime() - t0;
            if (children.size() != LIST_CHILDREN) {
                throw new IllegalStateException("list returned " + children.size() + " children");
            }
        }
        return new Stats(latencies, System.nanoTime() - wallStart);
    }

    private Stats pipeWrite(MetadataOps ops, byte[] payload, int window, String root)
            throws Exception {
        ops.ensure(root, payload);
        final ArrayDeque<CompletableFuture<?>> warmup = new ArrayDeque<>(window);
        for (int i = 0; i < window; i++) {
            warmup.add(ops.putAsync(root + "/w" + i, payload));
        }
        while (!warmup.isEmpty()) {
            warmup.poll().join();
        }
        final OpSlot[] slots = new OpSlot[pipeOps];
        final ArrayDeque<OpSlot> inFlight = new ArrayDeque<>(window);
        final long wallStart = System.nanoTime();
        for (int i = 0; i < pipeOps; i++) {
            if (inFlight.size() == window) {
                settle(inFlight.poll());
            }
            final OpSlot slot =
                    new OpSlot(System.nanoTime(), ops.putAsync(root + "/p" + i, payload));
            slots[i] = slot;
            inFlight.add(slot);
        }
        while (!inFlight.isEmpty()) {
            settle(inFlight.poll());
        }
        return new Stats(latenciesOf(slots), System.nanoTime() - wallStart);
    }

    /**
     * Kills the leader under a single-writer stream and reports the gap from the kill to the next
     * successful write; with the production 60s retry budget the failover is expected to be
     * transparent, i.e. the gap is carried by one slow operation rather than a failed one.
     */
    private void failover(String backend, MetadataOps ops, RaftCluster cluster) throws Exception {
        final String root = "/bf";
        ops.ensure(root, FILLER);
        final OpSlot[] slots = new OpSlot[failoverWrites];
        final AtomicInteger completed = new AtomicInteger();
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread writer =
                new Thread(
                        () -> {
                            try {
                                for (int i = 0; i < failoverWrites; i++) {
                                    final long t0 = System.nanoTime();
                                    ops.put(root + "/x" + i, FILLER);
                                    final OpSlot slot = new OpSlot(t0, null);
                                    slot.completedAt = System.nanoTime();
                                    slots[i] = slot;
                                    completed.incrementAndGet();
                                }
                            } catch (Throwable t) {
                                failure.set(t);
                            }
                        },
                        "failover-writer");
        writer.start();
        // Manual harness, not a test: a 10ms poll of the writer's progress is fine here.
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (completed.get() < killAfter) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException(
                        "The failover writer never reached " + killAfter + " ops");
            }
            Thread.sleep(10);
        }

        final long killedAt = System.nanoTime();
        cluster.killLeader();
        writer.join(TimeUnit.SECONDS.toMillis(120));
        if (writer.isAlive()) {
            throw new IllegalStateException("The failover writer did not finish within 120s");
        }
        if (failure.get() != null) {
            throw new IllegalStateException("A failure leaked through the failover", failure.get());
        }

        long firstOkAfterKill = Long.MAX_VALUE;
        long maxLatencyAfterKill = 0;
        for (OpSlot slot : slots) {
            if (slot != null && slot.completedAt > killedAt) {
                firstOkAfterKill = Math.min(firstOkAfterKill, slot.completedAt);
                maxLatencyAfterKill = Math.max(maxLatencyAfterKill, slot.latencyNanos());
            }
        }
        final double gapMs = (firstOkAfterKill - killedAt) / 1_000_000.0;
        System.out.printf(
                Locale.ROOT,
                "%-8s %-11s %-15s %7d %56s %10.1f %10.1f%n",
                backend,
                "failover",
                "kill leader",
                failoverWrites,
                "gap to first ok / max op (ms):",
                gapMs,
                maxLatencyAfterKill / 1_000_000.0);
        csvRows.add(
                String.format(
                        Locale.ROOT,
                        "%s,failover,kill leader,%d,,,,,,,%.3f",
                        backend,
                        failoverWrites,
                        gapMs));
    }

    // --------------------------------------------------------------------------------------------
    // measured operations
    // --------------------------------------------------------------------------------------------

    /** The path-level operations the harness measures. */
    private interface MetadataOps {

        /** Upserts; the measured ops always use fresh paths, so this creates. */
        void put(String path, byte[] data) throws Exception;

        byte[] get(String path) throws Exception;

        /**
         * The children of an existing parent, as paths or names - only the op cost is compared, not
         * the result shape.
         */
        List<String> children(String parent) throws Exception;

        /** The backend's idiomatic pipelined write, one future per operation. */
        CompletableFuture<?> putAsync(String path, byte[] data);

        /** Creates a structure node; unlike {@link #put} an existing node is fine. */
        void ensure(String path, byte[] data) throws Exception;
    }

    private static final class RaftOps implements MetadataOps {
        private final RaftMetadataClient client;

        RaftOps(RaftMetadataClient client) {
            this.client = client;
        }

        @Override
        public void put(String path, byte[] data) {
            client.put(path, data);
        }

        @Override
        public byte[] get(String path) {
            return client.get(path).getData();
        }

        @Override
        public List<String> children(String parent) {
            final List<RaftResult.Entry> entries = client.list(parent + "/");
            final List<String> paths = new ArrayList<>(entries.size());
            for (RaftResult.Entry entry : entries) {
                paths.add(entry.getPath());
            }
            return paths;
        }

        @Override
        public CompletableFuture<?> putAsync(String path, byte[] data) {
            return client.putAsync(path, data);
        }

        @Override
        public void ensure(String path, byte[] data) {
            client.put(path, data);
        }
    }

    // --------------------------------------------------------------------------------------------
    // cluster fixture
    // --------------------------------------------------------------------------------------------

    /** An in-process raft metadata group, wired exactly as MetadataServer wires it. */
    private static final class RaftCluster implements AutoCloseable {
        private final MetadataCluster cluster;
        private final RaftMetadataClient client;

        RaftCluster(int members, File baseDir) throws Exception {
            this.cluster =
                    MetadataCluster.newBuilder(baseDir).setNumServers(members).build();
            try {
                cluster.start();
                cluster.awaitLeader();
                this.client = cluster.newMetadataClient();
            } catch (Exception e) {
                close();
                throw e;
            }
        }

        RaftMetadataClient client() {
            return client;
        }

        /**
         * Closes the leader's server (its heartbeats stop, so the survivors elect), mimicking a
         * lost machine; the benchmark client addresses every member, so its retries carry it.
         */
        void killLeader() throws Exception {
            cluster.killServer(cluster.indexOf(cluster.awaitLeader()));
        }

        @Override
        public void close() throws Exception {
            if (client != null) {
                client.close();
            }
            if (cluster != null) {
                cluster.close();
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    // measurement
    // --------------------------------------------------------------------------------------------

    /** One measured operation: issue time, the pending future, and the observed completion. */
    private static final class OpSlot {
        final long issuedAt;
        final CompletableFuture<?> future;
        long completedAt;

        OpSlot(long issuedAt, CompletableFuture<?> future) {
            this.issuedAt = issuedAt;
            this.future = future;
        }

        long latencyNanos() {
            return completedAt - issuedAt;
        }
    }

    private static void settle(OpSlot slot) {
        slot.future.join();
        slot.completedAt = System.nanoTime();
    }

    private static long[] latenciesOf(OpSlot[] slots) {
        final long[] latencies = new long[slots.length];
        for (int i = 0; i < slots.length; i++) {
            latencies[i] = slots[i].latencyNanos();
        }
        return latencies;
    }

    /** Latency statistics of one scenario segment, in milliseconds for the report. */
    private static final class Stats {
        private final long[] sortedNanos;
        private final long wallNanos;

        Stats(long[] latencies, long wallNanos) {
            this.sortedNanos = latencies.clone();
            Arrays.sort(sortedNanos);
            this.wallNanos = wallNanos;
        }

        double opsPerSecond() {
            return sortedNanos.length * 1_000_000_000.0 / wallNanos;
        }

        double percentileMs(double p) {
            final int rank = (int) Math.ceil(p / 100.0 * sortedNanos.length);
            return sortedNanos[Math.max(1, rank) - 1] / 1_000_000.0;
        }

        double maxMs() {
            return sortedNanos[sortedNanos.length - 1] / 1_000_000.0;
        }
    }

    private void report(String backend, String scenario, String detail, Stats stats) {
        System.out.printf(
                Locale.ROOT,
                "%-8s %-11s %-15s %7d %9.0f %9.2f %9.2f %9.2f %9.2f%n",
                backend,
                scenario,
                detail,
                stats.sortedNanos.length,
                stats.opsPerSecond(),
                stats.percentileMs(50),
                stats.percentileMs(95),
                stats.percentileMs(99),
                stats.maxMs());
        csvRows.add(
                String.format(
                        Locale.ROOT,
                        "%s,%s,%s,%d,%.0f,%.0f,%.2f,%.2f,%.2f,%.2f,",
                        backend,
                        scenario,
                        detail,
                        stats.sortedNanos.length,
                        stats.wallNanos / 1_000_000.0,
                        stats.opsPerSecond(),
                        stats.percentileMs(50),
                        stats.percentileMs(95),
                        stats.percentileMs(99),
                        stats.maxMs()));
    }

    private static void verifyPayload(byte[] actual, byte[] expected) {
        if (!Arrays.equals(actual, expected)) {
            throw new IllegalStateException("A read-back check failed; the harness is broken");
        }
    }

    private static byte[] payloadOf(int size) {
        final byte[] payload = new byte[size];
        for (int i = 0; i < size; i++) {
            payload[i] = (byte) ('a' + i % 26);
        }
        return payload;
    }

    private void printHeader() {
        System.out.printf(
                Locale.ROOT,
                "MetadataBenchmark  jdk=%s  os=%s %s  cores=%d  maxHeapMB=%d%n",
                System.getProperty("java.version"),
                System.getProperty("os.name"),
                System.getProperty("os.arch"),
                Runtime.getRuntime().availableProcessors(),
                Runtime.getRuntime().maxMemory() >> 20);
        System.out.println(
                "columns: backend scenario detail ops ops/s p50_ms p95_ms p99_ms max_ms");
    }
}
