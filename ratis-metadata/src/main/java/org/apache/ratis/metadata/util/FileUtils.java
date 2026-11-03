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
package org.apache.ratis.metadata.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.stream.Stream;

/** File utilities for the metadata module: quiet deletes, atomic publishes, emptiness checks. */
public final class FileUtils {

    private static final Logger LOG = LoggerFactory.getLogger(FileUtils.class);

    private FileUtils() {}

    /**
     * Deletes the given directory recursively, swallowing every failure. A best-effort cleanup
     * path (staging leftovers, trash dirs) must never mask the failure that made it necessary.
     *
     * @param directory The directory to delete.
     */
    public static void deleteDirectoryQuietly(File directory) {
        if (directory == null) {
            return;
        }
        try {
            deleteDirectory(directory);
        } catch (Exception ignored) {
        }
    }

    /**
     * Deletes the given directory recursively.
     *
     * <p>If the directory does not exist, this does not throw an exception, but simply does
     * nothing. It considers the fact that a directory-to-be-deleted is not present a success.
     *
     * @param directory The directory to be deleted.
     * @throws IOException Thrown if the directory could not be deleted for some reason, for
     *     example due to missing access/write permissions.
     */
    public static void deleteDirectory(File directory) throws IOException {
        final Path dir = directory.toPath();
        if (!Files.exists(dir)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(dir)) {
            // children before parents, so a failure leaves a deletable prefix behind
            paths.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    /**
     * Attempts to move source to target atomically and falls back to a non-atomic move if it
     * fails. Flushes the parent directory afterwards to guarantee crash consistency.
     *
     * @throws IOException if both atomic and non-atomic moves fail, or the parent dir flush fails.
     */
    public static void atomicMoveWithFallback(Path source, Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException outer) {
            try {
                LOG.warn(
                        "Failed atomic move of {} to {} retrying with a non-atomic move",
                        source, target, outer);
                Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException inner) {
                inner.addSuppressed(outer);
                throw inner;
            }
        } finally {
            flushDirIfExists(target.toAbsolutePath().normalize().getParent());
        }
    }

    /**
     * Flushes a directory to guarantee crash consistency, swallowing {@link NoSuchFileException}
     * and {@link DirectoryNotEmptyException}: fsync of an unlinked or renamed-over path is not an
     * error for the publisher that already renamed it.
     */
    public static void flushDirIfExists(Path path) {
        try {
            flushDir(path);
        } catch (IOException e) {
            LOG.warn("Failed to flush directory {}", path, e);
        }
    }

    /**
     * Flushes a dirty directory to guarantee crash consistency. A no-op on Windows, where opening
     * a directory channel is not supported.
     *
     * @throws IOException if flushing the directory fails.
     */
    public static void flushDir(Path path) throws IOException {
        if (path == null || isWindows()) {
            return;
        }
        try (FileChannel dir = FileChannel.open(path, StandardOpenOption.READ)) {
            dir.force(true);
        }
    }

    /** Whether the JVM runs on Windows. */
    public static boolean isWindows() {
        return System.getProperty("os.name", "").toLowerCase().startsWith("windows");
    }

    /**
     * Checks whether the given directory is empty. A directory that does not exist or cannot be
     * listed counts as empty, matching the "nothing left over" question the callers ask.
     *
     * @param directory The directory to check.
     * @return True, if the directory is empty, false otherwise.
     */
    public static boolean isDirectoryEmpty(File directory) {
        File[] files = directory.listFiles();
        return files == null || files.length == 0;
    }
}
