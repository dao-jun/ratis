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

import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.PlatformUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

/** File-system and close utilities for the metadata server: RocksDB storage-dir lifecycle and quiet closes. */
public final class IOUtils {

    private static final Logger LOG = LoggerFactory.getLogger(IOUtils.class);

    private IOUtils() {}

    /**
     * Closes the given closeable, swallowing every failure.
     *
     * <p><b>Important:</b> This method is expected to never throw an exception.
     */
    public static void closeQuietly(AutoCloseable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (Throwable ignored) {
        }
    }

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
            FileUtils.deleteFully(directory);
        } catch (Exception ignored) {
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
                LOG.warn("Failed atomic move of {} to {} retrying with a non-atomic move", source, target, outer);
                Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException inner) {
                inner.addSuppressed(outer);
                throw inner;
            }
        } finally {
            flushParentIfExists(target.toAbsolutePath().normalize().getParent());
        }
    }

    /**
     * Flushes a directory to guarantee crash consistency, warning on failure: fsync of an
     * unlinked or renamed-over path is not an error for the publisher that already renamed it.
     * A no-op on Windows (or with no parent), where opening a directory channel does not apply.
     */
    private static void flushParentIfExists(Path parent) {
        if (parent == null || PlatformUtils.WINDOWS) {
            return;
        }
        try (FileChannel dir = FileChannel.open(parent, StandardOpenOption.READ)) {
            dir.force(true);
        } catch (IOException e) {
            LOG.warn("Failed to flush directory {}", parent, e);
        }
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
