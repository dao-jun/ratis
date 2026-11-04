/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ratis.metadata.statemachine;

import org.apache.ratis.protocol.RaftGroupId;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class StateMachineIdentity {

    /**
     * Covers the family set, per-family key/value encodings, and default-family markers; bump on
     * any incompatible change - foreign layout versions are refused, not misinterpreted.
     */
    static final int LAYOUT_VERSION = 2;

    /** Default-column-family key of the working database's identity record. */
    static final byte[] DB_KEY = "identity".getBytes(StandardCharsets.UTF_8);

    /** The manifest inside every published snapshot dir, written last (completeness proof). */
    static final String SNAPSHOT_FILE = "snapshot-identity";

    private StateMachineIdentity() {}

    /**
     * The manifest of the data this build writes for a group; the family line derives from {@link
     * MetadataStateMachine#COLUMN_FAMILIES}, so it cannot drift from the opened families.
     */
    static String encode(RaftGroupId groupId) {
        final StringBuilder out = new StringBuilder(128);
        out.append("codec-format-version=")
                .append(RaftCommandCodec.CURRENT_FORMAT_VERSION)
                .append('\n');
        out.append("layout-version=").append(LAYOUT_VERSION).append('\n');
        out.append("group-id=").append(groupId.getUuid()).append('\n');
        out.append("column-families=default");
        for (byte[] cfName : MetadataStateMachine.COLUMN_FAMILIES) {
            out.append(',').append(new String(cfName, StandardCharsets.UTF_8));
        }
        out.append('\n');
        return out.toString();
    }

    /** Writes the manifest last, as the completeness proof of the snapshot publication. */
    static void writeSnapshotManifest(Path snapshotDir, RaftGroupId groupId) throws IOException {
        Files.write(
                snapshotDir.resolve(SNAPSHOT_FILE),
                encode(groupId).getBytes(StandardCharsets.UTF_8));
    }

    /** Validates the snapshot's manifest before anything is copied out; mismatches are named. */
    static void validateSnapshotManifest(Path snapshotDir, RaftGroupId groupId) throws IOException {
        final String found;
        try {
            found =
                    new String(
                            Files.readAllBytes(snapshotDir.resolve(SNAPSHOT_FILE)),
                            StandardCharsets.UTF_8);
        } catch (IOException readFailure) {
            throw new IOException(
                    "Snapshot "
                            + snapshotDir
                            + " carries no readable "
                            + SNAPSHOT_FILE
                            + " manifest; treating it as an incomplete or foreign artifact",
                    readFailure);
        }
        validate(found, encode(groupId), "snapshot " + snapshotDir);
    }

    /**
     * Validates the persisted manifest ({@code found}) against this build's ({@code expected}).
     *
     * @throws IOException listing every mismatching field; {@code subject} names the target
     */
    static void validate(String found, String expected, String subject) throws IOException {
        if (found.equals(expected)) {
            return;
        }
        final List<String> differences = differences(found, expected);
        throw new IOException(
                "The metadata state at "
                        + subject
                        + " belongs to a different generation than this server serves ("
                        + String.join("; ", differences)
                        + "). An explicit decision is required: rebuild the working state from"
                        + " this group's raft log and snapshots, or restore the configuration the"
                        + " persisted identity describes. Found:\n"
                        + found
                        + "Expected:\n"
                        + expected);
    }

    /** Per-field differences, ordered as the expected manifest lists them. */
    private static List<String> differences(String found, String expected) {
        final Map<String, String> foundFields = parse(found);
        final Map<String, String> expectedFields = parse(expected);
        final List<String> differences = new ArrayList<>();
        for (Map.Entry<String, String> expectedField : expectedFields.entrySet()) {
            final String key = expectedField.getKey();
            final String foundValue = foundFields.get(key);
            if (foundValue == null) {
                differences.add(key + " missing, expected " + expectedField.getValue());
            } else if (!foundValue.equals(expectedField.getValue())) {
                differences.add(
                        key + ": found " + foundValue + ", expected " + expectedField.getValue());
            }
        }
        if (differences.isEmpty()) {
            differences.add("unexpected extra or reformatted content");
        }
        return differences;
    }

    private static Map<String, String> parse(String manifest) {
        final Map<String, String> fields = new LinkedHashMap<>();
        for (String line : manifest.split("\n")) {
            final int separator = line.indexOf('=');
            if (separator > 0) {
                fields.put(line.substring(0, separator), line.substring(separator + 1));
            }
        }
        return fields;
    }
}
