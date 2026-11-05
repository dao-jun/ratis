/*
 * Licensed to the Apache Software Foundation (ASF) under one or
 * contributor license agreements.  See the NOTICE file distributed with
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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** The mutations one committed raft log entry (a revision) applied, in apply order. */
public final class ChangeEvent {

    /** The kind of mutation one entry of {@link #getChanges()} describes. */
    public enum ChangeOp {
        /** A path was created (first version). */
        CREATE((byte) 1),
        /** An existing path's record was replaced (PUT/CAS hit or re-PUT). */
        UPDATE((byte) 2),
        /** A path's record was removed (explicit delete or lease expiry). */
        DELETE((byte) 3),
        /** A recursive delete; invalidate the whole prefix, victims are not enumerated. */
        DELETE_SUBTREE((byte) 8),
        /** A counter was incremented; {@link Change#getCounterValue()} holds the new value. */
        INCREMENT((byte) 4),
        /** A lease was registered; {@link Change#getLeaseId()} holds the lease id. */
        LEASE_REGISTER((byte) 5),
        /** A lease was renewed; {@link Change#getLeaseId()} holds the lease id. */
        LEASE_RENEW((byte) 6),
        /** A lease expired and its ephemeral paths were deleted. */
        LEASE_EXPIRE((byte) 7);

        private final byte code;

        ChangeOp(byte code) {
            this.code = code;
        }

        /** The wire/storage code of this operation. */
        public byte code() {
            return code;
        }

        static ChangeOp forCode(byte code) {
            for (ChangeOp op : values()) {
                if (op.code == code) {
                    return op;
                }
            }
            return null;
        }
    }

    private final long revision;
    private final List<Change> changes;

    /**
     * Creates an event for the given revision; the change list is defensively copied and made
     * unmodifiable, because the same list is handed to arbitrary listeners.
     *
     * @param revision the raft log index this event belongs to
     * @param changes the mutations applied at that revision, in apply order
     */
    public ChangeEvent(long revision, List<Change> changes) {
        this.revision = revision;
        this.changes = Collections.unmodifiableList(new ArrayList<>(changes));
    }

    /**
     * The raft log index (the revision) this event belongs to; revisions are strictly monotonic.
     */
    public long getRevision() {
        return revision;
    }

    /** The mutations applied at this revision, in apply order; empty revisions produce no event. */
    public List<Change> getChanges() {
        return changes;
    }

    @Override
    public String toString() {
        return "ChangeEvent{revision=" + revision + ", changes=" + changes + '}';
    }

    /** One mutation of a {@link ChangeEvent}. */
    public static final class Change {

        private final ChangeOp op;
        private final String path;
        private final byte[] data;

        /** Creates one change; {@code data} is the op-specific payload ({@code null} = empty). */
        public Change(ChangeOp op, String path, byte[] data) {
            this.op = op;
            this.path = path;
            this.data = data == null ? new byte[0] : data;
        }

        /** The kind of mutation. */
        public ChangeOp getOp() {
            return op;
        }

        /** The affected path; empty string for lease operations (the lease id is in the data). */
        public String getPath() {
            return path;
        }

        /** The op-specific payload, never {@code null}; see {@link #Change}. */
        public byte[] getData() {
            return data;
        }

        /** The new counter value, valid only for {@link ChangeOp#INCREMENT}. */
        public long getCounterValue() {
            return ByteBuffer.wrap(data).getLong();
        }

        /** The lease id, valid only for the lease operations. */
        public long getLeaseId() {
            return ByteBuffer.wrap(data).getLong();
        }

        @Override
        public String toString() {
            return "Change{op=" + op + ", path='" + path + "', dataBytes=" + data.length + '}';
        }
    }
}
