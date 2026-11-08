/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file distributed with
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
package org.apache.ratis.metadata.statemachine;

import org.apache.ratis.metadata.proto.MetadataProtos.Admission;
import org.apache.ratis.metadata.proto.MetadataProtos.Status;
import org.apache.ratis.metadata.proto.MetadataProtos.WriteReply;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/**
 * The three reply shapes of the admission layer: an accepted entry carries
 * the original business result, a replay carries the stored original bytes,
 * and a rejection carries only the admission code and diagnostics - never a
 * "historical business result".
 */
final class WriteReplies {
  private WriteReplies() {
  }

  static WriteReply accepted(byte[] originalBusinessResult, long expectedNextSequence,
      long currentGeneration, byte[] operationDigest) {
    return base(Admission.ACCEPTED, Status.S_OK, originalBusinessResult,
        expectedNextSequence, currentGeneration, operationDigest).build();
  }

  static WriteReply replay(byte[] originalBusinessResult, boolean staleGeneration,
      long expectedNextSequence, long currentGeneration, byte[] operationDigest) {
    return base(Admission.REPLAY, Status.S_OK, originalBusinessResult,
        expectedNextSequence, currentGeneration, operationDigest)
        .setStaleGeneration(staleGeneration).build();
  }

  static WriteReply rejected(Status status, long expectedNextSequence,
      long currentGeneration, byte[] operationDigest) {
    return base(Admission.REJECTED, status, null,
        expectedNextSequence, currentGeneration, operationDigest).build();
  }

  private static WriteReply.Builder base(Admission admission, Status status,
      byte[] originalBusinessResult, long expectedNextSequence, long currentGeneration,
      byte[] operationDigest) {
    final WriteReply.Builder reply = WriteReply.newBuilder()
        .setAdmission(admission)
        .setAdmissionStatus(status)
        .setExpectedNextSequence(expectedNextSequence)
        .setCurrentGeneration(currentGeneration);
    if (originalBusinessResult != null) {
      reply.setOriginalBusinessResult(ByteString.copyFrom(originalBusinessResult));
    }
    if (operationDigest != null) {
      reply.setOperationDigest(ByteString.copyFrom(operationDigest));
    }
    return reply;
  }
}
