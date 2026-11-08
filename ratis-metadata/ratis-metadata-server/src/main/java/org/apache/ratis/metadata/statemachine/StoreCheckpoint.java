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

import java.nio.file.Path;

/**
 * A frozen, self-consistent cut of the store taken under the lifecycle read
 * lease and the apply/checkpoint mutex. The directory is immutable once
 * returned; manifest computation and publication are a later, separate step.
 */
final class StoreCheckpoint {
    private final Path directory;
    private final long term;
    private final long index;

    StoreCheckpoint(Path directory, long term, long index) {
        this.directory = directory;
        this.term = term;
        this.index = index;
    }

    Path directory() {
        return directory;
    }

    long term() {
        return term;
    }

    long index() {
        return index;
    }

    @Override
    public String toString() {
        return "(dir=" + directory + ",term=" + term + ",index=" + index + ")";
    }
}
