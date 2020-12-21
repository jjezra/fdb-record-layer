/*
 * OnlineIndexer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.metadata.Index;

import javax.annotation.Nonnull;
import java.util.UUID;

/**
 * Shared structure to be used (only) by the OnlineIndexer* modules.
 */
public class OnlineIndexerCommon {
    private UUID uuid = UUID.randomUUID();

    private final boolean useSynchronizedSession;
    @Nonnull private final FDBDatabaseRunner runner;
    @Nonnull private final Index index;


    OnlineIndexerCommon(boolean useSynchronizedSession,
                        @Nonnull FDBDatabaseRunner runner,
                        Index index
    ) {
        this.useSynchronizedSession = useSynchronizedSession;
        this.runner = runner;
        this.index = index;
    }

    public UUID getUuid() {
        return uuid;
    }

    public boolean isUseSynchronizedSession() {
        return useSynchronizedSession;
    }

    @Nonnull
    public FDBDatabaseRunner getRunner() {
        return runner;
    }

    @Nonnull
    public Index getIndex() {
        return index;
    }
}
