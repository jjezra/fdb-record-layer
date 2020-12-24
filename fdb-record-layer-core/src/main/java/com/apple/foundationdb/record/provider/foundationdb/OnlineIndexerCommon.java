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
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.provider.foundationdb.synchronizedsession.SynchronizedSessionRunner;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Shared structure to be used (only) by the OnlineIndexer* modules.
 */
public class OnlineIndexerCommon {
    private UUID uuid = UUID.randomUUID();

    @Nonnull private final FDBDatabaseRunner runner;
    @Nullable private SynchronizedSessionRunner synchronizedSessionRunner = null;

    @Nonnull private final FDBRecordStore.Builder recordStoreBuilder;
    @Nonnull private final Index index;
    @Nonnull private final IndexStatePrecondition indexStatePrecondition;

    private final boolean useSynchronizedSession;
    private final boolean syntheticIndex;
    private final boolean trackProgress;

    // This is bad: the config is imported from OnlineIndexer - a user of this module. How can we move
    // the Config definition here without breaking backward compatibility? Shall we create the class here
    // and let OnlineIndexer.Config inherit it?
    @Nonnull public OnlineIndexer.Config config; // may be modified on the fly
    @Nonnull public Function<OnlineIndexer.Config, OnlineIndexer.Config> configLoader;

    @Nonnull public Collection<RecordType> recordTypes;

    private static final Object INDEX_BUILD_LOCK_KEY = 0L;
    private static final Object INDEX_BUILD_SCANNED_RECORDS = 1L;
    /**
     * Constant indicating that there should be no limit to some usually limited operation.
     */
    public static final int UNLIMITED = Integer.MAX_VALUE;

    OnlineIndexerCommon(@Nonnull FDBDatabaseRunner runner,
                        @Nonnull FDBRecordStore.Builder recordStoreBuilder,
                        @Nonnull Index index, @Nonnull Collection<RecordType> recordTypes,
                        @Nonnull Function<OnlineIndexer.Config, OnlineIndexer.Config> configLoader, @Nonnull OnlineIndexer.Config config,
                        boolean syntheticIndex,
                        @Nonnull IndexStatePrecondition indexStatePrecondition,
                        boolean trackProgress,
                        boolean useSynchronizedSession

    ) {
        this.useSynchronizedSession = useSynchronizedSession;
        this.runner = runner;
        this.index = index;
        this.recordTypes = recordTypes;
        this.configLoader = configLoader;
        this.config = config;
        this.syntheticIndex = syntheticIndex;
        this.indexStatePrecondition = indexStatePrecondition;
        this.trackProgress = trackProgress;
        this.recordStoreBuilder = recordStoreBuilder;
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

    public boolean isSyntheticIndex() {
        return syntheticIndex;
    }

    public boolean isTrackProgress() {
        return trackProgress;
    }

    @Nonnull
    public FDBRecordStore.Builder getRecordStoreBuilder() {
        return recordStoreBuilder;
    }

    @Nullable
    public SynchronizedSessionRunner getSynchronizedSessionRunner() {
        return synchronizedSessionRunner;
    }

    public void setSynchronizedSessionRunner(@Nullable final SynchronizedSessionRunner synchronizedSessionRunner) {
        this.synchronizedSessionRunner = synchronizedSessionRunner;
    }

    public void close() {
        runner.close();
        if (synchronizedSessionRunner != null) {
            synchronizedSessionRunner.close();
        }
    }

    @Nonnull
    public IndexStatePrecondition getIndexStatePrecondition() {
        return indexStatePrecondition;
    }

    @Nonnull
    public static Subspace indexBuildLockSubspace(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index) {
        return store.getUntypedRecordStore().indexBuildSubspace(index).subspace(Tuple.from(INDEX_BUILD_LOCK_KEY));
    }

    @Nonnull
    public Subspace indexBuildScannedRecordsSubspace(@Nonnull FDBRecordStoreBase<?> store) {
        return store.getUntypedRecordStore().indexBuildSubspace(index)
                .subspace(Tuple.from(INDEX_BUILD_SCANNED_RECORDS));
    }

    @SuppressWarnings("squid:S1452")
    public CompletableFuture<FDBRecordStore> openRecordStore(@Nonnull FDBRecordContext context) {
        return recordStoreBuilder.copyBuilder().setContext(context).openAsync();
    }


    /**
     * To avoid cyclic imports, this is the internal version of {@link OnlineIndexer.IndexStatePrecondition}.
     */
    public enum IndexStatePrecondition {
        BUILD_IF_DISABLED(false),
        BUILD_IF_DISABLED_CONTINUE_BUILD_IF_WRITE_ONLY(true),
        BUILD_IF_DISABLED_REBUILD_IF_WRITE_ONLY(false),
        FORCE_BUILD(false),
        ERROR_IF_DISABLED_CONTINUE_IF_WRITE_ONLY(true),
        ;

        private boolean continueIfWriteOnly;
        IndexStatePrecondition(boolean continueIfWriteOnly) {
            this.continueIfWriteOnly = continueIfWriteOnly;
        }
        public boolean isContinueIfWriteOnly() {
            return continueIfWriteOnly;
        }
    }
}
