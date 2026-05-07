/*
 * RecordStoreIndexState.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.MutableRecordStoreState;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Encapsulates the mutable record store state reference and all operations on it:
 * initialization, read/write locking, index state persistence, and conflict tracking.
 */
@API(API.Status.INTERNAL)
public class RecordStoreIndexState {
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordStoreIndexState.class);
    private static final Object INDEX_STATE_SPACE_KEY = FDBRecordStoreKeyspace.INDEX_STATE_SPACE.key();

    @Nonnull
    private final AtomicReference<MutableRecordStoreState> stateRef = new AtomicReference<>();
    @Nonnull
    private final Set<String> indexStateReadConflicts = ConcurrentHashMap.newKeySet(8);
    private boolean storeStateReadConflict;

    @Nonnull
    private final FDBRecordContext context;
    @Nonnull
    private final Supplier<Subspace> subspaceSupplier;
    @Nonnull
    private final SubspaceProvider subspaceProvider;
    @Nonnull
    private final RecordMetaDataProvider metaDataProvider;

    /**
     * Create a new index state manager.
     *
     * @param context the record context for transaction access
     * @param subspaceSupplier supplies the store's subspace (may be lazily resolved)
     * @param subspaceProvider the subspace provider (for logging)
     * @param metaDataProvider provides record metadata (for index existence validation)
     */
    public RecordStoreIndexState(@Nonnull FDBRecordContext context,
                                  @Nonnull Supplier<Subspace> subspaceSupplier,
                                  @Nonnull SubspaceProvider subspaceProvider,
                                  @Nonnull RecordMetaDataProvider metaDataProvider) {
        this.context = context;
        this.subspaceSupplier = subspaceSupplier;
        this.subspaceProvider = subspaceProvider;
        this.metaDataProvider = metaDataProvider;
    }

    // --- State access ---

    /**
     * Get the current mutable record store state, or {@code null} if not yet initialized.
     */
    @Nullable
    public MutableRecordStoreState get() {
        return stateRef.get();
    }

    /**
     * Whether the state has been initialized (loaded from the database).
     */
    public boolean isInitialized() {
        return stateRef.get() != null;
    }

    /**
     * Initialize the state if not already set. Uses compare-and-set to avoid overwriting
     * a concurrently-initialized state.
     *
     * @param state the loaded record store state
     */
    public void initialize(@Nonnull RecordStoreState state) {
        stateRef.compareAndSet(null, state.toMutable());
    }

    /**
     * Apply a mutation to the state atomically (for memory-visibility guarantees).
     * The caller must hold the write lock.
     *
     * @param mutator the mutation to apply
     */
    public void updateState(@Nonnull Consumer<MutableRecordStoreState> mutator) {
        stateRef.updateAndGet(state -> {
            mutator.accept(state);
            return state;
        });
    }

    // --- Locking ---

    public void beginRead() {
        stateRef.get().beginRead();
    }

    public void endRead() {
        stateRef.get().endRead();
    }

    public void beginWrite() {
        stateRef.get().beginWrite();
    }

    public void endWrite() {
        stateRef.get().endWrite();
    }

    // --- Subspace ---

    /**
     * Get the subspace used for storing index state entries.
     */
    @Nonnull
    public Subspace indexStateSubspace() {
        return subspaceSupplier.get().subspace(Tuple.from(INDEX_STATE_SPACE_KEY));
    }

    // --- Cacheability ---

    /**
     * Whether the store state is cacheable (determined by the store header).
     * Throws if the state is not yet initialized.
     */
    public boolean isStateCacheable() {
        MutableRecordStoreState state = stateRef.get();
        if (state == null) {
            throw new UninitializedRecordStoreException("cannot check record store state cacheability on uninitialized store",
                    subspaceProvider.logKey(), subspaceProvider.toString(context));
        }
        return state.getStoreHeader().getCacheable();
    }

    // --- Index state update ---

    /**
     * Write a new index state to the database and update the in-memory cache.
     * This acquires the write lock defensively (callers may already hold it).
     *
     * @param indexName the index whose state is changing
     * @param indexState the new state
     */
    @SuppressWarnings("PMD.CloseResource")
    public void updateIndexState(@Nonnull String indexName, @Nonnull IndexState indexState) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(KeyValueLogMessage.of("index state change",
                    LogMessageKeys.INDEX_NAME, indexName,
                    LogMessageKeys.TARGET_INDEX_STATE, indexState.name(),
                    subspaceProvider.logKey(), subspaceProvider.toString(context)
            ));
        }
        if (stateRef.get() == null) {
            throw new UninitializedRecordStoreException("cannot update index state on an uninitialized store",
                    subspaceProvider.logKey(), subspaceProvider.toString(context));
        }
        // This is generally called by someone who should already have a write lock, but adding them here
        // defensively shouldn't cause problems.
        beginWrite();
        try {
            context.setDirtyStoreState(true);
            if (isStateCacheable()) {
                // The cache contains index state information, so updates to this information must also
                // update the meta-data version stamp or instances might cache stale index states.
                context.setMetaDataVersionStamp();
            }
            Transaction tr = context.ensureActive();
            byte[] indexKey = indexStateSubspace().pack(indexName);
            if (IndexState.READABLE.equals(indexState)) {
                tr.clear(indexKey);
            } else {
                tr.set(indexKey, Tuple.from(indexState.code()).pack());
            }
            stateRef.updateAndGet(state -> {
                // See beginRead() on why setting state is done in updateAndGet().
                state.setState(indexName, indexState);
                return state;
            });
        } finally {
            endWrite();
        }
    }

    // --- Conflict tracking ---

    /**
     * Add a read-conflict key for the given index, ensuring that concurrent state changes
     * to this index will cause the transaction to fail.
     *
     * @param indexName the index name to add a conflict for
     * @throws MetaDataException if the index does not exist in the metadata
     */
    @SuppressWarnings("PMD.CloseResource")
    public void addIndexStateReadConflict(@Nonnull String indexName) {
        if (!metaDataProvider.getRecordMetaData().hasIndex(indexName)) {
            throw new MetaDataException("Index " + indexName + " does not exist in meta-data.");
        }
        if (indexStateReadConflicts.contains(indexName)) {
            return;
        }
        indexStateReadConflicts.add(indexName);
        Transaction tr = context.ensureActive();
        byte[] indexStateKey = subspaceSupplier.get().pack(Tuple.from(INDEX_STATE_SPACE_KEY, indexName));
        tr.addReadConflictKey(indexStateKey);
    }

    /**
     * Add a read-conflict range for the entire index state space.
     */
    @SuppressWarnings("PMD.CloseResource")
    public void addStoreStateReadConflict() {
        if (storeStateReadConflict) {
            return;
        }
        storeStateReadConflict = true;
        Transaction tr = context.ensureActive();
        byte[] indexStateKey = subspaceSupplier.get().pack(Tuple.from(INDEX_STATE_SPACE_KEY));
        tr.addReadConflictRange(indexStateKey, ByteArrayUtil.strinc(indexStateKey));
    }
}
