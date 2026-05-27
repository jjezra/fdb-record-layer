/*
 * IndexStateManager.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.MutableRecordStoreState;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordStoreState;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Owns the in-memory {@link MutableRecordStoreState} cached by an {@link FDBRecordStore}. This includes:
 * <ul>
 *   <li>Lazy loading of the state via a caller-supplied loader (so I/O remains on the store).</li>
 *   <li>The read/write coordination ({@link MutableRecordStoreState#beginRead}/{@code beginWrite}) that
 *       protects mutations against concurrent reads.</li>
 *   <li>Atomic mutation of the held state.</li>
 * </ul>
 *
 * <p>This class is deliberately <em>state-only</em>: it never writes to FDB. Code that persists index
 * state bytes or store-header updates does so against the transaction directly; this manager only keeps
 * the cached, in-memory state consistent.</p>
 */
@API(API.Status.INTERNAL)
public class IndexStateManager {

    @Nonnull
    private final AtomicReference<MutableRecordStoreState> ref = new AtomicReference<>();

    @Nonnull
    private final Supplier<CompletableFuture<Void>> defaultLoader;

    @Nonnull
    private final Function<String, UninitializedRecordStoreException> uninitializedExceptionFactory;

    /**
     * @param defaultLoader called by {@link #ensureLoaded()} when no state is cached. The loader is
     *                      expected to populate this manager via {@link #initializeIfAbsent(RecordStoreState)}
     *                      before its returned future completes normally.
     * @param uninitializedExceptionFactory builds the exception thrown when an operation requires loaded
     *                                      state but the store has not yet been initialized. Typically
     *                                      enriches the message with subspace/log info.
     */
    public IndexStateManager(@Nonnull Supplier<CompletableFuture<Void>> defaultLoader,
                             @Nonnull Function<String, UninitializedRecordStoreException> uninitializedExceptionFactory) {
        this.defaultLoader = defaultLoader;
        this.uninitializedExceptionFactory = uninitializedExceptionFactory;
    }

    /** @return whether the state has been loaded at least once. */
    public boolean isLoaded() {
        return ref.get() != null;
    }

    /** @return the cached state, or {@code null} if it has not been loaded yet. */
    @Nullable
    public MutableRecordStoreState getOrNull() {
        return ref.get();
    }

    /**
     * @param errorMessage message embedded in the thrown exception if the state has not been loaded
     * @return the cached state
     * @throws UninitializedRecordStoreException if the state has not yet been loaded
     */
    @Nonnull
    public MutableRecordStoreState getOrThrow(@Nonnull String errorMessage) {
        MutableRecordStoreState s = ref.get();
        if (s == null) {
            throw uninitializedExceptionFactory.apply(errorMessage);
        }
        return s;
    }

    /**
     * Cache {@code state} if no state has been cached yet. No-op otherwise. The given state is converted
     * to its mutable form via {@link RecordStoreState#toMutable()}.
     */
    public void initializeIfAbsent(@Nonnull RecordStoreState state) {
        if (ref.get() == null) {
            ref.compareAndSet(null, state.toMutable());
        }
    }

    /**
     * Returns a future that completes once the state is loaded. Invokes the {@link #defaultLoader default
     * loader} when needed; otherwise returns immediately with the cached state.
     */
    @Nonnull
    public CompletableFuture<MutableRecordStoreState> ensureLoaded() {
        MutableRecordStoreState s = ref.get();
        if (s != null) {
            return CompletableFuture.completedFuture(s);
        }
        return defaultLoader.get().thenApply(ignore -> getOrThrow("state loader did not initialize record store state"));
    }

    /**
     * Atomically apply {@code mutator} to the held state. The mutator runs inside
     * {@link AtomicReference#updateAndGet} so the write happens-before any subsequent
     * {@link #getOrNull()} on another thread, even though the wrapped object is unchanged.
     *
     * @throws UninitializedRecordStoreException if the state has not been loaded
     */
    public void mutate(@Nonnull Consumer<MutableRecordStoreState> mutator) {
        ref.updateAndGet(state -> {
            if (state == null) {
                throw uninitializedExceptionFactory.apply("cannot mutate uninitialized record store state");
            }
            mutator.accept(state);
            return state;
        });
    }

    /**
     * Acquire a read scope. While the scope is open, the state may not be mutated; concurrent read scopes
     * are allowed.
     *
     * @throws UninitializedRecordStoreException if the state has not been loaded
     * @throws RecordCoreException if the state is currently being modified
     */
    @Nonnull
    public Scope readLock() {
        MutableRecordStoreState s = getOrThrow("cannot read uninitialized record store state");
        s.beginRead();
        return new Scope(s, s::endRead);
    }

    /**
     * Acquire a write scope. While the scope is open, no read scopes may be acquired and the state may be
     * mutated through {@link Scope#state()}.
     *
     * @throws UninitializedRecordStoreException if the state has not been loaded
     * @throws RecordCoreException if the state is currently being read
     */
    @Nonnull
    public Scope writeLock() {
        MutableRecordStoreState s = getOrThrow("cannot write uninitialized record store state");
        s.beginWrite();
        return new Scope(s, s::endWrite);
    }

    /**
     * Auto-closeable scope around an acquired read or write lock.
     *
     * <p>Two usage patterns:</p>
     * <ol>
     *   <li>Try-with-resources, for purely synchronous work:
     *       <pre>{@code
     *       try (var scope = indexState.readLock()) {
     *           use(scope.state());
     *       }
     *       }</pre></li>
     *   <li>{@link #releaseWhen(CompletableFuture)}, for async work whose completion governs unlock:
     *       <pre>{@code
     *       try (var scope = indexState.writeLock()) {
     *           CompletableFuture<X> f = doAsyncWork(scope.state());
     *           return scope.releaseWhen(f);
     *       }
     *       }</pre>
     *       After {@code releaseWhen}, {@link #close()} is a no-op so the surrounding try-with-resources
     *       does not double-release. If {@code releaseWhen} is never reached (e.g. an exception is thrown
     *       before it), the try-with-resources still releases on the way out.</li>
     * </ol>
     */
    public static final class Scope implements AutoCloseable {
        @Nonnull
        private final MutableRecordStoreState state;
        @Nonnull
        private final Runnable releaser;
        private boolean released;

        Scope(@Nonnull MutableRecordStoreState state, @Nonnull Runnable releaser) {
            this.state = state;
            this.releaser = releaser;
        }

        @Nonnull
        public MutableRecordStoreState state() {
            return state;
        }

        /**
         * Tie release of this scope to completion of {@code future}. Returns {@code future} for chaining.
         * After this call, {@link #close()} becomes a no-op.
         *
         * @throws RecordCoreException if this scope has already been released
         */
        @Nonnull
        public <T> CompletableFuture<T> releaseWhen(@Nonnull CompletableFuture<T> future) {
            if (released) {
                throw new RecordCoreException("record store state scope already released");
            }
            released = true;
            return future.whenComplete((ignore, err) -> releaser.run());
        }

        @Override
        public void close() {
            if (!released) {
                released = true;
                releaser.run();
            }
        }
    }
}
