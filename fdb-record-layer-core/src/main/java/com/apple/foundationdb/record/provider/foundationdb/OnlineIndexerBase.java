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

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.synchronizedsession.SynchronizedSessionRunner;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * A base class for different types of online indexing scanners.
 */
public abstract class OnlineIndexerBase {

    @Nonnull
    private static final Logger LOGGER = LoggerFactory.getLogger(OnlineIndexerBase.class);
    @Nonnull
    protected final OnlineIndexerCommon common; // to be used by extenders
    @Nonnull
    protected final AtomicLong totalRecordsScanned;

    private long timeOfLastProgressLogMillis = 0;

    @Nullable OnlineIndexerThrottle throttle = null;


    OnlineIndexerBase(OnlineIndexerCommon common) {
        this.common = common;
        this.totalRecordsScanned = new AtomicLong(0);
    }

    @SuppressWarnings("squid:S1452")
    private CompletableFuture<FDBRecordStore> openRecordStore(@Nonnull FDBRecordContext context) {
        return common.getRecordStoreBuilder().copyBuilder().setContext(context).openAsync();
    }

    protected FDBDatabaseRunner getRunner() {
        SynchronizedSessionRunner synchronizedSessionRunner = common.getSynchronizedSessionRunner();
        if (synchronizedSessionRunner != null) {
            return synchronizedSessionRunner;
        } else {
            return common.getRunner();
        }
    }

    CompletableFuture<Void> buildIndexAsync(boolean markReadable, long leaseLengthMills) {
        KeyValueLogMessage message = KeyValueLogMessage.build("build index online",
                LogMessageKeys.SHOULD_MARK_READABLE, markReadable);
        final CompletableFuture<Void> buildIndexAsyncFuture;
        FDBDatabaseRunner runner = common.getRunner();
        Index index = common.getIndex();
        if (common.isUseSynchronizedSession()) {
            buildIndexAsyncFuture = runner
                    .runAsync(context -> openRecordStore(context).thenApply(store -> OnlineIndexerCommon.indexBuildLockSubspace(store, index)))
                    .thenCompose(lockSubspace -> runner.startSynchronizedSessionAsync(lockSubspace, leaseLengthMills))
                    .thenCompose(synchronizedRunner -> {
                        message.addKeyAndValue(LogMessageKeys.SESSION_ID, synchronizedRunner.getSessionId());
                        return runWithSynchronizedRunnerAndEndSession(synchronizedRunner,
                                () -> handleStateAndDoBuildIndexAsync(markReadable, message));
                    });
        } else {
            message.addKeyAndValue(LogMessageKeys.SESSION_ID, "none");
            common.setSynchronizedSessionRunner(null);
            buildIndexAsyncFuture = handleStateAndDoBuildIndexAsync(markReadable, message);
        }
        return buildIndexAsyncFuture.whenComplete((vignore, ex) -> {
            if (LOGGER.isWarnEnabled() && (ex != null)) {
                message.addKeyAndValue(LogMessageKeys.RESULT, "failure");
                LOGGER.warn(message.toString(), ex);
            } else if (LOGGER.isInfoEnabled()) {
                message.addKeyAndValue(LogMessageKeys.RESULT, "success");
                LOGGER.info(message.toString());
            }
        });
    }

    private <T> CompletableFuture<T> runWithSynchronizedRunnerAndEndSession(
            @Nonnull SynchronizedSessionRunner newSynchronizedRunner, @Nonnull Supplier<CompletableFuture<T>> runnable) {
        final SynchronizedSessionRunner currentSynchronizedRunner1 = common.getSynchronizedSessionRunner();
        if (currentSynchronizedRunner1 == null) {
            common.setSynchronizedSessionRunner(newSynchronizedRunner);
            return MoreAsyncUtil.composeWhenComplete(runnable.get(), (result, ex) -> {
                final SynchronizedSessionRunner currentSynchronizedRunner2 = common.getSynchronizedSessionRunner();
                if (newSynchronizedRunner.equals(currentSynchronizedRunner2)) {
                    common.setSynchronizedSessionRunner(null);
                } else {
                    LOGGER.warn(KeyValueLogMessage.of("synchronizedSessionRunner was modified during the run",
                            LogMessageKeys.SESSION_ID, newSynchronizedRunner.getSessionId(),
                            LogMessageKeys.INDEXER_SESSION_ID, currentSynchronizedRunner2 == null ? null : currentSynchronizedRunner2.getSessionId()));
                }
                return newSynchronizedRunner.endSessionAsync();
            }, getRunner().getDatabase()::mapAsyncToSyncException);
        } else {
            return newSynchronizedRunner.endSessionAsync().thenApply(vignore -> {
                throw new RecordCoreException("another synchronized session is running on the indexer",
                        LogMessageKeys.SESSION_ID, newSynchronizedRunner.getSessionId(),
                        LogMessageKeys.INDEXER_SESSION_ID, currentSynchronizedRunner1.getSessionId());
            });
        }
    }

    @Nonnull
    private CompletableFuture<Void> handleStateAndDoBuildIndexAsync(boolean markReadable, KeyValueLogMessage message) {

        OnlineIndexerCommon.IndexStatePrecondition indexStatePrecondition = common.getIndexStatePrecondition();
        message.addKeyAndValue(LogMessageKeys.INDEX_STATE_PRECONDITION, indexStatePrecondition);
        if (indexStatePrecondition == OnlineIndexerCommon.IndexStatePrecondition.ERROR_IF_DISABLED_CONTINUE_IF_WRITE_ONLY) {
            message.addKeyAndValue(LogMessageKeys.SHOULD_BUILD_INDEX, true);
            return doBuildIndexAsync(markReadable);
        }
        final Index index = common.getIndex();
        return getRunner().runAsync(context -> openRecordStore(context).thenCompose(store -> {
            IndexState indexState = store.getIndexState(index);
            boolean shouldBuild = shouldBuildIndex(indexState, indexStatePrecondition);
            message.addKeyAndValue(LogMessageKeys.INITIAL_INDEX_STATE, indexState);
            message.addKeyAndValue(LogMessageKeys.SHOULD_BUILD_INDEX, shouldBuild);
            if (shouldBuild) {
                boolean shouldClear = shouldClearExistingIndexEntries(indexState, indexStatePrecondition);
                message.addKeyAndValue(LogMessageKeys.SHOULD_CLEAR_EXISTING_DATA, shouldClear);
                if (shouldClear) {
                    store.clearIndexData(index);
                }
                return store.markIndexWriteOnly(index).thenApply(vignore -> true);
            } else {
                return AsyncUtil.READY_FALSE;
            }
        })).thenCompose(shouldBuild -> shouldBuild != null && shouldBuild ? doBuildIndexAsync(markReadable) : AsyncUtil.DONE);
    }

    @SuppressWarnings("fallthrough")
    private boolean shouldBuildIndex(@Nonnull IndexState indexState, @Nonnull OnlineIndexerCommon.IndexStatePrecondition indexStatePrecondition) {
        switch (indexStatePrecondition) {
            case BUILD_IF_DISABLED:
                return indexState == IndexState.DISABLED;

            case BUILD_IF_DISABLED_CONTINUE_BUILD_IF_WRITE_ONLY:
            case BUILD_IF_DISABLED_REBUILD_IF_WRITE_ONLY:
                return indexState == IndexState.DISABLED || indexState == IndexState.WRITE_ONLY;

            case FORCE_BUILD:
                return true;

            default:
                throw new RecordCoreException("unknown index state precondition " + indexStatePrecondition);
        }
    }

    private boolean shouldClearExistingIndexEntries(@Nonnull IndexState indexState,
                                                    @Nonnull OnlineIndexerCommon.IndexStatePrecondition indexStatePrecondition) {
        // If the index state is DISABLED, it is expected that there is no existing index entry. But we would like
        // to clear it anyway to play safe.
        return !(indexState == IndexState.WRITE_ONLY && indexStatePrecondition.isContinueIfWriteOnly());
    }

    @Nonnull
    private CompletableFuture<Void> doBuildIndexAsync(boolean markReadable) {
        CompletableFuture<Void> buildFuture = scanBuildIndexAsync();

        if (markReadable) {
            return buildFuture.thenCompose(vignore ->
                    getRunner().runAsync(context -> openRecordStore(context)
                            .thenCompose(store -> store.markIndexReadable(common.getIndex()))
                            .thenApply(ignore -> null)));
        } else {
            return buildFuture;
        }
    }

    abstract CompletableFuture<Void> scanBuildIndexAsync();

    protected boolean shouldLogBuildProgress() {
        long interval = common.config.getProgressLogIntervalMillis();
        long now = System.currentTimeMillis();
        if (interval == 0 || interval < (now - timeOfLastProgressLogMillis)) {
            return false;
        }
        timeOfLastProgressLogMillis = now;
        return true;
    }

    protected int getLimit() {
        return throttle == null ? common.config.getMaxLimit() : throttle.getLimit();
    }

    protected CompletableFuture<Boolean> throttleDelay() {
        int limit = getLimit();
        int recordsPerSecond = common.config.getRecordsPerSecond();
        int toWait = (recordsPerSecond == OnlineIndexerCommon.UNLIMITED) ? 0 : 1000 * limit / recordsPerSecond;
        return MoreAsyncUtil.delayedFuture(toWait, TimeUnit.MILLISECONDS).thenApply(vignore3 -> true);
    }

    @VisibleForTesting
    <R> CompletableFuture<R> buildAsync(@Nonnull BiFunction<FDBRecordStore, AtomicLong, CompletableFuture<R>> buildFunction,
                                        boolean limitControl,
                                        @Nullable List<Object> additionalLogMessageKeyValues) {

        throttle = new OnlineIndexerThrottle(totalRecordsScanned, common, getRunner());
        return throttle.buildAsync(buildFunction, limitControl, additionalLogMessageKeyValues);
    }
}

