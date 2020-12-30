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

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.RangeSet;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * This indexer scans records by a source index.
 */
@API(API.Status.UNSTABLE)
public class OnlineIndexerByIndex extends OnlineIndexerScanner {
    @Nonnull private static final Logger LOGGER = LoggerFactory.getLogger(OnlineIndexerByIndex.class);
    @Nonnull private final OnlineIndexer.IndexFromIndexPolicy policy;

    OnlineIndexerByIndex(@Nonnull OnlineIndexerCommon common,
                         @Nonnull OnlineIndexer.IndexFromIndexPolicy policy) {
        super(common);
        this.policy = policy;
    }

    @Nonnull
    @Override
    CompletableFuture<Void> scanBuildIndexAsync() {

        return getRunner().runAsync(context -> common.openRecordStore(context)
                .thenCompose( store -> {
                    // first verify that both src and tgt are of a single, similar, type
                    final RecordMetaData metaData = store.getRecordMetaData();
                    final Index srcIndex = metaData.getIndex(Objects.requireNonNull(policy.getSourceIndex()));
                    final Collection<RecordType> srcRecordTypes = metaData.recordTypesForIndex(srcIndex);

                    if (common.isSyntheticIndex() ||
                            common.recordTypes.size() != 1) {
                        buildIndexFromIndexThrowEx("IndexFromIndex: tgt cannot be scanned from index");
                    }
                    if (srcRecordTypes.size() != 1 ||
                            srcIndex.getRootExpression().createsDuplicates() ||
                            ! srcIndex.getType().equals(IndexTypes.VALUE) ||
                            ! common.recordTypes.equals(srcRecordTypes) ) {
                        buildIndexFromIndexThrowEx("IndexFromIndex: src cannot be used for this tgt");
                    }

                    return context.getReadVersionAsync()
                            .thenCompose(vignore -> {
                                SubspaceProvider subspaceProvider = common.getRecordStoreBuilder().getSubspaceProvider();
                                return subspaceProvider.getSubspaceAsync(context)
                                        .thenCompose(subspace ->
                                                buildIndexFromIndex(subspaceProvider, subspace)
                                                        .thenCompose(vignore2 -> markBuilt(subspace)
                                                        ));
                            });
                }));
    }

    private void buildIndexFromIndexThrowEx(@Nonnull String msg) {
        throw new OnlineIndexerException(msg);
    }

    @Nonnull
    private CompletableFuture<Void> markBuilt(Subspace subspace) {
        // Grand Finale - after fully building the index, remove all missing ranges in one gulp
        return getRunner().runAsync(context -> {
            final Index index = common.getIndex();
            RangeSet rangeSet = new RangeSet(subspace.subspace(Tuple.from(FDBRecordStore.INDEX_RANGE_SPACE_KEY, index.getSubspaceKey())));
            TransactionContext tc = context.ensureActive();
            return rangeSet.insertRange(tc, null, null).thenApply(bignore -> null);
        });
    }

    private void maybeLogBuildProgress(SubspaceProvider subspaceProvider, byte[] retCont) {
        if (LOGGER.isInfoEnabled() && shouldLogBuildProgress()) {
            final Index index = common.getIndex();
            LOGGER.info(KeyValueLogMessage.of("Built Range",
                    LogMessageKeys.INDEX_NAME, index.getName(),
                    LogMessageKeys.INDEX_VERSION, index.getLastModifiedVersion(),
                    subspaceProvider.logKey(), subspaceProvider,
                    LogMessageKeys.NEXT_CONTINUATION, retCont,
                    LogMessageKeys.RECORDS_SCANNED, common.getTotalRecordsScanned().get()),
                    LogMessageKeys.INDEXER_ID, common.getUuid());
        }
    }

    @Nonnull
    private CompletableFuture<Void> buildIndexFromIndex(SubspaceProvider subspaceProvider, @Nonnull Subspace subspace) {
        AtomicReference<byte[]> nextCont = new AtomicReference<>();

        return AsyncUtil.whileTrue(() -> {
            byte [] cont = nextCont.get();
            final List<Object> additionalLogMessageKeyValues = Arrays.asList(LogMessageKeys.CALLING_METHOD, "buildIndexFromIndex",
                    LogMessageKeys.NEXT_CONTINUATION, cont == null ? "" : cont);

            // apparently, buildAsync=buildAndCommitWithRetry
            return buildAsync( (store, recordsScanned) -> buildRangeOnly(store, cont, recordsScanned),
                    true,
                    additionalLogMessageKeyValues)
                    .handle((retCont, ex) -> {
                        if (ex == null) {
                            maybeLogBuildProgress(subspaceProvider, retCont);
                            if (retCont == null) {
                                return AsyncUtil.READY_FALSE;
                            }
                            nextCont.set(retCont); // continuation
                            return throttleDelay();
                        }
                        final RuntimeException unwrappedEx = getRunner().getDatabase().mapAsyncToSyncException(ex);
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info(KeyValueLogMessage.of("possibly non-fatal error encountered building range",
                                    LogMessageKeys.NEXT_CONTINUATION, nextCont,
                                    LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(subspace.pack())), ex);
                        }
                        throw unwrappedEx;
                    })
                    .thenCompose(Function.identity());
        }, getRunner().getExecutor());
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    private static RecordCursorResult<FDBIndexedRecord<Message>> castCursorResult(RecordCursorResult<?> result) {
        // helper function
        if (result == null) {
            throw new MetaDataException("Unexpected null result");
        }
        return (RecordCursorResult<FDBIndexedRecord<Message>>)result;
    }

    @Nullable
    private static FDBStoredRecord<Message> castCursorResultToStoreRecord(RecordCursorResult<?> result) {
        RecordCursorResult<FDBIndexedRecord<Message>> cursorResult = castCursorResult(result);
        FDBIndexedRecord<Message> indexResult = cursorResult.get();
        return indexResult == null ? null : indexResult.getStoredRecord();
    }

    @Nonnull
    private CompletableFuture<byte[]> buildRangeOnly(@Nonnull FDBRecordStore store, byte[] cont, @Nonnull AtomicLong recordsScanned) {

        Index index = common.getIndex();
        final Subspace scannedRecordsSubspace = common.indexBuildScannedRecordsSubspace(store);
        final RecordMetaDataProvider recordMetaDataProvider = common.getRecordStoreBuilder().getMetaDataProvider();
        if ( recordMetaDataProvider == null ||
                !store.getRecordMetaData().equals(recordMetaDataProvider.getRecordMetaData())) {
            throw new MetaDataException("Store does not have the same metadata");
        }
        final String srcIndex = policy.getSourceIndex();

        final IndexMaintainer maintainer = store.getIndexMaintainer(index);
        if (! maintainer.isIdempotent() ) {
            // idempotence - We could have verified it at the first iteration only, but the repeating checks seem harmless
            buildIndexFromIndexThrowEx("IndexFromIndex: tgt is not idempotent");
        }
        if (srcIndex == null) {
            // this should never happen. But it makes the compiler happy
            buildIndexFromIndexThrowEx("IndexFromIndex: src is not set");
        }
        if (! store.isIndexReadable(srcIndex)) {
            // readability - This method shouldn't block if one has already opened the record store (as we did)
            buildIndexFromIndexThrowEx("IndexFromIndex: src is not readable");
        }

        final ExecuteProperties.Builder executeProperties = ExecuteProperties.newBuilder()
                .setIsolationLevel(IsolationLevel.SNAPSHOT)
                .setReturnedRowLimit(getLimit()); // always respectLimit in this path
        final ScanProperties scanProperties = new ScanProperties(executeProperties.build());

        RecordCursor<FDBIndexedRecord<Message>> cursor =
                store.scanIndexRecords(srcIndex, IndexScanType.BY_VALUE, TupleRange.ALL, cont, scanProperties);

        final AtomicLong recordsScannedCounter = new AtomicLong();
        final AtomicReference<RecordCursorResult<FDBIndexedRecord<Message>>> lastResult = new AtomicReference<>(RecordCursorResult.exhausted());
        final AtomicBoolean isEmpty = new AtomicBoolean(true);

        return iterateRangeOnly(store, cursor,
                OnlineIndexerByIndex::castCursorResultToStoreRecord,
                result -> lastResult.set(castCursorResult(result)),
                isEmpty, recordsScannedCounter
        ).thenCompose(vignore -> {
            long recordsScannedInTransaction = recordsScannedCounter.get();
            recordsScanned.addAndGet(recordsScannedInTransaction);
            if (common.isTrackProgress()) {
                store.context.ensureActive().mutate(MutationType.ADD, scannedRecordsSubspace.getKey(),
                        FDBRecordStore.encodeRecordCount(recordsScannedInTransaction));
            }
            final byte[] retCont = isEmpty.get() ? null : lastResult.get().getContinuation().toBytes();
            return CompletableFuture.completedFuture( retCont );
        });
    }

    // support rebuildIndexAsync
    @Nonnull
    @Override
    CompletableFuture<Void> scanRebuildIndexAsync(FDBRecordStore store) {
        AtomicReference<byte[]> nextCont = new AtomicReference<>();
        AtomicLong recordScanned = new AtomicLong();
        return AsyncUtil.whileTrue(() ->
                buildRangeOnly(store, nextCont.get(), recordScanned).thenApply(cont -> {
                    if (cont == null) {
                        return false;
                    }
                    nextCont.set(cont);
                    return true;
                }), store.getExecutor());
    }
}
