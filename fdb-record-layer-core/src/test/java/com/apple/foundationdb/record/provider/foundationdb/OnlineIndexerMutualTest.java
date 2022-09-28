/*
 * OnlineIndexerMutualTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for mutually building indexes {@link OnlineIndexer}.
 */
public class OnlineIndexerMutualTest extends OnlineIndexerTest  {

    private void populateData(final long numRecords) {
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, numRecords).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).build()
        ).collect(Collectors.toList());

        try (FDBRecordContext context = openContext())  {
            records.forEach(recordStore::saveRecord);
            context.commit();
        }
    }

    private void assertAllReadable(List<Index> indexes) {
        openSimpleMetaData(allIndexesHook(indexes));
        try (FDBRecordContext context = openContext()) {
            for (Index index : indexes) {
                assertTrue(recordStore.isIndexReadable(index));
            }
            context.commit();
        }
    }

    private List<Tuple> getBoundariesList(final long numRecords, final long step) {


        List<Tuple> boundaries = new ArrayList<>();
        boundaries.add(null);
        for (long i = step; i < numRecords; i += step) {
            final TestRecords1Proto.MySimpleRecord rec = TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(i).build();
            final RecordType recordType = metaData.getRecordTypeForDescriptor(rec.getDescriptorForType());
            final KeyExpression primaryKeyExpression = recordType.getPrimaryKey();
            final FDBStoredRecordBuilder<TestRecords1Proto.MySimpleRecord> recordBuilder = FDBStoredRecord.newBuilder(rec).setRecordType(recordType);
            final Tuple primaryKey = primaryKeyExpression.evaluateSingleton(recordBuilder).toTuple();
            boundaries.add(primaryKey);
        }
        boundaries.add(null);
        return boundaries;
    }

    private static FDBRecordStoreTestBase.RecordMetaDataHook allIndexesHook(List<Index> indexes) {
        return metaDataBuilder -> {
            for (Index index: indexes) {
                metaDataBuilder.addIndex("MySimpleRecord", index);
            }
        } ;
    }

    @Test
    void testMutualIndexingNoBoundaries() {
        // Let a single thread build all the indexes - boundaries will be detected automatically - which means (null, null)
        final FDBStoreTimer timer = new FDBStoreTimer();

        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexB", field("num_value_3_indexed"), IndexTypes.VALUE));
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));

        openSimpleMetaData();
        long numRecords = 80;
        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setSubspace(subspace)
                .setTargetIndexes(indexes)
                .setTimer(timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setMutualIndexing() // no boundaries mean self detection - which will be no boundaries
                        .build())
                .build()) {

            indexBuilder.buildIndex(true);
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertAllReadable(indexes);
    }

    @Test
    void testMutualIndexingSingleThread1() {
        testMutualIndexing(103, 10);
    }

    @Test
    @Tag(Tags.Slow)
    void testMutualIndexingSingleThread2() {
        testMutualIndexing(417, 17);
    }

    @Test
    @Tag(Tags.Slow)
    void testMutualIndexingSingleThread3() {
        testMutualIndexing(1417, 57);
    }

    @Test
    void testMutualIndexingMultiThread1() {
        testMutualIndexing(4, 103, 17);
    }

    @Test
    @Tag(Tags.Slow)
    void testMutualIndexingMultiThread2() {
        testMutualIndexing(40, 773, 14);
    }

    @Test
    @Tag(Tags.Slow)
    void testMutualIndexingMultiThread3() {
        testMutualIndexing(20, 299, 19);
    }

    private void testMutualIndexing(long numRecords, long boundarySize) {
        testMutualIndexing(0, numRecords, boundarySize);
    }

    private void testMutualIndexing(int numThreads, long numRecords, long boundarySize) {
        // build indexing by boundaries.
        // If numThreads < 2 - do it in a single thread.
        // Else, perform it in parallele by multiple threads
        List<Index> indexes = new ArrayList<>();
        // Here: Value indexes only
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexB", field("num_value_3_indexed"), IndexTypes.VALUE));
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        // Here: Add a non-value index
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));

        openSimpleMetaData();
        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);
        final FDBStoreTimer timer = new FDBStoreTimer();
        final List<Tuple> boundariesList = getBoundariesList(numRecords, boundarySize);
        if (numThreads < 2) {
            oneThreadIndexing(indexes, timer, boundariesList);
        } else {
            IntStream range = IntStream.rangeClosed(0, numThreads);
            range.parallel().forEach(ignore -> oneThreadIndexing(indexes, timer, boundariesList));
        }

        if (numThreads < 2) {
            assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
            assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        }
        assertAllReadable(indexes);
        validateIndexes(indexes);
    }

    void oneThreadIndexing(List<Index> indexes, FDBStoreTimer timer, List<Tuple> boundaries) {
        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);

        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setSubspace(subspace)
                .setTargetIndexes(indexes)
                .setTimer(timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setMutualIndexing(boundaries)
                        .build())
                .build()) {

            indexBuilder.buildIndex(true);
        }
    }

    void oneThreadIndexingCrashHalfway(List<Index> indexes, FDBStoreTimer timer, List<Tuple> boundaries, int after) {
        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        final String testThrowMsg = "Intentionally crash during test";
        final AtomicLong counter = new AtomicLong(0);

        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setSubspace(subspace)
                .setTargetIndexes(indexes)
                .setTimer(timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setMutualIndexing(boundaries)
                        .build())
                .setConfigLoader(old -> {
                    if (counter.incrementAndGet() > 1) {
                        throw new RecordCoreException(testThrowMsg);
                    }
                    return old;
                })
                .build()) {
            RecordCoreException e = assertThrows(RecordCoreException.class, indexBuilder::buildIndex);
            assertTrue(e.getMessage().contains(testThrowMsg));
        }
    }

    @Test
    @Tag(Tags.Slow)
    void testMutualIndexingCrashFewThreads() {
        // Force few of the threads to crash during indexing. Note that this is not a stable test - as there
        // is a small probability that "building" threads will build all or most of the index before a "crashing"
        // thread will get a chance to crash - and then fail assertion.
        // If that ever happens, we'll handle it (or disable this test).
        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));

        int numRecords = 543;
        openSimpleMetaData();
        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);
        final FDBStoreTimer timer = new FDBStoreTimer();
        int boundarySize = 20;
        final List<Tuple> boundariesList = getBoundariesList(numRecords, boundarySize);

        // Crash the odd ones
        IntStream.rangeClosed(1, 9).parallel().forEach(i -> {
            if (0 == (i & 1)) {
                oneThreadIndexing(indexes, timer, boundariesList);
            } else {
                oneThreadIndexingCrashHalfway(indexes, timer, boundariesList, 1);
            }
        });

        // validate
        assertAllReadable(indexes);
        validateIndexes(indexes);
    }

    @Test
    void testMutualIndexingCrashAndContinue() {
        // Start building with multi threads, crash all
        // Continue with other threads, crash them too
        // Successfully finish indexing with other threads
        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));

        int numRecords = 412;
        openSimpleMetaData();
        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);
        final FDBStoreTimer timer = new FDBStoreTimer();
        int boundarySize = 10;
        final List<Tuple> boundariesList = getBoundariesList(numRecords, boundarySize);

        // First crash, 8 threads, crash after 1:
        IntStream.rangeClosed(0, 8).parallel().forEach(ignore ->
                oneThreadIndexingCrashHalfway(indexes, timer, boundariesList, 1));

        // Second crash: 3 threads, crash after i
        IntStream.rangeClosed(0, 3).parallel().forEach(i ->
                oneThreadIndexingCrashHalfway(indexes, timer, boundariesList, i));

        // Now succeed: 10 threads
        IntStream.rangeClosed(0, 10).parallel().forEach(ignore ->
                oneThreadIndexing(indexes, timer, boundariesList));

        // validate
        assertAllReadable(indexes);
        validateIndexes(indexes);
    }

    @Test
    void testMutualIndexingCrashAndRefuseContinueNonMutually() {
        // Start building with multi threads, crash all
        // Make sure that the regular indexing is blocked
        // Finish indexing, just for fun.
        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));

        int numRecords = 232;
        openSimpleMetaData();
        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);
        final FDBStoreTimer timer = new FDBStoreTimer();
        int boundarySize = 10;
        final List<Tuple> boundariesList = getBoundariesList(numRecords, boundarySize);

        // First crash, 8 threads, crash after 1:
        IntStream.rangeClosed(0, 10).parallel().forEach(ignore ->
                oneThreadIndexingCrashHalfway(indexes, timer, boundariesList, 1));

        // Fail to build with a regular indexer
        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setSubspace(subspace)
                .setTargetIndexes(indexes)
                .setTimer(timer)
                .build()) {

            RecordCoreException e = assertThrows(RecordCoreException.class, indexBuilder::buildIndex);
            assertTrue(e.getMessage().contains("Invalid previous indexing type stamp"));
        }

        // Successfully build
        IntStream.rangeClosed(0, 30).parallel().forEach(ignore ->
                oneThreadIndexing(indexes, timer, boundariesList));

        // validate
        assertAllReadable(indexes);
        validateIndexes(indexes);
    }

    @Test
    void testMutualIndexingCrashAndAllowContinueNonMutually() {
        // Start building with multi threads, crash all
        // Make sure that the regular indexing is blocked
        // Finish indexing, just for fun.
        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));

        int numRecords = 132;
        openSimpleMetaData();
        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);
        final FDBStoreTimer timer = new FDBStoreTimer();
        int boundarySize = 11;
        final List<Tuple> boundariesList = getBoundariesList(numRecords, boundarySize);

        // First crash, 8 threads, crash after 1:
        IntStream.rangeClosed(0, 5).parallel().forEach(ignore ->
                oneThreadIndexingCrashHalfway(indexes, timer, boundariesList, 1));

        // Build with a regular indexer, allow takeover
        openSimpleMetaData(hook);
        for (Index index: indexes) {
            try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                    .setDatabase(fdb).setMetaData(metaData).setSubspace(subspace)
                    .setIndex(index)
                    .setTimer(timer)
                    .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                            .allowTakeoverContinue()
                            .build())
                    .build()) {
                indexBuilder.buildIndex();
            }
        }

        // validate
        assertAllReadable(indexes);
        validateIndexes(indexes);
    }

    @Test
    void testMutualIndexingWeirdBoundaries() {
        // Start building with multi threads, crash all
        // Continue with other threads, crash them too
        // Successfully finish indexing with other threads
        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));

        int numRecords = 100;
        openSimpleMetaData();
        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);
        final FDBStoreTimer timer = new FDBStoreTimer();
        int boundarySize = 10;
        final List<Tuple> boundariesList = getBoundariesList(numRecords, boundarySize);
        assertEquals(boundariesList.size(), 11);

        // Add null in the middle, causing fragments to overlap
        boundariesList.add(0, null);
        // Build and validate
        IntStream.rangeClosed(0, 8).parallel().forEach(ignore ->
                oneThreadIndexing(indexes, timer, boundariesList));
        assertAllReadable(indexes);
        validateIndexes(indexes);

        disableAll(indexes);
        // Duplicate entry, causing empty fragments
        boundariesList.add(7, boundariesList.get(7));
        boundariesList.add(10, boundariesList.get(10));
        boundariesList.add(10, boundariesList.get(10));

        // Build and validate
        IntStream.rangeClosed(0, 3).parallel().forEach(ignore ->
                oneThreadIndexing(indexes, timer, boundariesList));
        assertAllReadable(indexes);
        validateIndexes(indexes);

        // pad with nulls, causing more empty fragments
        boundariesList.add(0, null);
        boundariesList.add(boundariesList.size() - 1, null);

        // Build and validate
        IntStream.rangeClosed(0, 18).parallel().forEach(ignore ->
                oneThreadIndexing(indexes, timer, boundariesList));
        assertAllReadable(indexes);
        validateIndexes(indexes);
    }
}
