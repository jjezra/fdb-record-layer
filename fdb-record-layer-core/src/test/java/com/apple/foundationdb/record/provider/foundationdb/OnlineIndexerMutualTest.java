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

import com.apple.foundationdb.Range;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for mutually building indexes {@link OnlineIndexer}.
 */
public class OnlineIndexerMutualTest extends OnlineIndexerTest  {
    private static final Logger LOGGER = LoggerFactory.getLogger(OnlineIndexerMutualTest.class);

    private void populateData(final long numRecords) {
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, numRecords).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(val)
                        .setNumValue2((int)val * 19)
                        .setNumValue3Indexed((int) val * 77)
                        .setNumValueUnique((int)val * 1139)
                        .build()
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
        // Let a single thread build all the indexes - boundaries will be detected automatically - which means (null, null) because the data set will be too small to have multiple shards in fdb
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
        validateIndexes(indexes);
    }

    @ParameterizedTest
    @CsvSource({
            // single threads:
            "0, 103, 10",
            "0, 417, 17",
            "0, 1417, 157",
            "0, 40, 2", // small fragments
            "0, 30, 1", // smaller fragments
            // multi threads:
            "4, 103, 17",
            "40, 773, 14",
            "20, 299, 19",
            "3, 40, 2", // small fragments
            "3, 30, 1", // smaller fragments
    })
    void testMutualIndexing(int numThreads, long numRecords, long boundarySize) {
        // build indexing by boundaries.
        // If numThreads < 2 - do it in a single thread.
        // Else, perform it in parallel by multiple threads
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
            range.parallel().forEach(ignore -> oneThreadIndexing(indexes, null, boundariesList));
        }

        if (numThreads < 2) {
            assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
            assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        }
        assertAllReadable(indexes);
        validateIndexes(indexes);
    }

    void oneThreadIndexing(List<Index> indexes, FDBStoreTimer callerTimer, List<Tuple> boundaries) {
        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        final FDBStoreTimer timer = callerTimer != null ? callerTimer : new FDBStoreTimer();
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setSubspace(subspace)
                .setTargetIndexes(indexes)
                .setTimer(timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setMutualIndexingBoundaries(boundaries)
                        .build())
                .build()) {
            indexBuilder.buildIndex(true);
        }
        if (callerTimer == null && LOGGER.isInfoEnabled()) {
            int numScanned = timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED);
            LOGGER.info(KeyValueLogMessage.of("oneThreadIndexing",
                    LogMessageKeys.RECORDS_SCANNED, numScanned,
                    "tid", Thread.currentThread().getId()
            ));
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
                        .setMutualIndexingBoundaries(boundaries)
                        .build())
                .setConfigLoader(old -> {
                    if (counter.incrementAndGet() > after) {
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
        Index unusedIndex = new Index("indexB", field("num_value_3_indexed"), IndexTypes.VALUE);

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

        // First crash, 10 threads, crash after 1:
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
        // Make sure that the regular indexing is unblocked
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

        // First crash, 5 threads, crash after 1:
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
        // test some boundaries end cases
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
        assertEquals(11, boundariesList.size());

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

    @Test
    void testMutualIndexingWithEmptyFragments() {
        // repeat testing boundaries end cases, but when most boundaries (well, fragments) contain no actual records
        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));

        openSimpleMetaData();
        List<TestRecords1Proto.MySimpleRecord> headRecords = LongStream.range(0, 100).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(val)
                        .setNumValue2((int)val * 19)
                        .setNumValue3Indexed((int) val * 77)
                        .setNumValueUnique((int)val * 1139)
                        .build()
        ).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> tailRecords = LongStream.range(938, 1000).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(val)
                        .setNumValue2((int)val * 19)
                        .setNumValue3Indexed((int) val * 77)
                        .setNumValueUnique((int)val * 1139)
                        .build()
        ).collect(Collectors.toList());

        try (FDBRecordContext context = openContext())  {
            headRecords.forEach(recordStore::saveRecord);
            tailRecords.forEach(recordStore::saveRecord);
            context.commit();
        }

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);
        final FDBStoreTimer timer = new FDBStoreTimer();
        int boundarySize = 10;
        int pseudoNumRecords = 1000;
        final List<Tuple> boundariesList = getBoundariesList(pseudoNumRecords, boundarySize);
        assertEquals(101, boundariesList.size());

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

    @Test
    void testSortAndSquash() {
        List<byte[]> points = new ArrayList<>();
        points.add(byteEmpty());
        for (int i = 2; i < 0xff; i += 3) {
            points.add(byteOf(i));
        }
        points.add(byteOf(0xff));
        List<Range> ranges = new ArrayList<>();
        for (int i = 0; i < points.size() - 1; i++) {
            ranges.add(new Range(points.get(i), points.get(i + 1)));
        }
        List<Range> partial = new ArrayList<>();
        for (int i = 1 ; i < ranges.size(); i += 4) {
            partial.add(ranges.get(i));
        }
        // test squash
        Collections.shuffle(ranges);
        List<Range> squashed = IndexingMutuallyByRecords.sortAndSquash(ranges);
        assertEquals(1, squashed.size());
        assertEquals(points.get(0), squashed.get(0).begin);
        assertEquals(points.get(points.size() - 1), squashed.get(0).end);

        // test sort without squash
        List<Range> partialShuffled = new ArrayList<>(partial);
        Collections.shuffle(partialShuffled);
        squashed = IndexingMutuallyByRecords.sortAndSquash(partialShuffled);
        assertEquals(partial, squashed);
    }

    @Test
    void testFullyUnBuiltRange() {
        List<Range> ranges = new ArrayList<>();
        ranges.add(rangeOf(0, 9));
        ranges.add(rangeOf(20, 29));
        ranges.add(rangeOf(40, 49));
        // fully unbuilt
        checkFully(ranges, 0, 9);
        checkFully(ranges, 0, 8);
        checkFully(ranges, 41, 49);
        checkFully(ranges, 23, 24);
        // not fully unbuilt
        assertNull(IndexingMutuallyByRecords.fullyUnBuiltRange(ranges, rangeOf(0, 10)));
        assertNull(IndexingMutuallyByRecords.fullyUnBuiltRange(ranges, rangeOf(8, 10)));
        assertNull(IndexingMutuallyByRecords.fullyUnBuiltRange(ranges, rangeOf(9, 10)));
        assertNull(IndexingMutuallyByRecords.fullyUnBuiltRange(ranges, rangeOf(100, 110)));
        assertNull(IndexingMutuallyByRecords.fullyUnBuiltRange(ranges, rangeOf(0, 20)));
        assertNull(IndexingMutuallyByRecords.fullyUnBuiltRange(ranges, rangeOf(20, 300)));
        assertNull(IndexingMutuallyByRecords.fullyUnBuiltRange(ranges, rangeOf(41, 50)));
        assertNull(IndexingMutuallyByRecords.fullyUnBuiltRange(ranges, rangeOf(20, 44)));
    }

    @Test
    void testPartlyUnBuiltRange() {
        List<Range> ranges = new ArrayList<>();
        ranges.add(rangeOf(0, 9));
        ranges.add(rangeOf(20, 29));
        ranges.add(rangeOf(40, 49));
        // fully unbuilt
        checkPartial(ranges, 0, 9, 0, 9);
        checkPartial(ranges, 0, 8, 0, 8);
        checkPartial(ranges, 41, 49, 41, 49);
        checkPartial(ranges, 23, 24, 23, 24);
        // partly unbuilt
        checkPartial(ranges, 14, 24, 20, 24);
        checkPartial(ranges, 0, 12, 0, 9);
        checkPartial(ranges, 0, 100, 0, 9);
        checkPartial(ranges, 40, 100, 40, 49);
        // no overlap
        assertNull(IndexingMutuallyByRecords.partlyUnBuiltRange(ranges, rangeOf(10, 11)));
        assertNull(IndexingMutuallyByRecords.partlyUnBuiltRange(ranges, rangeOf(100, 200)));
        assertNull(IndexingMutuallyByRecords.partlyUnBuiltRange(ranges, rangeOf(33, 40)));
    }

    private static void checkFully(List<Range> ranges, int rangeStart, int rangeEnd) {
        Range res = IndexingMutuallyByRecords.fullyUnBuiltRange(ranges, rangeOf(rangeStart, rangeEnd));
        assertNotNull(res);
        assertEquals(res.begin[0], byteOf(rangeStart)[0]);
        assertEquals(res.end[0], byteOf(rangeEnd)[0]);
    }

    private static void checkPartial(List<Range> ranges, int rangeStart, int rangeEnd, int expectStart, int expectEnd) {
        Range res = IndexingMutuallyByRecords.partlyUnBuiltRange(ranges, rangeOf(rangeStart, rangeEnd));
        assertNotNull(res);
        assertEquals(res.begin[0], byteOf(expectStart)[0]);
        assertEquals(res.end[0], byteOf(expectEnd)[0]);
    }

    private static Range rangeOf(int start, int end) {
        assertTrue(start < end);
        return new Range(byteOf(start), byteOf(end));
    }

    private static byte[] byteOf(int i) {
        return new byte[]{(byte) i};
    }

    private static byte[] byteEmpty() {
        return new byte[0];
    }
}
