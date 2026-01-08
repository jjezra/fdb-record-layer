/*
 * LuceneIndexMaintainerHelper.java
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.lucene.directory.FDBDirectoryManager;
import com.apple.foundationdb.record.lucene.idformat.LuceneIndexKeySerializer;
import com.apple.foundationdb.record.lucene.idformat.RecordCoreFormatException;
import com.apple.foundationdb.record.lucene.search.BooleanPointsConfig;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.lucene.document.BinaryPoint;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class LuceneIndexMaintainerHelper {
    private static final Logger LOG = LoggerFactory.getLogger(LuceneIndexMaintainerHelper.class);





    public static void writeDocument(FDBRecordContext context,
                                     FDBDirectoryManager directoryManager,
                                     Index index,
                                     Tuple groupingKey,
                                     Integer partitionId,
                                     Tuple primaryKey,
                                     List<LuceneDocumentFromRecord.DocumentField> fields
                                     ) throws IOException {
        // Partition count was increased preemptively by the index maintainer
        final long startTime = System.nanoTime();
        Document document = new Document();
        final IndexWriter newWriter = directoryManager.getIndexWriter(groupingKey, partitionId);
        String formatString = index.getOption(LuceneIndexOptions.PRIMARY_KEY_SERIALIZATION_FORMAT);
        LuceneIndexKeySerializer keySerializer = LuceneIndexKeySerializer.fromStringFormat(formatString);

        BytesRef ref = new BytesRef(keySerializer.asPackedByteArray(primaryKey));
        // use packed Tuple for the Stored and Sorted fields
        document.add(new StoredField(LuceneIndexMaintainer.PRIMARY_KEY_FIELD_NAME, ref));
        document.add(new SortedDocValuesField(LuceneIndexMaintainer.PRIMARY_KEY_SEARCH_NAME, ref));
        if (keySerializer.hasFormat()) {
            try {
                // Use BinaryPoint for fast lookup of ID when enabled
                document.add(new BinaryPoint(LuceneIndexMaintainer.PRIMARY_KEY_BINARY_POINT_NAME, keySerializer.asFormattedBinaryPoint(primaryKey)));
            } catch (RecordCoreFormatException ex) {
                // this can happen on format mismatch or encoding error
                // just don't write the field, but allow the document to continue
                logSerializationError("Failed to write using BinaryPoint encoded ID: {}", ex.getMessage());
            }
        }

        Map<IndexOptions, List<LuceneDocumentFromRecord.DocumentField>> indexOptionsToFieldsMap = getIndexOptionsToFieldsMap(fields);
        for (Map.Entry<IndexOptions, List<LuceneDocumentFromRecord.DocumentField>> entry : indexOptionsToFieldsMap.entrySet()) {
            for (LuceneDocumentFromRecord.DocumentField field : entry.getValue()) {
                insertField(field, document);
            }
        }
        newWriter.addDocument(document);
        context.record(LuceneEvents.Events.LUCENE_ADD_DOCUMENT, System.nanoTime() - startTime);

    }

    private static void logSerializationError(String format, Object ... arguments) {
        // TODO: report only once
        if (LOG.isWarnEnabled()) {
            LOG.warn(format, arguments);
        }
    }

    @Nonnull
    private static Map<IndexOptions, List<LuceneDocumentFromRecord.DocumentField>> getIndexOptionsToFieldsMap(@Nonnull List<LuceneDocumentFromRecord.DocumentField> fields) {
        final Map<IndexOptions, List<LuceneDocumentFromRecord.DocumentField>> map = new EnumMap<>(IndexOptions.class);
        fields.stream().forEach(f -> {
            final IndexOptions indexOptions = getIndexOptions((String) Objects.requireNonNullElse(f.getConfig(LuceneFunctionNames.LUCENE_AUTO_COMPLETE_FIELD_INDEX_OPTIONS),
                    LuceneFunctionNames.LuceneFieldIndexOptions.DOCS_AND_FREQS_AND_POSITIONS.name()));
            map.putIfAbsent(indexOptions, new ArrayList<>());
            map.get(indexOptions).add(f);
        });
        return map;
    }

    /**
     * Insert a field into the document and add a suggestion into the suggester if needed.
     */
    @SuppressWarnings("java:S3776")
    public static void insertField(LuceneDocumentFromRecord.DocumentField field, final Document document) {
        final String fieldName = field.getFieldName();
        final Object value = field.getValue();
        final Field luceneField;
        final Field sortedField;
        final StoredField storedField;
        switch (field.getType()) {
            case TEXT:
                luceneField = new Field(fieldName, (String) value, getTextFieldType(field));
                sortedField = null;
                storedField = null;
                break;
            case STRING:
                luceneField = new StringField(fieldName, (String)value, field.isStored() ? Field.Store.YES : Field.Store.NO);
                sortedField = field.isSorted() ? new SortedDocValuesField(fieldName, new BytesRef((String)value)) : null;
                storedField = null;
                break;
            case INT:
                luceneField = new IntPoint(fieldName, (Integer)value);
                sortedField = field.isSorted() ? new NumericDocValuesField(fieldName, (Integer)value) : null;
                storedField = field.isStored() ? new StoredField(fieldName, (Integer)value) : null;
                break;
            case LONG:
                luceneField = new LongPoint(fieldName, (Long)value);
                sortedField = field.isSorted() ? new NumericDocValuesField(fieldName, (Long)value) : null;
                storedField = field.isStored() ? new StoredField(fieldName, (Long)value) : null;
                break;
            case DOUBLE:
                luceneField = new DoublePoint(fieldName, (Double)value);
                sortedField = field.isSorted() ? new NumericDocValuesField(fieldName, NumericUtils.doubleToSortableLong((Double)value)) : null;
                storedField = field.isStored() ? new StoredField(fieldName, (Double)value) : null;
                break;
            case BOOLEAN:
                byte[] bytes = Boolean.TRUE.equals(value) ? BooleanPointsConfig.TRUE_BYTES : BooleanPointsConfig.FALSE_BYTES;
                luceneField = new BinaryPoint(fieldName, bytes);
                storedField = field.isStored() ? new StoredField(fieldName, bytes) : null;
                sortedField = field.isSorted() ? new SortedDocValuesField(fieldName, new BytesRef(bytes)) : null;
                break;
            default:
                throw new RecordCoreArgumentException("Invalid type for lucene index field", "type", field.getType());
        }
        document.add(luceneField);
        if (sortedField != null) {
            document.add(sortedField);
        }
        if (storedField != null) {
            document.add(storedField);
        }
    }

    private static FieldType getTextFieldType(LuceneDocumentFromRecord.DocumentField field) {
        FieldType ft = new FieldType();

        try {
            ft.setIndexOptions(getIndexOptions((String)Objects.requireNonNullElse(field.getConfig(LuceneFunctionNames.LUCENE_FULL_TEXT_FIELD_INDEX_OPTIONS),
                    LuceneFunctionNames.LuceneFieldIndexOptions.DOCS_AND_FREQS_AND_POSITIONS.name())));
            ft.setTokenized(true);
            ft.setStored(field.isStored());
            ft.setStoreTermVectors((boolean)Objects.requireNonNullElse(field.getConfig(LuceneFunctionNames.LUCENE_FULL_TEXT_FIELD_WITH_TERM_VECTORS), false));
            ft.setStoreTermVectorPositions((boolean)Objects.requireNonNullElse(field.getConfig(LuceneFunctionNames.LUCENE_FULL_TEXT_FIELD_WITH_TERM_VECTOR_POSITIONS), false));
            ft.setOmitNorms(true);
            ft.freeze();
        } catch (ClassCastException ex) {
            throw new RecordCoreArgumentException("Invalid value type for Lucene field config", ex);
        }

        return ft;
    }

    private static IndexOptions getIndexOptions(@Nonnull String value) {
        try {
            return IndexOptions.valueOf(value);
        } catch (IllegalArgumentException ex) {
            throw new RecordCoreArgumentException("Invalid enum value to parse for Lucene IndexOptions: " + value, ex);
        }
    }

}
