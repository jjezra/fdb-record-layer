/*
 * MetaDataProtoEditorTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecordsDoubleNestedProto;
import com.apple.foundationdb.record.provider.foundationdb.MetaDataProtoEditor.FieldTypeMatch;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for the meta-data proto editor. There are more tests for this class (in action) in the {@link FDBMetaDataStoreTest}
 * class. Those tests are more end-to-end, and they are for doing things like testing that when the meta-data
 * are read from the database, edited, and written back, everything works. These tests focus on just the editor
 * itself.
 */
public class MetaDataProtoEditorTest {

    @Nonnull
    private FieldTypeMatch fieldIsType(@Nonnull DescriptorProtos.FileDescriptorProtoOrBuilder file,
                                       @Nonnull String messageName, @Nonnull String fieldName,
                                       @Nonnull String typeName) {

        final DescriptorProtos.DescriptorProto record = file.getMessageTypeList().stream()
                .filter(message -> message.getName().equals(messageName))
                .findAny()
                .get();
        final DescriptorProtos.FieldDescriptorProto field = record.getFieldList().stream()
                .filter(f -> f.getName().equals(fieldName))
                .findAny()
                .get();
        return MetaDataProtoEditor.fieldIsType(file, record, field, typeName);
    }

    @Test
    public void fieldIsType() {
        final DescriptorProtos.FileDescriptorProto file = TestRecords1Proto.getDescriptor().toProto();
        assertEquals(FieldTypeMatch.MATCHES,
                fieldIsType(file, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", "MySimpleRecord"));
        assertEquals(FieldTypeMatch.MATCHES,
                fieldIsType(file, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", ".com.apple.foundationdb.record.test1.MySimpleRecord"));
        assertEquals(FieldTypeMatch.MATCHES_AS_NESTED,
                fieldIsType(file, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", ".com.apple.foundationdb.record"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(file, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", "MySimpleRecord.MyNestedRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(file, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", ".com.apple.foundationdb.record.test1.MySimpleRecord.MyNestedRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(file, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", ".com.apple.foundationdb.record.test2.MySimpleRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(file, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", "MyOtherRecord"));
    }

    @Test
    public void fieldIsTypeUnqualified() throws Descriptors.DescriptorValidationException {
        final DescriptorProtos.FileDescriptorProto.Builder fileBuilder = TestRecords1Proto.getDescriptor().toProto().toBuilder();
        final DescriptorProtos.FieldDescriptorProto.Builder fieldBuilder = fileBuilder.getMessageTypeBuilderList().stream()
                .filter(message -> message.getName().equals(RecordMetaDataBuilder.DEFAULT_UNION_NAME))
                .flatMap(message -> message.getFieldBuilderList().stream())
                .filter(field -> field.getName().equals("_MySimpleRecord"))
                .findAny()
                .get();

        // Unqualify the field in the union descriptor
        fieldBuilder.setTypeName("MySimpleRecord");

        // Ensure that the field still resolves to the same type
        Descriptors.FileDescriptor modifiedFileDescriptor = Descriptors.FileDescriptor.buildFrom(fileBuilder.build(), TestRecords1Proto.getDescriptor().getDependencies().toArray(new Descriptors.FileDescriptor[0]));
        Descriptors.Descriptor simpleRecordDescriptor = modifiedFileDescriptor.findMessageTypeByName("MySimpleRecord");
        assertNotNull(simpleRecordDescriptor);
        assertSame(simpleRecordDescriptor, modifiedFileDescriptor.findMessageTypeByName(RecordMetaDataBuilder.DEFAULT_UNION_NAME).findFieldByName("_MySimpleRecord").getMessageType());

        assertEquals(FieldTypeMatch.MIGHT_MATCH,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", "MySimpleRecord"));
        assertEquals(FieldTypeMatch.MIGHT_MATCH,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", ".com.apple.foundationdb.record.test1.MySimpleRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", ".com.apple.foundationdb.record.test2.MySimpleRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", "MyOtherRecord"));
        assertEquals(FieldTypeMatch.MIGHT_MATCH,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", ".com.apple.foundationdb.record.test1.RecordTypeUnion.MySimpleRecord"));
        assertEquals(FieldTypeMatch.MIGHT_MATCH_AS_NESTED,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", ".com.apple.foundationdb.record.test1.RecordTypeUnion"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", ".com.apple.foundationdb.record.test1.RecordTypeUnion.MySimpleRecord.InnerRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", "MySimpleRecord.MyNestedRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", ".com.apple.foundationdb.record.test1.MySimpleRecord.MyNestedRecord"));

        fieldBuilder.setTypeName("test1.MySimpleRecord");
        modifiedFileDescriptor = Descriptors.FileDescriptor.buildFrom(fileBuilder.build(), TestRecords1Proto.getDescriptor().getDependencies().toArray(new Descriptors.FileDescriptor[0]));
        simpleRecordDescriptor = modifiedFileDescriptor.findMessageTypeByName("MySimpleRecord");
        assertNotNull(simpleRecordDescriptor);
        assertSame(simpleRecordDescriptor, modifiedFileDescriptor.findMessageTypeByName(RecordMetaDataBuilder.DEFAULT_UNION_NAME).findFieldByName("_MySimpleRecord").getMessageType());

        assertEquals(FieldTypeMatch.MIGHT_MATCH,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", "MySimpleRecord"));
        assertEquals(FieldTypeMatch.MIGHT_MATCH,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", ".com.apple.foundationdb.record.test1.MySimpleRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", ".com.apple.foundationdb.record.test2.MySimpleRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", "MyOtherRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", ".com.apple.foundationdb.record.test1.RecordTypeUnion.MySimpleRecord"));
        assertEquals(FieldTypeMatch.MIGHT_MATCH_AS_NESTED,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", ".com.apple.foundationdb.record.test1.RecordTypeUnion"));
        assertEquals(FieldTypeMatch.MIGHT_MATCH_AS_NESTED,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", ".com.apple.foundationdb.record.test1.RecordTypeUnion.test1"));
        assertEquals(FieldTypeMatch.MIGHT_MATCH,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", ".com.apple.foundationdb.record.test1.RecordTypeUnion.test1.MySimpleRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", ".com.apple.foundationdb.record.test1.RecordTypeUnion.MySimpleRecord.InnerRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", "MySimpleRecord.MyNestedRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(fileBuilder, RecordMetaDataBuilder.DEFAULT_UNION_NAME, "_MySimpleRecord", ".com.apple.foundationdb.record.test1.MySimpleRecord.MyNestedRecord"));
    }

    @Test
    public void nestedFieldIsType() {
        final DescriptorProtos.FileDescriptorProto file = TestRecordsDoubleNestedProto.getDescriptor().toProto();
        assertEquals(FieldTypeMatch.MATCHES,
                fieldIsType(file, "OuterRecord", "inner", "OuterRecord.MiddleRecord.InnerRecord"));
        assertEquals(FieldTypeMatch.MATCHES,
                fieldIsType(file, "OuterRecord", "inner", ".com.apple.foundationdb.record.test.doublenested.OuterRecord.MiddleRecord.InnerRecord"));
        assertEquals(FieldTypeMatch.MATCHES_AS_NESTED,
                fieldIsType(file, "OuterRecord", "inner", "OuterRecord"));
        assertEquals(FieldTypeMatch.MATCHES_AS_NESTED,
                fieldIsType(file, "OuterRecord", "inner", "OuterRecord.MiddleRecord"));
        assertEquals(FieldTypeMatch.MATCHES_AS_NESTED,
                fieldIsType(file, "OuterRecord", "inner", ".com.apple.foundationdb.record.test.doublenested.OuterRecord"));
        assertEquals(FieldTypeMatch.MATCHES_AS_NESTED,
                fieldIsType(file, "OuterRecord", "inner", ".com.apple.foundationdb.record.test.doublenested.OuterRecord.MiddleRecord"));

        assertEquals(FieldTypeMatch.MATCHES,
                fieldIsType(file, "MiddleRecord", "middle", "MiddleRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(file, "MiddleRecord", "middle", "OuterRecord.MiddleRecord"));

        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(file, "MiddleRecord", "other_middle", "MiddleRecord"));
        assertEquals(FieldTypeMatch.MATCHES,
                fieldIsType(file, "MiddleRecord", "other_middle", "OuterRecord.MiddleRecord"));
    }

    @Test
    public void nestedFieldIsTypeUnqualified() throws Descriptors.DescriptorValidationException {
        final DescriptorProtos.FileDescriptorProto.Builder fileBuilder = TestRecordsDoubleNestedProto.getDescriptor().toProto().toBuilder();
        final DescriptorProtos.FieldDescriptorProto.Builder innerBuilder = fileBuilder.getMessageTypeBuilderList().stream()
                .filter(message -> message.getName().equals("OuterRecord"))
                .flatMap(message -> message.getFieldBuilderList().stream())
                .filter(field -> field.getName().equals("inner"))
                .findAny()
                .get();

        // Unqualify the inner field
        innerBuilder.setTypeName("MiddleRecord.InnerRecord");

        // Ensure that the type actually resolves to the same type
        Descriptors.FileDescriptor modifiedFileDescriptor = Descriptors.FileDescriptor.buildFrom(fileBuilder.build(), TestRecordsDoubleNestedProto.getDescriptor().getDependencies().toArray(new Descriptors.FileDescriptor[0]));
        Descriptors.Descriptor innerRecordDescriptor = modifiedFileDescriptor.findMessageTypeByName("OuterRecord").findNestedTypeByName("MiddleRecord").findNestedTypeByName("InnerRecord");
        assertNotNull(innerRecordDescriptor);
        assertSame(innerRecordDescriptor, modifiedFileDescriptor.findMessageTypeByName("OuterRecord").findFieldByName("inner").getMessageType());

        assertEquals(FieldTypeMatch.MIGHT_MATCH,
                fieldIsType(fileBuilder, "OuterRecord", "inner", "OuterRecord.MiddleRecord.InnerRecord"));
        assertEquals(FieldTypeMatch.MIGHT_MATCH_AS_NESTED,
                fieldIsType(fileBuilder, "OuterRecord", "inner", "OuterRecord.MiddleRecord"));
        assertEquals(FieldTypeMatch.MIGHT_MATCH_AS_NESTED,
                fieldIsType(fileBuilder, "OuterRecord", "inner", "OuterRecord"));
        assertEquals(FieldTypeMatch.MIGHT_MATCH,
                fieldIsType(fileBuilder, "OuterRecord", "inner", "MiddleRecord.InnerRecord"));
        assertEquals(FieldTypeMatch.MIGHT_MATCH_AS_NESTED,
                fieldIsType(fileBuilder, "OuterRecord", "inner", "MiddleRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(fileBuilder, "OuterRecord", "inner", ".com.apple.foundationdb.record.test.doublenested.OtherRecord"));

        innerBuilder.setTypeName("OuterRecord.MiddleRecord.InnerRecord");
        modifiedFileDescriptor = Descriptors.FileDescriptor.buildFrom(fileBuilder.build(), TestRecordsDoubleNestedProto.getDescriptor().getDependencies().toArray(new Descriptors.FileDescriptor[0]));
        innerRecordDescriptor = modifiedFileDescriptor.findMessageTypeByName("OuterRecord").findNestedTypeByName("MiddleRecord").findNestedTypeByName("InnerRecord");
        assertNotNull(innerRecordDescriptor);
        assertSame(innerRecordDescriptor, modifiedFileDescriptor.findMessageTypeByName("OuterRecord").findFieldByName("inner").getMessageType());

        assertEquals(FieldTypeMatch.MIGHT_MATCH,
                fieldIsType(fileBuilder, "OuterRecord", "inner", "OuterRecord.MiddleRecord.InnerRecord"));
        assertEquals(FieldTypeMatch.MIGHT_MATCH_AS_NESTED,
                fieldIsType(fileBuilder, "OuterRecord", "inner", "OuterRecord.MiddleRecord"));
        assertEquals(FieldTypeMatch.MIGHT_MATCH_AS_NESTED,
                fieldIsType(fileBuilder, "OuterRecord", "inner", "OuterRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(fileBuilder, "OuterRecord", "inner", "MiddleRecord.InnerRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(fileBuilder, "OuterRecord", "inner", "MiddleRecord"));
        assertEquals(FieldTypeMatch.DOES_NOT_MATCH,
                fieldIsType(fileBuilder, "OuterRecord", "inner", ".com.apple.foundationdb.record.test.doublenested.OtherRecord"));

        RecordMetaData metaData = RecordMetaData.build(modifiedFileDescriptor);
        RecordMetaDataProto.MetaData.Builder metaDataProtoBuilder = metaData.toProto().toBuilder();
        MetaDataProtoEditor.AmbiguousTypeNameException e = assertThrows(MetaDataProtoEditor.AmbiguousTypeNameException.class,
                () -> MetaDataProtoEditor.renameRecordType(metaDataProtoBuilder, "OuterRecord", "OtterRecord"));
        assertEquals("Field inner in message .com.apple.foundationdb.record.test.doublenested.OuterRecord of type OuterRecord.MiddleRecord.InnerRecord might be of type .com.apple.foundationdb.record.test.doublenested.OuterRecord", e.getMessage());
    }

    private void renameFieldTypes(@Nonnull DescriptorProtos.DescriptorProto.Builder messageTypeBuilder, @Nonnull String oldTypeName, @Nonnull String newTypeName) {
        messageTypeBuilder.getFieldBuilderList().forEach(field -> {
            if (field.getTypeName().equals(oldTypeName)) {
                field.setTypeName(newTypeName);
            } else if (field.getTypeName().startsWith(oldTypeName) && field.getTypeName().charAt(oldTypeName.length()) == '.') {
                field.setTypeName(newTypeName + field.getTypeName().substring(oldTypeName.length()));
            }
        });
        messageTypeBuilder.getNestedTypeBuilderList().forEach(nestedMessage -> renameFieldTypes(nestedMessage, oldTypeName, newTypeName));
    }

    @Test
    public void renameOuterTypeWithNestedTypeWithSameName() throws Descriptors.DescriptorValidationException {
        final DescriptorProtos.FileDescriptorProto.Builder fileBuilder = TestRecordsDoubleNestedProto.getDescriptor().toProto().toBuilder();
        fileBuilder.getMessageTypeBuilderList().forEach(message -> {
            if (message.getName().equals("OuterRecord")) {
                message.getNestedTypeBuilderList().forEach(nestedMessage -> {
                    if (nestedMessage.getName().equals("MiddleRecord")) {
                        nestedMessage.setName("OuterRecord");
                    }
                });
                renameFieldTypes(message, ".com.apple.foundationdb.record.test.doublenested.OuterRecord.MiddleRecord", "OuterRecord");
            } else {
                renameFieldTypes(message, ".com.apple.foundationdb.record.test.doublenested.OuterRecord.MiddleRecord", ".com.apple.foundationdb.record.test.doublenested.OuterRecord.OuterRecord");
            }
        });

        // Make sure the types were renamed in a way that preserves type, etc.
        Descriptors.FileDescriptor modifiedFile = Descriptors.FileDescriptor.buildFrom(fileBuilder.build(), TestRecordsDoubleNestedProto.getDescriptor().getDependencies().toArray(new Descriptors.FileDescriptor[0]));
        Descriptors.Descriptor outerOuterRecord = modifiedFile.findMessageTypeByName("OuterRecord");
        assertNotNull(outerOuterRecord);
        Descriptors.Descriptor nestedOuterRecord = outerOuterRecord.findNestedTypeByName("OuterRecord");
        assertNotNull(nestedOuterRecord);
        assertNotSame(outerOuterRecord, nestedOuterRecord);
        assertSame(outerOuterRecord, nestedOuterRecord.findNestedTypeByName("InnerRecord").findFieldByName("outer").getMessageType());
        assertSame(nestedOuterRecord, outerOuterRecord.findFieldByName("middle").getMessageType());
        assertSame(nestedOuterRecord, outerOuterRecord.findFieldByName("inner").getMessageType().getContainingType());
        assertSame(nestedOuterRecord, modifiedFile.findMessageTypeByName("MiddleRecord").findFieldByName("other_middle").getMessageType());

        RecordMetaData metaData = RecordMetaData.build(modifiedFile);
        RecordMetaDataProto.MetaData.Builder metaDataProtoBuilder = metaData.toProto().toBuilder();
        MetaDataProtoEditor.AmbiguousTypeNameException e = assertThrows(MetaDataProtoEditor.AmbiguousTypeNameException.class, () -> MetaDataProtoEditor.renameRecordType(metaDataProtoBuilder, "OuterRecord", "OtterRecord"));
        assertEquals("Field middle in message .com.apple.foundationdb.record.test.doublenested.OuterRecord of type OuterRecord might be of type .com.apple.foundationdb.record.test.doublenested.OuterRecord", e.getMessage());
    }
}
