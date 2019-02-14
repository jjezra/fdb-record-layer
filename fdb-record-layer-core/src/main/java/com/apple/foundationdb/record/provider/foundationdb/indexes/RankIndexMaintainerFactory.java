/*
 * RankIndexMaintainerFactory.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.API;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.IndexValidator;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.MetaDataValidator;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactory;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Set;

/**
 * A factory for {@link RankIndexMaintainer} indexes.
 */
@AutoService(IndexMaintainerFactory.class)
@API(API.Status.MAINTAINED)
public class RankIndexMaintainerFactory implements IndexMaintainerFactory {
    static final String[] TYPES = { IndexTypes.RANK };

    @Override
    @Nonnull
    public Iterable<String> getIndexTypes() {
        return Arrays.asList(TYPES);
    }

    @Override
    @Nonnull
    public IndexValidator getIndexValidator(Index index) {
        return new IndexValidator(index) {
            @Override
            public void validate(@Nonnull MetaDataValidator metaDataValidator) {
                super.validate(metaDataValidator);
                validateGrouping(1);
                validateNotVersion();
            }

            @Override
            public void validateChangedOptions(@Nonnull Index oldIndex, @Nonnull Set<String> changedOptions) {
                if (changedOptions.contains(IndexOptions.RANK_NLEVELS)) {
                    int oldLevels = RankIndexMaintainer.getNLevels(oldIndex);
                    int newLevels = RankIndexMaintainer.getNLevels(index);
                    if (oldLevels != newLevels) {
                        throw new MetaDataException("rank levels changed",
                                LogMessageKeys.INDEX_NAME, index.getName());
                    }
                    changedOptions.remove(IndexOptions.RANK_NLEVELS);
                }
                super.validateChangedOptions(oldIndex, changedOptions);
            }
        };
    }

    @Override
    @Nonnull
    public IndexMaintainer getIndexMaintainer(IndexMaintainerState state) {
        return new RankIndexMaintainer(state);
    }

}
