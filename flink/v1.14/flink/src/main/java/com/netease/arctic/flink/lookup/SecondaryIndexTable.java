/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netease.arctic.flink.lookup;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class SecondaryIndexTable extends UniqueIndexTable {
  private static final long serialVersionUID = 1L;
  private final int[] secondaryKeyIndexMapping;
  private final RocksDBSetState setState;

  public SecondaryIndexTable(
      StateFactory stateFactory,
      List<String> primaryKeys,
      List<String> joinKeys,
      long lruCacheSize,
      Schema projectSchema,
      Configuration config) {
    super(stateFactory, primaryKeys, lruCacheSize, projectSchema, config);

    this.setState =
        stateFactory.createSetState(
            "secondaryIndex",
            lruCacheSize,
            createKeySerializer(projectSchema, joinKeys),
            createKeySerializer(projectSchema, primaryKeys),
            createValueSerializer(projectSchema),
            config);

    List<String> fields = projectSchema.asStruct().fields()
        .stream().map(Types.NestedField::name).collect(Collectors.toList());
    secondaryKeyIndexMapping = joinKeys.stream().mapToInt(fields::indexOf).toArray();
  }

  @Override
  public List<RowData> get(RowData key) throws IOException {
    List<byte[]> uniqueKeys = setState.get(key);
    if (!uniqueKeys.isEmpty()) {
      List<RowData> result = new ArrayList<>(uniqueKeys.size());
      for (byte[] uniqueKey : uniqueKeys) {
        recordState.get(uniqueKey).ifPresent(result::add);
      }
      return result;
    }
    return Collections.emptyList();
  }

  @Override
  public void upsert(Iterator<RowData> dataStream) throws IOException {
    while (dataStream.hasNext()) {
      RowData value = dataStream.next();
      RowData uniqueKey = new KeyRowData(uniqueKeyIndexMapping, value);
      RowData joinKey = new KeyRowData(secondaryKeyIndexMapping, value);
      byte[] uniqueKeyBytes = recordState.serializeKey(uniqueKey);

      if (value.getRowKind() == RowKind.INSERT || value.getRowKind() == RowKind.UPDATE_AFTER) {
        recordState.put(uniqueKeyBytes, value);
        setState.merge(joinKey, uniqueKeyBytes);
      } else {
        recordState.delete(uniqueKeyBytes);
        setState.delete(joinKey, uniqueKeyBytes);
      }
    }
  }

  @Override
  public void close() {
    super.close();
    recordState.close();
  }
}
