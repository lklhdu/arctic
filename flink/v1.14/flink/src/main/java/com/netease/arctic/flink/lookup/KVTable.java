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

import com.netease.arctic.flink.shuffle.LogRecordV1;
import com.netease.arctic.log.LogDataJsonDeserialization;
import com.netease.arctic.log.LogDataJsonSerialization;
import com.netease.arctic.utils.SchemaUtil;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Schema;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

public interface KVTable extends Serializable, Closeable {

  List<RowData> get(RowData key) throws IOException;

  void upsert(Iterator<RowData> dataStream) throws IOException;

  void close();

  default LogDataJsonSerialization<RowData> createKeySerialization(Schema arcticTableSchema, List<String> keys) {
    Schema keySchema = SchemaUtil.convertFieldsToSchema(arcticTableSchema, keys);
    return new LogDataJsonSerialization<>(keySchema, LogRecordV1.fieldGetterFactory);
  }

  default LogDataJsonSerialization<RowData> createValueSerialization(Schema projectSchema) {
    return new LogDataJsonSerialization<>(projectSchema, LogRecordV1.fieldGetterFactory);
  }

  default LogDataJsonDeserialization<RowData> createValueDeserialization(Schema projectSchema) {
    return
        new LogDataJsonDeserialization<>(
            projectSchema,
            LogRecordV1.factory,
            LogRecordV1.arrayFactory,
            LogRecordV1.mapFactory);
  }

  static KVTable create(
      StateFactory stateFactory,
      List<String> primaryKeys,
      List<String> joinKeys,
      long lruCacheSize,
      Schema projectSchema) {
    if (new HashSet<>(primaryKeys).equals(new HashSet<>(joinKeys))) {
      return new UniqueIndexTable(stateFactory, primaryKeys, lruCacheSize, projectSchema);
    } else {
      return new SecondaryIndexTable(stateFactory, primaryKeys, joinKeys, lruCacheSize, projectSchema);
    }
  }
}
