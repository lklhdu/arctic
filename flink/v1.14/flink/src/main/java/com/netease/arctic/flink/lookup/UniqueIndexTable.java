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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class UniqueIndexTable implements KVTable {
  private static final long serialVersionUID = 1L;
  protected final RocksDBRecordState recordState;

  protected int[] uniqueKeyIndexMapping;

  protected long lruSize;

  public UniqueIndexTable(
      StateFactory stateFactory,
      List<String> primaryKeys,
      long lruCacheSize,
      Schema projectSchema,
      Configuration config) {

    recordState =
        stateFactory.createRecordState(
            "uniqueIndex",
            lruCacheSize,
            createKeySerializer(projectSchema, primaryKeys),
            createValueSerializer(projectSchema),
            config);
    List<String> fields = projectSchema.asStruct().fields()
        .stream().map(Types.NestedField::name).collect(Collectors.toList());
    this.uniqueKeyIndexMapping = primaryKeys.stream().mapToInt(fields::indexOf).toArray();
    this.lruSize = lruCacheSize;
  }

  @Override
  public void open() {
    recordState.open();
  }

  @Override
  public List<RowData> get(RowData key) throws IOException {
    Optional<RowData> record = recordState.get(key);
    return record.map(Collections::singletonList).orElse(Collections.emptyList());
  }

  @Override
  public void upsert(Iterator<RowData> dataStream) throws IOException {
    while (dataStream.hasNext()) {
      RowData value = dataStream.next();
      RowData key = new KeyRowData(uniqueKeyIndexMapping, value);
      //  todo
      if (lruSize == 10000) {
        // ignore serializing and putting data into cache
        continue;
      }

      if (value.getRowKind() == RowKind.INSERT || value.getRowKind() == RowKind.UPDATE_AFTER) {
        recordState.put(key, value);
      } else {
        recordState.delete(key);
      }
    }
  }

  @Override
  public void initial(Iterator<RowData> dataStream) throws IOException {
    while (dataStream.hasNext()) {
      RowData value = dataStream.next();
      RowData key = new KeyRowData(uniqueKeyIndexMapping, value);
      recordState.batchWrite(key, value);
    }
    recordState.flush();
  }

  @Override
  public boolean initialized() {
    return recordState.initialized();
  }

  @Override
  public void waitWriteRocksDBCompleted() {
    LOG.info("Waiting for Record State initialization");
    recordState.waitWriteRocksDBDone();
    LOG.info("The concurrent threads have finished writing data into the Record State.");
  }

  @Override
  public void close() {
    recordState.close();
  }

}
