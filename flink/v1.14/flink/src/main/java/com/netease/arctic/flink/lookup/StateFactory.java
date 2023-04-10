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

import com.netease.arctic.log.LogDataJsonDeserialization;
import com.netease.arctic.log.LogDataJsonSerialization;
import com.netease.arctic.utils.map.RocksDBBackend;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.iceberg.Schema;

public class StateFactory {

  private final RocksDBBackend db;

  public StateFactory(String dbPath) {
    // todo optimize the rocksDB through rocksdb options
    this.db = RocksDBBackend.getOrCreateInstance(dbPath);
  }

  public RocksDBRecordState createRecordState(
      String columnFamilyName,
      long lruMaximumSize,
      LogDataJsonSerialization<RowData> keySerialization,
      BinaryRowDataSerializer valueSerializer,
      LogDataJsonDeserialization<RowData> valueDeserialization,
      Schema projectSchema) {
    db.addColumnFamily(columnFamilyName);
    return
        new RocksDBRecordState(
            db,
            columnFamilyName,
            lruMaximumSize,
            keySerialization,
            valueSerializer,
            valueDeserialization,
            projectSchema);
  }

  public RocksDBSetState createSetState(
      String columnFamilyName,
      long lruMaximumSize,
      LogDataJsonSerialization<RowData> keySerialization,
      LogDataJsonSerialization<RowData> elementSerialization,
      BinaryRowDataSerializer valueSerializer,
      LogDataJsonDeserialization<RowData> valueDeserialization
  ) {
    db.addColumnFamily(columnFamilyName);
    return new RocksDBSetState(
        db,
        columnFamilyName,
        lruMaximumSize,
        keySerialization,
        elementSerialization,
        valueSerializer,
        valueDeserialization);
  }
}
