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

import com.netease.arctic.utils.map.RocksDBBackend;
import org.apache.flink.configuration.Configuration;
import org.rocksdb.ColumnFamilyOptions;

import static com.netease.arctic.flink.table.descriptors.ArcticValidator.ROCKSDB_WRITING_THREADS;

public class StateFactory {

  private final RocksDBBackend db;

  public StateFactory(String dbPath) {
    // todo optimize the rocksDB through rocksdb options
    this.db = RocksDBBackend.getOrCreateInstance(dbPath);
  }

  public RocksDBRecordState createRecordState(
      String columnFamilyName,
      long lruMaximumSize,
      BinaryRowDataSerializerWrapper keySerializer,
      BinaryRowDataSerializerWrapper valueSerializer,
      Configuration config) {
    ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
    // todo lruMaximumSize 10003 == auto compaction false
    if (lruMaximumSize == 10003) {
      columnFamilyOptions.disableAutoCompactions();
    }
    db.addColumnFamily(columnFamilyName, columnFamilyOptions);
    return
        new RocksDBRecordState(
            db,
            columnFamilyName,
            lruMaximumSize,
            keySerializer,
            valueSerializer,
            config.getInteger(ROCKSDB_WRITING_THREADS));
  }

  public RocksDBSetState createSetState(
      String columnFamilyName,
      long lruMaximumSize,
      BinaryRowDataSerializerWrapper keySerialization,
      BinaryRowDataSerializerWrapper elementSerialization,
      BinaryRowDataSerializerWrapper valueSerializer,
      Configuration config) {
    db.addColumnFamily(columnFamilyName);
    return new RocksDBSetState(
        db,
        columnFamilyName,
        lruMaximumSize,
        keySerialization,
        elementSerialization,
        valueSerializer,
        config.getInteger(ROCKSDB_WRITING_THREADS));
  }
}
