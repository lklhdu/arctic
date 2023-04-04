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

import com.ibm.icu.util.ByteArrayWrapper;
import com.netease.arctic.flink.shuffle.LogRecordV1;
import com.netease.arctic.log.LogDataJsonDeserialization;
import com.netease.arctic.log.LogDataJsonSerialization;
import com.netease.arctic.utils.map.RocksDBBackend;
import org.apache.flink.shaded.guava30.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheBuilder;
import org.apache.flink.table.data.RowData;

public abstract class RocksDBState<V> {
  protected RocksDBBackend rocksDB;

  protected Cache<ByteArrayWrapper, V> guavaCache;

  protected final String columnFamilyName;
  protected LogDataJsonSerialization<RowData> keySerialization;

  protected LogDataJsonSerialization<RowData> valueSerialization;
  protected LogDataJsonDeserialization<RowData> valueDeserialization;

  public RocksDBState(
      RocksDBBackend rocksDB,
      String columnFamilyName,
      long lruMaximumSize,
      LogDataJsonSerialization<RowData> keySerialization,
      LogDataJsonSerialization<RowData> valueSerialization,
      LogDataJsonDeserialization<RowData> valueDeserialization) {
    this.rocksDB = rocksDB;
    this.guavaCache = CacheBuilder.newBuilder().maximumSize(lruMaximumSize).build();
    this.columnFamilyName = columnFamilyName;
    this.keySerialization = keySerialization;
    this.valueSerialization = valueSerialization;
    this.valueDeserialization = valueDeserialization;
  }

  protected byte[] serializeKey(RowData key) {
    LogRecordV1 logData = new LogRecordV1(key);
    return keySerialization.serializeRow(logData);
  }

  protected ByteArrayWrapper wrap(byte[] bytes) {
    return new ByteArrayWrapper(bytes, bytes.length);
  }

  public void close() {
    rocksDB.close();
    guavaCache.cleanUp();
  }
}
