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
import com.netease.arctic.utils.map.RocksDBBackend;
import org.apache.flink.shaded.guava30.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheBuilder;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.rocksdb.ColumnFamilyHandle;

import java.io.IOException;

public abstract class RocksDBState<V> {
  protected RocksDBBackend rocksDB;

  protected Cache<ByteArrayWrapper, V> guavaCache;

  protected final String columnFamilyName;
  protected final ColumnFamilyHandle columnFamilyHandle;
  protected BinaryRowDataSerializerWrapper keySerializer;

  protected BinaryRowDataSerializerWrapper valueSerializer;
  protected long lruSize;

  public RocksDBState(
      RocksDBBackend rocksDB,
      String columnFamilyName,
      long lruMaximumSize,
      BinaryRowDataSerializerWrapper keySerializer,
      BinaryRowDataSerializerWrapper valueSerializer) {
    this.rocksDB = rocksDB;
    this.guavaCache = CacheBuilder.newBuilder().maximumSize(lruMaximumSize).build();
    this.columnFamilyName = columnFamilyName;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.columnFamilyHandle = rocksDB.getColumnFamilyHandle(columnFamilyName);
    this.lruSize = lruMaximumSize;
  }

  protected byte[] serializeKey(RowData key) throws IOException {
    return serializeKey(keySerializer, key);
  }

  protected byte[] serializeKey(
      BinaryRowDataSerializerWrapper keySerializer,
      RowData key) throws IOException {
    // key has a different RowKind would serialize different byte[], so unify the RowKind as INSERT.
    byte[] result;
    if (key.getRowKind() != RowKind.INSERT) {
      RowKind rowKind = key.getRowKind();
      key.setRowKind(RowKind.INSERT);
      result = keySerializer.serialize(key);
      key.setRowKind(rowKind);
      return result;
    }
    key.setRowKind(RowKind.INSERT);
    return keySerializer.serialize(key);
  }

  protected ByteArrayWrapper wrap(byte[] bytes) {
    return new ByteArrayWrapper(bytes, bytes.length);
  }

  public void close() {
    rocksDB.close();
    guavaCache.cleanUp();
  }
}
