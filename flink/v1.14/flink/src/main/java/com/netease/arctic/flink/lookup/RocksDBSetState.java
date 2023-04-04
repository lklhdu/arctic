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
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Guava cache structure: key -> list, the elements of this list are rocksdb keys.
 * RocksDB structure: element -> empty.
 */
public class RocksDBSetState extends RocksDBState<List<byte[]>> {

  protected LogDataJsonSerialization<RowData> keySerialization;

  private static final byte[] EMPTY = new byte[0];

  public RocksDBSetState(
      RocksDBBackend rocksDB,
      String columnFamilyName,
      long lruMaximumSize,
      LogDataJsonSerialization<RowData> keySerialization,
      LogDataJsonSerialization<RowData> elementSerialization,
      LogDataJsonSerialization<RowData> valueSerialization,
      LogDataJsonDeserialization<RowData> valueDeserialization) {
    super(
        rocksDB,
        columnFamilyName,
        lruMaximumSize,
        elementSerialization,
        valueSerialization, valueDeserialization);
    this.keySerialization = keySerialization;
  }

  /**
   * Retrieve the elements of the key.
   * <p>Fetch the Collection from guava cache,
   * if not present, fetch from rocksDB continuously, via prefix key scanning the rocksDB;
   * if present, just return the result.
   *
   * @return not null, but may be empty.
   */
  public List<byte[]> get(RowData key) throws IOException {
    final byte[] keyBytes = serializeKey(key);
    ByteArrayWrapper keyWrap = wrap(keyBytes);
    List<byte[]> result = guavaCache.getIfPresent(keyWrap);
    if (result == null) {
      // todo could be optimized, reduce this array copy.
      // Key [123, 34, 105, 100, 34, 58, 49, 125] -> {"id":1}, 125 byte is '}' always append the tail.
      byte[] seek = Arrays.copyOf(keyBytes, keyBytes.length - 1);

      try (RocksDBBackend.ValueIterator iterator =
               (RocksDBBackend.ValueIterator) rocksDB.values(columnFamilyName, seek)) {
        result = Lists.newArrayList();
        while (iterator.hasNext()) {
          byte[] targetKeyBytes = iterator.key();
          if (isPrefixKey(targetKeyBytes, seek)) {
            byte[] value = Arrays.copyOf(targetKeyBytes, targetKeyBytes.length);
            result.add(value);
          }
          iterator.next();
        }
        if (!result.isEmpty()) {
          guavaCache.put(keyWrap, result);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

    }
    return result;
  }

  private boolean isPrefixKey(byte[] targetKeyBytes, byte[] keyBytes) {
    for (int i = 0; i < keyBytes.length; i++) {
      if (targetKeyBytes[i] != keyBytes[i]) {
        return false;
      }
    }
    return true;
  }

  /**
   * Merge key and element into guava cache and rocksdb.
   */
  public void merge(RowData key, byte[] elementBytes) {
    final byte[] keyBytes = serializeKey(key);
    ByteArrayWrapper keyWrap = wrap(keyBytes);
    if (guavaCache.getIfPresent(keyWrap) != null) {
      guavaCache.invalidate(keyWrap);
    }
    rocksDB.put(columnFamilyName, elementBytes, EMPTY);
  }

  public void delete(RowData key, byte[] elementBytes) throws IOException {
    final byte[] keyBytes = serializeKey(key);
    ByteArrayWrapper keyWrap = wrap(keyBytes);
    if (guavaCache.getIfPresent(keyWrap) != null) {
      guavaCache.invalidate(keyWrap);
    }
    if (rocksDB.get(columnFamilyName, elementBytes) != null) {
      rocksDB.delete(columnFamilyName, elementBytes);
    }
  }

  protected byte[] serializeKey(RowData key) {
    LogRecordV1 logData = new LogRecordV1(key);
    return keySerialization.serializeRow(logData);
  }
}
