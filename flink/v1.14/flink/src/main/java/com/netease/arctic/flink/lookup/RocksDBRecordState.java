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
import com.netease.arctic.log.LogDataJsonDeserialization;
import com.netease.arctic.log.LogDataJsonSerialization;
import com.netease.arctic.utils.map.RocksDBBackend;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.FlinkSchemaUtil;

import java.io.IOException;
import java.util.Optional;

public class RocksDBRecordState extends RocksDBState<byte[]> {

  private Schema projectSchema;
  private RowDataSerializer rowDataSerializer;
  private DataOutputSerializer outputView;
  private DataInputDeserializer inputView;

  public RocksDBRecordState(
      RocksDBBackend rocksDB,
      String columnFamilyName,
      long lruMaximumSize,
      LogDataJsonSerialization<RowData> keySerialization,
      BinaryRowDataSerializer valueSerializer,
      LogDataJsonDeserialization<RowData> valueDeserialization,
      Schema projectSchema) {
    super(rocksDB, columnFamilyName, lruMaximumSize, keySerialization, valueSerializer, valueDeserialization);
    this.projectSchema = projectSchema;
  }

  /**
   * Retrieve the RowData from guava cache firstly, if value is null, fetch the value from the rocksDB.
   *
   * @param key try to find the record via this key.
   * @throws IOException if serialize the RowData variable <code>key</code> failed.
   */
  public Optional<RowData> get(RowData key) throws IOException {
    byte[] keyBytes = serializeKey(key);
    return get(keyBytes);
  }

  public Optional<RowData> get(byte[] keyBytes) throws IOException {
    ByteArrayWrapper key = wrap(keyBytes);
    byte[] recordBytes = guavaCache.getIfPresent(key);
    if (recordBytes == null) {
      recordBytes = rocksDB.get(columnFamilyName, key.bytes);
      if (recordBytes != null) {
        guavaCache.put(key, recordBytes);
      }
    }
    return Optional.ofNullable(deserializeValue(recordBytes));
  }

  public void put(RowData key, RowData value) throws IOException {
    byte[] keyBytes = serializeKey(key);
    put(keyBytes, value);
  }

  public void put(byte[] keyBytes, RowData value) throws IOException {
    Preconditions.checkNotNull(value);

    byte[] valueBytes = serializeValue(value);
    rocksDB.put(columnFamilyName, keyBytes, valueBytes);
    guavaCache.put(wrap(keyBytes), valueBytes);
  }

  public void delete(RowData key) throws IOException {
    byte[] keyBytes = serializeKey(key);
    delete(keyBytes);
  }

  public void delete(byte[] keyBytes) {
    if (contain(wrap(keyBytes))) {
      rocksDB.delete(columnFamilyName, keyBytes);
      guavaCache.invalidate(wrap(keyBytes));
    }
  }

  private boolean contain(ByteArrayWrapper byteArrayWrapper) {
    byte[] recordBytes = guavaCache.getIfPresent(byteArrayWrapper);
    if (recordBytes == null) {
      recordBytes = rocksDB.get(columnFamilyName, byteArrayWrapper.bytes);
    }
    return recordBytes != null;
  }

  private byte[] serializeValue(RowData value) throws IOException {
    if (rowDataSerializer == null) {
      RowType rowType = FlinkSchemaUtil.convert(projectSchema);
      rowDataSerializer = new RowDataSerializer(rowType);
    }
    BinaryRowData record = rowDataSerializer.toBinaryRow(value);
    if (outputView == null) {
      outputView = new DataOutputSerializer(32);
    }
    outputView.clear();
    valueSerializer.serialize(record, outputView);

    return outputView.getCopyOfBuffer();
  }

  private RowData deserializeValue(byte[] recordBytes) throws IOException {
    if (recordBytes == null) {
      return null;
    }
    if (inputView == null) {
      inputView = new DataInputDeserializer();
    }
    inputView.setBuffer(recordBytes);
    return valueSerializer.deserialize(inputView);
  }
}
