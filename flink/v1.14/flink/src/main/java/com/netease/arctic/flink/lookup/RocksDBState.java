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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.shaded.guava30.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheBuilder;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.FlinkRuntimeException;
import org.rocksdb.ColumnFamilyHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public abstract class RocksDBState<V> {
  private static final Logger LOG = LoggerFactory.getLogger(RocksDBState.class);
  protected RocksDBBackend rocksDB;

  protected Cache<ByteArrayWrapper, V> guavaCache;

  protected final String columnFamilyName;
  protected final ColumnFamilyHandle columnFamilyHandle;
  protected BinaryRowDataSerializerWrapper keySerializer;

  protected BinaryRowDataSerializerWrapper valueSerializer;
  private ExecutorService writeRocksDBService;
  private final AtomicBoolean initialized = new AtomicBoolean(false);
  protected Queue<RocksDBRecord> rocksDBRecordQueue;

  protected long lruSize;
  private final int writeRocksDBThreadNum;
  private List<Future<?>> writeRocksDBThreadFutures;

  public RocksDBState(
      RocksDBBackend rocksDB,
      String columnFamilyName,
      long lruMaximumSize,
      BinaryRowDataSerializerWrapper keySerializer,
      BinaryRowDataSerializerWrapper valueSerializer,
      int writeRocksDBThreadNum) {
    this.rocksDB = rocksDB;
    this.guavaCache = CacheBuilder.newBuilder().maximumSize(lruMaximumSize).build();
    this.columnFamilyName = columnFamilyName;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.columnFamilyHandle = rocksDB.getColumnFamilyHandle(columnFamilyName);
    this.lruSize = lruMaximumSize;
    this.writeRocksDBThreadNum = writeRocksDBThreadNum;
  }

  public void open() {
    writeRocksDBService = Executors.newFixedThreadPool(writeRocksDBThreadNum);
    rocksDBRecordQueue = new ConcurrentLinkedQueue<>();
    writeRocksDBThreadFutures =
        IntStream.range(0, writeRocksDBThreadNum).mapToObj(value ->
                writeRocksDBService.submit(new WriteRocksDBTask()))
            .collect(Collectors.toList());
    LOG.info("create {} writing rocksDB threads. ", writeRocksDBThreadNum);
  }


  @VisibleForTesting
  public byte[] serializeKey(RowData key) throws IOException {
    return serializeKey(keySerializer, key);
  }

  @VisibleForTesting
  public byte[] serializeKey(
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

  public abstract void batchWrite(RowData key, RowData value) throws IOException;

  public abstract void flush();

  public void waitWriteRocksDBDone() {
    long every5SecondsPrint = Long.MIN_VALUE;

    while (true) {
      if (rocksDBRecordQueue.isEmpty()) {
        initialized.set(true);
        break;
      } else if (every5SecondsPrint < System.currentTimeMillis()) {
        LOG.info("Currently rocksDB queue size is {}.", rocksDBRecordQueue.size());
        every5SecondsPrint = System.currentTimeMillis() + 5000;
      }
    }
    // Wait for all threads to finish
    for (Future<?> future : writeRocksDBThreadFutures) {
      try {
        // wait for the task to complete, with a timeout of 5 seconds
        future.get(5, TimeUnit.SECONDS);
      } catch (TimeoutException e) {
        // task took too long, interrupt the thread and terminate the task
        future.cancel(true);
      } catch (InterruptedException | ExecutionException e) {
        // handle other exceptions
        throw new FlinkRuntimeException(e);
      }
    }
  }

  public boolean initialized() {
    return initialized.get();
  }

  public void close() {
    rocksDB.close();
    guavaCache.cleanUp();
    if (writeRocksDBService != null) {
      writeRocksDBService.shutdown();
      writeRocksDBService = null;
    }
    if (rocksDBRecordQueue != null) {
      rocksDBRecordQueue.clear();
      rocksDBRecordQueue = null;
    }
  }

  /**
   * A Runnable task that writes the records{@link RocksDBRecord} to RocksDB.
   */
  class WriteRocksDBTask implements Runnable {

    @Override
    public void run() {
      while (!initialized.get()) {
        RocksDBRecord record = rocksDBRecordQueue.poll();
        if (record != null) {
          switch (record.opType()) {
            case PUT_BYTES:
              rocksDB.put(columnFamilyHandle, record.keyBytes(), record.valueBytes());
              break;
            case DELETE_BYTES:
              rocksDB.delete(columnFamilyName, record.keyBytes());
              break;
            default:
              throw new IllegalArgumentException(String.format("Not support this OpType %s", record.opType()));
          }

          // Speed up the initialization process of Lookup Join Function
//          ByteArrayWrapper key = wrap(record.keyBytes());
//          if (guavaCache.getIfPresent(wrap(record.keyBytes())) != null) {
//            guavaCache.put(key, record.valueBytes());
//          }
        }
      }
    }
  }
}
