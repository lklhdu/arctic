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

import com.netease.arctic.flink.read.MixedIncrementalLoader;
import com.netease.arctic.flink.read.hybrid.enumerator.MergeOnReadIncrementalPlanner;
import com.netease.arctic.flink.read.source.FlinkArcticMORDataReader;
import com.netease.arctic.flink.table.ArcticTableLoader;
import com.netease.arctic.table.ArcticTable;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.data.RowDataUtil;
import org.apache.iceberg.io.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.util.Preconditions.checkArgument;

public class ArcticLookupFunction extends TableFunction<RowData> {
  private static final Logger LOG = LoggerFactory.getLogger(ArcticLookupFunction.class);
  private ArcticTable arcticTable;
  private KVTable kvTable;
  private final List<String> joinKeys;
  private final long lruCacheSize;
  private final Schema projectSchema;
  private final List<Expression> filters;
  private final ArcticTableLoader loader;
  private boolean loading = false;
  private long nextLoadTime = Long.MIN_VALUE;
  private MixedIncrementalLoader<RowData> incrementalLoader;

  public ArcticLookupFunction(
      ArcticTable arcticTable,
      List<String> joinKeys,
      Schema projectSchema,
      long cacheMaxRows,
      List<Expression> filters,
      ArcticTableLoader tableLoader) {
    checkArgument(
        arcticTable.isKeyedTable(),
        String.format(
            "Only keyed arctic table support lookup join, this table [%s] is an unkeyed table.", arcticTable.name()));

    this.arcticTable = arcticTable;
    this.joinKeys = joinKeys;
    this.projectSchema = projectSchema;
    this.lruCacheSize = cacheMaxRows;
    this.filters = filters;
    this.loader = tableLoader;
  }

  @Override
  public void open(FunctionContext context) throws IOException {
    kvTable = KVTable.create(
        new StateFactory(generateRocksDBPath(context, arcticTable.name())),
        arcticTable.asKeyedTable().primaryKeySpec().fieldNames(),
        joinKeys,
        lruCacheSize,
        projectSchema);
    this.incrementalLoader =
        new MixedIncrementalLoader<>(
            new MergeOnReadIncrementalPlanner(loader),
            new FlinkArcticMORDataReader(
                arcticTable.io(),
                arcticTable.schema(),
                projectSchema,
                arcticTable.asKeyedTable().primaryKeySpec(),
                null,
                true,
                RowDataUtil::convertConstant,
                true
            ),
            filters
        );
    checkAndLoad();
  }

  public void eval(Object... values) {
    try {
      checkAndLoad();
      RowData lookupKey = GenericRowData.of(values);
      List<RowData> results = kvTable.get(lookupKey);
      results.forEach(this::collect);
    } catch (Exception e) {
      throw new FlinkRuntimeException(e);
    }
  }

  private synchronized void checkAndLoad() throws IOException {
    if (nextLoadTime > System.currentTimeMillis()) {
      return;
    }
    if (loading) {
      LOG.info("Mixed table incremental loader is running.");
      return;
    }
    // todo hard cod
    nextLoadTime = System.currentTimeMillis() + 1000 * 10;

    loading = true;
    while (incrementalLoader.hasNext()) {
      try (CloseableIterator<RowData> iterator = incrementalLoader.next()) {
        kvTable.upsert(iterator);
      }
    }
    loading = false;
  }

  @Override
  public void close() throws Exception {
    kvTable.close();
  }

  private String generateRocksDBPath(FunctionContext context, String tableName) {
    String tmpPath = getTmpDirectoryFromTMContainer(context);
    File db = new File(tmpPath, tableName + "-lookup-" + UUID.randomUUID());
    return db.toString();
  }

  private static String getTmpDirectoryFromTMContainer(FunctionContext context) {
    try {
      Field field = context.getClass().getDeclaredField("context");
      field.setAccessible(true);
      StreamingRuntimeContext runtimeContext = (StreamingRuntimeContext) field.get(context);
      String[] tmpDirectories =
          runtimeContext.getTaskManagerRuntimeInfo().getTmpDirectories();
      return tmpDirectories[ThreadLocalRandom.current().nextInt(tmpDirectories.length)];
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}
