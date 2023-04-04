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

package com.netease.arctic.flink.read;

import com.netease.arctic.ams.api.properties.TableFormat;
import com.netease.arctic.catalog.TableTestBase;
import com.netease.arctic.flink.read.hybrid.enumerator.ContinuousSplitPlannerImpl;
import com.netease.arctic.flink.read.hybrid.reader.RowDataReaderFunction;
import com.netease.arctic.flink.read.source.DataIterator;
import com.netease.arctic.flink.util.DataUtil;
import com.netease.arctic.flink.write.FlinkTaskWriterBaseTest;
import com.netease.arctic.table.ArcticTable;
import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.TaskWriter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@RunWith(value = Parameterized.class)
public class MixedIncrementalLoaderTest extends TableTestBase implements FlinkTaskWriterBaseTest {

  public MixedIncrementalLoaderTest(boolean partitionedTable) {
    super(TableFormat.MIXED_ICEBERG, true, partitionedTable);
  }

  @Parameterized.Parameters(name = "partitionedTable = {1}")
  public static Object[][] parameters() {
    // todo mix hive test
    return new Object[][]{
        {true},
        {false}};
  }

  @Before
  public void before() throws IOException {
    ArcticTable arcticTable = getArcticTable();
    TableSchema flinkPartialSchema = TableSchema.builder()
        .field("id", DataTypes.INT())
        .field("name", DataTypes.STRING())
        .field("ts", DataTypes.BIGINT())
        .field("op_time", DataTypes.TIMESTAMP())
        .build();
    RowType rowType = (RowType) flinkPartialSchema.toRowDataType().getLogicalType();

    List<RowData> expected = Lists.newArrayList(
        DataUtil.toRowData(1000011, "a", 1010L, LocalDateTime.parse("2022-06-18T10:10:11.0")),
        DataUtil.toRowData(1000012, "b", 1011L, LocalDateTime.parse("2022-06-18T10:10:11.0")),
        DataUtil.toRowData(1000013, "c", 1012L, LocalDateTime.parse("2022-06-18T10:10:11.0")),
        DataUtil.toRowData(1000014, "d", 1013L, LocalDateTime.parse("2022-06-21T10:10:11.0")),
        DataUtil.toRowData(1000015, "e", 1014L, LocalDateTime.parse("2022-06-21T10:10:11.0"))
    );
    for (RowData rowData : expected) {
      try (TaskWriter<RowData> taskWriter = createBaseTaskWriter(arcticTable, rowType)) {
        writeAndCommit(rowData, taskWriter, arcticTable);
      }
    }

    expected = Lists.newArrayList(
        DataUtil.toRowData(1000021, "a", 1020L, LocalDateTime.parse("2022-06-28T10:10:11.0")),
        DataUtil.toRowData(1000022, "b", 1021L, LocalDateTime.parse("2022-06-28T10:10:11.0")),
        DataUtil.toRowData(1000023, "c", 1022L, LocalDateTime.parse("2022-06-28T10:10:11.0")),
        DataUtil.toRowData(1000024, "d", 1023L, LocalDateTime.parse("2022-06-28T10:10:11.0")),
        DataUtil.toRowData(1000025, "e", 1024L, LocalDateTime.parse("2022-06-28T10:10:11.0"))
    );
    for (RowData rowData : expected) {
      try (TaskWriter<RowData> taskWriter = createTaskWriter(arcticTable, rowType)) {
        writeAndCommit(rowData, taskWriter, arcticTable);
      }
    }
  }

  @Test
  public void testIncrementalLoad() {
    ArcticTable arcticTable = getArcticTable();

    List<Expression> expressions =
        Lists.newArrayList(
            Expressions.greaterThan("id", 1000014),
            Expressions.greaterThan("op_time", "2022-06-20T10:10:11.0")
        );

    MixedIncrementalLoader<RowData> incrementalLoader =
        new MixedIncrementalLoader<>(
            new ContinuousSplitPlannerImpl(
                getTableLoader(getCatalogName(), getMetastoreUrl(), arcticTable)),
            new RowDataReaderFunction(
                new Configuration(),
                arcticTable.schema(),
                arcticTable.schema(),
                arcticTable.asKeyedTable().primaryKeySpec(),
                null,
                true,
                arcticTable.io()),
            expressions);

    List<RowData> actuals = new ArrayList<>();
    while (incrementalLoader.hasNext()) {
      DataIterator<RowData> iterator = incrementalLoader.next();
      while (iterator.hasNext()) {
        RowData rowData = iterator.next();
        System.out.println(rowData);
        actuals.add(rowData);
      }
    }
    Assert.assertEquals(6, actuals.size());
  }


  @Override
  public String getMetastoreUrl() {
    return getCatalogUrl();
  }

  @Override
  public String getCatalogName() {
    return getCatalog().name();
  }
}