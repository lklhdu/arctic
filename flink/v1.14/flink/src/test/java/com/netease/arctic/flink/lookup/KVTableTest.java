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


import com.netease.arctic.utils.SchemaUtil;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class KVTableTest {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  @Rule
  public TestName name = new TestName();

  private final Schema arcticSchema = new Schema(
      Types.NestedField.required(1, "id", Types.IntegerType.get()),
      Types.NestedField.required(2, "grade", Types.StringType.get()),
      Types.NestedField.required(3, "num", Types.IntegerType.get()));

  private String dbPath;

  @Before
  public void before() throws IOException {
    dbPath = temp.newFolder().getPath();
  }

  @Test
  public void testRowDataSerializer() throws IOException {
    BinaryRowDataSerializer binaryRowDataSerializer = new BinaryRowDataSerializer(3);

    GenericRowData genericRowData = (GenericRowData) row(1, "2", 3);
    RowType rowType = FlinkSchemaUtil.convert(arcticSchema);
    RowDataSerializer rowDataSerializer = new RowDataSerializer(rowType);
    BinaryRowData record = rowDataSerializer.toBinaryRow(genericRowData);

    DataOutputSerializer view = new DataOutputSerializer(32);
    binaryRowDataSerializer.serialize(record, view);
    System.out.println(Arrays.toString(view.getCopyOfBuffer()));

    BinaryRowData desRowData = binaryRowDataSerializer.deserialize(new DataInputDeserializer(view.getCopyOfBuffer()));
    Assert.assertNotNull(desRowData);
    Assert.assertEquals(record.getInt(0), desRowData.getInt(0));
    Assert.assertEquals(record.getInt(1), desRowData.getInt(1));
    Assert.assertEquals(record.getInt(2), desRowData.getInt(2));

    // test join key rowData
    binaryRowDataSerializer = new BinaryRowDataSerializer(2);
    List<String> keys = Lists.newArrayList("id", "grade");
    Schema keySchema = SchemaUtil.convertFieldsToSchema(arcticSchema, keys);
    rowType = FlinkSchemaUtil.convert(keySchema);
    rowDataSerializer = new RowDataSerializer(rowType);
    KeyRowData keyRowData = new KeyRowData(new int[]{0, 1}, row(2, "3", 4));
    KeyRowData keyRowData1 = new KeyRowData(new int[]{0, 1}, row(2, "3", 4));

    BinaryRowData binaryRowData = rowDataSerializer.toBinaryRow(keyRowData);
    view.clear();
    binaryRowDataSerializer.serialize(binaryRowData, view);
    byte[] rowBytes = view.getCopyOfBuffer();

    BinaryRowData binaryRowData1 = rowDataSerializer.toBinaryRow(keyRowData1);
    view.clear();
    binaryRowDataSerializer.serialize(binaryRowData1, view);
    byte[] rowBytes1 = view.getCopyOfBuffer();
    Assert.assertArrayEquals(rowBytes1, rowBytes);

  }

  @Test
  public void testUniqueKeyTable() throws IOException {
    List<String> primaryKeys = Lists.newArrayList("id", "grade");
    List<String> joinKeys = Lists.newArrayList("id", "grade");
    try (UniqueIndexTable uniqueIndexTable =
             (UniqueIndexTable) KVTable.create(
                 new StateFactory(dbPath),
                 primaryKeys,
                 joinKeys,
                 2,
                 arcticSchema
             )) {
      RowData expected = row(1, "2", 3);
      upsertAndAssert(uniqueIndexTable, upsertStream(expected), row(1, "2"), expected);

      expected = row(1, "2", 4);
      upsertAndAssert(uniqueIndexTable, upsertStream(expected), row(1, "2"), expected);

      upsertAndAssert(
          uniqueIndexTable,
          upsertStream(
              row(RowKind.INSERT, 2, "3", 4),
              row(RowKind.DELETE, 2, "3", 4),
              row(RowKind.UPDATE_BEFORE, 1, "2", 4),
              row(RowKind.UPDATE_AFTER, 1, "2", 6)),
          row(2, "3"),
          null,
          row(1, "2"),
          row(1, "2", 6));
    }
  }

  @Test
  public void testSecondaryIndexTable() throws IOException {
    dbPath = temp.newFolder().getPath();
    List<String> primaryKeys = Lists.newArrayList("id", "grade");
    List<String> joinKeys = Lists.newArrayList("id");

    try (SecondaryIndexTable secondaryIndexTable =
             (SecondaryIndexTable) KVTable.create(
                 new StateFactory(dbPath),
                 primaryKeys,
                 joinKeys,
                 2,
                 arcticSchema
             )) {
      RowData expected = row(1, "2", 3);
      upsertAndAssert(secondaryIndexTable, upsertStream(expected), row(1), expected);
      upsertAndAssert(secondaryIndexTable, null, row(1), expected);

      expected = row(1, "2", 4);
      upsertAndAssert(secondaryIndexTable, upsertStream(expected), row(1), expected);

      upsertAndAssert(
          secondaryIndexTable,
          upsertStream(
              row(RowKind.INSERT, 2, "3", 4),
              row(RowKind.DELETE, 2, "3", 4),
              row(RowKind.UPDATE_BEFORE, 1, "2", 4),
              row(RowKind.UPDATE_AFTER, 1, "2", 6)),
          row(2),
          null,
          row(1),
          row(1, "2", 6)
      );
    }
  }

  private void upsertAndAssert(
      KVTable table, Iterator<RowData> upsertStream, RowData... rows) throws IOException {
    if (upsertStream != null) {
      table.upsert(upsertStream);
    }
    for (int i = 0; i < rows.length; i = i + 2) {
      RowData key = rows[i], expected = rows[i + 1];
      List<RowData> values = table.get(key);
      if (expected == null) {
        Assert.assertEquals(0, values.size());
      } else {
        if (values.get(0) instanceof BinaryRowData) {
          BinaryRowData binaryRowData = (BinaryRowData) values.get(0);
          for (int j = 0; j < binaryRowData.getArity(); j++) {
            switch (j) {
              case 0:
              case 2:
                Assert.assertEquals(expected.getInt(j), binaryRowData.getInt(j));
                break;
              case 1:
                Assert.assertEquals(expected.getString(j), binaryRowData.getString(j));
                break;
            }
          }
        }
      }
    }
  }

  RowData row(RowKind rowKind, Object... objects) {
    return GenericRowData.ofKind(rowKind, wrapStringData(objects));
  }

  RowData row(Object... objects) {
    return GenericRowData.of(wrapStringData(objects));
  }

  Object[] wrapStringData(Object... objects) {
    for (int i = 0; i < objects.length; i++) {
      if (objects[i] instanceof String) {
        objects[i] = StringData.fromString(objects[i].toString());
      }
    }
    return objects;
  }

  Iterator<RowData> upsertStream(RowData... rows) {
    return Lists.newArrayList(rows).iterator();
  }
}