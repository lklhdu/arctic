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

package com.netease.arctic.spark.hive;


import com.netease.arctic.spark.SparkTestBase;
import com.netease.arctic.table.ArcticTable;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.util.StructLikeMap;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestKeyedHiveInsertOverwriteStatic extends SparkTestBase {
  private final String database = "db";
  private final String table = "testA";

  private String contextOverwriteMode;

  @Before
  public void before() throws IOException {
    contextOverwriteMode = spark.conf().get("spark.sql.sources.partitionOverwriteMode");
    System.out.println("spark.sql.sources.partitionOverwriteMode = " + contextOverwriteMode);
    sql("set spark.sql.sources.partitionOverwriteMode = {0}", "STATIC");

    sql("use " + catalogNameHive);
    sql("create database if not exists {0}", database);
    sql("create table {0}.{1} ( \n" +
        " id int , \n" +
        " data string , \n " +
        " dt string , \n" +
        " primary key (id) \n" +
        ") using arctic \n" +
        " partitioned by ( dt ) \n", database, table);


    sql("insert overwrite {0}.{1} values \n" +
        "(1, ''aaa'',  ''2021-1-1''), \n " +
        "(2, ''bbb'',  ''2021-1-2''), \n " +
        "(3, ''ccc'',  ''2021-1-3'') \n ", database, table);

  }

  @After
  public void after() {
    sql("use " + catalogNameHive);
    sql("drop table {0}.{1}", database, table);
    sql("set spark.sql.sources.partitionOverwriteMode = {0}", contextOverwriteMode);
  }

  @Test
  public void testInsertOverwriteAllPartitionByValue() throws TException {
    // insert overwrite by values, no partition expr
    sql("insert overwrite {0}.{1} values \n" +
        "(4, ''aaa'',  ''2021-1-1''), \n " +
        "(5, ''bbb'',  ''2021-1-2''), \n " +
        "(6, ''ccc'',  ''2021-1-2'') \n ", database, table);

    rows = sql("select * from {0}.{1}", database, table);
    Assert.assertEquals(3, rows.size());
    assertContainIdSet(rows, 0, 4, 5, 6);

    List<Partition> partitions = hms.getClient().listPartitions(
        database,
        table,
        (short) -1);
    Assert.assertEquals(2, partitions.size());

    sql("use spark_catalog");
    rows = sql("select * from {0}.{1}", database, table);
    Assert.assertEquals(3, rows.size());
    assertContainIdSet(rows, 0, 4, 5, 6);
  }

  @Test
  public void testInsertOverwriteSomePartitionByValue() throws TException {
    sql("insert overwrite {0}.{1} \n" +
        "partition( dt = ''2021-1-1'')  values \n" +
        "(4, ''aaa''), \n " +
        "(5, ''bbb''), \n " +
        "(6, ''ccc'') \n ", database, table);

    rows = sql("select * from {0}.{1} order by id", database, table);
    Assert.assertEquals(5, rows.size());
    assertContainIdSet(rows, 0, 4, 5, 6, 2, 3);

    List<Partition> partitions = hms.getClient().listPartitions(
        database,
        table,
        (short) -1);
    Assert.assertEquals(3, partitions.size());

    sql("use spark_catalog");
    rows = sql("select * from {0}.{1} order by id", database, table);
    Assert.assertEquals(5, rows.size());
    assertContainIdSet(rows, 0, 4, 5, 6, 2, 3);
  }

  @Test
  public void testInsertOverwriteNonePartition() throws TException {
    sql("create table {0}.{1}( \n" +
        " id int, \n" +
        " name string, \n" +
        " data string, primary key(id))\n" +
        " using arctic partitioned by (data) ", database, "testPks");
    sql("insert overwrite {0}.{1} \n" +
        "partition( dt = ''2021-1-1'')  select id, name from {0}.{2}", database, table, "testPks");

    rows = sql("select * from {0}.{1} order by id", database, table);
    Assert.assertEquals(2, rows.size());
    List<Partition> partitions = hms.getClient().listPartitions(
        database,
        table,
        (short) -1);
    Assert.assertEquals(2, partitions.size());
    ArcticTable t = loadTable(catalogNameHive, database, table);
    StructLikeMap<Map<String, String>> partitionProperty = t.asKeyedTable().baseTable().partitionProperty();
    Map<String, StructLike> icebergPartitionMap = new HashMap<>();
    for (StructLike structLike : partitionProperty.keySet()) {
      System.out.println(structLike);
      icebergPartitionMap.put(t.spec().partitionToPath(structLike), structLike);
    }
    Map<String, String> propertiesMap = partitionProperty.get(icebergPartitionMap.get("dt=2021-1-1"));
    Assert.assertFalse(propertiesMap.containsKey("hive-location"));
    sql("drop table if exists {0}.{1}", database, "testPks");
  }
}
