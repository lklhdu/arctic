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

package com.netease.arctic.ams.server.service;

import com.netease.arctic.ams.api.TableIdentifier;
import com.netease.arctic.ams.server.AmsTestBase;
import com.netease.arctic.ams.server.ArcticMetaStore;
import com.netease.arctic.ams.server.model.DDLInfo;
import com.netease.arctic.ams.server.model.TableMetadata;
import com.netease.arctic.ams.server.service.impl.CatalogMetadataService;
import com.netease.arctic.ams.server.service.impl.DDLTracerService;
import com.netease.arctic.ams.server.utils.JDBCSqlSessionFactoryProvider;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.table.PrimaryKeySpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;

import static com.netease.arctic.ams.server.AmsTestBase.AMS_TEST_CATALOG_NAME;
import static com.netease.arctic.ams.server.AmsTestBase.AMS_TEST_DB_NAME;

@PowerMockIgnore({"javax.management.*"})
@PrepareForTest({
    ServiceContainer.class,
    JDBCSqlSessionFactoryProvider.class,
    DDLTracerService.class,
    ArcticMetaStore.class,
    CatalogMetadataService.class
})
public class TestDDLTracerService {

  private static List<String> commitSqls = new ArrayList<>();
  private static final String commitTableName = "tblDDlCommit";
  private static String fullCommitTableName = AMS_TEST_CATALOG_NAME + "." + AMS_TEST_DB_NAME + "." + commitTableName;
  private static ArcticTable testCommitTable;
  private static TableIdentifier testCommotIdentifier;

  private static List<String> syncSqls = new ArrayList<>();
  private static final String syncTableName = "syncTblDDlCommit";
  private static String syncFullTableName = AMS_TEST_CATALOG_NAME + "." + AMS_TEST_DB_NAME + "." + syncTableName;
  private static ArcticTable testSyncTable;
  private static TableIdentifier testSyncIdentifier;
  private static DDLTracerService service = ServiceContainer.getDdlTracerService();

  private static Schema schema;

  @BeforeClass
  public static void before() {

    schema = new Schema(
        Types.NestedField.required(1, "beforeCol", Types.IntegerType.get()),
        Types.NestedField.required(2, "dropCol", Types.IntegerType.get()),
        Types.NestedField.required(3, "alterCol", Types.FloatType.get()),
        Types.NestedField.required(4, "oldCol", Types.TimestampType.withoutZone()),
        Types.NestedField.required(5, "afterCol", Types.IntegerType.get()),
        Types.NestedField.required(6, "firstCol", Types.IntegerType.get()),
        Types.NestedField.required(7, "id", Types.IntegerType.get())
    );
   PrimaryKeySpec PRIMARY_KEY_SPEC = PrimaryKeySpec.builderFor(schema)
        .addColumn("id").build();

    testCommotIdentifier = new TableIdentifier();
    testCommotIdentifier.catalog = AMS_TEST_CATALOG_NAME;
    testCommotIdentifier.database = AMS_TEST_DB_NAME;
    testCommotIdentifier.tableName = commitTableName;
    testCommitTable = AmsTestBase.catalog.newTableBuilder(
        com.netease.arctic.table.TableIdentifier.of(testCommotIdentifier),
        schema).withPrimaryKeySpec(PRIMARY_KEY_SPEC).create();

    testSyncIdentifier = new TableIdentifier();
    testSyncIdentifier.catalog = AMS_TEST_CATALOG_NAME;
    testSyncIdentifier.database = AMS_TEST_DB_NAME;
    testSyncIdentifier.tableName = syncTableName;
    testSyncTable = AmsTestBase.catalog.newTableBuilder(
        com.netease.arctic.table.TableIdentifier.of(testSyncIdentifier),
        schema).withPrimaryKeySpec(PRIMARY_KEY_SPEC).create();
  }

  @Test
  public void testCommit() {
    addColumn(testCommitTable, fullCommitTableName, commitSqls);
    dropColumn(testCommitTable, fullCommitTableName, commitSqls);
    alterColumn(testCommitTable, fullCommitTableName, commitSqls);
    renameColumn(testCommitTable, fullCommitTableName, commitSqls);
    moveAfterColumn(testCommitTable, fullCommitTableName, commitSqls);
    moveFirstColumn(testCommitTable, fullCommitTableName, commitSqls);
    List<DDLInfo> ddlInfos = service.getDDL(testCommotIdentifier);
    Assert.assertEquals(commitSqls.size(), ddlInfos.size());
    for (DDLInfo d : ddlInfos) {
      String sql = d.getDdl().replace(";\\n", "").trim();
      Assert.assertTrue(commitSqls.contains(sql));
    }
  }

  @Test
  public void testSyncAddDDl() {
    service.dropTableData(testSyncIdentifier);
    Schema schema = testSyncTable.schema();
    DDLTracerService.DDLSyncTask task = new DDLTracerService.DDLSyncTask();
    addColumn(testSyncTable, syncFullTableName, syncSqls);
    syncSqls.forEach(System.out::println);
    Assert.assertTrue(syncSqls.contains(task.compareSchema(syncFullTableName, schema, testSyncTable.schema())
        .replace(";\\n", "")
        .trim()));
  }

  @Test
  public void testSyncDropDDl() {
    service.dropTableData(testSyncIdentifier);
    Schema schema = testSyncTable.schema();
    DDLTracerService.DDLSyncTask task = new DDLTracerService.DDLSyncTask();
    dropColumn(testSyncTable, syncFullTableName, syncSqls);
    syncSqls.forEach(System.out::println);
    Assert.assertTrue(syncSqls.contains(task.compareSchema(syncFullTableName, schema, testSyncTable.schema())
        .replace(";\\n", "")
        .trim()));
  }

  @Test
  public void testSyncAlterDDl() {
    service.dropTableData(testSyncIdentifier);
    Schema schema = testSyncTable.schema();
    DDLTracerService.DDLSyncTask task = new DDLTracerService.DDLSyncTask();
    alterColumn(testSyncTable, syncFullTableName, syncSqls);
    syncSqls.forEach(System.out::println);
    Assert.assertTrue(syncSqls.contains(task.compareSchema(syncFullTableName, schema, testSyncTable.schema())
        .replace(";\\n", "")
        .trim()));
  }

  @Test
  public void testSyncRenameDDl() {
    service.dropTableData(testSyncIdentifier);
    Schema schema = testSyncTable.schema();
    DDLTracerService.DDLSyncTask task = new DDLTracerService.DDLSyncTask();
    renameColumn(testSyncTable, syncFullTableName, syncSqls);
    syncSqls.forEach(System.out::println);
    Assert.assertTrue(syncSqls.contains(task.compareSchema(syncFullTableName, schema, testSyncTable.schema())
        .replace(";\\n", "")
        .trim()));
  }

  @Test
  public void testSyncMoveAfterDDl() {
    service.dropTableData(testSyncIdentifier);
    Schema schema = testSyncTable.schema();
    DDLTracerService.DDLSyncTask task = new DDLTracerService.DDLSyncTask();
    moveAfterColumn(testSyncTable, syncFullTableName, syncSqls);
    syncSqls.forEach(System.out::println);
    System.out.println(task.compareSchema(syncFullTableName, schema, testSyncTable.schema())
        .replace(";\\n", "")
        .trim());
    Assert.assertTrue(syncSqls.contains(task.compareSchema(syncFullTableName, schema, testSyncTable.schema())
        .replace(";\\n", "")
        .trim()));
  }

  @Test
  public void testSyncMoveFirstDDl() {
    service.dropTableData(testSyncIdentifier);
    Schema schema = testSyncTable.schema();
    DDLTracerService.DDLSyncTask task = new DDLTracerService.DDLSyncTask();
    moveFirstColumn(testSyncTable, syncFullTableName, syncSqls);
    syncSqls.forEach(System.out::println);
    System.out.println(task.compareSchema(syncFullTableName, schema, testSyncTable.schema())
        .replace(";\\n", "")
        .trim());
    Assert.assertTrue(syncSqls.contains(task.compareSchema(syncFullTableName, schema, testSyncTable.schema())
        .replace(";\\n", "")
        .trim()));
  }

  @Test
  public void testProperties() {
    DDLTracerService.DDLSyncTask task = new DDLTracerService.DDLSyncTask();

    //init
    TableMetadata tableMetadata = new TableMetadata();
    Map<String, String> properties = new HashMap<>();
    properties.put("key1", "val1");
    properties.put("meta_store_site", "val3");
    testSyncTable.asKeyedTable().updateProperties().set("key1", "val1").commit();
    testSyncTable.asKeyedTable().updateProperties().set("meta_store_site", "val3").commit();
    service.dropTableData(testSyncIdentifier);
    properties.putAll(testSyncTable.properties());
    tableMetadata.setProperties(properties);

    testSyncTable.asKeyedTable().updateProperties().set("key2", "val2").commit();
    testSyncTable.asKeyedTable().updateProperties().remove("key1").commit();

    //test commit
    List<DDLInfo> commitDdlInfos = ServiceContainer.getDdlTracerService().getDDL(testSyncIdentifier);
    List<String> sqls = new ArrayList<>();
    sqls.add("ALTER TABLE " + syncFullTableName + "  SET TBLPROPERTIES ('key2'='val2')");
    sqls.add("ALTER TABLE " + syncFullTableName + "  UNSET TBLPROPERTIES ('key1')");
    Assert.assertEquals(sqls.size(), commitDdlInfos.size());
    commitDdlInfos.stream()
        .filter(e -> e.getDdlType().equals(DDLTracerService.DDLType.UPDATE_PROPERTIES.name()))
        .forEach(e -> Assert.assertTrue(sqls.contains(e.getDdl())));

    service.dropTableData(testSyncIdentifier);

    //test sync
    task.syncProperties(tableMetadata, testSyncTable);
    List<DDLInfo> syncDdlInfos = ServiceContainer.getDdlTracerService().getDDL(testSyncIdentifier);
    Assert.assertEquals(sqls.size(), syncDdlInfos.size());
    syncDdlInfos.stream()
        .filter(e -> e.getDdlType().equals(DDLTracerService.DDLType.UPDATE_PROPERTIES.name()))
        .forEach(e -> Assert.assertTrue(sqls.contains(e.getDdl())));
  }

  private void addColumn(ArcticTable table, String tableName, List<String> sqls) {
    table.asKeyedTable().updateSchema().addColumn("addCol", Types.IntegerType.get(), "addCol doc").commit();
    sqls.add("ALTER TABLE " + tableName + "  ADD COLUMN addCol int  COMMENT 'addCol doc'");
  }

  private void dropColumn(ArcticTable table, String tableName, List<String> sqls) {
    table.asKeyedTable().updateSchema().deleteColumn("dropCol").commit();
    sqls.add("ALTER TABLE " + tableName + "  DROP COLUMN dropCol");
  }

  private void alterColumn(ArcticTable table, String tableName, List<String> sqls) {
    table.asKeyedTable().updateSchema().updateColumn("alterCol", Types.DoubleType.get()).commit();
    sqls.add("ALTER TABLE " + tableName + "  ALTER COLUMN alterCol  TYPE double");
  }

  private void renameColumn(ArcticTable table, String tableName, List<String> sqls) {
    table.asKeyedTable().updateSchema().renameColumn("oldCol", "newCol").commit();
    sqls.add("ALTER TABLE " + tableName + "  RENAME COLUMN oldCol TO newCol");
  }

  private void moveAfterColumn(ArcticTable table, String tableName, List<String> sqls) {
    table.asKeyedTable().updateSchema().moveAfter("afterCol", "beforeCol").commit();
    sqls.add("ALTER TABLE " + tableName + "  ALTER COLUMN afterCol AFTER beforeCol");
  }

  private void moveFirstColumn(ArcticTable table, String tableName, List<String> sqls) {
    table.asKeyedTable().updateSchema().moveFirst("firstCol").commit();
    sqls.add("ALTER TABLE " + tableName + "  ALTER COLUMN firstCol FIRST");
  }
}
