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

package org.apache.iceberg.flink.source;

import org.apache.amoro.shade.guava32.com.google.common.base.Preconditions;
import org.apache.amoro.shade.guava32.com.google.common.collect.ImmutableList;
import org.apache.amoro.shade.guava32.com.google.common.collect.Lists;
import org.apache.amoro.table.TableMetaStore;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.FlinkFilters;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.assigner.SplitAssignerType;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Flink Iceberg table source. */
@Internal
public class IcebergTableSource
    implements ScanTableSource,
        SupportsProjectionPushDown,
        SupportsFilterPushDown,
        SupportsLimitPushDown {

  private int[] projectedFields;
  protected Long limit;
  protected List<Expression> filters;

  protected final TableLoader loader;
  private final TableSchema schema;
  protected final Map<String, String> properties;
  private final boolean isLimitPushDown;
  protected final ReadableConfig readableConfig;
  private final TableMetaStore tableMetaStore;

  private IcebergTableSource(IcebergTableSource toCopy) {
    this.loader = toCopy.loader;
    this.schema = toCopy.schema;
    this.properties = toCopy.properties;
    this.projectedFields = toCopy.projectedFields;
    this.isLimitPushDown = toCopy.isLimitPushDown;
    this.limit = toCopy.limit;
    this.filters = toCopy.filters;
    this.readableConfig = toCopy.readableConfig;
    this.tableMetaStore = toCopy.tableMetaStore;
  }

  public IcebergTableSource(
      TableLoader loader,
      TableSchema schema,
      Map<String, String> properties,
      ReadableConfig readableConfig,
      TableMetaStore tableMetaStore) {
    this(
        loader,
        schema,
        properties,
        null,
        false,
        null,
        ImmutableList.of(),
        readableConfig,
        tableMetaStore);
  }

  private IcebergTableSource(
      TableLoader loader,
      TableSchema schema,
      Map<String, String> properties,
      int[] projectedFields,
      boolean isLimitPushDown,
      Long limit,
      List<Expression> filters,
      ReadableConfig readableConfig,
      TableMetaStore tableMetaStore) {
    this.loader = loader;
    this.schema = schema;
    this.properties = properties;
    this.projectedFields = projectedFields;
    this.isLimitPushDown = isLimitPushDown;
    this.limit = limit;
    this.filters = filters;
    this.readableConfig = readableConfig;
    this.tableMetaStore = tableMetaStore;
  }

  @Override
  public void applyProjection(int[][] projectFields) {
    this.projectedFields = new int[projectFields.length];
    for (int i = 0; i < projectFields.length; i++) {
      Preconditions.checkArgument(
          projectFields[i].length == 1, "Don't support nested projection in iceberg source now.");
      this.projectedFields[i] = projectFields[i][0];
    }
  }

  protected DataStream<RowData> createDataStream(StreamExecutionEnvironment execEnv) {
    return FlinkSource.forRowData()
        .env(execEnv)
        .tableLoader(loader)
        .properties(properties)
        .project(getProjectedSchema())
        .limit(limit)
        .filters(filters)
        .flinkConf(readableConfig)
        .tableMetaStore(tableMetaStore)
        .build();
  }

  protected DataStreamSource<RowData> createFLIP27Stream(StreamExecutionEnvironment env) {
    SplitAssignerType assignerType =
        readableConfig.get(FlinkConfigOptions.TABLE_EXEC_SPLIT_ASSIGNER_TYPE);
    IcebergSource<RowData> source =
        IcebergSource.forRowData()
            .tableLoader(loader)
            .assignerFactory(assignerType.factory())
            .properties(properties)
            .project(getProjectedSchema())
            .limit(limit)
            .filters(filters)
            .flinkConfig(readableConfig)
            .build();
    DataStreamSource stream =
        env.fromSource(
            source,
            WatermarkStrategy.noWatermarks(),
            source.name(),
            TypeInformation.of(RowData.class));
    return stream;
  }

  protected TableSchema getProjectedSchema() {
    if (projectedFields == null) {
      return schema;
    } else {
      String[] fullNames = schema.getFieldNames();
      DataType[] fullTypes = schema.getFieldDataTypes();
      return TableSchema.builder()
          .fields(
              Arrays.stream(projectedFields).mapToObj(i -> fullNames[i]).toArray(String[]::new),
              Arrays.stream(projectedFields).mapToObj(i -> fullTypes[i]).toArray(DataType[]::new))
          .build();
    }
  }

  @Override
  public void applyLimit(long newLimit) {
    this.limit = newLimit;
  }

  @Override
  public Result applyFilters(List<ResolvedExpression> flinkFilters) {
    List<ResolvedExpression> acceptedFilters = Lists.newArrayList();
    List<Expression> expressions = Lists.newArrayList();

    for (ResolvedExpression resolvedExpression : flinkFilters) {
      Optional<Expression> icebergExpression = FlinkFilters.convert(resolvedExpression);
      if (icebergExpression.isPresent()) {
        expressions.add(icebergExpression.get());
        acceptedFilters.add(resolvedExpression);
      }
    }

    this.filters = expressions;
    return Result.of(acceptedFilters, flinkFilters);
  }

  @Override
  public boolean supportsNestedProjection() {
    // TODO: support nested projection
    return false;
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.insertOnly();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
    return new DataStreamScanProvider() {
      @Override
      public DataStream<RowData> produceDataStream(
          ProviderContext providerContext, StreamExecutionEnvironment execEnv) {
        if (readableConfig.get(FlinkConfigOptions.TABLE_EXEC_ICEBERG_USE_FLIP27_SOURCE)) {
          return createFLIP27Stream(execEnv);
        } else {
          return createDataStream(execEnv);
        }
      }

      @Override
      public boolean isBounded() {
        return FlinkSource.isBounded(properties);
      }
    };
  }

  @Override
  public DynamicTableSource copy() {
    return new IcebergTableSource(this);
  }

  @Override
  public String asSummaryString() {
    return "Iceberg table source";
  }
}
