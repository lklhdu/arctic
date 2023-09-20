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

import com.netease.arctic.data.DataFileType;
import com.netease.arctic.flink.read.hybrid.split.ArcticSplit;
import com.netease.arctic.flink.read.hybrid.split.ChangelogSplit;
import com.netease.arctic.flink.read.hybrid.split.MergeOnReadSplit;
import com.netease.arctic.flink.read.hybrid.split.SnapshotSplit;
import com.netease.arctic.scan.ArcticFileScanTask;
import com.netease.arctic.scan.ChangeTableIncrementalScan;
import com.netease.arctic.scan.CombinedScanTask;
import com.netease.arctic.scan.KeyedTableScan;
import com.netease.arctic.table.KeyedTable;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * An util class that plans arctic table(base and change) or just plans change table. invoked by arctic enumerator.
 */
public class FlinkSplitPlanner {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkSplitPlanner.class);

  private FlinkSplitPlanner() {
  }

  public static List<ArcticSplit> planFullTable(KeyedTable keyedTable, AtomicInteger splitCount) {
    CloseableIterable<CombinedScanTask> combinedScanTasks = keyedTable.newScan().planTasks();
    BaseAndChangeTask baseAndChangeTask = BaseAndChangeTask.of(combinedScanTasks);
    return planFullTable(baseAndChangeTask, splitCount);
  }

  /**
   * Plans full table scanning for a {@link KeyedTable} with optional filters and a specified split count.
   *
   * @param keyedTable The {@link KeyedTable} to scan.
   * @param filters    Optional list of filters to apply to the scan.
   * @param splitCount The atomic integer to track the split count.
   * @return The list of planned {@link ArcticSplit} included {@link SnapshotSplit}, {@link ChangelogSplit}.
   */
  public static List<ArcticSplit> planFullTable(
      KeyedTable keyedTable, List<Expression> filters, AtomicInteger splitCount) {
    KeyedTableScan keyedTableScan = keyedTable.newScan();
    if (filters != null) {
      filters.forEach(keyedTableScan::filter);
    }
    CloseableIterable<CombinedScanTask> combinedScanTasks = keyedTableScan.planTasks();
    BaseAndChangeTask baseAndChangeTask = BaseAndChangeTask.of(combinedScanTasks);
    return planFullTable(baseAndChangeTask, splitCount);
  }

  private static List<ArcticSplit> planFullTable(
      BaseAndChangeTask baseAndChangeTask, AtomicInteger splitCount) {
    Collection<ArcticFileScanTask> baseTasks = baseAndChangeTask.allBaseTasks();
    List<ArcticSplit> allSplits = baseTasks.stream()
        .map(arcticFileScanTask -> new SnapshotSplit(
            Collections.singleton(arcticFileScanTask),
            splitCount.incrementAndGet())).collect(Collectors.toList());

    Collection<TransactionTask> changeTasks = baseAndChangeTask.transactionTasks();
    List<ArcticSplit> changeSplits = planChangeTable(changeTasks, splitCount);
    allSplits.addAll(changeSplits);

    return allSplits;
  }

  /**
   * Plans full table scanning for a {@link KeyedTable} with optional filters and a specified split count.
   *
   * @param keyedTable The {@link KeyedTable} to scan.
   * @param filters    Optional list of filters to apply to the scan.
   * @param splitCount The atomic integer to track the split count.
   * @return The list of planned {@link ArcticSplit} included {@link MergeOnReadSplit}.
   */
  public static List<ArcticSplit> mergeOnReadPlan(
      KeyedTable keyedTable, List<Expression> filters, AtomicInteger splitCount) {
    KeyedTableScan keyedTableScan = keyedTable.newScan();
    if (filters != null) {
      filters.forEach(keyedTableScan::filter);
    }
    CloseableIterable<CombinedScanTask> combinedScanTasks = keyedTableScan.planTasks();
    List<ArcticSplit> morSplits = Lists.newArrayList();
    try (CloseableIterator<CombinedScanTask> initTasks = combinedScanTasks.iterator()) {

      while (initTasks.hasNext()) {
        CombinedScanTask combinedScanTask = initTasks.next();
        combinedScanTask.tasks().forEach(
            keyedTableScanTask -> morSplits.add(new MergeOnReadSplit(splitCount.get(), keyedTableScanTask)));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return morSplits;
  }

  public static List<ArcticSplit> planChangeTable(ChangeTableIncrementalScan tableIncrementalScan,
      AtomicInteger splitCount) {
    CloseableIterable<FileScanTask> tasks = tableIncrementalScan.planFiles();
    BaseAndChangeTask baseAndChangeTask = BaseAndChangeTask.ofIceberg(tasks);
    return planChangeTable(baseAndChangeTask.transactionTasks(), splitCount);
  }

  private static List<ArcticSplit> planChangeTable(Collection<TransactionTask> transactionTasks,
      AtomicInteger splitCount) {
    List<ArcticSplit> changeTasks = new ArrayList<>(transactionTasks.size());
    transactionTasks
        .forEach(transactionTask -> {
          PartitionAndNodeGroup partitionAndNodeGroup =
              new PartitionAndNodeGroup()
                  .insertFileScanTask(transactionTask.insertTasks)
                  .deleteFileScanTask(transactionTask.deleteTasks)
                  .splitCount(splitCount);
          changeTasks.addAll(partitionAndNodeGroup.planSplits());
        });
    return changeTasks;
  }

  private static class TransactionTask {
    private Set<ArcticFileScanTask> insertTasks;
    private Set<ArcticFileScanTask> deleteTasks;
    Long transactionId;

    public TransactionTask(Long transactionId) {
      this.transactionId = transactionId;
    }

    public void putInsertTask(ArcticFileScanTask insert) {
      if (insertTasks == null) {
        insertTasks = new HashSet<>();
      }
      insertTasks.add(insert);
    }

    public void putDeleteTask(ArcticFileScanTask delete) {
      if (deleteTasks == null) {
        deleteTasks = new HashSet<>();
      }
      deleteTasks.add(delete);
    }
  }

  public static class BaseAndChangeTask {
    Collection<ArcticFileScanTask> allBaseTasks;
    Collection<TransactionTask> changeTableTasks;

    private BaseAndChangeTask(
        Collection<ArcticFileScanTask> allBaseTasks,
        Map<Long, TransactionTask> changeTableTaskMap) {
      this.allBaseTasks = allBaseTasks;
      if (changeTableTaskMap == null || changeTableTaskMap.isEmpty()) {
        this.changeTableTasks = Collections.emptyList();
      } else {
        this.changeTableTasks = changeTableTaskMap.values().stream()
            .sorted(Comparator.comparing(o -> o.transactionId)).collect(Collectors.toList());
      }
    }

    public static BaseAndChangeTask ofIceberg(CloseableIterable<FileScanTask> tasks) {
      try (CloseableIterator<FileScanTask> tasksIterator = tasks.iterator()) {
        Map<Long, TransactionTask> transactionTasks = new HashMap<>();
        long startTime = System.currentTimeMillis();
        int count = 0;
        while (tasksIterator.hasNext()) {
          count++;
          ArcticFileScanTask fileScanTask = (ArcticFileScanTask) tasksIterator.next();
          if (fileScanTask.file().type().equals(DataFileType.INSERT_FILE)) {
            taskMap(Collections.singleton(fileScanTask), true, transactionTasks);
          } else if (fileScanTask.file().type().equals(DataFileType.EQ_DELETE_FILE)) {
            taskMap(Collections.singleton(fileScanTask), false, transactionTasks);
          } else {
            throw new IllegalArgumentException(
                String.format(
                    "DataFileType %s is not supported during change log reading period.",
                    fileScanTask.file().type()));
          }
        }
        LOG.info("Read {} change log from {} in {} ms", count, tasksIterator.getClass(),
            System.currentTimeMillis() - startTime);
        return new BaseAndChangeTask(Collections.emptySet(), transactionTasks);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    public static BaseAndChangeTask of(CloseableIterable<CombinedScanTask> combinedScanTasks) {
      try (CloseableIterator<CombinedScanTask> initTasks = combinedScanTasks.iterator()) {
        final Set<ArcticFileScanTask> allBaseTasks = new HashSet<>();
        final Map<Long, TransactionTask> transactionTasks = new HashMap<>();

        while (initTasks.hasNext()) {
          CombinedScanTask combinedScanTask = initTasks.next();
          combinedScanTask.tasks().forEach(keyedTableScanTask -> {
            allBaseTasks.addAll(keyedTableScanTask.baseTasks());

            taskMap(keyedTableScanTask.insertTasks(), true, transactionTasks);
            taskMap(keyedTableScanTask.arcticEquityDeletes(), false, transactionTasks);
          });
        }
        List<ArcticFileScanTask> baseTasks = allBaseTasks.stream()
            .sorted(Comparator.comparing(t -> t.file().transactionId())).collect(Collectors.toList());

        return new BaseAndChangeTask(baseTasks, transactionTasks);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    private static void taskMap(
        Collection<ArcticFileScanTask> tasks, boolean insert, Map<Long, TransactionTask> transactionTaskMap) {
      tasks.forEach(task -> {
        long transactionId = task.file().transactionId();
        TransactionTask tasksInSingleTransaction =
            transactionTaskMap.getOrDefault(transactionId, new TransactionTask(transactionId));
        if (insert) {
          tasksInSingleTransaction.putInsertTask(task);
        } else {
          tasksInSingleTransaction.putDeleteTask(task);
        }
        transactionTaskMap.put(transactionId, tasksInSingleTransaction);
      });
    }

    public Collection<ArcticFileScanTask> allBaseTasks() {
      return allBaseTasks;
    }

    public Collection<TransactionTask> transactionTasks() {
      return changeTableTasks;
    }
  }
}
