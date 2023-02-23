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

package com.netease.arctic.optimizer.operator;

import com.netease.arctic.ams.api.ErrorMessage;
import com.netease.arctic.ams.api.JobId;
import com.netease.arctic.ams.api.JobType;
import com.netease.arctic.ams.api.OptimizeStatus;
import com.netease.arctic.ams.api.OptimizeTask;
import com.netease.arctic.ams.api.OptimizeTaskStat;
import com.netease.arctic.ams.api.TableIdentifier;
import com.netease.arctic.ams.api.properties.OptimizeTaskProperties;
import com.netease.arctic.catalog.ArcticCatalog;
import com.netease.arctic.catalog.CatalogLoader;
import com.netease.arctic.data.DataFileType;
import com.netease.arctic.data.DataTreeNode;
import com.netease.arctic.data.file.ContentFileWithSequence;
import com.netease.arctic.optimizer.OptimizerConfig;
import com.netease.arctic.optimizer.TaskWrapper;
import com.netease.arctic.optimizer.exception.TimeoutException;
import com.netease.arctic.optimizer.operator.executor.Executor;
import com.netease.arctic.optimizer.operator.executor.ExecutorFactory;
import com.netease.arctic.optimizer.operator.executor.NodeTask;
import com.netease.arctic.optimizer.operator.executor.OptimizeTaskResult;
import com.netease.arctic.optimizer.operator.executor.TableIdentificationInfo;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.table.TableProperties;
import com.netease.arctic.utils.SerializationUtils;
import com.netease.arctic.utils.TableTypeUtil;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Execute task.
 */
public class BaseTaskExecutor implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(BaseTaskExecutor.class);

  private final OptimizerConfig config;

  private final ExecuteListener listener;

  public BaseTaskExecutor(OptimizerConfig config) {
    this(config, null);
  }

  public BaseTaskExecutor(
      OptimizerConfig config,
      ExecuteListener listener) {
    this.config = config;
    this.listener = listener;
  }

  private static ArcticTable buildTable(TableIdentificationInfo tableIdentifierInfo) {
    String amsUrl = tableIdentifierInfo.getAmsUrl();
    amsUrl = amsUrl.trim();
    if (!amsUrl.endsWith("/")) {
      amsUrl = amsUrl + "/";
    }
    ArcticCatalog arcticCatalog = CatalogLoader.load(amsUrl + tableIdentifierInfo.getTableIdentifier().getCatalog());
    return arcticCatalog.loadTable(tableIdentifierInfo.getTableIdentifier());
  }

  private static DataTreeNode toTreeNode(com.netease.arctic.ams.api.TreeNode treeNode) {
    if (treeNode == null) {
      return null;
    }
    return DataTreeNode.of(treeNode.getMask(), treeNode.getIndex());
  }

  public static com.netease.arctic.table.TableIdentifier toTableIdentifier(
      TableIdentifier tableIdentifier) {
    if (tableIdentifier == null) {
      return null;
    }
    return com.netease.arctic.table.TableIdentifier.of(tableIdentifier.getCatalog(),
        tableIdentifier.getDatabase(), tableIdentifier.getTableName());
  }

  /**
   * Execute task.
   *
   * @param sourceTask -
   * @return task execute result
   */
  public OptimizeTaskStat execute(TaskWrapper sourceTask) {
    long startTime = System.currentTimeMillis();
    NodeTask task;
    String amsUrl = config.getAmsUrl();
    ArcticTable table = buildTable(
        new TableIdentificationInfo(
            amsUrl,
            toTableIdentifier(sourceTask.getTask().getTableIdentifier())));
    LOG.info("start execute {}", sourceTask.getTask().getTaskId());
    try {
      task = constructTask(table, sourceTask.getTask(), sourceTask.getAttemptId());
    } catch (Throwable t) {
      LOG.error("failed to build task {}", sourceTask.getTask(), t);
      throw new IllegalArgumentException(t);
    }
    onTaskStart(task.files());
    try {
      setPartition(task);
    } catch (Exception e) {
      LOG.error("failed to set partition info {}", task.getTaskId(), e);
      onTaskFailed(e);
      return constructFailedResult(task, e);
    }
    Executor optimize = ExecutorFactory.constructOptimize(task, table, startTime, config);
    try {
      OptimizeTaskResult result = optimize.execute();
      onTaskFinish(result.getTargetFiles());
      return result.getOptimizeTaskStat();
    } catch (TimeoutException timeoutException) {
      LOG.error("execute task timeout {}", task.getTaskId());
      onTaskFailed(timeoutException);
      return constructFailedResult(task, timeoutException);
    } catch (Throwable t) {
      LOG.error("failed to execute task {}", task.getTaskId(), t);
      onTaskFailed(t);
      return constructFailedResult(task, t);
    } finally {
      optimize.close();
    }
  }

  private void onTaskStart(Iterable<ContentFile<?>> inputFiles) {
    if (listener != null) {
      listener.onTaskStart(inputFiles);
    }
  }

  private void onTaskFinish(Iterable<? extends ContentFile<?>> outputFiles) {
    List<ContentFile<?>> targetFiles = new ArrayList<>();
    for (ContentFile<?> outputFile : outputFiles) {
      targetFiles.add(outputFile);
    }
    if (listener != null) {
      listener.onTaskFinish(targetFiles);
    }
  }

  private void onTaskFailed(Throwable t) {
    if (listener != null) {
      listener.onTaskFailed(t);
    }
  }

  private void setPartition(NodeTask nodeTask) {
    // partition
    if (nodeTask.files().size() == 0) {
      LOG.warn("task: {} no files to optimize.", nodeTask.getTaskId());
    } else {
      nodeTask.setPartition(nodeTask.files().get(0).partition());
    }
  }

  private OptimizeTaskStat constructFailedResult(NodeTask task, Throwable t) {
    try {
      OptimizeTaskStat optimizeTaskStat = new OptimizeTaskStat();
      // TODO refactor
      BeanUtils.copyProperties(optimizeTaskStat, task);
      JobId jobId = new JobId();
      jobId.setId(config.getOptimizerId());
      jobId.setType(JobType.Optimize);
      optimizeTaskStat.setJobId(jobId);
      optimizeTaskStat.setStatus(OptimizeStatus.Failed);
      optimizeTaskStat.setTableIdentifier(task.getTableIdentifier().buildTableIdentifier());
      optimizeTaskStat.setAttemptId(task.getAttemptId() + "");
      optimizeTaskStat.setTaskId(task.getTaskId());

      optimizeTaskStat.setErrorMessage(new ErrorMessage(System.currentTimeMillis(), buildErrorMessage(t, 3)));
      return optimizeTaskStat;
    } catch (Throwable e) {
      throw new IllegalStateException(e);
    }
  }

  private String buildErrorMessage(Throwable t, int deep) {
    StringBuilder message = new StringBuilder();
    Throwable error = t;
    int i = 0;
    while (i++ < deep && error != null) {
      if (i > 1) {
        message.append(". caused by ");
      }
      message.append(error.getMessage());
      error = error.getCause();
    }
    String result = message.toString();
    return result.length() > 4000 ? result.substring(0, 4000) : result;
  }

  private NodeTask constructTask(ArcticTable table, OptimizeTask task, int attemptId) {
    NodeTask nodeTask;
    if (TableTypeUtil.isIcebergTableFormat(table)) {
      List<ContentFileWithSequence<?>> base =
          task.getBaseFiles().stream().map(SerializationUtils::toIcebergContentFile).collect(Collectors.toList());
      List<ContentFileWithSequence<?>> insert =
          task.getInsertFiles().stream().map(SerializationUtils::toIcebergContentFile).collect(Collectors.toList());
      List<ContentFileWithSequence<?>> eqDelete =
          task.getDeleteFiles().stream().map(SerializationUtils::toIcebergContentFile).collect(Collectors.toList());
      List<ContentFileWithSequence<?>> posDelete =
          task.getPosDeleteFiles().stream().map(SerializationUtils::toIcebergContentFile).collect(Collectors.toList());
      nodeTask = new NodeTask(base,insert, eqDelete, posDelete, false);
    } else {
      List<ContentFileWithSequence<?>> base =
          task.getBaseFiles().stream().map(SerializationUtils::toInternalTableFile).collect(Collectors.toList());
      List<ContentFileWithSequence<?>> insert =
          task.getInsertFiles().stream().map(SerializationUtils::toInternalTableFile).collect(Collectors.toList());
      List<ContentFileWithSequence<?>> eqDelete =
          task.getDeleteFiles().stream().map(SerializationUtils::toInternalTableFile).collect(Collectors.toList());
      List<ContentFileWithSequence<?>> posDelete =
          task.getPosDeleteFiles().stream().map(SerializationUtils::toInternalTableFile).collect(Collectors.toList());
      nodeTask = new NodeTask(base,insert, eqDelete, posDelete, true);
    }

    if (CollectionUtils.isNotEmpty(task.getSourceNodes())) {
      nodeTask.setSourceNodes(
          task.getSourceNodes().stream().map(BaseTaskExecutor::toTreeNode).collect(Collectors.toSet()));
    }
    nodeTask.setTableIdentifier(toTableIdentifier(task.getTableIdentifier()));
    nodeTask.setTaskId(task.getTaskId());
    nodeTask.setAttemptId(attemptId);

    Map<String, String> properties = task.getProperties();
    if (properties != null) {
      String allFileCnt = properties.get(OptimizeTaskProperties.ALL_FILE_COUNT);
      int fileCnt = nodeTask.baseFiles().size() + nodeTask.insertFiles().size() +
          nodeTask.deleteFiles().size() + nodeTask.posDeleteFiles().size() +
          nodeTask.allIcebergDataFiles().size() + nodeTask.allIcebergDeleteFiles().size();
      if (allFileCnt != null && Integer.parseInt(allFileCnt) != fileCnt) {
        LOG.error("{} check file cnt error, expected {}, actual {}, {}, value = {}", task.getTaskId(), allFileCnt,
            fileCnt, nodeTask, task);
        throw new IllegalStateException("check file cnt error");
      }

      String customHiveSubdirectory = properties.get(OptimizeTaskProperties.CUSTOM_HIVE_SUB_DIRECTORY);
      nodeTask.setCustomHiveSubdirectory(customHiveSubdirectory);

      Long maxExecuteTime = PropertyUtil.propertyAsLong(properties,
          OptimizeTaskProperties.MAX_EXECUTE_TIME, TableProperties.SELF_OPTIMIZING_EXECUTE_TIMEOUT_DEFAULT);
      nodeTask.setMaxExecuteTime(maxExecuteTime);
    }

    return nodeTask;
  }

  public interface ExecuteListener {
    default void onTaskStart(Iterable<ContentFile<?>> inputFiles) {
    }

    default void onTaskFinish(Iterable<ContentFile<?>> outputFiles) {
    }

    default void onTaskFailed(Throwable t) {
    }
  }
}