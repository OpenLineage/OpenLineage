/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.filters;

import io.openlineage.spark.agent.util.SparkSessionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NamedRelation;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.V2WriteCommand;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;

@Slf4j
public class EventFilterUtils {

  /**
   * Method that verifies based on OpenLineageContext and SparkListenerEvent if OpenLineage event
   * has to be sent.
   */
  public static boolean isDisabled(OpenLineageContext context, SparkListenerEvent event) {
    return Stream.of(
            new DeltaEventFilter(context),
            new DatabricksEventFilter(context),
            new SparkNodesFilter(context),
            new CreateViewFilter(context),
            new BigQueryIntermediateJobFilter(context),
            new AdaptivePlanEventFilter(context))
        .anyMatch(
            filter -> {
              boolean isDisabled = filter.isDisabled(event.getClass().cast(event));
              if (isDisabled) {
                String logicalPlanNode =
                    getLogicalPlan(context)
                        .map(plan -> plan.getClass().getCanonicalName())
                        .orElse("UnparsableLogicalPlan");
                if (log.isDebugEnabled()) {
                  log.debug(
                      "Rejecting event : {} with plan : {} due to filter : {}",
                      event.toString(),
                      logicalPlanNode,
                      filter.getClass().getCanonicalName());
                }
              }
              return isDisabled;
            });
  }

  static Optional<LogicalPlan> getLogicalPlan(OpenLineageContext context) {
    return context.getQueryExecution().map(QueryExecution::optimizedPlan);
  }

  /**
   * Returns true when the current optimized logical plan writes into Delta. Only the write target
   * matters here: a query that merely reads Delta tables but writes elsewhere (e.g. plain Parquet)
   * must keep its adaptive-plan events, because with AQE enabled those are the only terminal events
   * of a V1 write.
   */
  static boolean isCurrentPlanDeltaWrite(OpenLineageContext context) {
    try {
      return getLogicalPlan(context).map(EventFilterUtils::isDeltaWriteRoot).orElse(false);
    } catch (Exception | LinkageError e) {
      log.debug("Unable to determine whether the current logical plan writes to Delta", e);
      return false;
    }
  }

  private static boolean isDeltaWriteRoot(LogicalPlan root) {
    if (isDeltaClassName(root.getClass().getName())) {
      // Delta command nodes: MergeIntoCommand, DeleteCommand, UpdateCommand, WriteIntoDelta, ...
      return true;
    }
    if (root instanceof SaveIntoDataSourceCommand) {
      // V1 save with an explicit source, e.g. df.write.format("delta").save(...)
      SaveIntoDataSourceCommand command = (SaveIntoDataSourceCommand) root;
      return command.dataSource() != null
          && isDeltaClassName(command.dataSource().getClass().getName());
    }
    if (root instanceof V2WriteCommand) {
      // V2 writes: AppendData, OverwriteByExpression, OverwritePartitionsDynamic, ...
      NamedRelation table = ((V2WriteCommand) root).table();
      if (table instanceof DataSourceV2Relation) {
        DataSourceV2Relation relation = (DataSourceV2Relation) table;
        return relation.table() != null && isDeltaClassName(relation.table().getClass().getName());
      }
      return false;
    }
    if (root instanceof InsertIntoHadoopFsRelationCommand) {
      // V1 file write; Delta only when the file format itself is Delta's
      InsertIntoHadoopFsRelationCommand command = (InsertIntoHadoopFsRelationCommand) root;
      return command.fileFormat() != null
          && isDeltaClassName(command.fileFormat().getClass().getName());
    }
    return false;
  }

  private static boolean isDeltaClassName(String className) {
    return className.startsWith("org.apache.spark.sql.delta");
  }

  /**
   * Verifies if `spark.sql.extensions` is set in Spark configuration and checks if it is a delta
   * extension.
   */
  static boolean isDeltaPlan() {
    return SparkSessionUtils.activeSession()
        .map(SparkSession::sparkContext)
        .map(SparkContext::conf)
        .map(conf -> conf.get("spark.sql.extensions", ""))
        .map(ext -> Arrays.asList(ext.split(",")))
        .filter(
            list ->
                list.stream()
                    .map(String::trim)
                    .anyMatch("io.delta.sql.DeltaSparkSessionExtension"::equals))
        .isPresent();
  }
}
