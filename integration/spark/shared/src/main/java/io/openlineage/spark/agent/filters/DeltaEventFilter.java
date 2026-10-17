/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.filters;

import static io.openlineage.spark.agent.filters.EventFilterUtils.getLogicalPlan;
import static io.openlineage.spark.agent.filters.EventFilterUtils.isDeltaPlan;

import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.plans.logical.SerializeFromObject;
import org.apache.spark.sql.catalyst.plans.logical.Union;
import org.apache.spark.sql.catalyst.trees.TreeNode;
import org.apache.spark.sql.execution.LogicalRDD;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SparkPlanInfo;
import org.apache.spark.sql.execution.datasources.FileIndex;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import scala.collection.JavaConverters;

@Slf4j
public class DeltaEventFilter implements EventFilter {

  private final OpenLineageContext context;

  private static final List<String> DELTA_INTERNAL_RDD_COLUMNS =
      Arrays.asList("txn", "add", "remove", "metaData", "cdc", "protocol", "commitInfo");

  private static final List<String> DELTA_LOG_INTERNAL_COLUMNS =
      Arrays.asList("protocol", "metaData", "action_sort_column");

  private static final String STAGING_DELTA_TABLE =
      "AppendDataExecV1 org.apache.spark.sql.delta.catalog.DeltaCatalog$StagedDeltaTableV2";

  public DeltaEventFilter(OpenLineageContext context) {
    this.context = context;
  }

  @Override
  public boolean isDisabled(SparkListenerEvent event) {
    if (!isDeltaPlan(context)) {
      return false;
    }

    // These broad plan shapes also occur in user queries; restrict them when Spark knows the root.
    boolean commandChildOrLegacy = context.getCommandChildExecution().orElse(true);
    return isOnJobStartOrEnd(event)
        || (commandChildOrLegacy
            && (isFilterRoot() || isLocalRelationOnly() || isSerializeFromObject()))
        || isLogicalRDDWithInternalDataColumns()
        || isStagedDeltaTable(event)
        || isDeltaLogProjection()
        || isDeltaMetadataReconstruction();
  }

  /**
   * We get exact copies of OL events for org.apache.spark.scheduler.SparkListenerJobStart and
   * org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart. The same happens for end
   * events.
   */
  private boolean isOnJobStartOrEnd(SparkListenerEvent event) {
    return event instanceof SparkListenerJobStart || event instanceof SparkListenerJobEnd;
  }

  /** Returns true if Staged Delta table is written */
  private boolean isStagedDeltaTable(SparkListenerEvent event) {
    if (event instanceof SparkListenerSQLExecutionStart) {
      return Optional.of((SparkListenerSQLExecutionStart) event)
          .map(SparkListenerSQLExecutionStart::sparkPlanInfo)
          .map(SparkPlanInfo::simpleString)
          .filter(s -> s.contains(STAGING_DELTA_TABLE))
          .isPresent();
    } else if (event instanceof SparkListenerSQLExecutionEnd) {
      return Optional.of((SparkListenerSQLExecutionEnd) event)
          .map(SparkListenerSQLExecutionEnd::qe)
          .map(QueryExecution::executedPlan)
          .map(TreeNode::toString)
          .filter(s -> s.contains(STAGING_DELTA_TABLE))
          .isPresent();
    }
    return false;
  }

  /** Returns true if LocalRelation is the only element of LogicalPlan. */
  private boolean isLocalRelationOnly() {
    return getLogicalPlan(context)
        .filter(plan -> plan.children() != null)
        .filter(plan -> plan.children().isEmpty())
        .filter(plan -> plan instanceof LocalRelation)
        .isPresent();
  }

  /** Returns true if Filter is a root node of LogicalPlan */
  private boolean isFilterRoot() {
    return getLogicalPlan(context).filter(plan -> plan instanceof Filter).isPresent();
  }

  /**
   * Delta 3.1 reconstructs protocol/metadata by unioning checkpoint and commit-log reads. The
   * Delta-owned index distinguishes this from deliberate user inspection of the same log files.
   */
  private boolean isDeltaMetadataReconstruction() {
    return getLogicalPlan(context)
        .filter(plan -> plan instanceof Union)
        .filter(this::hasMetadataColumns)
        .map(plan -> ScalaConversionUtils.fromSeq(plan.children()))
        .filter(children -> !children.isEmpty())
        .map(children -> children.stream().allMatch(this::isDeltaMetadataBranch))
        .orElse(false);
  }

  private boolean hasMetadataColumns(LogicalPlan plan) {
    return ScalaConversionUtils.fromSeq(plan.output()).stream()
        .map(NamedExpression::name)
        .collect(Collectors.toList())
        .equals(Arrays.asList("protocol", "metaData", "version"));
  }

  private boolean isDeltaMetadataBranch(LogicalPlan plan) {
    if (!(plan instanceof Project) || !hasMetadataColumns(plan)) {
      return false;
    }
    LogicalPlan child = ((Project) plan).child();
    if (!(child instanceof Filter)) {
      return false;
    }
    LogicalPlan leaf = ((Filter) child).child();
    if (!(leaf instanceof LogicalRelation)
        || !(((LogicalRelation) leaf).relation() instanceof HadoopFsRelation)) {
      return false;
    }
    FileIndex index = ((HadoopFsRelation) ((LogicalRelation) leaf).relation()).location();
    if (!isDeltaLogFileIndex(index)) {
      return false;
    }
    List<Path> roots = ScalaConversionUtils.fromSeq(index.rootPaths());
    return !roots.isEmpty() && roots.stream().allMatch(DeltaEventFilter::isDeltaLogPath);
  }

  private static boolean isDeltaLogFileIndex(FileIndex index) {
    // Delta is an optional dependency; avoid loading its classes for other Spark applications.
    for (Class<?> type = index.getClass(); type != null; type = type.getSuperclass()) {
      if ("org.apache.spark.sql.delta.DeltaLogFileIndex".equals(type.getName())) {
        return true;
      }
    }
    return false;
  }

  private static boolean isDeltaLogPath(Path path) {
    for (Path current = path; current != null; current = current.getParent()) {
      if ("_delta_log".equals(current.getName())) {
        return true;
      }
    }
    return false;
  }

  private boolean isDeltaLogProjection() {
    return getLogicalPlan(context)
        .filter(plan -> plan instanceof Project)
        .map(project -> JavaConverters.seqAsJavaListConverter(project.output()).asJava())
        .map(
            attributes ->
                attributes.stream()
                    .map(NamedExpression::name)
                    .collect(Collectors.toSet())
                    .containsAll(DELTA_LOG_INTERNAL_COLUMNS))
        .orElse(false);
  }

  /**
   * Delta internally performs actions on 'LogicalRDD [txn#92, add#93, remove#94, metaData#95,
   * protocol#96, cdc#97, commitInfo#98]'. If the leaf of logical plan is LogicalRDD with such
   * columns, we disable OL event.
   */
  private boolean isLogicalRDDWithInternalDataColumns() {
    return getLogicalPlan(context)
        .map(
            plan ->
                JavaConverters.seqAsJavaListConverter(plan.collectLeaves()).asJava().stream()
                    .filter(node -> node instanceof LogicalRDD)
                    .map(node -> (LogicalRDD) node)
                    .map(node -> JavaConverters.seqAsJavaListConverter(node.output()).asJava())
                    .map(
                        attributes ->
                            attributes.stream()
                                .map(NamedExpression::name)
                                .collect(Collectors.toSet()))
                    .anyMatch(attrs -> attrs.containsAll(DELTA_INTERNAL_RDD_COLUMNS)))
        .orElse(false);
  }

  private boolean isSerializeFromObject() {
    return getLogicalPlan(context).map(plan -> plan instanceof SerializeFromObject).orElse(false);
  }
}
