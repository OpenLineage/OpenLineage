/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.filters;

import io.openlineage.spark.api.OpenLineageContext;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.LogicalRDD;
import scala.collection.JavaConverters;

@Slf4j
public class DeltaEventFilter implements EventFilter {

  private final OpenLineageContext context;

  private static final List<String> DELTA_INTERNAL_RDD_COLUMNS =
      Arrays.asList("txn", "add", "remove", "metaData", "cdc", "protocol", "commitInfo");

  public DeltaEventFilter(OpenLineageContext context) {
    this.context = context;
  }

  public boolean isDisabled(SparkListenerEvent event) {
    if (!isDelta()) {
      return false;
    }

    return isFilterRoot()
        || isLocalRelationOnly()
        || isLogicalRDDWithInternalDataColumns()
        || isOnJobStartOrEnd(event);
  }

  /**
   * We get exact copies of OL events for org.apache.spark.scheduler.SparkListenerJobStart and
   * org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart. The same happens for end
   * events.
   *
   * @return
   */
  private boolean isOnJobStartOrEnd(SparkListenerEvent event) {
    return event instanceof SparkListenerJobStart || event instanceof SparkListenerJobEnd;
  }

  /**
   * Returns true if LocalRelation is the only element of LogicalPlan.
   *
   * @return
   */
  private boolean isLocalRelationOnly() {
    return getLogicalPlan()
        .filter(plan -> plan.children() != null)
        .filter(plan -> plan.children().size() == 0)
        .filter(plan -> plan instanceof LocalRelation)
        .isPresent();
  }

  /**
   * Returns true if Filter is a root node of LogicalPlan
   *
   * @return
   */
  private boolean isFilterRoot() {
    return getLogicalPlan().filter(plan -> plan instanceof Filter).isPresent();
  }

  /**
   * Verifies if `spark.sql.extensions` is set in Spark configuration and checks if it is a delta
   * extension.
   *
   * @return
   */
  private boolean isDelta() {
    return Optional.of(SparkSession.active())
        .map(SparkSession::sparkContext)
        .filter(context -> context != null)
        .map(SparkContext::conf)
        .map(conf -> conf.get("spark.sql.extensions", ""))
        .filter(extension -> extension.equals("io.delta.sql.DeltaSparkSessionExtension"))
        .isPresent();
  }

  /**
   * Delta internally performs actions on 'LogicalRDD [txn#92, add#93, remove#94, metaData#95,
   * protocol#96, cdc#97, commitInfo#98]'. If the leaf of logical plan is LogicalRDD with such
   * columns, we disable OL event.
   */
  private boolean isLogicalRDDWithInternalDataColumns() {
    return getLogicalPlan()
        .map(
            plan ->
                JavaConverters.seqAsJavaListConverter(plan.collectLeaves()).asJava().stream()
                    .filter(node -> node instanceof LogicalRDD)
                    .map(node -> (LogicalRDD) node)
                    .map(node -> JavaConverters.seqAsJavaListConverter(node.output()).asJava())
                    .map(
                        attributes ->
                            attributes.stream().map(a -> a.name()).collect(Collectors.toList()))
                    .filter(attrs -> attrs.containsAll(DELTA_INTERNAL_RDD_COLUMNS))
                    .findAny()
                    .isPresent())
        .orElse(false);
  }

  private Optional<LogicalPlan> getLogicalPlan() {
    return context
        .getQueryExecution()
        .filter(queryExecution -> queryExecution != null)
        .map(queryExecution -> queryExecution.optimizedPlan())
        .filter(plan -> plan != null);
  }
}
