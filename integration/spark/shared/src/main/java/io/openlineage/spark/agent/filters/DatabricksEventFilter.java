/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.filters;

import static io.openlineage.spark.agent.filters.EventFilterUtils.getLogicalPlan;

import io.openlineage.spark.agent.util.DatabricksUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.SerializeFromObject;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.WholeStageCodegenExec;

/** Class created to filter some types of event sent by Databricks */
@Slf4j
public class DatabricksEventFilter implements EventFilter {

  private final OpenLineageContext context;

  private static final List<String> excludedNodes =
      Arrays.asList(
          "collect_limit",
          "describe_table",
          "local_table_scan",
          "append_data_exec_v1",
          "serialize_from_object",
          "execute_set_catalog_command");

  public DatabricksEventFilter(OpenLineageContext context) {
    this.context = context;
  }

  @Override
  public boolean isDisabled(SparkListenerEvent event) {
    return isSerializeFromObject() || isWriteIntoDeltaCommand() || isDisabledDatabricksPlan(event);
  }

  public boolean isDisabledDatabricksPlan(SparkListenerEvent ignoredEvent) {
    if (!DatabricksUtils.isRunOnDatabricksPlatform(context)
        || !context.getQueryExecution().isPresent()) {
      return false;
    }

    SparkPlan node = context.getQueryExecution().get().executedPlan();

    // Unwrap SparkPlan from WholeStageCodegen, as that's not a descriptive or helpful job name
    if (node instanceof WholeStageCodegenExec) {
      node = ((WholeStageCodegenExec) node).child();
    }

    String nodeName = node.nodeName().replace("_", "");

    return excludedNodes.stream()
        .map(n -> n.replace("_", ""))
        .anyMatch(n -> n.equalsIgnoreCase(nodeName));
  }

  private boolean isSerializeFromObject() {
    return getLogicalPlan(context).map(plan -> plan instanceof SerializeFromObject).orElse(false);
  }

  private boolean isWriteIntoDeltaCommand() {
    return getLogicalPlan(context)
        .map(
            plan ->
                plan.getClass()
                    .getCanonicalName()
                    .contains("sql.transaction.tahoe.commands.WriteIntoDeltaCommand"))
        .orElse(false);
  }
}
