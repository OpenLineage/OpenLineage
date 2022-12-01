/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.filters;

import io.openlineage.spark.agent.util.DatabricksUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.WholeStageCodegenExec;

/** Class created to filter some types of event sent by Databricks */
@Slf4j
public class DatabricksEventFilter implements EventFilter {

  private final OpenLineageContext context;

  private static final List<String> excludedNodes =
      Arrays.asList(
          "show_namespaces",
          "show_tables",
          "collect_limit",
          "describe_table",
          "local_table_scan",
          "adaptive_spark_plan",
          "serialize_from_object",
          "execute_set_catalog_command");

  public DatabricksEventFilter(OpenLineageContext context) {
    this.context = context;
  }

  public boolean isDisabled(SparkListenerEvent event) {
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
        .filter(n -> n.equalsIgnoreCase(nodeName))
        .findAny()
        .isPresent();
  }
}
