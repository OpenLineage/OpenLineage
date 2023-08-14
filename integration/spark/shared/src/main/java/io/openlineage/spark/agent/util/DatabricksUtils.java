/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.spark.api.OpenLineageContext;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.QueryExecution;

/** Utils method to help exctact values from Databricks environmemt */
@Slf4j
public class DatabricksUtils {

  public static final String SPARK_DATABRICKS_WORKSPACE_URL = "spark.databricks.workspaceUrl";

  /**
   * Determines if a Spark job is run on Databricks platform
   *
   * @return
   */
  public static boolean isRunOnDatabricksPlatform(OpenLineageContext context) {
    return getWorkspaceUrl(context).isPresent();
  }

  public static Optional<String> getWorkspaceUrl(OpenLineageContext context) {
    return context
        .getQueryExecution()
        .map(QueryExecution::sparkSession)
        .map(SparkSession::sparkContext)
        .map(SparkContext::getConf)
        .filter(conf -> conf.contains(SPARK_DATABRICKS_WORKSPACE_URL))
        .map(conf -> conf.get(SPARK_DATABRICKS_WORKSPACE_URL));
  }

  public static String prettifyDatabricksJobName(OpenLineageContext context, String jobName) {
    // replace default job name with workspace id when no app name specified;
    return jobName.replace(
        "databricks_shell.", // default name
        extractWorkspaceId(DatabricksUtils.getWorkspaceUrl(context).get() + "."));
  }

  private static String extractWorkspaceId(String workspaceUrl) {
    return workspaceUrl
        .replace(".cloud.databricks.com/", "") // extract workspace id from workspaceUrl
        .replace("https://", "");
  }
}
