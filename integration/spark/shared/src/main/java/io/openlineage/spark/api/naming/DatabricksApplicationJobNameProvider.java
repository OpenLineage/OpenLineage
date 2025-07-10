/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api.naming;

import io.openlineage.spark.agent.util.DatabricksUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

/** Provides the job name in the Databricks environment. */
@Slf4j
class DatabricksApplicationJobNameProvider implements ApplicationJobNameProvider {
  @Override
  public boolean isDefinedAt(OpenLineageContext openLineageContext) {
    Optional<SparkConf> sparkConf = openLineageContext.getSparkContext().map(SparkContext::getConf);
    boolean isDatabricksEnvironment =
        sparkConf.isPresent() && DatabricksUtils.isRunOnDatabricksPlatform(sparkConf.get());
    if (isDatabricksEnvironment) {
      log.debug("The application is deployed in Databricks environment.");
    } else {
      log.debug("The environment is not Databricks environment. Skipping the provider");
    }
    return isDatabricksEnvironment;
  }

  @Override
  public String getJobName(OpenLineageContext openLineageContext) {
    SparkContext sparkContext = openLineageContext.getSparkContext().get();
    SparkConf sparkConf = sparkContext.getConf();
    String appName = openLineageContext.getApplicationName();

    if (appName != "databricks_shell") {
      return appName;
    }
    return DatabricksUtils.getWorkspaceUrl(sparkConf).map(this::extractWorkspaceId).get();
  }

  private String extractWorkspaceId(String workspaceUrl) {
    return workspaceUrl
        .replace(".cloud.databricks.com/", "") // extract workspace id from workspaceUrl
        .replace("https://", "");
  }
}
