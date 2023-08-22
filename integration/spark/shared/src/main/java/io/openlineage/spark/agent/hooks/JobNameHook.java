/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.hooks;

import static io.openlineage.spark.agent.util.DatabricksUtils.prettifyDatabricksJobName;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.Job;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.spark.agent.util.DatabricksUtils;
import io.openlineage.spark.api.OpenLineageContext;

public class JobNameHook implements RunEventBuilderHook {

  public static final String SPARK_CONF_JOB_NAME_APPEND_DATASET_NAME =
      "spark.openlineage.jobName.appendDatasetName";

  public static final String SPARK_CONF_JOB_NAME_REPLACE_DOT_WITH_UNDERSCORE =
      "spark.openlineage.jobName.replaceDotWithUnderscore";

  private static final String JOB_NAME_PARTS_SEPARATOR = ".";
  private static final String INNER_SEPARATOR = "_";

  private final OpenLineageContext openLineageContext;

  public JobNameHook(OpenLineageContext openLineageContext) {
    this.openLineageContext = openLineageContext;
  }

  @Override
  public void preBuild(OpenLineage.RunEventBuilder runEventBuilder) {
    if (openLineageContext != null
        && openLineageContext.getSparkContext() != null
        && openLineageContext.getSparkContext().conf() != null
        && !Boolean.valueOf(
            openLineageContext
                .getSparkContext()
                .conf()
                .get(SPARK_CONF_JOB_NAME_APPEND_DATASET_NAME, "true"))) {
      return;
    }

    OpenLineage.RunEvent runEvent = runEventBuilder.build();
    OpenLineage.Job job = runEvent.getJob();

    OpenLineage.Job newJob =
        openLineageContext
            .getOpenLineage()
            .newJobBuilder()
            .facets(job.getFacets())
            .namespace(job.getNamespace())
            .name(buildJobName(job, runEvent))
            .build();

    runEventBuilder.job(newJob);
  }

  private String buildJobName(Job job, RunEvent runEvent) {
    // first, look if the job name is not present in the context
    if (!openLineageContext.getJobName().isEmpty()) {
      return openLineageContext.getJobName().get(0);
    }

    StringBuilder jobNameBuilder = new StringBuilder();
    if (!DatabricksUtils.isRunOnDatabricksPlatform(openLineageContext)) {
      jobNameBuilder.append(job.getName());
    } else {
      jobNameBuilder.append(prettifyDatabricksJobName(openLineageContext, job.getName()));
    }

    if (runEvent.getOutputs() != null && runEvent.getOutputs().size() > 0) {
      // append output dataset name to job name
      jobNameBuilder
          .append(JOB_NAME_PARTS_SEPARATOR)
          .append(trimPath(runEvent.getOutputs().get(0).getName()).replace(".", INNER_SEPARATOR));
    }

    String jobName = jobNameBuilder.toString();

    if (openLineageContext != null
        && openLineageContext.getSparkContext() != null
        && openLineageContext.getSparkContext().conf() != null
        && Boolean.valueOf(
            openLineageContext
                .getSparkContext()
                .conf()
                .get(SPARK_CONF_JOB_NAME_REPLACE_DOT_WITH_UNDERSCORE, "false"))) {
      // replace dots with underscore to get consistent with legacy databricks integration
      // switched off by default
      jobName = jobName.replace(".", "_");
    }

    openLineageContext.getJobName().add(jobName);

    return jobName;
  }

  private static String trimPath(String path) {
    if (path.lastIndexOf("/") > 0) {
      // is path
      String[] parts = path.split("/");
      if (parts.length >= 2) {
        // concat two last elements of the path
        return parts[parts.length - 2] + INNER_SEPARATOR + parts[parts.length - 1];
      } else {
        // get last path element
        return parts[parts.length - 1];
      }
    } else {
      // is something else
      return path;
    }
  }
}
