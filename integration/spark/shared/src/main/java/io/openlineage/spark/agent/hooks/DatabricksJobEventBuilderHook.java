/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.hooks;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.DatabricksUtils;
import io.openlineage.spark.api.OpenLineageContext;

public class DatabricksJobEventBuilderHook implements RunEventBuilderHook {

  private static final String SEPARATOR = "_";
  private final OpenLineageContext openLineageContext;

  public DatabricksJobEventBuilderHook(OpenLineageContext openLineageContext) {
    this.openLineageContext = openLineageContext;
  }

  @Override
  public void preBuild(OpenLineage.RunEventBuilder runEventBuilder) {
    if (!DatabricksUtils.isRunOnDatabricksPlatform(openLineageContext)) {
      return;
    }

    OpenLineage.RunEvent runEvent = runEventBuilder.build();
    OpenLineage.Job job = runEvent.getJob();

    StringBuilder jobNameBuilder = new StringBuilder();
    jobNameBuilder.append(
        job.getName()
            .replace(
                "databricks_shell.", // default name
                extractWorkspaceId(
                    DatabricksUtils.getWorkspaceUrl(openLineageContext).get()
                        + SEPARATOR))); // replace default job name with workspace id when no app
    // name
    // specified

    if (runEvent.getOutputs() != null) {
      // append output dataset name to job name
      runEvent.getOutputs().stream()
          .findAny()
          .map(dataset -> dataset.getName())
          .map(path -> trimPath(path)) // if dataset name is a path -> get last path element
          .ifPresent(name -> jobNameBuilder.append(SEPARATOR).append(name));
    }

    OpenLineage.Job newJob =
        openLineageContext
            .getOpenLineage()
            .newJobBuilder()
            .facets(job.getFacets())
            .namespace(job.getNamespace())
            .name(jobNameBuilder.toString().replace(".", SEPARATOR))
            .build();

    runEventBuilder.job(newJob);
  }

  private static String extractWorkspaceId(String workspaceUrl) {
    return workspaceUrl
        .replace(".cloud.databricks.com/", "") // extract workspace id from workspaceUrl
        .replace("https://", "");
  }

  private static String trimPath(String path) {
    if (path.lastIndexOf("/") > 0) {
      // is path
      String[] parts = path.split("/");
      if (parts.length >= 2) {
        // concat two last elements of the path
        return parts[parts.length - 2] + SEPARATOR + parts[parts.length - 1];
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
