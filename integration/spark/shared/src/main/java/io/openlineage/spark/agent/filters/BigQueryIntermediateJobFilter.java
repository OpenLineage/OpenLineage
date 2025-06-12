/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.filters;

import static io.openlineage.spark.agent.filters.EventFilterUtils.getLogicalPlan;

import io.openlineage.spark.api.OpenLineageContext;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;

@Slf4j
public class BigQueryIntermediateJobFilter implements EventFilter {

  private final OpenLineageContext context;

  private static final int UUID_LENGTH = 36;

  public BigQueryIntermediateJobFilter(OpenLineageContext context) {
    this.context = context;
  }

  @Override
  public boolean isDisabled(SparkListenerEvent event) {
    String path =
        getLogicalPlan(context)
            .filter(InsertIntoHadoopFsRelationCommand.class::isInstance)
            .map(InsertIntoHadoopFsRelationCommand.class::cast)
            .map(InsertIntoHadoopFsRelationCommand::outputPath)
            .map(Path::toString)
            .orElse("");

    if (!path.startsWith("gs://") || !path.contains(".spark-bigquery-local")) {
      // If the output path is a GCS or BigQuery path, we assume it's an indirect job.
      return false;
    }

    return endsWithValidUuid(path);
  }

  private boolean endsWithValidUuid(String path) {
    if (path.length() < UUID_LENGTH) {
      return false;
    }
    String uuid = path.substring(path.length() - UUID_LENGTH);
    try {
      UUID.fromString(uuid);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }
}
