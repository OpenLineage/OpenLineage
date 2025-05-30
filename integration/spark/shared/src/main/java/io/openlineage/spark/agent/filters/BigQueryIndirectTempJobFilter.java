/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.filters;

import static io.openlineage.spark.agent.filters.EventFilterUtils.getLogicalPlan;

import io.openlineage.spark.api.OpenLineageContext;
import java.util.Optional;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;

@Slf4j
public class BigQueryIndirectTempJobFilter implements EventFilter {

  private final OpenLineageContext context;

  public BigQueryIndirectTempJobFilter(OpenLineageContext context) {
    this.context = context;
  }

  @Override
  public boolean isDisabled(SparkListenerEvent event) {
    Optional<LogicalPlan> logicalPlan = getLogicalPlan(context);

    if (!logicalPlan.isPresent()) {
      return false;
    }

    if (!(logicalPlan.get() instanceof InsertIntoHadoopFsRelationCommand)) {
      return false;
    }

    InsertIntoHadoopFsRelationCommand command =
        (InsertIntoHadoopFsRelationCommand) logicalPlan.get();

    String path = command.outputPath().toString();

    if (!path.startsWith("gs://") || !path.contains(".spark-bigquery-local")) {
      // If the output path is a GCS or BigQuery path, we assume it's an indirect job.
      return false;
    }

    // check if the uuid is appended onto the path - uuid is always 36 characters long
    String uuid = path.subSequence(path.length() - 36, path.length()).toString();

    try {
      UUID.fromString(uuid);
      // this is a valid UUID
      return true;
    } catch (IllegalArgumentException exception) {
      // this was not a valid UUID, so we assume it's not an indirect job
      return false;
    }
  }
}
