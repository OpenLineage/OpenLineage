/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.api.AbstractQueryPlanInputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.CopyIntoCommandUtils;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * Extracts input datasets from the Databricks-specific {@code CopyIntoCommand} or {@code
 * CopyIntoCommandEdge}. The source is typically a file path (e.g., S3, ADLS, DBFS, Unity Catalog
 * Volumes). Since these classes belong to the Databricks runtime, reflection is used to access
 * source-related members.
 */
@Slf4j
public class CopyIntoCommandInputDatasetBuilder
    extends AbstractQueryPlanInputDatasetBuilder<LogicalPlan> {

  public CopyIntoCommandInputDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return CopyIntoCommandUtils.isCopyIntoCommand(x);
  }

  @Override
  protected List<InputDataset> apply(SparkListenerEvent event, LogicalPlan x) {
    return CopyIntoCommandUtils.sourcePath(x)
        .flatMap(this::inputDatasetFromPath)
        .map(Collections::singletonList)
        .orElseGet(
            () ->
                CopyIntoCommandUtils.sourceQuery(x)
                    .map(query -> delegate(query, event))
                    .orElse(Collections.emptyList()));
  }

  private java.util.Optional<InputDataset> inputDatasetFromPath(String sourcePath) {
    try {
      return java.util.Optional.of(
          inputDataset().sparkDatasetBuilder().dataset(PathUtils.fromPath(new Path(sourcePath))).build());
    } catch (Exception e) {
      log.warn("Failed to construct input dataset from COPY INTO source path: {}", sourcePath, e);
      return java.util.Optional.empty();
    }
  }
}
