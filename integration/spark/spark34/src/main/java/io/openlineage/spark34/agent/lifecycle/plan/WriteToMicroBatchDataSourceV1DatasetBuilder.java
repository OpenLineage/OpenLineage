/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark34.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.streaming.FileStreamSink;
import org.apache.spark.sql.execution.streaming.sources.WriteToMicroBatchDataSourceV1;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;

/**
 * {@link LogicalPlan} visitor that matches {@link WriteToMicroBatchDataSourceV1} commands and
 * extracts the output {@link OpenLineage.Dataset} being written to micro batch data sources using
 * the V1 API.
 */
@Slf4j
public class WriteToMicroBatchDataSourceV1DatasetBuilder
    extends AbstractQueryPlanOutputDatasetBuilder<WriteToMicroBatchDataSourceV1> {

  private final DatasetFactory<OpenLineage.OutputDataset> factory;

  public WriteToMicroBatchDataSourceV1DatasetBuilder(
      OpenLineageContext context, DatasetFactory<OpenLineage.OutputDataset> factory) {
    super(context, false);
    this.factory = factory;
  }

  @Override
  public boolean isDefinedAt(SparkListenerEvent event) {
    if (!(event instanceof SparkListenerSQLExecutionEnd)) {
      return false;
    }
    SparkListenerSQLExecutionEnd see = (SparkListenerSQLExecutionEnd) event;
    return isDefinedAtLogicalPlan(see.qe().analyzed());
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan logicalPlan) {
    return logicalPlan instanceof WriteToMicroBatchDataSourceV1;
  }

  @Override
  protected List<OpenLineage.OutputDataset> apply(
      SparkListenerEvent event, WriteToMicroBatchDataSourceV1 writeToMicroBatchV1) {
    if (writeToMicroBatchV1.sink() instanceof FileStreamSink) {
      if (writeToMicroBatchV1.catalogTable().isDefined()) {
        DatasetIdentifier di =
            PathUtils.fromCatalogTable(
                writeToMicroBatchV1.catalogTable().get(), context.getSparkSession().get());
        return Collections.singletonList(factory.getDataset(di, writeToMicroBatchV1.schema()));
      }
      return fileStreamSinkPath(writeToMicroBatchV1)
          .map(
              p ->
                  factory.getDataset(PathUtils.fromPath(new Path(p)), writeToMicroBatchV1.schema()))
          .map(Collections::singletonList)
          .orElse(Collections.emptyList());
    }
    log.debug("Unsupported Sink type: {}", writeToMicroBatchV1.sink().getClass().getName());
    return Collections.emptyList();
  }

  @Override
  public Optional<String> jobNameSuffix(WriteToMicroBatchDataSourceV1 plan) {
    if (!(plan.sink() instanceof FileStreamSink)) {
      return Optional.empty();
    }
    if (plan.catalogTable().isDefined()) {
      DatasetIdentifier di =
          PathUtils.fromCatalogTable(plan.catalogTable().get(), context.getSparkSession().get());
      return Optional.of(trimPath(context, di.getName()));
    }
    return fileStreamSinkPath(plan).map(p -> trimPath(context, p));
  }

  /** Extracts the output path from {@code writeOptions}, where Spark stores the "path" option. */
  private Optional<String> fileStreamSinkPath(WriteToMicroBatchDataSourceV1 plan) {
    return ScalaConversionUtils.toJavaMap(plan.writeOptions()).entrySet().stream()
        .filter(e -> "path".equalsIgnoreCase(e.getKey()))
        .map(java.util.Map.Entry::getValue)
        .findFirst();
  }
}
