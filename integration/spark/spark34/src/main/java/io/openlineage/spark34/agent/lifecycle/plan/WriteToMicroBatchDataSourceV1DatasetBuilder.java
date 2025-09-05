/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark34.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
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
    // Currently, only FileStreamSink is supported
    if (writeToMicroBatchV1.sink() instanceof FileStreamSink) {
      FileStreamSink fileStreamSink = (FileStreamSink) writeToMicroBatchV1.sink();
      DatasetIdentifier datasetIdentifier =
          getFileSinkDatasetIdentifier(writeToMicroBatchV1, getFileStreamSinkPath(fileStreamSink));
      OpenLineage.OutputDataset dataset =
          factory.getDataset(datasetIdentifier, writeToMicroBatchV1.schema());
      return Collections.singletonList(dataset);
    }
    log.debug("Unsupported Sink type: {}", writeToMicroBatchV1.sink().getClass().getName());
    return Collections.emptyList();
  }

  private DatasetIdentifier getFileSinkDatasetIdentifier(
      WriteToMicroBatchDataSourceV1 writeToMicroBatchV1, String fileStreamSinkPath) {
    DatasetIdentifier datasetIdentifier = PathUtils.fromURI(URI.create(fileStreamSinkPath));

    // Add symlink if catalog table is available
    if (writeToMicroBatchV1.catalogTable().isDefined()) {
      DatasetIdentifier catalogIdentifier =
          PathUtils.fromCatalogTable(
              writeToMicroBatchV1.catalogTable().get(), context.getSparkSession().orElse(null));
      return datasetIdentifier.withSymlink(
          catalogIdentifier.getName(),
          catalogIdentifier.getNamespace(),
          DatasetIdentifier.SymlinkType.TABLE);
    }
    return datasetIdentifier;
  }

  private String getFileStreamSinkPath(FileStreamSink sink) {
    // The only way to get the path without using reflection
    return sink.toString().substring("FileSink[".length(), sink.toString().length() - 1).trim();
  }
}
