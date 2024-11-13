/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.InputStatisticsInputDatasetFacet;
import io.openlineage.client.dataset.DatasetCompositeFacetsBuilder;
import io.openlineage.spark.api.AbstractQueryPlanInputDatasetBuilder;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.DataSourceV2RelationDatasetExtractor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ScanTask;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;

@Slf4j
public final class DataSourceV2ScanRelationOnEndInputDatasetBuilder
    extends AbstractQueryPlanInputDatasetBuilder<DataSourceV2ScanRelation> {
  private final DatasetFactory<InputDataset> factory;

  public DataSourceV2ScanRelationOnEndInputDatasetBuilder(
      OpenLineageContext context, DatasetFactory<InputDataset> factory) {
    super(context, true);
    this.factory = Objects.requireNonNull(factory, "parameter: factory");
  }

  @Override
  public boolean isDefinedAt(SparkListenerEvent event) {
    return event instanceof SparkListenerSQLExecutionEnd;
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan plan) {
    return plan instanceof DataSourceV2ScanRelation;
  }

  @Override
  public List<InputDataset> apply(DataSourceV2ScanRelation plan) {
    if (context.getSparkExtensionVisitorWrapper().isDefinedAt(plan.relation().table())) {
      return getTableInputs(plan);
    }

    DataSourceV2Relation relation = plan.relation();
    DatasetCompositeFacetsBuilder datasetFacetsBuilder =
        new DatasetCompositeFacetsBuilder(context.getOpenLineage());

    getInputStatisticsInputDatasetFacet(plan)
        .ifPresent(stats -> datasetFacetsBuilder.getInputFacets().inputStatistics(stats));

    return DataSourceV2RelationDatasetExtractor.extract(
        factory, context, relation, datasetFacetsBuilder);
  }

  private Optional<InputStatisticsInputDatasetFacet> getInputStatisticsInputDatasetFacet(
      DataSourceV2ScanRelation plan) {
    if (plan.scan() == null) {
      return Optional.empty();
    }
    try {
      List<ScanTask> tasks = (List<ScanTask>) MethodUtils.invokeMethod(plan.scan(), true, "tasks");

      Collection<DataFile> dataFiles =
          tasks.stream()
              .flatMap(
                  task -> {
                    if (task instanceof BaseCombinedScanTask) {
                      return ((BaseCombinedScanTask) task).files().stream();
                    }
                    return Stream.of(task);
                  })
              .filter(ScanTask::isFileScanTask)
              .map(ScanTask::asFileScanTask)
              .map(FileScanTask::file)
              .collect(Collectors.toMap(ContentFile::path, f -> f))
              .values();

      return Optional.of(
          context
              .getOpenLineage()
              .newInputStatisticsInputDatasetFacetBuilder()
              .fileCount((long) dataFiles.size())
              .rowCount(dataFiles.stream().map(ContentFile::fileSizeInBytes).reduce(0L, Long::sum))
              .size(dataFiles.stream().map(ContentFile::recordCount).reduce(0L, Long::sum))
              .build());
    } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
      log.warn("Unable to extract input statistics", e);
      return Optional.empty();
    }
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }
}
