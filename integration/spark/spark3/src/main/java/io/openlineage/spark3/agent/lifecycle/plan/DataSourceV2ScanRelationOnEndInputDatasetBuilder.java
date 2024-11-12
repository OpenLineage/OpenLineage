/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.InputStatisticsInputDatasetFacet;
import io.openlineage.spark.api.AbstractQueryPlanInputDatasetBuilder;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.DataSourceV2RelationDatasetExtractor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.DataFile;
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
    OpenLineage.DatasetFacetsBuilder datasetFacetsBuilder =
        context.getOpenLineage().newDatasetFacetsBuilder();

    try {
      // TODO: try doing this cleaner
      List<BaseFileScanTask> tasks =
          (List<BaseFileScanTask>) MethodUtils.invokeMethod(plan.scan(), true, "tasks");

      Collection<DataFile> dataFiles =
          tasks.stream()
              .map(t -> t.file())
              .collect(Collectors.toMap(f -> f.path(), f -> f))
              .values();

      Long records = dataFiles.stream().map(f -> f.recordCount()).reduce(0L, Long::sum);
      Long files = dataFiles.stream().count();
      Long bytes = dataFiles.stream().map(f -> f.fileSizeInBytes()).reduce(0L, Long::sum);

      log.warn(
          "{} records={}, files={}, bytes={}",
          plan.relation().toString(),
          records,
          files,
          bytes); // this is working locally

      InputStatisticsInputDatasetFacet inputStatisticsInputDatasetFacet =
          context
              .getOpenLineage()
              .newInputStatisticsInputDatasetFacetBuilder()
              .fileCount(dataFiles.stream().count())
              .rowCount(dataFiles.stream().map(f -> f.fileSizeInBytes()).reduce(0L, Long::sum))
              .size(dataFiles.stream().map(f -> f.recordCount()).reduce(0L, Long::sum))
              .build();

      // TODO: create input statistics facet
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e);
    }

    return DataSourceV2RelationDatasetExtractor.extract(
        factory, context, relation, datasetFacetsBuilder);
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }
}
