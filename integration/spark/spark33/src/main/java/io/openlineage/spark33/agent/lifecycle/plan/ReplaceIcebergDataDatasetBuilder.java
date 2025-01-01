/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark33.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.dataset.DatasetCompositeFacetsBuilder;
import io.openlineage.spark.agent.util.DatasetVersionUtils;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.DataSourceV2RelationDatasetExtractor;
import io.openlineage.spark3.agent.utils.DatasetVersionDatasetFacetUtils;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.analysis.NamedRelation;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceIcebergData;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;

@Slf4j
public class ReplaceIcebergDataDatasetBuilder
    extends AbstractQueryPlanOutputDatasetBuilder<LogicalPlan> {

  public ReplaceIcebergDataDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  public static boolean hasClasses() {
    try {
      ReplaceIcebergDataDatasetBuilder.class
          .getClassLoader()
          .loadClass("org.apache.spark.sql.catalyst.plans.logical.ReplaceIcebergData");
      return true;
    } catch (Exception e) {
      // swallow- we don't care
    }
    return false;
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return (x instanceof ReplaceIcebergData);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(SparkListenerEvent event, LogicalPlan plan) {
    ReplaceIcebergData replace = (ReplaceIcebergData) plan;

    if (!(replace.table() instanceof DataSourceV2Relation)) {
      return Collections.emptyList();
    }
    DatasetFactory<OutputDataset> datasetFactory = outputDataset();
    DataSourceV2Relation table = (DataSourceV2Relation) replace.table();

    final DatasetCompositeFacetsBuilder datasetFacetsBuilder =
        datasetFactory.createCompositeFacetBuilder();
    datasetFacetsBuilder
        .getFacets()
        .lifecycleStateChange(
            context
                .getOpenLineage()
                .newLifecycleStateChangeDatasetFacet(
                    OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE,
                    null));

    if (includeDatasetVersion(event)) {
      DatasetVersionDatasetFacetUtils.extractVersionFromDataSourceV2Relation(context, table)
          .ifPresent(
              s -> DatasetVersionUtils.buildVersionOutputFacets(context, datasetFacetsBuilder, s));
    }

    return DataSourceV2RelationDatasetExtractor.extract(
        datasetFactory, context, table, datasetFacetsBuilder);
  }

  @Override
  public Optional<String> jobNameSuffix(LogicalPlan plan) {
    if (!(plan instanceof ReplaceIcebergData)) {
      return Optional.empty();
    }
    ReplaceIcebergData replace = (ReplaceIcebergData) plan;
    return Optional.ofNullable(replace.table()).map(NamedRelation::name);
  }
}
