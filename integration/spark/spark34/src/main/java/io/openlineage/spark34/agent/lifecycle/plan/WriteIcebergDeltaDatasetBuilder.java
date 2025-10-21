/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark34.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.dataset.DatasetCompositeFacetsBuilder;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.DataSourceV2RelationDatasetExtractor;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.analysis.NamedRelation;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.WriteIcebergDelta;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;

@Slf4j
public class WriteIcebergDeltaDatasetBuilder
    extends AbstractQueryPlanOutputDatasetBuilder<LogicalPlan> {

  public WriteIcebergDeltaDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  public static boolean hasClasses() {
    try {
      WriteIcebergDeltaDatasetBuilder.class
          .getClassLoader()
          .loadClass("org.apache.spark.sql.catalyst.plans.logical.WriteIcebergDelta");
      return true;
    } catch (NoClassDefFoundError | Exception e) {
      // If class does not exist or it's loading fails for some reason, we handle that failure by
      // returning false
    }
    return false;
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return (x instanceof WriteIcebergDelta);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(SparkListenerEvent event, LogicalPlan plan) {
    WriteIcebergDelta replace = (WriteIcebergDelta) plan;

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

    return DataSourceV2RelationDatasetExtractor.extract(
        datasetFactory, context, table, datasetFacetsBuilder, includeDatasetVersion(event));
  }

  @Override
  public Optional<String> jobNameSuffix(LogicalPlan plan) {
    if (!(plan instanceof WriteIcebergDelta)) {
      return Optional.empty();
    }
    WriteIcebergDelta replace = (WriteIcebergDelta) plan;
    return Optional.ofNullable(replace.table()).map(NamedRelation::name);
  }
}
