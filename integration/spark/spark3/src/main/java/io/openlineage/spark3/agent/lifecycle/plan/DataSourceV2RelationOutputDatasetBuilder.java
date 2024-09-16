/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.DataSourceV2RelationDatasetExtractor;
import io.openlineage.spark3.agent.utils.DatasetVersionDatasetFacetUtils;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;

/**
 * Find {@link org.apache.spark.sql.sources.BaseRelation}s and {@link
 * org.apache.spark.sql.connector.catalog.Table}.
 *
 * <p>Note that while the {@link DataSourceV2Relation} is a {@link
 * org.apache.spark.sql.catalyst.analysis.NamedRelation}, the returned name is that of the source,
 * not the specific dataset (e.g., "bigquery" not the table).
 */
@Slf4j
public class DataSourceV2RelationOutputDatasetBuilder
    extends AbstractQueryPlanOutputDatasetBuilder<DataSourceV2Relation> {

  private final DatasetFactory<OpenLineage.OutputDataset> factory;

  public DataSourceV2RelationOutputDatasetBuilder(
      OpenLineageContext context, DatasetFactory<OpenLineage.OutputDataset> factory) {
    super(context, false);
    this.factory = factory;
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan logicalPlan) {
    return logicalPlan instanceof DataSourceV2Relation;
  }

  @Override
  protected List<OpenLineage.OutputDataset> apply(
      SparkListenerEvent event, DataSourceV2Relation relation) {
    if (context.getSparkExtensionVisitorWrapper().isDefinedAt(relation.table())) {
      return getTableOutputs(relation);
    }

    OpenLineage.DatasetFacetsBuilder datasetFacetsBuilder =
        context.getOpenLineage().newDatasetFacetsBuilder();

    if (includeDatasetVersion(event)) {
      DatasetVersionDatasetFacetUtils.includeDatasetVersion(
          context, datasetFacetsBuilder, relation);
    }
    return DataSourceV2RelationDatasetExtractor.extract(
        factory, context, relation, datasetFacetsBuilder);
  }

  @Override
  public Optional<String> jobNameSuffix(DataSourceV2Relation relation) {
    String suffix = relation.table().name();
    if (relation.catalog().isDefined()
        && !suffix.startsWith(relation.catalog().get().name() + ".")) {
      // for some Spark versions relation.table().name() already contains catalog part
      suffix = relation.catalog().get().name() + "." + suffix;
    }
    return Optional.of(suffix);
  }
}
