/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.dataset.DatasetCompositeFacetsBuilder;
import io.openlineage.spark.api.AbstractQueryPlanInputDatasetBuilder;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.DataSourceV2RelationDatasetExtractor;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;

/**
 * Find {@link org.apache.spark.sql.sources.BaseRelation}s and {@link
 * org.apache.spark.sql.connector.catalog.Table}.
 *
 * <p>Note that while the {@link DataSourceV2Relation} is a {@link
 * org.apache.spark.sql.catalyst.analysis.NamedRelation}, the returned name is that of the source,
 * not the specific dataset (e.g., "bigquery" not the table).
 */
@Slf4j
public class DataSourceV2RelationInputOnEndDatasetBuilder
    extends AbstractQueryPlanInputDatasetBuilder<DataSourceV2Relation> {

  private final DatasetFactory<OpenLineage.InputDataset> factory;

  public DataSourceV2RelationInputOnEndDatasetBuilder(
      OpenLineageContext context, DatasetFactory<OpenLineage.InputDataset> factory) {
    super(context, true);
    this.factory = factory;
  }

  @Override
  public boolean isDefinedAt(SparkListenerEvent event) {
    return event instanceof SparkListenerJobEnd || event instanceof SparkListenerSQLExecutionEnd;
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan logicalPlan) {
    return logicalPlan instanceof DataSourceV2Relation;
  }

  @Override
  public List<OpenLineage.InputDataset> apply(DataSourceV2Relation relation) {
    DatasetCompositeFacetsBuilder datasetFacetsBuilder = factory.createCompositeFacetBuilder();

    // don't get dataset version on job end for inputs
    return DataSourceV2RelationDatasetExtractor.extractIncludingVersionFacet(
        factory, context, relation, datasetFacetsBuilder);
  }
}
