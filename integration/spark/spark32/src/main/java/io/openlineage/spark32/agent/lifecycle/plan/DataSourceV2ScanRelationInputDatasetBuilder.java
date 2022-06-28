/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark32.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.AbstractQueryPlanInputDatasetBuilder;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark32.agent.utils.DatasetVersionDatasetFacetUtils;
import io.openlineage.spark32.agent.utils.PlanUtils3;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;

import java.util.List;

@Slf4j
public class DataSourceV2ScanRelationInputDatasetBuilder
    extends AbstractQueryPlanInputDatasetBuilder<DataSourceV2ScanRelation> {

  private final DatasetFactory<OpenLineage.InputDataset> factory;

  public DataSourceV2ScanRelationInputDatasetBuilder(
      OpenLineageContext context, DatasetFactory<OpenLineage.InputDataset> factory) {
    super(context, true);
    this.factory = factory;
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan logicalPlan) {
    return logicalPlan instanceof DataSourceV2ScanRelation;
  }

  @Override
  public List<OpenLineage.InputDataset> apply(DataSourceV2ScanRelation scanRelation) {
    DataSourceV2Relation relation = (scanRelation).relation();
    OpenLineage.DatasetFacetsBuilder datasetFacetsBuilder =
        context.getOpenLineage().newDatasetFacetsBuilder();

    DatasetVersionDatasetFacetUtils.includeDatasetVersion(context, datasetFacetsBuilder, relation);
    return PlanUtils3.fromDataSourceV2Relation(factory, context, relation, datasetFacetsBuilder);
  }
}
