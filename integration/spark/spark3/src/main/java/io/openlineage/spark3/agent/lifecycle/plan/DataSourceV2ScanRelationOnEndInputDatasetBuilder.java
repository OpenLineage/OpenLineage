/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.InputDatasetFacet;
import io.openlineage.client.dataset.DatasetCompositeFacetsBuilder;
import io.openlineage.spark.api.AbstractQueryPlanInputDatasetBuilder;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import io.openlineage.spark.api.Vendors;
import io.openlineage.spark3.agent.utils.DataSourceV2RelationDatasetExtractor;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;
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

    // input dataset facets builders on scan
    Collection<OpenLineageEventHandlerFactory> handlerFactories =
        Optional.ofNullable(context.getVendors())
            .map(Vendors::getEventHandlerFactories)
            .orElse(Collections.emptyList());

    handlerFactories.stream()
        .flatMap(v -> v.createInputDatasetFacetBuilders(context).stream())
        .forEach(
            f ->
                f.accept(
                    plan.scan(),
                    new BiConsumer<String, InputDatasetFacet>() {
                      @Override
                      public void accept(String s, InputDatasetFacet inputDatasetFacet) {
                        datasetFacetsBuilder.getInputFacets().put(s, inputDatasetFacet);
                      }
                    }));

    return DataSourceV2RelationDatasetExtractor.extract(
        factory, context, relation, datasetFacetsBuilder);
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }
}
