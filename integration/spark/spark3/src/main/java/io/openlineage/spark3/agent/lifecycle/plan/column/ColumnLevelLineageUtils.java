/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * Utility functions for detecting column level lineage within {@link
 * org.apache.spark.sql.catalyst.plans.logical.LogicalPlan}.
 */
@Slf4j
public class ColumnLevelLineageUtils {

  public static Optional<OpenLineage.ColumnLineageDatasetFacet> buildColumnLineageDatasetFacet(
      OpenLineageContext context, OpenLineage.SchemaDatasetFacet schemaFacet) {
    if (!context.getQueryExecution().isPresent()
        || context.getQueryExecution().get().optimizedPlan() == null
        || schemaFacet == null) {
      return Optional.empty();
    }

    ColumnLevelLineageBuilder builder = new ColumnLevelLineageBuilder(schemaFacet, context);
    LogicalPlan plan = context.getQueryExecution().get().optimizedPlan();

    ExpressionDependencyCollector.collect(plan, builder);
    OutputFieldsCollector.collect(plan, builder);
    InputFieldsCollector.collect(context, plan, builder);

    OpenLineage.ColumnLineageDatasetFacetBuilder facetBuilder =
        context.getOpenLineage().newColumnLineageDatasetFacetBuilder();

    facetBuilder.fields(builder.build());
    OpenLineage.ColumnLineageDatasetFacet facet = facetBuilder.build();

    if (facet.getFields().getAdditionalProperties().isEmpty()) {
      return Optional.empty();
    } else {
      return Optional.of(facetBuilder.build());
    }
  }
}
