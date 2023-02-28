/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.columnar.InMemoryRelation;

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

    collectInputsAndExpressionDependencies(context, plan, builder);
    OutputFieldsCollector.collect(plan, builder);

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

  private static void collectInputsAndExpressionDependencies(
      OpenLineageContext context, LogicalPlan plan, ColumnLevelLineageBuilder builder) {
    ExpressionDependencyCollector.collect(plan, builder);
    InputFieldsCollector.collect(context, plan, builder);

    // iterate children plans and see if they contain dataset caching
    if (plan.children() != null) {
      plan.foreach(
          node -> {
            if (node instanceof InMemoryRelation) {
              PlanUtils3.getLogicalPlanOf(context, (InMemoryRelation) node)
                  .ifPresent(
                      cachedPlan -> {
                        // run self for the cached plan
                        collectInputsAndExpressionDependencies(context, cachedPlan, builder);

                        // map outputs of cachedPlan onto inputs of InMemoryRelation
                        Map<String, ExprId> idMap =
                            ScalaConversionUtils.<Attribute>fromSeq(node.output()).stream()
                                .collect(Collectors.toMap(Attribute::name, Attribute::exprId));

                        OutputFieldsCollector.getOutputExpressionsFromTree(cachedPlan).stream()
                            .filter(namedExpression -> idMap.containsKey(namedExpression.name()))
                            .forEach(
                                namedExpression ->
                                    builder.addDependency(
                                        namedExpression.exprId(),
                                        idMap.get(namedExpression.name())));
                      });
            }
            return scala.runtime.BoxedUnit.UNIT;
          });
    }
  }
}
