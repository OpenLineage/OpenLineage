/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.dataset.namespace.resolver.DatasetNamespaceCombinedResolver;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.ColumnLineageConfig;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.columnar.InMemoryRelation;
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;

/**
 * Utility functions for detecting column level lineage within {@link
 * org.apache.spark.sql.catalyst.plans.logical.LogicalPlan}.
 */
@Slf4j
public class ColumnLevelLineageUtils {

  public static Optional<OpenLineage.ColumnLineageDatasetFacet> buildColumnLineageDatasetFacet(
      SparkListenerEvent event,
      OpenLineageContext olContext,
      OpenLineage.SchemaDatasetFacet schemaFacet) {
    if (!olContext.getQueryExecution().isPresent()
        || olContext.getQueryExecution().get().optimizedPlan() == null
        || schemaFacet == null
        || isSchemaExceedsLimit(olContext, schemaFacet)) {
      return Optional.empty();
    }

    ColumnLevelLineageContext context =
        new ColumnLevelLineageContext(
            event,
            olContext,
            new ColumnLevelLineageBuilder(schemaFacet, olContext),
            new DatasetNamespaceCombinedResolver(olContext.getOpenLineageConfig()));

    LogicalPlan plan = getAdjustedPlan(olContext);

    OutputFieldsCollector.collect(context, plan);
    collectInputsAndExpressionDependencies(context, plan);

    OpenLineage.ColumnLineageDatasetFacetBuilder facetBuilder =
        olContext.getOpenLineage().newColumnLineageDatasetFacetBuilder();

    boolean datasetLineageEnabled =
        Optional.of(context.getOlContext().getOpenLineageConfig())
            .map(SparkOpenLineageConfig::getColumnLineageConfig)
            .map(ColumnLineageConfig::getDatasetLineageEnabled)
            .orElse(true);
    facetBuilder.fields(context.getBuilder().buildFields(datasetLineageEnabled));
    context
        .getBuilder()
        .buildDatasetDependencies(datasetLineageEnabled)
        .ifPresent(facetBuilder::dataset);
    OpenLineage.ColumnLineageDatasetFacet facet = facetBuilder.build();

    if (facet.getFields().getAdditionalProperties().isEmpty()) {
      return Optional.empty();
    } else {
      return Optional.of(facet);
    }
  }

  /**
   * Checks if the schema size exceeds the configured limit for column lineage processing. When the
   * schema is too large, column lineage facet creation is skipped to avoid performance issues.
   */
  private static boolean isSchemaExceedsLimit(
      OpenLineageContext context, OpenLineage.SchemaDatasetFacet schemaFacet) {
    Integer schemaSizeLimit =
        Optional.of(context.getOpenLineageConfig().getColumnLineageConfig())
            .map(ColumnLineageConfig::getSchemaSizeLimit)
            .orElse(1_000);
    boolean exceedsLimit = schemaFacet.getFields().size() > schemaSizeLimit;

    if (exceedsLimit) {
      log.warn(
          "Schema size ({} fields) exceeds configured limit ({} fields). "
              + "Consider increasing spark.openlineage.columnLineage.schemaSizeLimit if column lineage is needed for large schemas.",
          schemaFacet.getFields().size(),
          schemaSizeLimit);
    }

    return exceedsLimit;
  }

  private static LogicalPlan getAdjustedPlan(OpenLineageContext context) {
    LogicalPlan logicalPlan = context.getOptimizedPlan();

    LogicalPlan plan;
    if (logicalPlan instanceof SaveIntoDataSourceCommand) {
      plan = ((SaveIntoDataSourceCommand) logicalPlan).query();
    } else if (logicalPlan instanceof CreateDataSourceTableAsSelectCommand) {
      plan = ((CreateDataSourceTableAsSelectCommand) logicalPlan).query();
    } else {
      plan = logicalPlan;
    }
    return plan;
  }

  static void collectInputsAndExpressionDependencies(
      ColumnLevelLineageContext context, LogicalPlan plan) {
    ExpressionDependencyCollector.collect(context, plan);
    InputFieldsCollector.collect(context, plan);

    // iterate children plans and see if they contain dataset caching
    if (plan.children() != null) {
      plan.foreach(
          node -> {
            if (node instanceof InMemoryRelation) {
              PlanUtils3.getLogicalPlanOf(context.getOlContext(), (InMemoryRelation) node)
                  .ifPresent(
                      cachedPlan -> {
                        // run self for the cached plan
                        collectInputsAndExpressionDependencies(context, cachedPlan);

                        // map outputs of cachedPlan onto inputs of InMemoryRelation
                        Map<String, ExprId> idMap =
                            ScalaConversionUtils.<Attribute>fromSeq(node.output()).stream()
                                .collect(Collectors.toMap(Attribute::name, Attribute::exprId));

                        OutputFieldsCollector.getOutputExpressionsFromTree(cachedPlan).stream()
                            .filter(namedExpression -> idMap.containsKey(namedExpression.name()))
                            .forEach(
                                namedExpression ->
                                    context
                                        .getBuilder()
                                        .addDependency(
                                            namedExpression.exprId(),
                                            idMap.get(namedExpression.name())));
                      });
            }
            return scala.runtime.BoxedUnit.UNIT;
          });
    }
  }
}
