/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.dataset.namespace.resolver.DatasetNamespaceCombinedResolver;
import io.openlineage.spark.agent.lifecycle.plan.column.CheckpointColumnLineage;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.ColumnLineageConfig;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import java.util.List;
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
import org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand;

/**
 * Utility functions for detecting column level lineage within {@link
 * org.apache.spark.sql.catalyst.plans.logical.LogicalPlan}.
 */
@Slf4j
public class ColumnLevelLineageUtils {

  private static final String CREATE_HIVE_TABLE_AS_SELECT_COMMAND =
      "org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand";

  public static Optional<OpenLineage.ColumnLineageDatasetFacet> buildColumnLineageDatasetFacet(
      SparkListenerEvent event,
      OpenLineageContext olContext,
      OpenLineage.SchemaDatasetFacet schemaFacet) {
    Optional<ColumnLevelLineageContext> collected =
        collectColumnLineage(event, olContext, schemaFacet);
    if (!collected.isPresent()) {
      return Optional.empty();
    }
    ColumnLevelLineageContext context = collected.get();

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
   * Captures column lineage of a checkpointed plan, see {@link
   * io.openlineage.spark.api.Checkpoint}. Unlike {@link #buildColumnLineageDatasetFacet}, it's
   * returned even if no field level lineage was found, so that dataset-level dependencies alone
   * (e.g. columns used only for filtering) aren't lost.
   */
  public static Optional<CheckpointColumnLineage> buildCheckpointColumnLineage(
      SparkListenerEvent event,
      OpenLineageContext olContext,
      OpenLineage.SchemaDatasetFacet schemaFacet) {
    return collectColumnLineage(event, olContext, schemaFacet)
        .map(context -> context.getBuilder().buildCheckpointColumnLineage());
  }

  private static Optional<ColumnLevelLineageContext> collectColumnLineage(
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
    return Optional.of(context);
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
    } else if (PlanUtils.safeIsInstanceOf(logicalPlan, CREATE_HIVE_TABLE_AS_SELECT_COMMAND)) {
      plan = ((CreateHiveTableAsSelectCommand) logicalPlan).query();
    } else {
      plan = logicalPlan;
    }
    return plan;
  }

  /**
   * Makes each output column of an {@link InMemoryRelation} depend on the cached plan's output
   * column at the same position.
   *
   * <p>Spark serves a cached plan to a query through {@code InMemoryRelation.withOutput}, which
   * renames the cached columns to the ones the query expects but keeps their order, so the position
   * is reliable where names are not: Spark 4.1, for instance, answers {@code cached.select(a AS
   * c).cache().select(c AS e)} straight from the first cache, with an {@code InMemoryRelation}
   * whose output is {@code e} over a cached plan whose output is {@code a}.
   *
   * @return {@code false} when the outputs differ in size and no dependency was added
   */
  static boolean mapCachedOutputsByPosition(
      ColumnLevelLineageContext context, LogicalPlan inMemoryRelation, LogicalPlan cachedPlan) {
    List<Attribute> relationOutput = ScalaConversionUtils.fromSeq(inMemoryRelation.output());
    List<Attribute> cachedOutput = ScalaConversionUtils.fromSeq(cachedPlan.output());
    if (relationOutput.size() != cachedOutput.size()) {
      return false;
    }
    for (int i = 0; i < relationOutput.size(); i++) {
      context
          .getBuilder()
          .addDependency(
              relationOutput.get(i).exprId(),
              cachedOutput.get(i).exprId(),
              relationOutput.get(i).qualifiedName());
    }
    return true;
  }

  private static void mapCachedOutputsByName(
      ColumnLevelLineageContext context, LogicalPlan inMemoryRelation, LogicalPlan cachedPlan) {
    Map<String, ExprId> idMap =
        ScalaConversionUtils.<Attribute>fromSeq(inMemoryRelation.output()).stream()
            .collect(Collectors.toMap(Attribute::qualifiedName, Attribute::exprId));

    OutputFieldsCollector.getOutputExpressionsFromTree(cachedPlan).stream()
        .filter(namedExpression -> idMap.containsKey(namedExpression.qualifiedName()))
        .forEach(
            namedExpression ->
                context
                    .getBuilder()
                    .addDependency(
                        namedExpression.exprId(),
                        idMap.get(namedExpression.name()),
                        namedExpression.qualifiedName()));
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

                        // Map outputs of cachedPlan onto inputs of InMemoryRelation. Matching by
                        // name as well would add links in the opposite direction, which chain
                        // into wrong lineage when the query swaps column names.
                        if (!mapCachedOutputsByPosition(context, node, cachedPlan)) {
                          mapCachedOutputsByName(context, node, cachedPlan);
                        }
                      });
            }
            return scala.runtime.BoxedUnit.UNIT;
          });
    }
  }
}
