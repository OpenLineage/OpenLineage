/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.spark.agent.util.DatasetFacetsUtils;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkDatasetBuilder;
import io.openlineage.spark3.agent.utils.HudiUtils;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.types.StructType;

/**
 * Extracts the output dataset of Hudi's {@code INSERT INTO/OVERWRITE}, {@code MERGE INTO}, {@code
 * UPDATE}, {@code DELETE} and {@code CREATE TABLE ... AS SELECT} commands. The target relation of
 * the first four is not a child of the command, so it is delegated to the other output builders
 * explicitly; {@code INSERT OVERWRITE} adds an {@code OVERWRITE} lifecycle state change facet. The
 * table a {@code CREATE TABLE ... AS SELECT} writes may have no relation yet, so it is built from
 * its catalog entry, with a lifecycle state change facet that follows the command's save mode.
 */
public class HudiCommandOutputDatasetBuilder
    extends AbstractQueryPlanOutputDatasetBuilder<LogicalPlan> {

  public HudiCommandOutputDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return HudiUtils.isInsertIntoHoodieTableCommand(x)
        || HudiUtils.isHudiDmlCommand(x)
        || HudiUtils.isCreateHoodieTableAsSelectCommand(x);
  }

  @Override
  protected List<OutputDataset> apply(SparkListenerEvent event, LogicalPlan x) {
    if (HudiUtils.isCreateHoodieTableAsSelectCommand(x)) {
      return HudiUtils.ctasTable(x)
          .map(table -> catalogTableDataset(table, ctasSchema(x, table), ctasLifecycle(x)))
          .orElse(Collections.emptyList());
    }
    if (HudiUtils.isInsertIntoHoodieTableCommand(x)) {
      List<OutputDataset> outputs =
          HudiUtils.insertTarget(x)
              .map(target -> delegate(target, event))
              .orElse(Collections.emptyList());
      if (!HudiUtils.isInsertOverwrite(x)) {
        return outputs;
      }
      return outputs.stream().map(this::withOverwriteLifecycle).collect(Collectors.toList());
    }
    Optional<LogicalRelation> target = HudiUtils.dmlTarget(x);
    if (target.isPresent()) {
      return delegate(target.get(), event);
    }
    // the query of a Hudi 1.x DELETE whose predicate folded to false reads no relation
    return HudiUtils.dmlCatalogTable(x)
        .map(table -> catalogTableDataset(table, table.schema(), Optional.empty()))
        .orElse(Collections.emptyList());
  }

  private List<OutputDataset> catalogTableDataset(
      CatalogTable table, StructType schema, Optional<LifecycleStateChange> lifecycle) {
    if (!context.getSparkSession().isPresent()) {
      return Collections.emptyList();
    }
    SparkDatasetBuilder<OutputDataset> builder =
        outputDataset().sparkDatasetBuilder().dataset(table).schema(schema);
    lifecycle.ifPresent(builder::lifecycleStateChange);
    return Collections.singletonList(builder.build());
  }

  private static StructType ctasSchema(LogicalPlan command, CatalogTable table) {
    // the catalog may not have the schema yet, the query always has it
    return Optional.ofNullable(table.schema())
        .filter(s -> s.fields().length > 0)
        .orElseGet(
            () -> HudiUtils.ctasQuery(command).map(LogicalPlan::schema).orElse(table.schema()));
  }

  /**
   * {@code CREATE OR REPLACE TABLE ... AS SELECT} and {@code saveAsTable} in overwrite mode replace
   * the table, and {@code saveAsTable} in append mode only adds to it.
   */
  static Optional<LifecycleStateChange> ctasLifecycle(LogicalPlan command) {
    SaveMode mode = HudiUtils.ctasMode(command).orElse(SaveMode.ErrorIfExists);
    switch (mode) {
      case Overwrite:
        return Optional.of(LifecycleStateChange.OVERWRITE);
      case Append:
        return Optional.empty();
      default:
        return Optional.of(LifecycleStateChange.CREATE);
    }
  }

  private OutputDataset withOverwriteLifecycle(OutputDataset dataset) {
    OpenLineage openLineage = context.getOpenLineage();
    OpenLineage.DatasetFacets facets =
        DatasetFacetsUtils.copyToBuilder(context, dataset.getFacets())
            .lifecycleStateChange(
                openLineage
                    .newLifecycleStateChangeDatasetFacetBuilder()
                    .lifecycleStateChange(LifecycleStateChange.OVERWRITE)
                    .build())
            .build();
    return openLineage
        .newOutputDatasetBuilder()
        .namespace(dataset.getNamespace())
        .name(dataset.getName())
        .facets(facets)
        .outputFacets(dataset.getOutputFacets())
        .build();
  }

  @Override
  public Optional<String> jobNameSuffix(LogicalPlan plan) {
    if (HudiUtils.isCreateHoodieTableAsSelectCommand(plan)) {
      return HudiUtils.ctasTable(plan).map(table -> tableIdentifierToSuffix(table.identifier()));
    }
    Optional<LogicalRelation> target =
        HudiUtils.isInsertIntoHoodieTableCommand(plan)
            ? HudiUtils.insertTarget(plan)
                .filter(LogicalRelation.class::isInstance)
                .map(LogicalRelation.class::cast)
            : HudiUtils.dmlTarget(plan);
    Optional<CatalogTable> table =
        target.isPresent()
            ? ScalaConversionUtils.asJavaOptional(target.get().catalogTable())
            : HudiUtils.dmlCatalogTable(plan);
    return table
        .flatMap(
            catalogTable ->
                context
                    .getSparkSession()
                    .map(session -> PathUtils.fromCatalogTable(catalogTable, session).getName()))
        .map(name -> trimPath(context, name));
  }
}
