/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.HudiUtils;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.StructType;

/**
 * Extracts the output dataset of Hudi's {@code MERGE INTO}, {@code UPDATE}, {@code DELETE} and
 * {@code CREATE TABLE ... AS SELECT} commands. The target relation of the first three is not a
 * child of the command, so it is delegated to the other output builders explicitly. The table a
 * {@code CREATE TABLE ... AS SELECT} creates has no relation yet, so it is built from its catalog
 * entry, with a {@code CREATE} lifecycle state change facet.
 */
public class HudiCommandOutputDatasetBuilder
    extends AbstractQueryPlanOutputDatasetBuilder<LogicalPlan> {

  public HudiCommandOutputDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return HudiUtils.isHudiDmlCommand(x) || HudiUtils.isCreateHoodieTableAsSelectCommand(x);
  }

  @Override
  protected List<OutputDataset> apply(SparkListenerEvent event, LogicalPlan x) {
    if (HudiUtils.isCreateHoodieTableAsSelectCommand(x)) {
      return HudiUtils.ctasTable(x)
          .map(table -> createdTable(x, table))
          .orElse(Collections.emptyList());
    }
    return HudiUtils.dmlTarget(x)
        .map(target -> delegate(target, event))
        .orElse(Collections.emptyList());
  }

  private List<OutputDataset> createdTable(LogicalPlan command, CatalogTable table) {
    if (!context.getSparkSession().isPresent()) {
      return Collections.emptyList();
    }
    // the catalog may not have the schema yet, the query always has it
    StructType schema =
        Optional.of(table.schema())
            .filter(s -> s.fields().length > 0)
            .orElseGet(
                () -> HudiUtils.ctasQuery(command).map(LogicalPlan::schema).orElse(table.schema()));
    return Collections.singletonList(
        outputDataset()
            .sparkDatasetBuilder()
            .dataset(table)
            .schema(schema)
            .lifecycleStateChange(LifecycleStateChange.CREATE)
            .build());
  }

  @Override
  public Optional<String> jobNameSuffix(LogicalPlan plan) {
    if (HudiUtils.isCreateHoodieTableAsSelectCommand(plan)) {
      return HudiUtils.ctasTable(plan).map(table -> tableIdentifierToSuffix(table.identifier()));
    }
    return HudiUtils.dmlTarget(plan)
        .flatMap(target -> ScalaConversionUtils.asJavaOptional(target.catalogTable()))
        .flatMap(
            table ->
                context
                    .getSparkSession()
                    .map(session -> PathUtils.fromCatalogTable(table, session).getName()))
        .map(name -> trimPath(context, name));
  }
}
