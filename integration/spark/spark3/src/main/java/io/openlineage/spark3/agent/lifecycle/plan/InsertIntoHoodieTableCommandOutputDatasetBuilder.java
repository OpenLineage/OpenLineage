/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.spark.agent.util.DatasetFacetsUtils;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.HudiUtils;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.LogicalRelation;

/**
 * Extracts the output dataset of Hudi's {@code InsertIntoHoodieTableCommand} (Hudi SQL {@code
 * INSERT INTO/OVERWRITE}). The target relation is not a child of the command, so it is delegated to
 * the other output builders explicitly. {@code INSERT OVERWRITE} adds an {@code OVERWRITE}
 * lifecycle state change facet.
 */
public class InsertIntoHoodieTableCommandOutputDatasetBuilder
    extends AbstractQueryPlanOutputDatasetBuilder<LogicalPlan> {

  public InsertIntoHoodieTableCommandOutputDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return HudiUtils.isInsertIntoHoodieTableCommand(x);
  }

  @Override
  protected List<OutputDataset> apply(SparkListenerEvent event, LogicalPlan x) {
    List<OutputDataset> outputs =
        HudiUtils.insertTarget(x)
            .map(target -> delegate(target, event))
            .orElse(Collections.emptyList());
    if (!HudiUtils.isInsertOverwrite(x)) {
      return outputs;
    }
    return outputs.stream().map(this::withOverwriteLifecycle).collect(Collectors.toList());
  }

  @Override
  public Optional<String> jobNameSuffix(LogicalPlan plan) {
    return HudiUtils.insertTarget(plan)
        .filter(LogicalRelation.class::isInstance)
        .flatMap(
            target ->
                ScalaConversionUtils.asJavaOptional(((LogicalRelation) target).catalogTable()))
        .flatMap(this::targetName)
        .map(name -> trimPath(context, name));
  }

  private Optional<String> targetName(CatalogTable table) {
    return context
        .getSparkSession()
        .map(session -> PathUtils.fromCatalogTable(table, session).getName());
  }

  private OutputDataset withOverwriteLifecycle(OutputDataset dataset) {
    OpenLineage openLineage = context.getOpenLineage();
    OpenLineage.DatasetFacets facets =
        DatasetFacetsUtils.copyToBuilder(context, dataset.getFacets())
            .lifecycleStateChange(
                openLineage
                    .newLifecycleStateChangeDatasetFacetBuilder()
                    .lifecycleStateChange(
                        OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE)
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
}
