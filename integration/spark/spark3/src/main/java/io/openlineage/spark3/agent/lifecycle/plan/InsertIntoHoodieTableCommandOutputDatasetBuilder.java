/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.HudiUtils;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

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

  private OutputDataset withOverwriteLifecycle(OutputDataset dataset) {
    OpenLineage openLineage = context.getOpenLineage();
    OpenLineage.DatasetFacets facets = dataset.getFacets();
    OpenLineage.DatasetFacetsBuilder facetsBuilder = openLineage.newDatasetFacetsBuilder();
    if (facets != null) {
      facetsBuilder
          .hierarchy(facets.getHierarchy())
          .dataSource(facets.getDataSource())
          .version(facets.getVersion())
          .datasetType(facets.getDatasetType())
          .storage(facets.getStorage())
          .columnLineage(facets.getColumnLineage())
          .dataQualityMetrics(facets.getDataQualityMetrics())
          .tags(facets.getTags())
          .documentation(facets.getDocumentation())
          .schema(facets.getSchema())
          .ownership(facets.getOwnership())
          .catalog(facets.getCatalog())
          .symlinks(facets.getSymlinks())
          .lineage(facets.getLineage());
      facets.getAdditionalProperties().forEach(facetsBuilder::put);
    }
    facetsBuilder.lifecycleStateChange(
        openLineage
            .newLifecycleStateChangeDatasetFacetBuilder()
            .lifecycleStateChange(
                OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE)
            .build());
    return openLineage
        .newOutputDatasetBuilder()
        .namespace(dataset.getNamespace())
        .name(dataset.getName())
        .facets(facetsBuilder.build())
        .outputFacets(dataset.getOutputFacets())
        .build();
  }
}
