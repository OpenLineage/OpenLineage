/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.DatasetFacetsUtils;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.InsertIntoDataSourceCommand;

/**
 * {@link LogicalPlan} visitor that matches an {@link InsertIntoDataSourceCommand} and extracts the
 * output {@link OpenLineage.Dataset} being written.
 */
public class InsertIntoDataSourceVisitor
    extends QueryPlanVisitor<InsertIntoDataSourceCommand, OpenLineage.OutputDataset> {

  public InsertIntoDataSourceVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    InsertIntoDataSourceCommand command = (InsertIntoDataSourceCommand) x;

    return PlanUtils.applyAll(
            context.getOutputDatasetQueryPlanVisitors(), command.logicalRelation())
        .stream()
        // constructed datasets don't include the output stats, so add that facet here
        .map(
            ds -> {
              Builder<String, OpenLineage.DatasetFacet> facetsMap =
                  ImmutableMap.<String, OpenLineage.DatasetFacet>builder();
              if (ds.getFacets().getAdditionalProperties() != null) {
                facetsMap.putAll(ds.getFacets().getAdditionalProperties());
              }
              ds.getFacets().getAdditionalProperties().putAll(facetsMap.build());
              if (command.overwrite()) {
                // rebuild whole dataset with a LifecycleStateChange facet added
                OpenLineage.DatasetFacets facets =
                    DatasetFacetsUtils.copyToBuilder(context, ds.getFacets())
                        .lifecycleStateChange(
                            context
                                .getOpenLineage()
                                .newLifecycleStateChangeDatasetFacet(
                                    OpenLineage.LifecycleStateChangeDatasetFacet
                                        .LifecycleStateChange.OVERWRITE,
                                    null))
                        .build();

                OpenLineage.OutputDataset newDs =
                    context
                        .getOpenLineage()
                        .newOutputDataset(
                            ds.getNamespace(), ds.getName(), facets, ds.getOutputFacets());
                return newDs;
              }
              return ds;
            })
        .collect(Collectors.toList());
  }
}
