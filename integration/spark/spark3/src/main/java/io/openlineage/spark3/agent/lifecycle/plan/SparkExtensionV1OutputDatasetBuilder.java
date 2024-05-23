/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.spark.agent.util.ExtensionPlanUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.extension.scala.v1.OutputDatasetWithDelegate;
import io.openlineage.spark.extension.scala.v1.OutputDatasetWithFacets;
import io.openlineage.spark.extension.scala.v1.OutputDatasetWithIdentifier;
import io.openlineage.spark.extension.scala.v1.OutputLineageNode;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/** {@link LogicalPlan} visitor that matches nodes which implement `OutputLineageNode` interface. */
@Slf4j
public class SparkExtensionV1OutputDatasetBuilder
    extends AbstractQueryPlanOutputDatasetBuilder<LogicalPlan> {

  public SparkExtensionV1OutputDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan logicalPlan) {
    return logicalPlan instanceof OutputLineageNode;
  }

  @Override
  protected List<OpenLineage.OutputDataset> apply(SparkListenerEvent event, LogicalPlan x) {
    // Needs to cast to logical plan despite IntelliJ claiming otherwise.
    OutputLineageNode lineageNode = (OutputLineageNode) x;

    List<OutputDatasetWithFacets> datasets =
        ScalaConversionUtils.<OutputDatasetWithFacets>fromSeq(
            lineageNode.getOutputs(ExtensionPlanUtils.context(event, context)).toSeq());

    // extract datasets with delegate
    List<OutputDataset> outputDatasets =
        datasets.stream()
            .filter(d -> d instanceof OutputDatasetWithDelegate)
            .map(d -> (OutputDatasetWithDelegate) d)
            .map(outputDelegate -> (LogicalPlan) (outputDelegate.node()))
            .flatMap(d -> delegate(event, (LogicalPlan) d).stream())
            .collect(Collectors.toList());

    // extract datasets with identifier
    outputDatasets.addAll(
        datasets.stream()
            .filter(d -> d instanceof OutputDatasetWithIdentifier)
            .map(d -> (OutputDatasetWithIdentifier) d)
            .map(
                d ->
                    getContext()
                        .getOpenLineage()
                        .newOutputDatasetBuilder()
                        .namespace(d.datasetIdentifier().getNamespace())
                        .name(d.datasetIdentifier().getName())
                        .facets(d.facetsBuilder().build())
                        .outputFacets(d.outputFacetsBuilder().build())
                        .build())
            .collect(Collectors.toList()));

    return outputDatasets;
  }

  protected List<OutputDataset> delegate(SparkListenerEvent event, LogicalPlan plan) {
    return delegate(
            context.getOutputDatasetQueryPlanVisitors(), context.getOutputDatasetBuilders(), event)
        .applyOrElse(
            plan,
            ScalaConversionUtils.toScalaFn(
                (lp) -> Collections.<OpenLineage.OutputDataset>emptyList()))
        .stream()
        .collect(Collectors.toList());
  }

  /**
   * For testing purpose
   *
   * @return
   */
  protected OpenLineageContext getContext() {
    return context;
  }
}
