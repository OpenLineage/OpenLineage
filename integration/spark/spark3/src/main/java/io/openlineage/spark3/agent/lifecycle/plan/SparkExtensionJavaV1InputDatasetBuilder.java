/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.spark.agent.util.ExtensionPlanUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.AbstractQueryPlanInputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.extension.v1.InputDatasetWithDelegate;
import io.openlineage.spark.extension.v1.InputDatasetWithFacets;
import io.openlineage.spark.extension.v1.InputDatasetWithIdentifier;
import io.openlineage.spark.extension.v1.InputLineageNode;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/** {@link LogicalPlan} visitor that matches nodes which implement `InputLineageNode` interface. */
@Slf4j
public class SparkExtensionJavaV1InputDatasetBuilder
    extends AbstractQueryPlanInputDatasetBuilder<LogicalPlan> {

  public SparkExtensionJavaV1InputDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan logicalPlan) {
    return logicalPlan instanceof InputLineageNode;
  }

  @Override
  protected List<InputDataset> apply(SparkListenerEvent event, LogicalPlan x) {
    List<InputDatasetWithFacets> datasets =
        ((InputLineageNode) x).getInputs(ExtensionPlanUtils.javaContext(event, context));

    // extract datasets with delegate
    List<InputDataset> inputDatasets =
        datasets.stream()
            .filter(d -> d instanceof InputDatasetWithDelegate)
            .map(d -> (InputDatasetWithDelegate) d)
            .map(inputDelegate -> (LogicalPlan) (inputDelegate.getNode()))
            .flatMap(d -> delegate(event, (LogicalPlan) d).stream())
            .collect(Collectors.toList());

    // extract datasets with identifier
    inputDatasets.addAll(
        datasets.stream()
            .filter(d -> d instanceof InputDatasetWithIdentifier)
            .map(d -> (InputDatasetWithIdentifier) d)
            .map(
                d ->
                    getContext()
                        .getOpenLineage()
                        .newInputDatasetBuilder()
                        .namespace(d.getDatasetIdentifier().getNamespace())
                        .name(d.getDatasetIdentifier().getName())
                        .facets(d.getDatasetFacetBuilder().build())
                        .inputFacets(d.getInputFacetsBuilder().build())
                        .build())
            .collect(Collectors.toList()));

    return inputDatasets;
  }

  protected List<InputDataset> delegate(SparkListenerEvent event, LogicalPlan plan) {
    return delegate(
            context.getInputDatasetQueryPlanVisitors(), context.getInputDatasetBuilders(), event)
        .applyOrElse(
            plan, ScalaConversionUtils.toScalaFn((lp) -> Collections.<InputDataset>emptyList()))
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
