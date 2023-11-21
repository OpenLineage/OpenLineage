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
import io.openlineage.spark.extension.scala.v1.InputDatasetWithDelegate;
import io.openlineage.spark.extension.scala.v1.InputDatasetWithFacets;
import io.openlineage.spark.extension.scala.v1.InputDatasetWithIdentifier;
import io.openlineage.spark.extension.scala.v1.InputLineageNode;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/** {@link LogicalPlan} visitor that matches nodes which implement `InputLineageNode` interface. */
@Slf4j
public class SparkExtensionV1InputDatasetBuilder
    extends AbstractQueryPlanInputDatasetBuilder<LogicalPlan> {

  public SparkExtensionV1InputDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan logicalPlan) {
    return logicalPlan instanceof InputLineageNode;
  }

  @Override
  protected List<InputDataset> apply(SparkListenerEvent event, LogicalPlan x) {
    // Needs to cast to logical plan despite IntelliJ claiming otherwise.
    InputLineageNode lineageNode = (InputLineageNode) x;

    List<InputDatasetWithFacets> datasets =
        ScalaConversionUtils.<InputDatasetWithFacets>fromSeq(
            lineageNode.getInputs(ExtensionPlanUtils.context(context)).toList().toSeq());

    // extract datasets with delegate
    List<InputDataset> inputDatasets =
        datasets.stream()
            .filter(d -> d instanceof InputDatasetWithDelegate)
            .map(d -> (InputDatasetWithDelegate) d)
            .map(inputDelegate -> (LogicalPlan) (inputDelegate.node()))
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
                        .namespace(d.datasetIdentifier().getNamespace())
                        .name(d.datasetIdentifier().getName())
                        .facets(d.facetsBuilder().build())
                        .inputFacets(d.inputFacetsBuilder().build())
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
