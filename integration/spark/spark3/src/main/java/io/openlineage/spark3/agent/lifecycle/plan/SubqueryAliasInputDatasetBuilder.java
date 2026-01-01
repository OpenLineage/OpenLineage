/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import com.google.common.collect.ImmutableList;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.spark.agent.lifecycle.plan.ViewInputDatasetBuilder;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.AbstractQueryPlanInputDatasetBuilder;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import scala.PartialFunction;

@Slf4j
public class SubqueryAliasInputDatasetBuilder
    extends AbstractQueryPlanInputDatasetBuilder<SubqueryAlias> {

  private final List<PartialFunction<Object, Collection<InputDataset>>> inputDatasetBuilders;

  public SubqueryAliasInputDatasetBuilder(OpenLineageContext context) {
    super(context, true);
    inputDatasetBuilders =
        ImmutableList.<PartialFunction<Object, List<Collection>>>builder()
            .add(new LogicalRelationDatasetBuilder(context, DatasetFactory.input(context), true))
            .add(new MergeIntoCommandInputDatasetBuilder(context))
            .add(new ViewInputDatasetBuilder(context))
            .add(this)
            .build();
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return x instanceof SubqueryAlias && ((SubqueryAlias) x).child() != null;
  }

  @Override
  protected List<InputDataset> apply(SparkListenerEvent event, SubqueryAlias x) {
    if (x.child() instanceof Project) {
      return delegate(event, ((Project) x.child()).child());
    }

    // this should not run query visitors again
    return delegate(event, x.child());
  }

  private List<InputDataset> delegate(SparkListenerEvent event, LogicalPlan node) {
    return delegate(Collections.emptyList(), inputDatasetBuilders, event)
        .applyOrElse(
            node, ScalaConversionUtils.toScalaFn((lp) -> Collections.<InputDataset>emptyList()))
        .stream()
        .collect(Collectors.toList());
  }
}
