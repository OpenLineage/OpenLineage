/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.AbstractQueryPlanInputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.HudiUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.MergeIntoTable;
import scala.PartialFunction;

/**
 * Extracts the input datasets of Hudi's {@code MERGE INTO}, {@code UPDATE}, {@code DELETE} and
 * {@code CREATE TABLE ... AS SELECT} commands. The target table, which these statements read, and
 * the {@code MERGE} source are not children of the command, and the {@code CREATE TABLE ... AS
 * SELECT} query is one only from Hudi 1.x, so they are traversed explicitly with the other input
 * builders. Visited-node tracking keeps children from being counted twice.
 */
public class HudiCommandInputDatasetBuilder
    extends AbstractQueryPlanInputDatasetBuilder<LogicalPlan> {

  public HudiCommandInputDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return HudiUtils.isHudiDmlCommand(x) || HudiUtils.isCreateHoodieTableAsSelectCommand(x);
  }

  @Override
  protected List<InputDataset> apply(SparkListenerEvent event, LogicalPlan x) {
    List<InputDataset> datasets = new ArrayList<>();
    HudiUtils.dmlTarget(x).ifPresent(target -> datasets.addAll(delegate(target, event)));
    Optional<LogicalPlan> query =
        HudiUtils.isCreateHoodieTableAsSelectCommand(x)
            ? HudiUtils.ctasQuery(x)
            : HudiUtils.mergeInto(x).map(MergeIntoTable::sourceTable);
    query.ifPresent(plan -> datasets.addAll(collect(plan, event)));
    return datasets;
  }

  private List<InputDataset> collect(LogicalPlan plan, SparkListenerEvent event) {
    PartialFunction<LogicalPlan, Collection<InputDataset>> delegateFn =
        delegate(
            context.getInputDatasetQueryPlanVisitors(), context.getInputDatasetBuilders(), event);
    List<InputDataset> datasets = new ArrayList<>();
    ScalaConversionUtils.fromSeq(plan.collect(delegateFn)).forEach(datasets::addAll);
    return datasets;
  }
}
