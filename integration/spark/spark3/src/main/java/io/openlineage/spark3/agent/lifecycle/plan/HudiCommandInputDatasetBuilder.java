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
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.MergeIntoTable;
import scala.PartialFunction;

/**
 * Extracts the input datasets of Hudi's {@code INSERT INTO/OVERWRITE}, {@code MERGE INTO}, {@code
 * UPDATE}, {@code DELETE} and {@code CREATE TABLE ... AS SELECT} commands. The target table, which
 * the DML statements read, the {@code MERGE} source and the subqueries of the DML statements are
 * not children of the command, and the {@code INSERT} and {@code CREATE TABLE ... AS SELECT} query
 * is one only from Hudi 1.x, so they are traversed explicitly with the other input builders.
 * Visited-node tracking keeps children from being counted twice.
 */
public class HudiCommandInputDatasetBuilder
    extends AbstractQueryPlanInputDatasetBuilder<LogicalPlan> {

  public HudiCommandInputDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return HudiUtils.isInsertIntoHoodieTableCommand(x)
        || HudiUtils.isHudiDmlCommand(x)
        || HudiUtils.isCreateHoodieTableAsSelectCommand(x);
  }

  @Override
  protected List<InputDataset> apply(SparkListenerEvent event, LogicalPlan x) {
    if (HudiUtils.isInsertIntoHoodieTableCommand(x)) {
      return HudiUtils.insertQuery(x).map(query -> collect(query, event)).orElseGet(ArrayList::new);
    }
    if (HudiUtils.isCreateHoodieTableAsSelectCommand(x)) {
      return HudiUtils.ctasQuery(x).map(query -> collect(query, event)).orElseGet(ArrayList::new);
    }
    List<InputDataset> datasets = new ArrayList<>();
    HudiUtils.dmlTarget(x).ifPresent(target -> datasets.addAll(delegate(target, event)));
    HudiUtils.mergeInto(x)
        .map(MergeIntoTable::sourceTable)
        .ifPresent(source -> datasets.addAll(collect(source, event)));
    HudiUtils.dmlSubqueries(x).forEach(subquery -> datasets.addAll(collect(subquery, event)));
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
