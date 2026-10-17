/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import com.google.common.collect.ImmutableList;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.Dataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.spark.agent.lifecycle.plan.WriteToDataSourceV2Visitor;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.DropTableVisitor;
import io.openlineage.spark32.agent.lifecycle.plan.RepairTableCommandVisitor;
import java.util.List;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.PartialFunction;

class Spark32VisitorFactoryImpl extends BaseVisitorFactory {

  @Override
  public List<PartialFunction<LogicalPlan, List<OpenLineage.OutputDataset>>> getOutputVisitors(
      OpenLineageContext context) {
    return ImmutableList.<PartialFunction<LogicalPlan, List<OutputDataset>>>builder()
        .addAll(super.getOutputVisitors(context))
        .add(new DropTableVisitor(context))
        .add(new WriteToDataSourceV2Visitor(context))
        .add(new RepairTableCommandVisitor(context))
        .build();
  }

  @Override
  public <D extends Dataset> List<PartialFunction<LogicalPlan, List<D>>> getCommonVisitors(
      OpenLineageContext context, DatasetFactory<D> factory) {
    return super.getBaseCommonVisitors(context, factory);
  }
}
