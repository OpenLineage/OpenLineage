/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import com.google.common.collect.ImmutableList;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark2.agent.lifecycle.plan.CreateTableLikeCommandVisitor;
import java.util.List;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.PartialFunction;

class Spark2VisitorFactoryImpl extends BaseVisitorFactory {

  @Override
  public List<PartialFunction<LogicalPlan, List<OutputDataset>>> getOutputVisitors(
      OpenLineageContext context) {
    return ImmutableList.<PartialFunction<LogicalPlan, List<OpenLineage.OutputDataset>>>builder()
        .addAll(super.getOutputVisitors(context))
        .add(new CreateTableLikeCommandVisitor(context))
        .build();
  }

  @Override
  public <D extends OpenLineage.Dataset>
      List<PartialFunction<LogicalPlan, List<D>>> getCommonVisitors(
          OpenLineageContext context, DatasetFactory<D> factory) {
    return ImmutableList.<PartialFunction<LogicalPlan, List<D>>>builder()
        .addAll(super.getBaseCommonVisitors(context, factory))
        .build();
  }
}
