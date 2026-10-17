/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.Dataset;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.spark.agent.lifecycle.plan.HiveTableRelationVisitor;
import io.openlineage.spark.agent.lifecycle.plan.InsertIntoHiveTableDatasetBuilder;
import io.openlineage.spark.agent.lifecycle.plan.KafkaRelationVisitor;
import io.openlineage.spark.agent.lifecycle.plan.KustoRelationVisitor;
import io.openlineage.spark.agent.lifecycle.plan.LogicalRDDVisitor;
import io.openlineage.spark.agent.lifecycle.plan.SqlDWDatabricksVisitor;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.PartialFunction;

abstract class BaseVisitorFactory implements VisitorFactory {

  protected <D extends Dataset> List<PartialFunction<LogicalPlan, List<D>>> getBaseCommonVisitors(
      OpenLineageContext context, DatasetFactory<D> factory) {
    List<PartialFunction<LogicalPlan, List<D>>> list = new ArrayList<>();
    list.add(new LogicalRDDVisitor(context, factory));
    if (KafkaRelationVisitor.hasKafkaClasses()) {
      list.add(new KafkaRelationVisitor(context, factory));
    }
    if (SqlDWDatabricksVisitor.hasSqlDWDatabricksClasses()) {
      list.add(new SqlDWDatabricksVisitor(context, factory));
    }
    if (InsertIntoHiveTableDatasetBuilder.hasHiveClasses()) {
      list.add(new HiveTableRelationVisitor<>(context, factory));
    }
    if (KustoRelationVisitor.hasKustoClasses()) {
      list.add(new KustoRelationVisitor(context, factory));
    }
    return list;
  }

  public abstract <D extends Dataset> List<PartialFunction<LogicalPlan, List<D>>> getCommonVisitors(
      OpenLineageContext context, DatasetFactory<D> factory);

  @Override
  public List<PartialFunction<LogicalPlan, List<InputDataset>>> getInputVisitors(
      OpenLineageContext context) {
    DatasetFactory<InputDataset> factory = DatasetFactory.input(context);
    return new ArrayList<>(getCommonVisitors(context, factory));
  }

  @Override
  public List<PartialFunction<LogicalPlan, List<OpenLineage.OutputDataset>>> getOutputVisitors(
      OpenLineageContext context) {
    DatasetFactory<OpenLineage.OutputDataset> factory = DatasetFactory.output(context);

    return new ArrayList<>(getCommonVisitors(context, factory));
  }
}
