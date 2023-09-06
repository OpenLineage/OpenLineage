/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark34.agent.lifecycle.plan.column;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageVisitor;
import io.openlineage.spark.agent.util.ReflectionUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.column.InputFieldsCollector;
import io.openlineage.spark3.agent.lifecycle.plan.column.OutputFieldsCollector;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.analysis.NamedRelation;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceIcebergData;

@Slf4j
public class MergeIntoIceberg13ColumnLineageVisitor implements ColumnLevelLineageVisitor {
  protected OpenLineageContext context;

  public MergeIntoIceberg13ColumnLineageVisitor(OpenLineageContext context) {
    this.context = context;
  }

  public static boolean hasClasses() {
    return ReflectionUtils.hasClass(
        "org.apache.spark.sql.catalyst.plans.logical.ReplaceIcebergData");
  }

  @Override
  public void collectInputs(LogicalPlan node, ColumnLevelLineageBuilder builder) {
    if (node instanceof ReplaceIcebergData) {
      InputFieldsCollector.collect(context, ((ReplaceIcebergData) node).child(), builder);
      InputFieldsCollector.collect(
          context, (LogicalPlan) ((ReplaceIcebergData) node).table(), builder);
    }
  }

  @Override
  public void collectOutputs(LogicalPlan node, ColumnLevelLineageBuilder builder) {
    if (node instanceof ReplaceIcebergData) {
      OutputFieldsCollector.collect(
          context, (LogicalPlan) ((ReplaceIcebergData) node).table(), builder);
    }
  }

  @Override
  public void collectExpressionDependencies(LogicalPlan node, ColumnLevelLineageBuilder builder) {
    if (node instanceof ReplaceIcebergData) {
      ReplaceIcebergData replaceIcebergData = (ReplaceIcebergData) node;
      NamedRelation namedRelation = replaceIcebergData.table();

      LogicalPlan query = replaceIcebergData.query();

      if (query instanceof Project) {
        List<NamedExpression> queryOutputs =
            ScalaConversionUtils.<NamedExpression>fromSeq(((Project) query).projectList());

        if (queryOutputs.size() != ((LogicalPlan) namedRelation).output().size()) {
          log.warn("Project and table output sizes do not match");
          return;
        }

        for (int i = 0; i < queryOutputs.size(); i++) {
          builder.addDependency(
              ((LogicalPlan) namedRelation).output().apply(i).exprId(),
              queryOutputs.get(i).exprId());
        }
      }
    }
  }
}
