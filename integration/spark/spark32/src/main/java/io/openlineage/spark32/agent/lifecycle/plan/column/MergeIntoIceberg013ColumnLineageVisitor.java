/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark32.agent.lifecycle.plan.column;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageVisitor;
import io.openlineage.spark.agent.util.ReflectionUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.column.InputFieldsCollector;
import io.openlineage.spark3.agent.lifecycle.plan.column.OutputFieldsCollector;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.analysis.NamedRelation;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.MergeRows;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceData;
import org.apache.spark.sql.catalyst.plans.logical.WriteDelta;

@Slf4j
public class MergeIntoIceberg013ColumnLineageVisitor implements ColumnLevelLineageVisitor {
  protected OpenLineageContext context;

  public MergeIntoIceberg013ColumnLineageVisitor(OpenLineageContext context) {
    this.context = context;
  }

  public static boolean hasClasses() {
    return ReflectionUtils.hasClass("org.apache.spark.sql.catalyst.plans.logical.ReplaceData")
        && ReflectionUtils.hasClass("org.apache.spark.sql.catalyst.plans.logical.WriteDelta");
  }

  @Override
  public void collectInputs(ColumnLevelLineageContext context, LogicalPlan node) {
    if (node instanceof ReplaceData) {
      InputFieldsCollector.collect(context, ((ReplaceData) node).child());
      InputFieldsCollector.collect(context, (LogicalPlan) ((ReplaceData) node).table());
    }
    if (node instanceof WriteDelta) {
      InputFieldsCollector.collect(context, ((WriteDelta) node).child());
      InputFieldsCollector.collect(context, (LogicalPlan) ((WriteDelta) node).table());
    }
  }

  @Override
  public void collectOutputs(ColumnLevelLineageContext context, LogicalPlan node) {
    if (node instanceof ReplaceData) {
      OutputFieldsCollector.collect(context, (LogicalPlan) ((ReplaceData) node).table());
    }
    if (node instanceof WriteDelta) {
      OutputFieldsCollector.collect(context, (LogicalPlan) ((WriteDelta) node).table());
    }
  }

  @Override
  public void collectExpressionDependencies(ColumnLevelLineageContext context, LogicalPlan node) {
    if (node instanceof ReplaceData) {
      ReplaceData replaceIcebergData = (ReplaceData) node;
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
          context
              .getBuilder()
              .addDependency(
                  ((LogicalPlan) namedRelation).output().apply(i).exprId(),
                  queryOutputs.get(i).exprId());
        }
      }
    }

    if (node instanceof WriteDelta) {
      WriteDelta writeIcebergDelta = (WriteDelta) node;
      NamedRelation namedRelation = writeIcebergDelta.table();
      LogicalPlan query = writeIcebergDelta.query();
      LogicalPlan originalQuery = query;

      log.debug("Query is a {}", query);

      while (query.children().length() > 0) {
        if (query instanceof MergeRows) {
          List<Attribute> inputs = ScalaConversionUtils.fromSeq(originalQuery.output());
          List<Attribute> outputs =
              ScalaConversionUtils.fromSeq(((LogicalPlan) namedRelation).output());

          for (int i = 0; i < outputs.size(); i++) {
            if (log.isDebugEnabled()) {
              log.debug(
                  "Adding dependency: {}, {}", outputs.get(i).exprId(), inputs.get(i + 1).exprId());
            }
            context.getBuilder().addDependency(outputs.get(i).exprId(), inputs.get(i + 1).exprId());
          }
          break;
        } else {
          query = query.children().apply(0);
        }
      }

      // RebalancePartitions rp = (RebalancePartitions) writeIcebergDelta.query() ;
      // log.debug("RP output {}", rp.output());

      // MergeRows mr = (MergeRows) rp.child();
      // log.debug("MR output {}", mr.output());

      // log.debug("Projections: {}", writeIcebergDelta.projections());
    }
  }
}
