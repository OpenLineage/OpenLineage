/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark35.agent.lifecycle.plan;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageVisitor;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionTraverser;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.MergeRows;
import org.apache.spark.sql.catalyst.plans.logical.MergeRows.Instruction;
import scala.collection.Seq;

@Slf4j
public class MergeRowsColumnLineageVisitor implements ColumnLevelLineageVisitor {

  protected OpenLineageContext context;

  public MergeRowsColumnLineageVisitor(OpenLineageContext context) {
    this.context = context;
  }

  @Override
  public void collectInputs(ColumnLevelLineageContext context, LogicalPlan node) {
    return;
  }

  @Override
  public void collectOutputs(ColumnLevelLineageContext context, LogicalPlan node) {
    return;
  }

  @Override
  public void collectExpressionDependencies(ColumnLevelLineageContext context, LogicalPlan node) {
    if (!(node instanceof MergeRows)) {
      return;
    }

    MergeRows mergeRows = (MergeRows) node;

    List<Instruction> instructions = new ArrayList<>();
    instructions.addAll(ScalaConversionUtils.<Instruction>fromSeq(mergeRows.matchedInstructions()));
    instructions.addAll(
        ScalaConversionUtils.<Instruction>fromSeq(mergeRows.notMatchedInstructions()));
    instructions.addAll(
        ScalaConversionUtils.<Instruction>fromSeq(mergeRows.notMatchedBySourceInstructions()));

    IntStream.range(0, mergeRows.output().size())
        .forEach(
            position -> {
              instructions.stream()
                  .forEach(
                      instruction -> {
                        ScalaConversionUtils.fromSeq(instruction.outputs()).stream()
                            .map(s -> ScalaConversionUtils.<Expression>fromSeq((Seq<Expression>) s))
                            .filter(l -> l.size() > position)
                            .map(l -> l.get(position))
                            .filter(expr -> expr instanceof NamedExpression)
                            .forEach(
                                expr ->
                                    ExpressionTraverser.of(
                                            expr,
                                            mergeRows.output().apply(position).exprId(),
                                            context.getBuilder())
                                        .traverse());
                      });
            });
  }
}
