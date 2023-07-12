/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark34.agent.lifecycle.plan.column;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageVisitor;
import io.openlineage.spark.agent.util.ReflectionUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.DeltaMergeIntoMatchedClause;
import org.apache.spark.sql.catalyst.plans.logical.DeltaMergeIntoNotMatchedClause;
import org.apache.spark.sql.delta.commands.MergeIntoCommand;

@Slf4j
public class MergeIntoDelta24ColumnLineageVisitor
    extends io.openlineage.spark3.agent.lifecycle.plan.column.MergeIntoDeltaColumnLineageVisitor
    implements ColumnLevelLineageVisitor {

  public static boolean hasClasses() {
    return ReflectionUtils.hasClasses(
        "org.apache.spark.sql.delta.commands.MergeIntoCommand",
        "org.apache.spark.sql.catalyst.plans.logical.DeltaMergeIntoNotMatchedClause");
  }

  public MergeIntoDelta24ColumnLineageVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public Stream<Expression> getMergeActions(MergeIntoCommand mergeIntoCommand) {
    return Stream.concat(
        ScalaConversionUtils.<DeltaMergeIntoMatchedClause>fromSeq(mergeIntoCommand.matchedClauses())
            .stream()
            .flatMap(clause -> ScalaConversionUtils.<Expression>fromSeq(clause.actions()).stream()),
        ScalaConversionUtils.<DeltaMergeIntoNotMatchedClause>fromSeq(
                mergeIntoCommand
                    .notMatchedClauses()) // DeltaMergeIntoNotMatchedClause class does not
            // exist in earlier versions
            .stream()
            .flatMap(
                clause -> ScalaConversionUtils.<Expression>fromSeq(clause.actions()).stream()));
  }
}
