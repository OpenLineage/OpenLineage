/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark32.agent.lifecycle.plan.column;

import io.openlineage.spark.agent.util.ReflectionUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.stream.Stream;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.DeltaMergeIntoInsertClause;
import org.apache.spark.sql.catalyst.plans.logical.DeltaMergeIntoMatchedClause;
import org.apache.spark.sql.delta.commands.MergeIntoCommand;

public class MergeIntoDelta11ColumnLineageVisitor
    extends io.openlineage.spark3.agent.lifecycle.plan.column.MergeIntoDeltaColumnLineageVisitor {

  public static boolean hasClasses() {
    return ReflectionUtils.hasClasses(
        "org.apache.spark.sql.delta.commands.MergeIntoCommand",
        "org.apache.spark.sql.catalyst.plans.logical.DeltaMergeIntoInsertClause");
  }

  public MergeIntoDelta11ColumnLineageVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public Stream<Expression> getMergeActions(MergeIntoCommand mergeIntoCommand) {
    return Stream.concat(
        ScalaConversionUtils.<DeltaMergeIntoMatchedClause>asJavaCollection(
                mergeIntoCommand.matchedClauses())
            .stream()
            .flatMap(
                clause ->
                    ScalaConversionUtils.<Expression>asJavaCollection(clause.actions()).stream()),
        ScalaConversionUtils.<DeltaMergeIntoInsertClause>asJavaCollection(
                mergeIntoCommand.notMatchedClauses())
            .stream()
            .flatMap(
                clause ->
                    ScalaConversionUtils.<Expression>asJavaCollection(clause.actions()).stream()));
  }
}
