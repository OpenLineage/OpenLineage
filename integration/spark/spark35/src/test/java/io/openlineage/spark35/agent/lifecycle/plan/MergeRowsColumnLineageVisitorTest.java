/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark35.agent.lifecycle.plan;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Arrays;
import java.util.Collections;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.Cast;
import org.apache.spark.sql.catalyst.expressions.Concat;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.Literal$;
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.MergeRows;
import org.apache.spark.sql.catalyst.plans.logical.MergeRows.Instruction;
import org.apache.spark.sql.catalyst.plans.logical.MergeRows.Keep;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.Metadata$;
import org.apache.spark.sql.types.StringType$;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Option;

class MergeRowsColumnLineageVisitorTest {

  private static final Literal TRUE = Literal$.MODULE$.apply(true);

  ColumnLevelLineageBuilder builder = mock(ColumnLevelLineageBuilder.class);
  ColumnLevelLineageContext context = mock(ColumnLevelLineageContext.class);
  MergeRowsColumnLineageVisitor visitor =
      new MergeRowsColumnLineageVisitor(mock(OpenLineageContext.class));

  AttributeReference operation = attribute("__row_operation", IntegerType$.MODULE$, 1);
  AttributeReference outputId = attribute("id", LongType$.MODULE$, 2);
  AttributeReference outputAmount = attribute("amount", LongType$.MODULE$, 3);
  AttributeReference outputNote = attribute("note", StringType$.MODULE$, 4);

  AttributeReference targetId = attribute("id", LongType$.MODULE$, 11);
  AttributeReference sourceId = attribute("id", LongType$.MODULE$, 21);
  AttributeReference sourceAmount = attribute("amount", IntegerType$.MODULE$, 22);
  AttributeReference sourceNote = attribute("note", StringType$.MODULE$, 23);

  @BeforeEach
  void setup() {
    when(context.getBuilder()).thenReturn(builder);
  }

  @Test
  void testCollectsDependenciesOfNonAttributeAssignments() {
    // WHEN MATCHED THEN UPDATE SET amount = s.amount, note = concat(s.note, '!')
    Keep update =
        keep(
            Literal$.MODULE$.apply(3),
            targetId,
            new Cast(sourceAmount, LongType$.MODULE$, Option.empty()),
            new Concat(
                ScalaConversionUtils.fromList(
                    Arrays.asList(sourceNote, Literal$.MODULE$.apply("!")))));
    // WHEN NOT MATCHED THEN INSERT (id, amount, note) VALUES (s.id, s.amount, s.note)
    Keep insert =
        keep(
            Literal$.MODULE$.apply(1),
            new AssertNotNull(sourceId, ScalaConversionUtils.asScalaSeqEmpty()),
            new Cast(sourceAmount, LongType$.MODULE$, Option.empty()),
            sourceNote);

    visitor.collectExpressionDependencies(context, mergeRows(update, insert));

    verifyDependency(outputId, targetId);
    verifyDependency(outputId, sourceId);
    verifyDependency(outputAmount, sourceAmount);
    verifyDependency(outputNote, sourceNote);
    verify(builder, never())
        .addDependency(eq(operation.exprId()), any(ExprId.class), anyString(), any());
  }

  private void verifyDependency(AttributeReference output, AttributeReference input) {
    verify(builder, atLeastOnce())
        .addDependency(
            eq(output.exprId()), eq(input.exprId()), anyString(), any(TransformationInfo.class));
  }

  private static Keep keep(Expression... outputs) {
    return new Keep(TRUE, ScalaConversionUtils.fromList(Arrays.asList(outputs)));
  }

  private MergeRows mergeRows(Instruction matched, Instruction notMatched) {
    return new MergeRows(
        TRUE,
        TRUE,
        ScalaConversionUtils.fromList(Collections.singletonList(matched)),
        ScalaConversionUtils.fromList(Collections.singletonList(notMatched)),
        ScalaConversionUtils.asScalaSeqEmpty(),
        false,
        ScalaConversionUtils.<Attribute>fromList(
            Arrays.asList(operation, outputId, outputAmount, outputNote)),
        mock(LogicalPlan.class));
  }

  private static AttributeReference attribute(String name, DataType dataType, long exprId) {
    return new AttributeReference(
        name,
        dataType,
        true,
        Metadata$.MODULE$.empty(),
        ExprId.apply(exprId),
        ScalaConversionUtils.asScalaSeqEmpty());
  }
}
