/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.AliasBuilder.alias;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_1;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_2;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_3;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_4;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.NAME_1;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.NAME_2;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.NAME_3;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.asSeq;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.field;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionDependencyCollector;
import org.apache.spark.api.python.PythonFunction;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.PythonUDF;
import org.apache.spark.sql.catalyst.expressions.Sha1;
import org.apache.spark.sql.catalyst.plans.logical.ArrowEvalPython;
import org.apache.spark.sql.catalyst.plans.logical.BatchEvalPython;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.types.IntegerType$;
import org.junit.jupiter.api.Test;

class EvalPythonVisitorTest {
  private static final String NAME_4 = "name4";
  private static final String RESULT = "pythonUDF0";
  private static final ExprId UDF_ID = ExprId.apply(30);
  private static final ExprId RESULT_ID = ExprId.apply(31);
  // PythonEvalType.SQL_BATCHED_UDF and PythonEvalType.SQL_SCALAR_PANDAS_UDF
  private static final int BATCHED_UDF = 100;
  private static final int SCALAR_PANDAS_UDF = 200;

  EvalPythonVisitor visitor = new EvalPythonVisitor();
  ColumnLevelLineageBuilder builder = mock(ColumnLevelLineageBuilder.class);

  @Test
  void testIsDefinedAt() {
    assertTrue(visitor.isDefinedAt(mock(BatchEvalPython.class)));
    assertTrue(visitor.isDefinedAt(mock(ArrowEvalPython.class)));
    assertFalse(visitor.isDefinedAt(mock(LogicalPlan.class)));
  }

  @Test
  void testApplyBatchEvalPython() {
    // first_udf(name1, name2) -> name3, second_udf(name2) -> name4
    PythonUDF firstUdf = mock(PythonUDF.class);
    when(firstUdf.children()).thenReturn(asSeq(field(NAME_1, EXPR_ID_1), field(NAME_2, EXPR_ID_2)));
    when(firstUdf.sql()).thenReturn("first_udf(name1, name2)");
    PythonUDF secondUdf = mock(PythonUDF.class);
    when(secondUdf.children()).thenReturn(asSeq(field(NAME_2, EXPR_ID_2)));
    when(secondUdf.sql()).thenReturn("second_udf(name2)");

    BatchEvalPython node = mock(BatchEvalPython.class);
    when(node.udfs()).thenReturn(asSeq(firstUdf, secondUdf));
    when(node.resultAttrs()).thenReturn(asSeq(field(NAME_3, EXPR_ID_3), field(NAME_4, EXPR_ID_4)));

    visitor.apply(node, builder);

    TransformationInfo first = TransformationInfo.transformation("first_udf(name1, name2)");
    verify(builder).addDependency(EXPR_ID_3, EXPR_ID_1, "name3", first);
    verify(builder).addDependency(EXPR_ID_3, EXPR_ID_2, "name3", first);
    verify(builder)
        .addDependency(
            EXPR_ID_4, EXPR_ID_2, NAME_4, TransformationInfo.transformation("second_udf(name2)"));
    verifyNoMoreInteractions(builder);
  }

  @Test
  void testApplyArrowEvalPython() {
    PythonUDF udf = mock(PythonUDF.class);
    when(udf.children()).thenReturn(asSeq(field(NAME_1, EXPR_ID_1)));
    when(udf.sql()).thenReturn("pandas_udf(name1)");

    ArrowEvalPython node = mock(ArrowEvalPython.class);
    when(node.udfs()).thenReturn(asSeq(udf));
    when(node.resultAttrs()).thenReturn(asSeq(field(NAME_3, EXPR_ID_3)));

    visitor.apply(node, builder);

    verify(builder)
        .addDependency(
            EXPR_ID_3, EXPR_ID_1, "name3", TransformationInfo.transformation("pandas_udf(name1)"));
    verifyNoMoreInteractions(builder);
  }

  @Test
  void testCollectFromProjectOverBatchEvalPython() {
    // Project [name1, pythonUDF0 AS name4] over BatchEvalPython [my_udf(name1, name2)],
    // [pythonUDF0]
    Attribute result = field(RESULT, RESULT_ID);
    BatchEvalPython node =
        new BatchEvalPython(asSeq(pythonUdf(BATCHED_UDF)), asSeq(result), mock(LogicalPlan.class));
    Project project =
        new Project(
            asSeq(
                (NamedExpression) field(NAME_1, EXPR_ID_1),
                alias(field(RESULT, RESULT_ID)).as(NAME_4, EXPR_ID_4)),
            node);

    // the visitors registered in VisitorFactory are applied to both operators
    ExpressionDependencyCollector.collectFromOperator(builder, project);
    ExpressionDependencyCollector.collectFromOperator(builder, node);

    TransformationInfo udf = TransformationInfo.transformation("my_udf(name1, name2)");
    verify(builder).addDependency(RESULT_ID, EXPR_ID_1, RESULT, udf);
    verify(builder).addDependency(RESULT_ID, EXPR_ID_2, RESULT, udf);
    verify(builder)
        .addDependency(
            EXPR_ID_4, RESULT_ID, NAME_4, TransformationInfo.identity("pythonUDF0 AS name4"));
  }

  @Test
  void testCollectFromArrowEvalPython() {
    ArrowEvalPython node =
        new ArrowEvalPython(
            asSeq(pythonUdf(SCALAR_PANDAS_UDF)),
            asSeq((Attribute) field(RESULT, RESULT_ID)),
            mock(LogicalPlan.class),
            SCALAR_PANDAS_UDF);

    ExpressionDependencyCollector.collectFromOperator(builder, node);

    TransformationInfo udf = TransformationInfo.transformation("my_udf(name1, name2)");
    verify(builder).addDependency(RESULT_ID, EXPR_ID_1, RESULT, udf);
    verify(builder).addDependency(RESULT_ID, EXPR_ID_2, RESULT, udf);
    verifyNoMoreInteractions(builder);
  }

  @Test
  void testApplyKeepsMaskingOfArgument() {
    // my_udf(sha1(name1), name2): the UDF is not masking, the first argument is
    PythonUDF udf =
        pythonUdf(BATCHED_UDF, new Sha1(field(NAME_1, EXPR_ID_1)), field(NAME_2, EXPR_ID_2));
    BatchEvalPython node =
        new BatchEvalPython(
            asSeq(udf), asSeq((Attribute) field(RESULT, RESULT_ID)), mock(LogicalPlan.class));

    visitor.apply(node, builder);

    verify(builder)
        .addDependency(
            RESULT_ID, EXPR_ID_1, RESULT, TransformationInfo.transformation(udf.sql(), true));
    verify(builder)
        .addDependency(
            RESULT_ID, EXPR_ID_2, RESULT, TransformationInfo.transformation(udf.sql(), false));
    verifyNoMoreInteractions(builder);
  }

  /** my_udf(name1, name2) */
  private static PythonUDF pythonUdf(int evalType) {
    return pythonUdf(evalType, field(NAME_1, EXPR_ID_1), field(NAME_2, EXPR_ID_2));
  }

  private static PythonUDF pythonUdf(int evalType, Expression... arguments) {
    return new PythonUDF(
        "my_udf",
        mock(PythonFunction.class),
        IntegerType$.MODULE$,
        asSeq(arguments),
        evalType,
        true,
        UDF_ID);
  }
}
