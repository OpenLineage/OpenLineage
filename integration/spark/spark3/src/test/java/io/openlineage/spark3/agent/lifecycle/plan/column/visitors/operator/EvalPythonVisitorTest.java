/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

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
import org.apache.spark.sql.catalyst.expressions.PythonUDF;
import org.apache.spark.sql.catalyst.plans.logical.ArrowEvalPython;
import org.apache.spark.sql.catalyst.plans.logical.BatchEvalPython;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.jupiter.api.Test;

class EvalPythonVisitorTest {
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
    when(node.resultAttrs()).thenReturn(asSeq(field(NAME_3, EXPR_ID_3), field("name4", EXPR_ID_4)));

    visitor.apply(node, builder);

    TransformationInfo first = TransformationInfo.transformation("first_udf(name1, name2)");
    verify(builder).addDependency(EXPR_ID_3, EXPR_ID_1, "name3", first);
    verify(builder).addDependency(EXPR_ID_3, EXPR_ID_2, "name3", first);
    verify(builder)
        .addDependency(
            EXPR_ID_4, EXPR_ID_2, "name4", TransformationInfo.transformation("second_udf(name2)"));
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
}
