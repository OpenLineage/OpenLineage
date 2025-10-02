/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.expression;

import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.AliasBuilder.alias;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_1;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_2;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.NAME_1;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.NAME_2;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.field;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionTraverser;
import org.apache.spark.sql.catalyst.expressions.Alias;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.junit.jupiter.api.Test;

class AliasVisitorTest {
  AliasVisitor visitor = new AliasVisitor();
  ExpressionTraverser traverser = mock(ExpressionTraverser.class);
  ExpressionTraverser newTraverser = mock(ExpressionTraverser.class);

  @Test
  void testIsDefinedAt() {
    assertTrue(visitor.isDefinedAt(mock(Alias.class)));
    assertFalse(visitor.isDefinedAt(mock(AttributeReference.class)));
  }

  @Test
  void testApply() {
    when(traverser.copyFor(any(), any())).thenReturn(newTraverser);

    visitor.apply(alias(field(NAME_1, EXPR_ID_1)).as(NAME_2, EXPR_ID_2), traverser);

    verify(traverser).copyFor(field(NAME_1, EXPR_ID_1), TransformationInfo.identity());
    verify(newTraverser).traverse();
  }
}
