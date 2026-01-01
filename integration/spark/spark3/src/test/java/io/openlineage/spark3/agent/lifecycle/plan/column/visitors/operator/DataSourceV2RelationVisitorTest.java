/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_1;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_2;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.asSeq;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.field;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.mockNewExprId;
import static java.util.Objects.nonNull;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAccumulator;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import scala.Option;
import scala.collection.immutable.Seq;

class DataSourceV2RelationVisitorTest {
  DataSourceV2RelationVisitor visitor = new DataSourceV2RelationVisitor();
  ColumnLevelLineageBuilder builder =
      spy(
          new ColumnLevelLineageBuilder(
              mock(OpenLineage.SchemaDatasetFacet.class), mock(OpenLineageContext.class)));
  LongAccumulator exprIdAccumulator = new LongAccumulator(Long::sum, 0L);

  @BeforeEach
  void setup() {
    exprIdAccumulator.reset();
  }

  @Test
  void testNotDefinedAtWhenNotDataSourceV2RelationNode() {
    assertFalse(visitor.isDefinedAt(mock(LogicalPlan.class)));
  }

  @Test
  void testNotDefinedAtWhenNamespaceIsMissing() {
    assertFalse(
        visitor.isDefinedAt(
            aDataSourceV2Relation(
                properties(null, "SELECT a, b FROM db.t"),
                asSeq(field("a", EXPR_ID_1), field("b", EXPR_ID_2)))));
  }

  @Test
  void testNotDefinedAtWhenQueryIsMissing() {
    assertFalse(
        visitor.isDefinedAt(
            aDataSourceV2Relation(
                properties("db", null), asSeq(field("a", EXPR_ID_1), field("b", EXPR_ID_2)))));
  }

  @Test
  void testIsDefinedAt() {
    assertTrue(
        visitor.isDefinedAt(
            aDataSourceV2Relation(
                properties("db", "SELECT a, b FROM db.t"),
                asSeq(field("a", EXPR_ID_1), field("b", EXPR_ID_2)))));
  }

  @Test
  void testApply() {
    try (MockedStatic<NamedExpression> utilities = mockStatic(NamedExpression.class)) {
      mockNewExprId(exprIdAccumulator, utilities);
      DataSourceV2Relation relation =
          aDataSourceV2Relation(
              properties("db", "SELECT a, b FROM db.t"),
              asSeq(field("a", EXPR_ID_1), field("b", EXPR_ID_2)));

      visitor.apply(relation, builder);

      verify(builder, times(1))
          .addDependency(EXPR_ID_1, ExprId.apply(0), TransformationInfo.identity());
      verify(builder, times(1))
          .addDependency(EXPR_ID_2, ExprId.apply(1), TransformationInfo.identity());
      utilities.verify(NamedExpression::newExprId, times(2));
    }
  }

  private static Map<String, String> properties(String namespace, String query) {
    Map<String, String> props = new HashMap<>();
    if (nonNull(namespace)) {
      props.put("openlineage.dataset.namespace", namespace);
    }
    if (nonNull(query)) {
      props.put("openlineage.dataset.query", query);
    }
    return props;
  }

  private static DataSourceV2Relation aDataSourceV2Relation(
      Map<String, String> props, Seq<AttributeReference> output) {
    Table table = mock(Table.class);
    when(table.properties()).thenReturn(props);
    return new DataSourceV2Relation(table, output, Option.empty(), Option.empty(), null);
  }
}
