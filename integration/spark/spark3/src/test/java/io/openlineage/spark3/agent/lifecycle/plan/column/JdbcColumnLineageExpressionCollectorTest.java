/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.sql.ColumnMeta;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata$;
import org.apache.spark.sql.types.StringType$;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class JdbcColumnLineageExpressionCollectorTest {
  ColumnLevelLineageBuilder builder = mock(ColumnLevelLineageBuilder.class);
  ExprId exprId1 = ExprId.apply(20);
  ExprId exprId2 = ExprId.apply(21);
  ExprId dependencyId1 = ExprId.apply(0);
  ExprId dependencyId2 = ExprId.apply(1);

  Attribute expression1 =
      new AttributeReference(
          "k", IntegerType$.MODULE$, false, Metadata$.MODULE$.empty(), exprId1, null);
  Attribute expression2 =
      new AttributeReference(
          "j", StringType$.MODULE$, false, Metadata$.MODULE$.empty(), exprId2, null);

  JDBCRelation relation = mock(JDBCRelation.class);
  JDBCOptions jdbcOptions = mock(JDBCOptions.class);
  String jdbcQuery =
      "(select js1.k, CONCAT(js1.j1, js2.j2) as j from jdbc_source1 js1 join jdbc_source2 js2 on js1.k = js2.k) SPARK_GEN_SUBQ_0";
  String invalidJdbcQuery = "(INVALID) SPARK_GEN_SUBQ_0";
  String url = "postgresql://localhost:5432/test";
  Map<ColumnMeta, ExprId> mockMap = new HashMap<>();

  @BeforeEach
  void setup() {
    when(relation.jdbcOptions()).thenReturn(jdbcOptions);
  }

  @Test
  void testInputCollection() {
    when(jdbcOptions.tableOrQuery()).thenReturn(jdbcQuery);
    when(jdbcOptions.url()).thenReturn("jdbc:" + url);
    doAnswer(
            invocation -> mockMap.putIfAbsent(invocation.getArgument(0), invocation.getArgument(1)))
        .when(builder)
        .addExternalMapping(any(ColumnMeta.class), any(ExprId.class));

    when(builder.getMapping(any(ColumnMeta.class)))
        .thenAnswer(invocation -> mockMap.get(invocation.getArgument(0)));

    JdbcColumnLineageCollector.extractExpressionsFromJDBC(
        relation, builder, Arrays.asList(expression1, expression2));

    verify(builder, times(1)).addDependency(exprId2, dependencyId1);
    verify(builder, times(1)).addDependency(exprId2, dependencyId2);
  }

  @Test
  void testInvalidQuery() {
    when(jdbcOptions.tableOrQuery()).thenReturn(invalidJdbcQuery);
    when(jdbcOptions.url()).thenReturn("jdbc:" + url);

    JdbcColumnLineageCollector.extractExpressionsFromJDBC(
        relation, builder, Arrays.asList(expression1, expression2));

    verify(builder, never()).addDependency(any(ExprId.class), any(ExprId.class));
  }
}
