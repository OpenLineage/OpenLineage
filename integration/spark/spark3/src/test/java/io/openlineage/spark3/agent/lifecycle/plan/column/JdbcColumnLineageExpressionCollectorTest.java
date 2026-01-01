/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineageConfig;
import io.openlineage.client.dataset.namespace.resolver.DatasetNamespaceCombinedResolver;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.sql.ColumnMeta;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAccumulator;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap$;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata$;
import org.apache.spark.sql.types.StringType$;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.stubbing.Answer;

class JdbcColumnLineageExpressionCollectorTest {
  ColumnLevelLineageBuilder builder = mock(ColumnLevelLineageBuilder.class);
  ColumnLevelLineageContext context = mock(ColumnLevelLineageContext.class);
  OpenLineageContext openLineageContext = mock(OpenLineageContext.class);
  ExprId exprId1 = ExprId.apply(20);
  ExprId exprId2 = ExprId.apply(21);
  ExprId dependencyId1 = ExprId.apply(0);
  ExprId dependencyId2 = ExprId.apply(1);

  AttributeReference expression1 =
      new AttributeReference(
          "k", IntegerType$.MODULE$, false, Metadata$.MODULE$.empty(), exprId1, null);
  AttributeReference expression2 =
      new AttributeReference(
          "j", StringType$.MODULE$, false, Metadata$.MODULE$.empty(), exprId2, null);

  JDBCRelation relation = mock(JDBCRelation.class);
  LogicalRelation logicalRelation = mock(LogicalRelation.class);
  JDBCOptions jdbcOptions = mock(JDBCOptions.class);
  String jdbcQuery =
      "(select js1.k, CONCAT(js1.j1, js2.j2) as j from jdbc_source1 js1 join jdbc_source2 js2 on js1.k = js2.k) SPARK_GEN_SUBQ_0";
  String invalidJdbcQuery = "(INVALID) SPARK_GEN_SUBQ_0";
  String url = "jdbc:postgresql://localhost:5432/test";
  Map<ColumnMeta, ExprId> mockMap = new HashMap<>();

  JdbcColumnLineageVisitor visitor = new JdbcColumnLineageVisitor(openLineageContext);

  @BeforeEach
  void setup() {
    when(logicalRelation.relation()).thenReturn(relation);
    when(logicalRelation.output())
        .thenReturn(ScalaConversionUtils.fromList(Arrays.asList(expression1, expression2)));

    when(relation.jdbcOptions()).thenReturn(jdbcOptions);

    scala.collection.immutable.Map<String, String> properties =
        ScalaConversionUtils.<String, String>asScalaMapEmpty();
    when(jdbcOptions.parameters())
        .thenReturn(CaseInsensitiveMap$.MODULE$.<String>apply(properties));
    when(context.getBuilder()).thenReturn(builder);
    when(context.getNamespaceResolver())
        .thenReturn(new DatasetNamespaceCombinedResolver(new OpenLineageConfig()));
  }

  @Test
  void testInputCollection() {
    when(jdbcOptions.tableOrQuery()).thenReturn(jdbcQuery);
    when(jdbcOptions.url()).thenReturn(url);
    final LongAccumulator id = new LongAccumulator(Long::sum, 0L);
    try (MockedStatic<NamedExpression> utilities = mockStatic(NamedExpression.class)) {
      utilities
          .when(NamedExpression::newExprId)
          .thenAnswer(
              (Answer<ExprId>)
                  invocation -> {
                    ExprId exprId = ExprId.apply(id.get());
                    id.accumulate(1);
                    return exprId;
                  });
      doAnswer(
              invocation ->
                  mockMap.putIfAbsent(invocation.getArgument(0), invocation.getArgument(1)))
          .when(builder)
          .addExternalMapping(any(ColumnMeta.class), any(ExprId.class));

      when(builder.getMapping(any(ColumnMeta.class)))
          .thenAnswer(invocation -> mockMap.get(invocation.getArgument(0)));

      visitor.collectExpressionDependencies(context, logicalRelation);

      verify(builder, times(1)).addDependency(exprId2, dependencyId1);
      verify(builder, times(1)).addDependency(exprId2, dependencyId2);
      utilities.verify(NamedExpression::newExprId, times(3));
    }
  }

  @Test
  void testInvalidQuery() {
    when(jdbcOptions.tableOrQuery()).thenReturn(invalidJdbcQuery);
    when(jdbcOptions.url()).thenReturn(url);

    visitor.collectExpressionDependencies(context, logicalRelation);

    verify(builder, never()).addDependency(any(ExprId.class), any(ExprId.class));
  }
}
