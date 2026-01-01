/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.client.dataset.namespace.resolver.DatasetNamespaceCombinedResolver;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import io.openlineage.sql.ColumnMeta;
import io.openlineage.sql.DbTableMeta;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
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

class JdbcColumnLineageInputCollectorTest {
  ColumnLevelLineageBuilder builder = mock(ColumnLevelLineageBuilder.class);
  ColumnLevelLineageContext context = mock(ColumnLevelLineageContext.class);
  OpenLineageContext openLineageContext = mock(OpenLineageContext.class);
  LogicalRelation logicalRelation = mock(LogicalRelation.class);
  JDBCRelation jdbcRelation = mock(JDBCRelation.class);
  JDBCOptions jdbcOptions = mock(JDBCOptions.class);
  String jdbcQuery =
      "(select js1.k, CONCAT(js1.j1, js2.j2) as j from jdbc_source1 js1 join jdbc_source2 js2 on js1.k = js2.k) SPARK_GEN_SUBQ_0";
  String invalidJdbcQuery = "(INVALID) SPARK_GEN_SUBQ_0";
  DatasetIdentifier datasetIdentifier1 = new DatasetIdentifier("test.jdbc_source1", "jdbc");
  DatasetIdentifier datasetIdentifier2 = new DatasetIdentifier("test.jdbc_source2", "jdbc");
  String url = "jdbc:postgresql://localhost:5432/test";

  JdbcColumnLineageVisitor visitor = new JdbcColumnLineageVisitor(openLineageContext);

  static ExprId exprId1 = ExprId.apply(1);
  static ExprId exprId2 = ExprId.apply(2);
  static ExprId exprId3 = ExprId.apply(3);
  Map<ColumnMeta, ExprId> mockMap = getMockMap();

  private static Map<ColumnMeta, ExprId> getMockMap() {
    Map<ColumnMeta, ExprId> map = new HashMap<>();

    map.put(new ColumnMeta(new DbTableMeta(null, null, "jdbc_source1"), "k"), exprId1);

    map.put(new ColumnMeta(new DbTableMeta(null, null, "jdbc_source1"), "j1"), exprId2);

    map.put(new ColumnMeta(new DbTableMeta(null, null, "jdbc_source2"), "j2"), exprId3);
    return map;
  }

  @BeforeEach
  void setup() {
    when(jdbcRelation.jdbcOptions()).thenReturn(jdbcOptions);
    when(logicalRelation.output())
        .thenReturn(ScalaConversionUtils.fromList(Collections.emptyList()));
    when(jdbcOptions.url()).thenReturn(url);

    scala.collection.immutable.Map<String, String> properties =
        ScalaConversionUtils.<String, String>asScalaMapEmpty();
    when(jdbcOptions.parameters())
        .thenReturn(CaseInsensitiveMap$.MODULE$.<String>apply(properties));
    when(context.getBuilder()).thenReturn(builder);
    when(context.getOlContext()).thenReturn(openLineageContext);
    when(context.getNamespaceResolver())
        .thenReturn(new DatasetNamespaceCombinedResolver(new SparkOpenLineageConfig()));
    when(openLineageContext.getOpenLineageConfig()).thenReturn(new SparkOpenLineageConfig());
    when(logicalRelation.relation()).thenReturn(jdbcRelation);
  }

  @Test
  void testInputCollection() {
    when(jdbcOptions.tableOrQuery()).thenReturn(jdbcQuery);
    when(builder.getMapping(any(ColumnMeta.class)))
        .thenAnswer(invocation -> mockMap.get(invocation.getArgument(0)));

    try (MockedStatic mockedStatic = mockStatic(InputFieldsCollector.class)) {
      when(InputFieldsCollector.extractDatasetIdentifier(
              any(ColumnLevelLineageContext.class), any(JDBCRelation.class)))
          .thenReturn(Arrays.asList(datasetIdentifier1, datasetIdentifier2));
      visitor.collectInputs(context, logicalRelation);
      verify(builder, times(1)).addInput(exprId1, datasetIdentifier1, "k");
      verify(builder, times(1)).addInput(exprId2, datasetIdentifier1, "j1");
      verify(builder, times(1)).addInput(exprId3, datasetIdentifier2, "j2");
    }
  }

  @Test
  void testInvalidQuery() {
    // simulate an invalid query that should not collect inputs
    when(jdbcOptions.tableOrQuery()).thenReturn(invalidJdbcQuery);

    try (MockedStatic mockedStatic = mockStatic(InputFieldsCollector.class)) {
      // simulate that the dataset identifier is extracted for the table
      when(InputFieldsCollector.extractDatasetIdentifier(
              any(ColumnLevelLineageContext.class), any(JDBCRelation.class)))
          .thenReturn(Arrays.asList(datasetIdentifier1, datasetIdentifier2));

      // verify that no inputs are collected for an invalid query
      visitor.collectInputs(context, logicalRelation);
      verify(builder, never())
          .addInput(any(ExprId.class), any(DatasetIdentifier.class), any(String.class));
    }
  }

  @Test
  void testSelectWildcardFromTableQuery() {
    // simulate a query that selects all columns from a table
    when(jdbcOptions.tableOrQuery())
        .thenReturn("(select * from jdbc_source1 where x = 9) SPARK_GEN_SUBQ_0");
    when(builder.getMapping(any(ColumnMeta.class)))
        .thenAnswer(invocation -> mockMap.get(invocation.getArgument(0)));

    AttributeReference expression1 =
        new AttributeReference(
            "k", IntegerType$.MODULE$, false, Metadata$.MODULE$.empty(), exprId1, null);
    AttributeReference expression2 =
        new AttributeReference(
            "j1", StringType$.MODULE$, false, Metadata$.MODULE$.empty(), exprId2, null);

    // simulate that the logical relation has these attributes
    when(logicalRelation.output())
        .thenReturn(ScalaConversionUtils.fromList(Arrays.asList(expression1, expression2)));

    try (MockedStatic mockedStatic = mockStatic(InputFieldsCollector.class)) {
      // simulate that the dataset identifier is extracted for the table
      when(InputFieldsCollector.extractDatasetIdentifier(
              any(ColumnLevelLineageContext.class), any(JDBCRelation.class)))
          .thenReturn(Arrays.asList(datasetIdentifier1));

      // verify that the input attributes are collected from JdbcRelation
      visitor.collectInputs(context, logicalRelation);
      verify(builder, times(1)).addInput(exprId1, datasetIdentifier1, "k");
      verify(builder, times(1)).addInput(exprId2, datasetIdentifier1, "j1");
    }
  }
}
