/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.sql.ColumnMeta;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import io.openlineage.sql.DbTableMeta;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.junit.jupiter.api.Test;

public class JdbcColumnLineageCollectorTest {
  ColumnLevelLineageBuilder builder = mock(ColumnLevelLineageBuilder.class);
  JDBCRelation relation = mock(JDBCRelation.class);
  JDBCOptions jdbcOptions = mock(JDBCOptions.class);
  String jdbcQuery =
      "(select js1.k, CONCAT(js1.j1, js2.j2) as j from jdbc_source1 js1 join jdbc_source2 js2 on js1.k = js2.k) SPARK_GEN_SUBQ_0";
  JDBCRelation invalidRelation = mock(JDBCRelation.class);
  JDBCOptions invalidJdbcOptions = mock(JDBCOptions.class);
  String invalidJdbcQuery = "(INVALID) SPARK_GEN_SUBQ_0";
  DatasetIdentifier datasetIdentifier1 = new DatasetIdentifier("jdbc_source1", "jdbc");
  DatasetIdentifier datasetIdentifier2 = new DatasetIdentifier("jdbc_source2", "jdbc");


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

  @Test
  void testInputCollection() {
    when(relation.jdbcOptions()).thenReturn(jdbcOptions);
    when(jdbcOptions.tableOrQuery()).thenReturn(jdbcQuery);
    when(builder.getMapping(any(ColumnMeta.class))).thenAnswer(invocation -> mockMap.get(invocation.getArgument(0)));
    
    JdbcColumnLineageCollector.extractExternalInputs(
        relation, builder, Arrays.asList(datasetIdentifier1, datasetIdentifier2));
    
    verify(builder, times(1)).addInput(exprId1, datasetIdentifier1, "k");
    verify(builder, times(1)).addInput(exprId2, datasetIdentifier1, "j1");
    verify(builder, times(1)).addInput(exprId3, datasetIdentifier2, "j2");
  }
  
  @Test
  void testInvalidQuery() {
    when(invalidRelation.jdbcOptions()).thenReturn(invalidJdbcOptions);
    when(invalidJdbcOptions.tableOrQuery()).thenReturn(invalidJdbcQuery);
    
    JdbcColumnLineageCollector.extractExternalInputs(
        invalidRelation, builder, Arrays.asList(datasetIdentifier1, datasetIdentifier2));
    
    verify(builder, times(0)).addInput(any(ExprId.class), any(DatasetIdentifier.class), any(String.class));
  }
}
