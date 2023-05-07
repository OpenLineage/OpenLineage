/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.spark.agent.lifecycle.plan.handlers.JdbcRelationHandler;
import io.openlineage.spark.api.DatasetFactory;
import java.util.List;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class JdbcRelationHandlerTest {
  JdbcRelationHandler jdbcRelationHandler;
  DatasetFactory datasetFactory = mock(DatasetFactory.class);
  JDBCRelation relation = mock(JDBCRelation.class);
  JDBCOptions jdbcOptions = mock(JDBCOptions.class);
  String jdbcQuery =
      "(select js1.k, CONCAT(js1.j1, js2.j2) as j from jdbc_source1 js1 join jdbc_source2 js2 on js1.k = js2.k) SPARK_GEN_SUBQ_0";
  String jdbcDbTableAsSubQuery =
      "(select js1.k, CONCAT(js1.j1, js2.j2) as j from jdbc_source1 js1 join jdbc_source2 js2 on js1.k = js2.k) as table1";
  String mysqlQuery =
      "(select `js1`.`k`, CONCAT(js1.j1, js2.j2) as j from `jdbc_source1` js1 join `jdbc_source2` js2 on js1.k = js2.k) as table1";
  String jdbcTable = "tablename";
  String invalidJdbc = "(test) SPARK_GEN_SUBQ_0";
  String url = "postgresql://localhost:5432/test";
  String mysqlUrl = "mysql://localhost:3306/test";
  StructType schema =
      new StructType().add("k", DataTypes.IntegerType).add("j", DataTypes.StringType);

  @BeforeEach
  void setup() {
    when(relation.jdbcOptions()).thenReturn(jdbcOptions);
    when(jdbcOptions.url()).thenReturn("jdbc:" + url);
    when(relation.schema()).thenReturn(schema);
    jdbcRelationHandler = new JdbcRelationHandler(datasetFactory);
  }

  @Test
  void testHandlingJdbcQuery() {
    when(jdbcOptions.tableOrQuery()).thenReturn(jdbcQuery);
    StructType schema1 =
        new StructType().add("k", DataTypes.IntegerType).add("j1", DataTypes.StringType);
    StructType schema2 = new StructType().add("j2", DataTypes.StringType);

    jdbcRelationHandler.getDatasets(relation, url);

    verify(datasetFactory, times(1)).getDataset("jdbc_source1", url, schema1);
    verify(datasetFactory, times(1)).getDataset("jdbc_source2", url, schema2);
  }

  @Test
  void testHandlingJdbcTable() {
    when(jdbcOptions.tableOrQuery()).thenReturn(jdbcTable);
    when(relation.schema()).thenReturn(schema);

    jdbcRelationHandler.getDatasets(relation, url);

    verify(datasetFactory, times(1)).getDataset("tablename", url, schema);
  }

  @Test
  void testHandlingJdbcDbTableAsSubQuery() {
    when(jdbcOptions.tableOrQuery()).thenReturn(jdbcDbTableAsSubQuery);
    StructType schema1 =
        new StructType().add("k", DataTypes.IntegerType).add("j1", DataTypes.StringType);
    StructType schema2 = new StructType().add("j2", DataTypes.StringType);

    jdbcRelationHandler.getDatasets(relation, url);

    verify(datasetFactory, times(1)).getDataset("jdbc_source1", url, schema1);
    verify(datasetFactory, times(1)).getDataset("jdbc_source2", url, schema2);
  }

  @Test
  void testMysqlDialect() {
    when(jdbcOptions.tableOrQuery()).thenReturn(mysqlQuery);
    when(jdbcOptions.url()).thenReturn("jdbc:" + mysqlUrl);
    StructType schema1 =
        new StructType().add("k", DataTypes.IntegerType).add("j1", DataTypes.StringType);
    StructType schema2 = new StructType().add("j2", DataTypes.StringType);

    jdbcRelationHandler.getDatasets(relation, mysqlUrl);

    verify(datasetFactory, times(1)).getDataset("jdbc_source1", mysqlUrl, schema1);
    verify(datasetFactory, times(1)).getDataset("jdbc_source2", mysqlUrl, schema2);
  }

  @Test
  void testInvalidJdbcString() {
    when(jdbcOptions.tableOrQuery()).thenReturn(invalidJdbc);
    List datasets = jdbcRelationHandler.getDatasets(relation, url);
    assertTrue(datasets.isEmpty());
    verify(datasetFactory, never())
        .getDataset(any(String.class), any(String.class), any(StructType.class));
  }
}
