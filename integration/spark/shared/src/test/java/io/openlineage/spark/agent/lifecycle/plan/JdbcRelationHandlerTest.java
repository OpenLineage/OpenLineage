/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.lifecycle.plan.handlers.JdbcRelationHandler;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap;
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap$;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions$;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import scala.collection.immutable.Map$;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class JdbcRelationHandlerTest {
  JdbcRelationHandler jdbcRelationHandler;
  DatasetFactory datasetFactory = mock(DatasetFactory.class, RETURNS_DEEP_STUBS);
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
  String unknownUrl = "unknown://localhost:1234/test";
  StructType schema =
      new StructType().add("k", DataTypes.IntegerType).add("j", DataTypes.StringType);
  OpenLineageContext context = mock(OpenLineageContext.class);

  @BeforeEach
  void setup() {
    when(relation.jdbcOptions()).thenReturn(jdbcOptions);
    when(jdbcOptions.parameters())
        .thenReturn(CaseInsensitiveMap$.MODULE$.apply(Map$.MODULE$.empty()));
    when(jdbcOptions.url()).thenReturn("jdbc:" + url);
    when(relation.schema()).thenReturn(schema);
    jdbcRelationHandler = new JdbcRelationHandler(datasetFactory);
    when(context.getOpenLineageConfig()).thenReturn(new SparkOpenLineageConfig());
  }

  @Test
  void testHandlingJdbcQuery() {
    when(jdbcOptions.tableOrQuery()).thenReturn(jdbcQuery);

    jdbcRelationHandler.getDatasets(relation);

    ArgumentCaptor<DatasetIdentifier> diCaptor = ArgumentCaptor.forClass(DatasetIdentifier.class);
    verify(datasetFactory.sparkDatasetBuilder(), times(2)).dataset(diCaptor.capture());
    List<DatasetIdentifier> allDis = diCaptor.getAllValues();
    assertTrue(
        allDis.stream()
            .anyMatch(
                di ->
                    "test.jdbc_source1".equals(di.getName())
                        && "postgres://localhost:5432".equals(di.getNamespace())));
    assertTrue(
        allDis.stream()
            .anyMatch(
                di ->
                    "test.jdbc_source2".equals(di.getName())
                        && "postgres://localhost:5432".equals(di.getNamespace())));
  }

  @Test
  void testHandlingJdbcTable() {
    CaseInsensitiveMap params =
        CaseInsensitiveMap$.MODULE$.apply(
            ScalaConversionUtils.fromJavaMap(
                Collections.singletonMap(JDBCOptions$.MODULE$.JDBC_TABLE_NAME(), jdbcTable)));
    when(jdbcOptions.parameters()).thenReturn(params);
    when(jdbcOptions.tableOrQuery()).thenReturn(jdbcTable);
    when(relation.schema()).thenReturn(schema);

    jdbcRelationHandler.getDatasets(relation);

    ArgumentCaptor<DatasetIdentifier> diCaptor = ArgumentCaptor.forClass(DatasetIdentifier.class);
    verify(datasetFactory.sparkDatasetBuilder(), times(1)).dataset(diCaptor.capture());
    DatasetIdentifier di = diCaptor.getValue();
    assertEquals("test.tablename", di.getName());
    assertEquals("postgres://localhost:5432", di.getNamespace());
  }

  @Test
  void testHandlingJdbcDbTableAsSubQuery() {
    CaseInsensitiveMap params =
        CaseInsensitiveMap$.MODULE$.apply(
            ScalaConversionUtils.fromJavaMap(
                Collections.singletonMap(
                    JDBCOptions$.MODULE$.JDBC_TABLE_NAME(), jdbcDbTableAsSubQuery)));
    when(jdbcOptions.parameters()).thenReturn(params);
    when(jdbcOptions.tableOrQuery()).thenReturn(jdbcDbTableAsSubQuery);

    jdbcRelationHandler.getDatasets(relation);

    ArgumentCaptor<DatasetIdentifier> diCaptor = ArgumentCaptor.forClass(DatasetIdentifier.class);
    verify(datasetFactory.sparkDatasetBuilder(), times(2)).dataset(diCaptor.capture());
    List<DatasetIdentifier> allDis = diCaptor.getAllValues();
    assertTrue(
        allDis.stream()
            .anyMatch(
                di ->
                    "test.jdbc_source1".equals(di.getName())
                        && "postgres://localhost:5432".equals(di.getNamespace())));
    assertTrue(
        allDis.stream()
            .anyMatch(
                di ->
                    "test.jdbc_source2".equals(di.getName())
                        && "postgres://localhost:5432".equals(di.getNamespace())));
  }

  @Test
  void testMysqlDialect() {
    when(jdbcOptions.tableOrQuery()).thenReturn(mysqlQuery);
    when(jdbcOptions.url()).thenReturn("jdbc:" + mysqlUrl);

    jdbcRelationHandler.getDatasets(relation);

    ArgumentCaptor<DatasetIdentifier> diCaptor = ArgumentCaptor.forClass(DatasetIdentifier.class);
    verify(datasetFactory.sparkDatasetBuilder(), times(2)).dataset(diCaptor.capture());
    List<DatasetIdentifier> allDis = diCaptor.getAllValues();
    assertTrue(
        allDis.stream()
            .anyMatch(
                di ->
                    "test.jdbc_source1".equals(di.getName())
                        && "mysql://localhost:3306".equals(di.getNamespace())));
    assertTrue(
        allDis.stream()
            .anyMatch(
                di ->
                    "test.jdbc_source2".equals(di.getName())
                        && "mysql://localhost:3306".equals(di.getNamespace())));
  }

  @Test
  void testUnknownDialect() {
    when(jdbcOptions.tableOrQuery()).thenReturn(jdbcQuery);
    when(jdbcOptions.url()).thenReturn("jdbc:" + unknownUrl);

    jdbcRelationHandler.getDatasets(relation);

    ArgumentCaptor<DatasetIdentifier> diCaptor = ArgumentCaptor.forClass(DatasetIdentifier.class);
    verify(datasetFactory.sparkDatasetBuilder(), times(2)).dataset(diCaptor.capture());
    List<DatasetIdentifier> allDis = diCaptor.getAllValues();
    assertTrue(
        allDis.stream()
            .anyMatch(
                di ->
                    "test.jdbc_source1".equals(di.getName())
                        && "unknown://localhost:1234".equals(di.getNamespace())));
    assertTrue(
        allDis.stream()
            .anyMatch(
                di ->
                    "test.jdbc_source2".equals(di.getName())
                        && "unknown://localhost:1234".equals(di.getNamespace())));
  }

  @Test
  void testInvalidJdbcString() {
    when(jdbcOptions.tableOrQuery()).thenReturn(invalidJdbc);
    when(jdbcOptions.url()).thenReturn("jdbc:" + url);
    List datasets = jdbcRelationHandler.getDatasets(relation);
    assertTrue(datasets.isEmpty());
    verify(datasetFactory.sparkDatasetBuilder(), never()).dataset(any(DatasetIdentifier.class));
  }
}
