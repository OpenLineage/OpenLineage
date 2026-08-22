/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.filters;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.agent.util.SparkSessionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Collections;
import java.util.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.AppendData;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.delta.FakeDeltaCommand;
import org.apache.spark.sql.delta.FakeDeltaFileFormat;
import org.apache.spark.sql.delta.FakeDeltaProvider;
import org.apache.spark.sql.delta.FakeDeltaTable;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.datasources.FileFormat;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class EventFilterUtilsTest {

  private static final String SPARK_SQL_EXTENSIONS = "spark.sql.extensions";

  @Test
  void testCurrentPlanAbsentIsNotDeltaWrite() {
    OpenLineageContext context = mock(OpenLineageContext.class);
    when(context.getQueryExecution()).thenReturn(Optional.empty());

    assertFalse(EventFilterUtils.isCurrentPlanDeltaWrite(context));
  }

  @Test
  void testNonWriteRootIsNotDeltaWrite() {
    OpenLineageContext context = mock(OpenLineageContext.class);
    QueryExecution queryExecution = mock(QueryExecution.class);
    LogicalPlan plan = mock(LogicalPlan.class);
    when(context.getQueryExecution()).thenReturn(Optional.of(queryExecution));
    when(queryExecution.optimizedPlan()).thenReturn(plan);

    assertFalse(EventFilterUtils.isCurrentPlanDeltaWrite(context));
  }

  @Test
  void testDeltaCommandRootIsDeltaWrite() {
    OpenLineageContext context = mock(OpenLineageContext.class);
    QueryExecution queryExecution = mock(QueryExecution.class);
    FakeDeltaCommand plan = mock(FakeDeltaCommand.class);
    when(context.getQueryExecution()).thenReturn(Optional.of(queryExecution));
    when(queryExecution.optimizedPlan()).thenReturn(plan);

    assertTrue(EventFilterUtils.isCurrentPlanDeltaWrite(context));
  }

  @Test
  void testPureDeltaReadRootIsNotDeltaWrite() {
    OpenLineageContext context = mock(OpenLineageContext.class);
    QueryExecution queryExecution = mock(QueryExecution.class);
    DataSourceV2Relation plan = mock(DataSourceV2Relation.class);
    when(context.getQueryExecution()).thenReturn(Optional.of(queryExecution));
    when(queryExecution.optimizedPlan()).thenReturn(plan);
    when(plan.table()).thenReturn(new FakeDeltaTable());

    assertFalse(EventFilterUtils.isCurrentPlanDeltaWrite(context));
  }

  @Test
  void testDeltaSaveIntoDataSourceCommandIsDeltaWrite() {
    OpenLineageContext context = mock(OpenLineageContext.class);
    QueryExecution queryExecution = mock(QueryExecution.class);
    SaveIntoDataSourceCommand plan = mock(SaveIntoDataSourceCommand.class);
    when(context.getQueryExecution()).thenReturn(Optional.of(queryExecution));
    when(queryExecution.optimizedPlan()).thenReturn(plan);
    when(plan.dataSource()).thenReturn(new FakeDeltaProvider());

    assertTrue(EventFilterUtils.isCurrentPlanDeltaWrite(context));
  }

  @Test
  void testNonDeltaSaveIntoDataSourceCommandIsNotDeltaWrite() {
    OpenLineageContext context = mock(OpenLineageContext.class);
    QueryExecution queryExecution = mock(QueryExecution.class);
    SaveIntoDataSourceCommand plan = mock(SaveIntoDataSourceCommand.class);
    when(context.getQueryExecution()).thenReturn(Optional.of(queryExecution));
    when(queryExecution.optimizedPlan()).thenReturn(plan);
    when(plan.dataSource()).thenReturn(mock(CreatableRelationProvider.class));

    assertFalse(EventFilterUtils.isCurrentPlanDeltaWrite(context));
  }

  @Test
  void testV2WriteToDeltaTableIsDeltaWrite() {
    OpenLineageContext context = mock(OpenLineageContext.class);
    QueryExecution queryExecution = mock(QueryExecution.class);
    AppendData plan = mock(AppendData.class);
    DataSourceV2Relation relation = mock(DataSourceV2Relation.class);
    when(context.getQueryExecution()).thenReturn(Optional.of(queryExecution));
    when(queryExecution.optimizedPlan()).thenReturn(plan);
    when(plan.table()).thenReturn(relation);
    when(relation.table()).thenReturn(new FakeDeltaTable());

    assertTrue(EventFilterUtils.isCurrentPlanDeltaWrite(context));
  }

  @Test
  void testV2WriteToNonDeltaTableIsNotDeltaWrite() {
    OpenLineageContext context = mock(OpenLineageContext.class);
    QueryExecution queryExecution = mock(QueryExecution.class);
    AppendData plan = mock(AppendData.class);
    DataSourceV2Relation relation = mock(DataSourceV2Relation.class);
    when(context.getQueryExecution()).thenReturn(Optional.of(queryExecution));
    when(queryExecution.optimizedPlan()).thenReturn(plan);
    when(plan.table()).thenReturn(relation);
    when(relation.table()).thenReturn(mock(Table.class));

    assertFalse(EventFilterUtils.isCurrentPlanDeltaWrite(context));
  }

  @Test
  void testDeltaFormatFileWriteIsDeltaWrite() {
    OpenLineageContext context = mock(OpenLineageContext.class);
    QueryExecution queryExecution = mock(QueryExecution.class);
    InsertIntoHadoopFsRelationCommand plan = mock(InsertIntoHadoopFsRelationCommand.class);
    when(context.getQueryExecution()).thenReturn(Optional.of(queryExecution));
    when(queryExecution.optimizedPlan()).thenReturn(plan);
    when(plan.fileFormat()).thenReturn(mock(FakeDeltaFileFormat.class));

    assertTrue(EventFilterUtils.isCurrentPlanDeltaWrite(context));
  }

  @Test
  void testParquetWriteWithDeltaChildrenIsNotDeltaWrite() {
    // Regression: a plain Parquet write whose *inputs* are Delta tables must not be
    // classified as a Delta write, or AQE-deduplication erases its only terminal event.
    OpenLineageContext context = mock(OpenLineageContext.class);
    QueryExecution queryExecution = mock(QueryExecution.class);
    InsertIntoHadoopFsRelationCommand plan = mock(InsertIntoHadoopFsRelationCommand.class);
    DataSourceV2Relation deltaInput = mock(DataSourceV2Relation.class);
    when(context.getQueryExecution()).thenReturn(Optional.of(queryExecution));
    when(queryExecution.optimizedPlan()).thenReturn(plan);
    when(plan.fileFormat()).thenReturn(mock(FileFormat.class));
    when(deltaInput.table()).thenReturn(new FakeDeltaTable());
    when(plan.children())
        .thenReturn(ScalaConversionUtils.fromList(Collections.singletonList(deltaInput)));

    assertFalse(EventFilterUtils.isCurrentPlanDeltaWrite(context));
  }

  @Test
  void testPlanInspectionFailureDoesNotEscapeListener() {
    OpenLineageContext context = mock(OpenLineageContext.class);
    QueryExecution queryExecution = mock(QueryExecution.class);
    when(context.getQueryExecution()).thenReturn(Optional.of(queryExecution));
    when(queryExecution.optimizedPlan()).thenThrow(new NoSuchMethodError());

    assertFalse(EventFilterUtils.isCurrentPlanDeltaWrite(context));
  }

  @Test
  void testIsDeltaPlanWithSingleExtension() {
    SparkSession session = mock(SparkSession.class);
    SparkContext sparkContext = mock(SparkContext.class);
    SparkConf sparkConf = mock(SparkConf.class);

    when(session.sparkContext()).thenReturn(sparkContext);
    when(sparkContext.conf()).thenReturn(sparkConf);
    when(sparkConf.get(SPARK_SQL_EXTENSIONS, ""))
        .thenReturn("io.delta.sql.DeltaSparkSessionExtension");

    try (MockedStatic<SparkSessionUtils> mocked = mockStatic(SparkSessionUtils.class)) {
      mocked.when(SparkSessionUtils::activeSession).thenReturn(Optional.of(session));
      assertTrue(EventFilterUtils.isDeltaPlan());
    }
  }

  @Test
  void testIsDeltaPlanWithMultipleExtensions() {
    SparkSession session = mock(SparkSession.class);
    SparkContext sparkContext = mock(SparkContext.class);
    SparkConf sparkConf = mock(SparkConf.class);

    when(session.sparkContext()).thenReturn(sparkContext);
    when(sparkContext.conf()).thenReturn(sparkConf);
    when(sparkConf.get(SPARK_SQL_EXTENSIONS, ""))
        .thenReturn(
            "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");

    try (MockedStatic<SparkSessionUtils> mocked = mockStatic(SparkSessionUtils.class)) {
      mocked.when(SparkSessionUtils::activeSession).thenReturn(Optional.of(session));
      assertTrue(EventFilterUtils.isDeltaPlan());
    }
  }

  @Test
  void testIsDeltaPlanWithMultipleExtensionsAndSpaces() {
    SparkSession session = mock(SparkSession.class);
    SparkContext sparkContext = mock(SparkContext.class);
    SparkConf sparkConf = mock(SparkConf.class);

    when(session.sparkContext()).thenReturn(sparkContext);
    when(sparkContext.conf()).thenReturn(sparkConf);
    when(sparkConf.get(SPARK_SQL_EXTENSIONS, ""))
        .thenReturn(
            "io.delta.sql.DeltaSparkSessionExtension , org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");

    try (MockedStatic<SparkSessionUtils> mocked = mockStatic(SparkSessionUtils.class)) {
      mocked.when(SparkSessionUtils::activeSession).thenReturn(Optional.of(session));
      assertTrue(EventFilterUtils.isDeltaPlan());
    }
  }

  @Test
  void testIsDeltaPlanWithNonDeltaExtensions() {
    SparkSession session = mock(SparkSession.class);
    SparkContext sparkContext = mock(SparkContext.class);
    SparkConf sparkConf = mock(SparkConf.class);

    when(session.sparkContext()).thenReturn(sparkContext);
    when(sparkContext.conf()).thenReturn(sparkConf);
    when(sparkConf.get(SPARK_SQL_EXTENSIONS, ""))
        .thenReturn("org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");

    try (MockedStatic<SparkSessionUtils> mocked = mockStatic(SparkSessionUtils.class)) {
      mocked.when(SparkSessionUtils::activeSession).thenReturn(Optional.of(session));
      assertFalse(EventFilterUtils.isDeltaPlan());
    }
  }

  @Test
  void testIsDeltaPlanWithEmptyExtensions() {
    SparkSession session = mock(SparkSession.class);
    SparkContext sparkContext = mock(SparkContext.class);
    SparkConf sparkConf = mock(SparkConf.class);

    when(session.sparkContext()).thenReturn(sparkContext);
    when(sparkContext.conf()).thenReturn(sparkConf);
    when(sparkConf.get(SPARK_SQL_EXTENSIONS, "")).thenReturn("");

    try (MockedStatic<SparkSessionUtils> mocked = mockStatic(SparkSessionUtils.class)) {
      mocked.when(SparkSessionUtils::activeSession).thenReturn(Optional.of(session));
      assertFalse(EventFilterUtils.isDeltaPlan());
    }
  }

  @Test
  void testIsDeltaPlanWithNoActiveSession() {
    try (MockedStatic<SparkSessionUtils> mocked = mockStatic(SparkSessionUtils.class)) {
      mocked.when(SparkSessionUtils::activeSession).thenReturn(Optional.empty());
      assertFalse(EventFilterUtils.isDeltaPlan());
    }
  }
}
