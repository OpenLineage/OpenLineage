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

import io.openlineage.spark.agent.util.SparkSessionUtils;
import java.util.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class EventFilterUtilsTest {

  private static final String SPARK_SQL_EXTENSIONS = "spark.sql.extensions";

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
