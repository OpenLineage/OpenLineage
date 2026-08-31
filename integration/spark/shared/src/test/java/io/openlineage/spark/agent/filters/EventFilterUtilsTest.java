/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.filters;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.spark.api.OpenLineageContext;
import java.util.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.junit.jupiter.api.Test;

class EventFilterUtilsTest {

  private static final String SPARK_SQL_EXTENSIONS = "spark.sql.extensions";

  @Test
  void testIsDeltaPlanWithSingleExtension() {
    OpenLineageContext context = mock(OpenLineageContext.class);
    SparkContext sparkContext = mock(SparkContext.class);
    SparkConf sparkConf = mock(SparkConf.class);

    when(context.getSparkContext()).thenReturn(Optional.of(sparkContext));
    when(sparkContext.conf()).thenReturn(sparkConf);
    when(sparkConf.get(SPARK_SQL_EXTENSIONS, ""))
        .thenReturn("io.delta.sql.DeltaSparkSessionExtension");

    assertTrue(EventFilterUtils.isDeltaPlan(context));
  }

  @Test
  void testIsDeltaPlanWithMultipleExtensions() {
    OpenLineageContext context = mock(OpenLineageContext.class);
    SparkContext sparkContext = mock(SparkContext.class);
    SparkConf sparkConf = mock(SparkConf.class);

    when(context.getSparkContext()).thenReturn(Optional.of(sparkContext));
    when(sparkContext.conf()).thenReturn(sparkConf);
    when(sparkConf.get(SPARK_SQL_EXTENSIONS, ""))
        .thenReturn(
            "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");

    assertTrue(EventFilterUtils.isDeltaPlan(context));
  }

  @Test
  void testIsDeltaPlanWithMultipleExtensionsAndSpaces() {
    OpenLineageContext context = mock(OpenLineageContext.class);
    SparkContext sparkContext = mock(SparkContext.class);
    SparkConf sparkConf = mock(SparkConf.class);

    when(context.getSparkContext()).thenReturn(Optional.of(sparkContext));
    when(sparkContext.conf()).thenReturn(sparkConf);
    when(sparkConf.get(SPARK_SQL_EXTENSIONS, ""))
        .thenReturn(
            "io.delta.sql.DeltaSparkSessionExtension , org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");

    assertTrue(EventFilterUtils.isDeltaPlan(context));
  }

  @Test
  void testIsDeltaPlanWithNonDeltaExtensions() {
    OpenLineageContext context = mock(OpenLineageContext.class);
    SparkContext sparkContext = mock(SparkContext.class);
    SparkConf sparkConf = mock(SparkConf.class);

    when(context.getSparkContext()).thenReturn(Optional.of(sparkContext));
    when(sparkContext.conf()).thenReturn(sparkConf);
    when(sparkConf.get(SPARK_SQL_EXTENSIONS, ""))
        .thenReturn("org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");

    assertFalse(EventFilterUtils.isDeltaPlan(context));
  }

  @Test
  void testIsDeltaPlanWithEmptyExtensions() {
    OpenLineageContext context = mock(OpenLineageContext.class);
    SparkContext sparkContext = mock(SparkContext.class);
    SparkConf sparkConf = mock(SparkConf.class);

    when(context.getSparkContext()).thenReturn(Optional.of(sparkContext));
    when(sparkContext.conf()).thenReturn(sparkConf);
    when(sparkConf.get(SPARK_SQL_EXTENSIONS, "")).thenReturn("");

    assertFalse(EventFilterUtils.isDeltaPlan(context));
  }

  @Test
  void testIsDeltaPlanWithNoSparkContext() {
    OpenLineageContext context = mock(OpenLineageContext.class);
    when(context.getSparkContext()).thenReturn(Optional.empty());

    assertFalse(EventFilterUtils.isDeltaPlan(context));
  }
}
