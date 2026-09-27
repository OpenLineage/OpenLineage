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
    assertTrue(
        EventFilterUtils.isDeltaPlan(
            contextWithExtensions("io.delta.sql.DeltaSparkSessionExtension")));
  }

  @Test
  void testIsDeltaPlanWithMultipleExtensions() {
    assertTrue(
        EventFilterUtils.isDeltaPlan(
            contextWithExtensions(
                "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")));
  }

  @Test
  void testIsDeltaPlanWithMultipleExtensionsAndSpaces() {
    assertTrue(
        EventFilterUtils.isDeltaPlan(
            contextWithExtensions(
                "io.delta.sql.DeltaSparkSessionExtension , org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")));
  }

  @Test
  void testIsDeltaPlanWithNonDeltaExtensions() {
    assertFalse(
        EventFilterUtils.isDeltaPlan(
            contextWithExtensions(
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")));
  }

  @Test
  void testIsDeltaPlanWithEmptyExtensions() {
    assertFalse(EventFilterUtils.isDeltaPlan(contextWithExtensions("")));
  }

  @Test
  void testIsDeltaPlanWithNoSparkContext() {
    assertFalse(EventFilterUtils.isDeltaPlan(mock(OpenLineageContext.class)));
  }

  private OpenLineageContext contextWithExtensions(String extensions) {
    OpenLineageContext context = mock(OpenLineageContext.class);
    SparkContext sparkContext = mock(SparkContext.class);
    SparkConf sparkConf = mock(SparkConf.class);
    when(context.getSparkContext()).thenReturn(Optional.of(sparkContext));
    when(sparkContext.conf()).thenReturn(sparkConf);
    when(sparkConf.get(SPARK_SQL_EXTENSIONS, "")).thenReturn(extensions);
    return context;
  }
}
