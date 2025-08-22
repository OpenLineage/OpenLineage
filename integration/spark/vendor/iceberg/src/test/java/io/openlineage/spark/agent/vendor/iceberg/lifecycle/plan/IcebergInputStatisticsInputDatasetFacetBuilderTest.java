/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.iceberg.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.openlineage.spark.api.OpenLineageContext;
import org.apache.spark.sql.connector.read.Scan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class IcebergInputStatisticsInputDatasetFacetBuilderTest {

  @Mock private OpenLineageContext context;

  private IcebergInputStatisticsInputDatasetFacetBuilder builder;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    builder = new IcebergInputStatisticsInputDatasetFacetBuilder(context);
  }

  @Test
  void testIsDefinedAtReturnsFalseForNonScanObject() {
    // Given
    Object nonScanObject = new Object();

    // When
    boolean result = builder.isDefinedAt(nonScanObject);

    // Then
    assertThat(result).isFalse();
  }

  @Test
  void testIsDefinedAtReturnsFalseForNullObject() {
    // Given
    Object nullObject = null;

    // When
    boolean result = builder.isDefinedAt(nullObject);

    // Then
    assertThat(result).isFalse();
  }

  @Test
  void testIsDefinedAtReturnsFalseForGenericScanObject() {
    // Given
    Scan genericScan = mock(Scan.class);

    // When
    boolean result = builder.isDefinedAt(genericScan);

    // Then
    assertThat(result).isFalse();
  }

  @Test
  void testIsDefinedAtReturnsFalseWhenSparkPartitioningAwareScanClassNotFound() {
    // This test verifies the behavior when the Iceberg class is not on the classpath
    // Since we're testing with a mock Scan that doesn't implement SparkPartitioningAwareScan,
    // the method should return false

    // Given
    Scan scan = mock(Scan.class);

    // When
    boolean result = builder.isDefinedAt(scan);

    // Then
    assertThat(result).isFalse();
  }

  // Note: Testing the positive case (when isDefinedAt returns true) would require
  // having the actual SparkPartitioningAwareScan class on the classpath and creating
  // a proper implementation or mock that extends/implements it. This would typically
  // be done in integration tests where the full Iceberg dependency is available.
}
