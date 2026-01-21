/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import com.google.cloud.spark.bigquery.BigQueryRelation;
import io.openlineage.client.utils.DatasetIdentifier;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class BigQueryUtilsTest {

  @Test
  void testExtractDatasetIdentifierUsesBigQueryNamespace() {
    BigQueryRelation mockRelation = mock(BigQueryRelation.class);
    Object mockTableId = mock(Object.class);

    try (MockedStatic<ReflectionUtils> reflectionUtils = mockStatic(ReflectionUtils.class)) {
      reflectionUtils
          .when(() -> ReflectionUtils.tryExecuteMethod(mockRelation, "getTableId"))
          .thenReturn(Optional.of(mockTableId));

      reflectionUtils
          .when(
              () ->
                  ReflectionUtils.tryExecuteStaticMethodForClassName(
                      "com.google.cloud.bigquery.connector.common.BigQueryUtil",
                      "friendlyTableName",
                      mockTableId))
          .thenReturn(Optional.of("my-project.my-dataset.my-table"));

      List<DatasetIdentifier> result = BigQueryUtils.extractDatasetIdentifier(mockRelation);

      assertThat(result).hasSize(1);
      assertThat(result.get(0).getNamespace()).isEqualTo("bigquery");
      assertThat(result.get(0).getName()).isEqualTo("my-project.my-dataset.my-table");
    }
  }

  @Test
  void testExtractDatasetIdentifierReturnsEmptyWhenTableIdNotFound() {
    BigQueryRelation mockRelation = mock(BigQueryRelation.class);

    try (MockedStatic<ReflectionUtils> reflectionUtils = mockStatic(ReflectionUtils.class)) {
      reflectionUtils
          .when(() -> ReflectionUtils.tryExecuteMethod(mockRelation, "getTableId"))
          .thenReturn(Optional.empty());

      List<DatasetIdentifier> result = BigQueryUtils.extractDatasetIdentifier(mockRelation);

      assertThat(result).isEmpty();
    }
  }

  @Test
  void testExtractDatasetIdentifierReturnsEmptyWhenFriendlyNameNotFound() {
    BigQueryRelation mockRelation = mock(BigQueryRelation.class);
    Object mockTableId = mock(Object.class);

    try (MockedStatic<ReflectionUtils> reflectionUtils = mockStatic(ReflectionUtils.class)) {
      reflectionUtils
          .when(() -> ReflectionUtils.tryExecuteMethod(mockRelation, "getTableId"))
          .thenReturn(Optional.of(mockTableId));

      reflectionUtils
          .when(
              () ->
                  ReflectionUtils.tryExecuteStaticMethodForClassName(
                      "com.google.cloud.bigquery.connector.common.BigQueryUtil",
                      "friendlyTableName",
                      mockTableId))
          .thenReturn(Optional.empty());

      reflectionUtils
          .when(
              () ->
                  ReflectionUtils.tryExecuteStaticMethodForClassName(
                      "com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.connector.common.BigQueryUtil",
                      "friendlyTableName",
                      mockTableId))
          .thenReturn(Optional.empty());

      List<DatasetIdentifier> result = BigQueryUtils.extractDatasetIdentifier(mockRelation);

      assertThat(result).isEmpty();
    }
  }
}
