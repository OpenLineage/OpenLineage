/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.dataset.DatasetCompositeFacetsBuilder;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.DataSourceV2RelationDatasetExtractor;
import java.util.List;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class DataSourceV2RelationInputDatasetOnStartBuilderTest {

  OpenLineageContext context = mock(OpenLineageContext.class);
  DatasetFactory factory = mock(DatasetFactory.class);
  DataSourceV2RelationInputOnStartDatasetBuilder builder =
      new DataSourceV2RelationInputOnStartDatasetBuilder(context, factory);

  @Test
  void testDataSourceV2RelationInputDatasetBuilderIsDefinedAtLogicalPlan() {
    assertFalse(builder.isDefinedAtLogicalPlan(mock(LogicalPlan.class)));
    assertTrue(builder.isDefinedAtLogicalPlan(mock(DataSourceV2Relation.class)));
  }

  @Test
  void testDataSourceV2RelationOutputDatasetBuilderIsDefinedAtLogicalPlan() {
    assertFalse(builder.isDefinedAtLogicalPlan(mock(LogicalPlan.class)));
    assertTrue(builder.isDefinedAtLogicalPlan(mock(DataSourceV2Relation.class)));
  }

  @Test
  void testIsDefinedAt() {
    assertTrue(builder.isDefinedAt(mock(SparkListenerSQLExecutionStart.class)));
    assertTrue(builder.isDefinedAt(mock(SparkListenerJobStart.class)));
    assertFalse(builder.isDefinedAt(mock(SparkListenerSQLExecutionEnd.class)));
    assertFalse(builder.isDefinedAt(mock(SparkListenerJobEnd.class)));
  }

  @Test
  void testIsApplied() {
    DataSourceV2Relation relation = mock(DataSourceV2Relation.class);
    DatasetCompositeFacetsBuilder datasetFacetsBuilder = mock(DatasetCompositeFacetsBuilder.class);
    List<OpenLineage.InputDataset> datasets = mock(List.class);

    when(factory.createCompositeFacetBuilder()).thenReturn(datasetFacetsBuilder);

    try (MockedStatic planUtils3MockedStatic =
        mockStatic(DataSourceV2RelationDatasetExtractor.class)) {
      when(DataSourceV2RelationDatasetExtractor.extractIncludingVersionFacet(
              factory, context, relation))
          .thenReturn(datasets);
      Assertions.assertEquals(datasets, builder.apply(relation));
    }
  }
}
