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
import io.openlineage.spark.agent.lifecycle.SparkOpenLineageExtensionVisitorWrapper;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.DataSourceV2RelationDatasetExtractor;
import java.util.List;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class DataSourceV2RelationOutputDatasetBuilderTest {

  OpenLineageContext context = mock(OpenLineageContext.class);
  DatasetFactory factory = mock(DatasetFactory.class);
  DataSourceV2RelationOutputDatasetBuilder builder =
      new DataSourceV2RelationOutputDatasetBuilder(context, factory);

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
  void testIsApplied() {
    List<OpenLineage.InputDataset> datasets = mock(List.class);
    DataSourceV2Relation relation = mock(DataSourceV2Relation.class);

    when(context.getSparkExtensionVisitorWrapper())
        .thenReturn(mock(SparkOpenLineageExtensionVisitorWrapper.class));

    try (MockedStatic planUtils3MockedStatic =
        mockStatic(DataSourceV2RelationDatasetExtractor.class)) {
      when(DataSourceV2RelationDatasetExtractor.extract(factory, context, relation, true))
          .thenReturn(datasets);

      Assertions.assertEquals(
          datasets, builder.apply(new SparkListenerSQLExecutionEnd(1L, 1L), relation));
    }
  }
}
