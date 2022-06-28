/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark32.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark32.agent.utils.DatasetVersionDatasetFacetUtils;
import io.openlineage.spark32.agent.utils.PlanUtils3;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NamedRelation;
import org.apache.spark.sql.catalyst.plans.logical.AppendData;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

class AppendDataDatasetBuilderTest {

  OpenLineageContext context =
      OpenLineageContext.builder()
          .sparkSession(Optional.of(mock(SparkSession.class)))
          .sparkContext(mock(SparkContext.class))
          .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
          .build();
  DatasetFactory<OpenLineage.OutputDataset> factory = mock(DatasetFactory.class);
  AppendDataDatasetBuilder builder = new AppendDataDatasetBuilder(context, factory);

  @Test
  void isDefinedAtLogicalPlan() {
    assertTrue(builder.isDefinedAtLogicalPlan(mock(AppendData.class)));
    assertFalse(builder.isDefinedAtLogicalPlan(mock(LogicalPlan.class)));
  }

  @Test
  void testApplyWithDataSourceV2Relation() {
    AppendData appendData = mock(AppendData.class);
    DataSourceV2Relation relation = mock(DataSourceV2Relation.class);
    OpenLineage.OutputDataset dataset = mock(OpenLineage.OutputDataset.class);
    when(appendData.table()).thenReturn(relation);

    try (MockedStatic mockedPlanUtils3 = mockStatic(PlanUtils3.class)) {
      try (MockedStatic mockedFacetUtils = mockStatic(DatasetVersionDatasetFacetUtils.class)) {
        when(DatasetVersionDatasetFacetUtils.extractVersionFromDataSourceV2Relation(relation))
            .thenReturn(Optional.of("v2"));
        when(PlanUtils3.fromDataSourceV2Relation(eq(factory), eq(context), eq(relation), any()))
            .thenReturn(Collections.singletonList(dataset));

        List<OpenLineage.OutputDataset> datasets =
            builder.apply(new SparkListenerSQLExecutionEnd(1L, 1L), appendData);

        assertEquals(1, datasets.size());
        assertEquals(dataset, datasets.get(0));
      }
    }
  }

  @Test
  void testApplyWithoutDataSourceV2Relation() {
    AppendData appendData = mock(AppendData.class);
    when(appendData.table())
        .thenReturn(
            (NamedRelation)
                mock(LogicalPlan.class, withSettings().extraInterfaces(NamedRelation.class)));

    assertEquals(0, builder.apply(new SparkListenerSQLExecutionEnd(1L, 1L), appendData).size());
  }
}
