/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark33.agent.lifecycle.plan;

import static io.openlineage.client.OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.DatasetVersionDatasetFacetUtils;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NamedRelation;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceIcebergData;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class ReplaceIcebergDataDatasetBuilderTest {

  OpenLineage openLineage = mock(OpenLineage.class);
  OpenLineageContext openLineageContext =
      OpenLineageContext.builder()
          .sparkSession(Optional.of(mock(SparkSession.class)))
          .sparkContext(mock(SparkContext.class))
          .openLineage(openLineage)
          .build();

  ReplaceIcebergDataDatasetBuilder builder =
      new ReplaceIcebergDataDatasetBuilder(openLineageContext);

  ReplaceIcebergData plan = mock(ReplaceIcebergData.class);
  SparkListenerEvent event = mock(SparkListenerSQLExecutionEnd.class);

  @Test
  void testIsDefinedAtLogicalPlan() {
    assertTrue(builder.isDefinedAtLogicalPlan(mock(ReplaceIcebergData.class)));
    assertFalse(builder.isDefinedAtLogicalPlan(mock(LogicalPlan.class)));
  }

  @Test
  void testApplyWhenNoDataSourceV2RelationTable() {
    when(plan.table()).thenReturn(mock(NamedRelation.class));
    assertEquals(0, builder.apply(event, plan).size());
  }

  @Test
  void testApply() {
    DataSourceV2Relation table = mock(DataSourceV2Relation.class);
    OpenLineage.OutputDataset expectedDataset = mock(OpenLineage.OutputDataset.class);
    OpenLineage.DatasetFacetsBuilder datasetFacetsBuilder =
        mock(OpenLineage.DatasetFacetsBuilder.class);
    when(openLineage.newDatasetFacetsBuilder()).thenReturn(datasetFacetsBuilder);

    when(plan.table()).thenReturn(table);
    try (MockedStatic mocked = mockStatic(PlanUtils3.class)) {
      try (MockedStatic mockedFacetUtils = mockStatic(DatasetVersionDatasetFacetUtils.class)) {
        when(DatasetVersionDatasetFacetUtils.extractVersionFromDataSourceV2Relation(
                openLineageContext, table))
            .thenReturn(Optional.of("v2"));

        when(PlanUtils3.fromDataSourceV2Relation(
                any(DatasetFactory.class),
                eq(openLineageContext),
                eq(table),
                eq(datasetFacetsBuilder)))
            .thenReturn(Arrays.asList(expectedDataset));

        List<OpenLineage.OutputDataset> datasets = builder.apply(event, plan);

        assertEquals(1, datasets.size());
        assertEquals(expectedDataset, datasets.get(0));

        verify(datasetFacetsBuilder)
            .lifecycleStateChange(openLineage.newLifecycleStateChangeDatasetFacet(OVERWRITE, null));

        mockedFacetUtils.verify(
            () ->
                DatasetVersionDatasetFacetUtils.includeDatasetVersion(
                    openLineageContext, datasetFacetsBuilder, table),
            times(1));
      }
    }
  }
}
