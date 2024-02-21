/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark33.vendor.iceberg.agent.lifecycle.plan.plan;

import static io.openlineage.client.OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.DatasetVersionDatasetFacetUtils;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import io.openlineage.spark33.vendor.iceberg.agent.lifecycle.plan.ReplaceIcebergDataDatasetBuilder;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class ReplaceIcebergDataDatasetBuilderTest {

  OpenLineage openLineage = mock(OpenLineage.class);
  OpenLineageContext openLineageContext =
      OpenLineageContext.builder()
          .sparkSession(mock(SparkSession.class))
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
    Mockito.when(plan.table()).thenReturn(mock(NamedRelation.class));
    assertEquals(0, builder.apply(event, plan).size());
  }

  @Test
  void testApply() {
    DataSourceV2Relation table = mock(DataSourceV2Relation.class);
    OpenLineage.OutputDataset expectedDataset = mock(OpenLineage.OutputDataset.class);
    OpenLineage.DatasetFacetsBuilder datasetFacetsBuilder =
        mock(OpenLineage.DatasetFacetsBuilder.class);
    Mockito.when(openLineage.newDatasetFacetsBuilder()).thenReturn(datasetFacetsBuilder);

    Mockito.when(plan.table()).thenReturn(table);
    try (MockedStatic mocked = Mockito.mockStatic(PlanUtils3.class)) {
      try (MockedStatic mockedFacetUtils =
          Mockito.mockStatic(DatasetVersionDatasetFacetUtils.class)) {
        Mockito.when(
                DatasetVersionDatasetFacetUtils.extractVersionFromDataSourceV2Relation(
                    openLineageContext, table))
            .thenReturn(Optional.of("v2"));

        Mockito.when(
                PlanUtils3.fromDataSourceV2Relation(
                    ArgumentMatchers.any(DatasetFactory.class),
                    ArgumentMatchers.eq(openLineageContext),
                    ArgumentMatchers.eq(table),
                    ArgumentMatchers.eq(datasetFacetsBuilder)))
            .thenReturn(Arrays.asList(expectedDataset));

        List<OpenLineage.OutputDataset> datasets = builder.apply(event, plan);

        Assertions.assertEquals(1, datasets.size());
        Assertions.assertEquals(expectedDataset, datasets.get(0));

        Mockito.verify(datasetFacetsBuilder)
            .lifecycleStateChange(openLineage.newLifecycleStateChangeDatasetFacet(OVERWRITE, null));

        mockedFacetUtils.verify(
            () ->
                DatasetVersionDatasetFacetUtils.includeDatasetVersion(
                    openLineageContext, datasetFacetsBuilder, table),
            Mockito.times(1));
      }
    }
  }
}
