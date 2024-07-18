/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark33.agent.lifecycle.plan;

import static io.openlineage.client.OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE;
import static org.assertj.core.api.Assertions.assertThat;
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

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import io.openlineage.spark3.agent.utils.DataSourceV2RelationDatasetExtractor;
import io.openlineage.spark3.agent.utils.DatasetVersionDatasetFacetUtils;
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

class ReplaceIcebergDataDatasetBuilderTest {

  OpenLineage openLineage = mock(OpenLineage.class);
  OpenLineageContext openLineageContext =
      OpenLineageContext.builder()
          .sparkSession(mock(SparkSession.class))
          .sparkContext(mock(SparkContext.class))
          .openLineage(openLineage)
          .meterRegistry(new SimpleMeterRegistry())
          .openLineageConfig(new SparkOpenLineageConfig())
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
    try (MockedStatic mocked = mockStatic(DataSourceV2RelationDatasetExtractor.class)) {
      try (MockedStatic mockedFacetUtils = mockStatic(DatasetVersionDatasetFacetUtils.class)) {
        when(DatasetVersionDatasetFacetUtils.extractVersionFromDataSourceV2Relation(
                openLineageContext, table))
            .thenReturn(Optional.of("v2"));

        when(DataSourceV2RelationDatasetExtractor.extract(
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

  @Test
  void testJobNameSuffix() {
    assertThat(builder.jobNameSuffix(mock(LogicalPlan.class))).isEmpty();

    ReplaceIcebergData replaceIcebergData = mock(ReplaceIcebergData.class);
    NamedRelation namedRelation = mock(NamedRelation.class);
    when(replaceIcebergData.table()).thenReturn(namedRelation);
    when(namedRelation.name()).thenReturn("table");
    assertThat(builder.jobNameSuffix(replaceIcebergData).get()).isEqualTo("table");
  }
}
