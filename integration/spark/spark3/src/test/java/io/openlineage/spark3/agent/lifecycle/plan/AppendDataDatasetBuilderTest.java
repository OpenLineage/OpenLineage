/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.DatasetFacetsBuilder;
import io.openlineage.client.dataset.DatasetCompositeFacetsBuilder;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.lifecycle.SparkOpenLineageExtensionVisitorWrapper;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import io.openlineage.spark3.agent.utils.DataSourceV2RelationDatasetExtractor;
import io.openlineage.spark3.agent.utils.DatasetVersionDatasetFacetUtils;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NamedRelation;
import org.apache.spark.sql.catalyst.plans.logical.AppendData;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class AppendDataDatasetBuilderTest {

  public static final String TABLE_NAME = "table";
  OpenLineageContext context =
      OpenLineageContext.builder()
          .sparkSession(mock(SparkSession.class))
          .sparkContext(mock(SparkContext.class))
          .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
          .meterRegistry(new SimpleMeterRegistry())
          .openLineageConfig(new SparkOpenLineageConfig())
          .sparkExtensionVisitorWrapper(mock(SparkOpenLineageExtensionVisitorWrapper.class))
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
    DatasetCompositeFacetsBuilder compositeFacetsBuilder =
        mock(DatasetCompositeFacetsBuilder.class);
    DatasetFacetsBuilder datasetFacetsBuilder = mock(DatasetFacetsBuilder.class);
    when(compositeFacetsBuilder.getFacets()).thenReturn(datasetFacetsBuilder);
    when(datasetFacetsBuilder.version(any())).thenReturn(datasetFacetsBuilder);
    when(factory.createCompositeFacetBuilder()).thenReturn(compositeFacetsBuilder);

    try (MockedStatic mockedPlanUtils3 = mockStatic(DataSourceV2RelationDatasetExtractor.class)) {
      try (MockedStatic mockedFacetUtils = mockStatic(DatasetVersionDatasetFacetUtils.class)) {
        when(DatasetVersionDatasetFacetUtils.extractVersionFromDataSourceV2Relation(
                context, relation))
            .thenReturn(Optional.of("v2"));
        when(DataSourceV2RelationDatasetExtractor.extract(
                eq(factory), eq(context), eq(relation), any()))
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

  @Test
  void testJobNameSuffix() {
    AppendData appendData = mock(AppendData.class);
    NamedRelation table = mock(NamedRelation.class);

    when(appendData.table()).thenReturn(table);
    when(table.name()).thenReturn(TABLE_NAME);

    assertThat(builder.jobNameSuffix(appendData)).isPresent().get().isEqualTo(TABLE_NAME);
    assertThat(builder.jobNameSuffix(mock(AppendData.class))).isEmpty();
  }

  @Test
  void testJobNameSuffixForDataSourceV2Relation() {
    AppendData appendData = mock(AppendData.class);
    DataSourceV2Relation table = mock(DataSourceV2Relation.class, RETURNS_DEEP_STUBS);

    when(appendData.table()).thenReturn(table);
    when(table.table().name()).thenReturn(TABLE_NAME);

    assertThat(builder.jobNameSuffix(appendData)).isPresent().get().isEqualTo(TABLE_NAME);
    assertThat(builder.jobNameSuffix(mock(AppendData.class))).isEmpty();
  }
}
