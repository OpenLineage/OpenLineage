/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.dataset.DatasetCompositeFacetsBuilder;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.lifecycle.SparkOpenLineageExtensionVisitorWrapper;
import io.openlineage.spark.api.AbstractQueryPlanDatasetBuilder;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import io.openlineage.spark3.agent.utils.DataSourceV2RelationDatasetExtractor;
import io.openlineage.spark3.agent.utils.DatasetVersionDatasetFacetUtils;
import java.util.List;
import java.util.stream.Stream;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;

class DataSourceV2RelationDatasetBuilderTest {

  OpenLineageContext context =
      OpenLineageContext.builder()
          .sparkSession(mock(SparkSession.class))
          .sparkContext(mock(SparkContext.class))
          .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
          .meterRegistry(new SimpleMeterRegistry())
          .openLineageConfig(new SparkOpenLineageConfig())
          .sparkExtensionVisitorWrapper(mock(SparkOpenLineageExtensionVisitorWrapper.class))
          .build();
  DatasetFactory factory = mock(DatasetFactory.class);

  @Test
  void testDataSourceV2RelationInputDatasetBuilderIsDefinedAtLogicalPlan() {
    DataSourceV2RelationInputDatasetBuilder builder =
        new DataSourceV2RelationInputDatasetBuilder(context, factory);
    assertFalse(builder.isDefinedAtLogicalPlan(mock(LogicalPlan.class)));
    assertTrue(builder.isDefinedAtLogicalPlan(mock(DataSourceV2Relation.class)));
  }

  @Test
  void testDataSourceV2RelationOutputDatasetBuilderIsDefinedAtLogicalPlan() {
    DataSourceV2RelationOutputDatasetBuilder builder =
        new DataSourceV2RelationOutputDatasetBuilder(context, factory);
    assertFalse(builder.isDefinedAtLogicalPlan(mock(LogicalPlan.class)));
    assertTrue(builder.isDefinedAtLogicalPlan(mock(DataSourceV2Relation.class)));
  }

  @ParameterizedTest
  @MethodSource("provideBuilders")
  void testIsApplied(
      AbstractQueryPlanDatasetBuilder builder,
      DataSourceV2Relation relation,
      OpenLineageContext context,
      DatasetFactory factory,
      OpenLineage openLineage) {
    DatasetCompositeFacetsBuilder datasetFacetsBuilder = mock(DatasetCompositeFacetsBuilder.class);
    List<OpenLineage.InputDataset> datasets = mock(List.class);

    when(context.getOpenLineage()).thenReturn(openLineage);
    when(context.getSparkExtensionVisitorWrapper())
        .thenReturn(mock(SparkOpenLineageExtensionVisitorWrapper.class));
    when(factory.createCompositeFacetBuilder()).thenReturn(datasetFacetsBuilder);

    try (MockedStatic planUtils3MockedStatic =
        mockStatic(DataSourceV2RelationDatasetExtractor.class)) {
      try (MockedStatic facetUtilsMockedStatic =
          mockStatic(DatasetVersionDatasetFacetUtils.class)) {
        when(DataSourceV2RelationDatasetExtractor.extract(
                factory, context, relation, datasetFacetsBuilder))
            .thenReturn(datasets);

        if (builder instanceof DataSourceV2RelationOutputDatasetBuilder) {
          Assertions.assertEquals(
              datasets,
              ((DataSourceV2RelationOutputDatasetBuilder) builder)
                  .apply(new SparkListenerSQLExecutionEnd(1L, 1L), relation));
        } else {
          Assertions.assertEquals(datasets, builder.apply(relation));
        }

        facetUtilsMockedStatic.verify(
            () ->
                DatasetVersionDatasetFacetUtils.includeDatasetVersion(
                    context, datasetFacetsBuilder.getFacets(), relation),
            times(1));
      }
    }
  }

  private static Stream<Arguments> provideBuilders() {
    OpenLineageContext context = mock(OpenLineageContext.class);
    DatasetFactory factory = mock(DatasetFactory.class);
    OpenLineage openLineage = mock(OpenLineage.class);

    return Stream.of(
        Arguments.of(
            new DataSourceV2RelationInputDatasetBuilder(context, factory),
            mock(DataSourceV2Relation.class),
            context,
            factory,
            openLineage),
        Arguments.of(
            new DataSourceV2RelationOutputDatasetBuilder(context, factory),
            mock(DataSourceV2Relation.class),
            context,
            factory,
            openLineage));
  }
}
