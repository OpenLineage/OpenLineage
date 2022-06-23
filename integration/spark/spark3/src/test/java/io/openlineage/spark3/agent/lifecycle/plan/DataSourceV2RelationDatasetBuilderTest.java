/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.AbstractQueryPlanDatasetBuilder;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.DatasetVersionDatasetFacetUtils;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import java.util.List;
import java.util.stream.Stream;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;

public class DataSourceV2RelationDatasetBuilderTest {

  OpenLineageContext context = mock(OpenLineageContext.class);
  DatasetFactory factory = mock(DatasetFactory.class);

  @Test
  public void testDataSourceV2RelationInputDatasetBuilderIsDefinedAtLogicalPlan() {
    DataSourceV2RelationInputDatasetBuilder builder =
        new DataSourceV2RelationInputDatasetBuilder(context, factory);
    assertFalse(builder.isDefinedAtLogicalPlan(mock(LogicalPlan.class)));
    assertTrue(builder.isDefinedAtLogicalPlan(mock(DataSourceV2Relation.class)));
  }

  @Test
  public void testDataSourceV2RelationOutputDatasetBuilderIsDefinedAtLogicalPlan() {
    DataSourceV2RelationOutputDatasetBuilder builder =
        new DataSourceV2RelationOutputDatasetBuilder(context, factory);
    assertFalse(builder.isDefinedAtLogicalPlan(mock(LogicalPlan.class)));
    assertTrue(builder.isDefinedAtLogicalPlan(mock(DataSourceV2Relation.class)));
  }

  @ParameterizedTest
  @MethodSource("provideBuilders")
  public void testIsApplied(
      AbstractQueryPlanDatasetBuilder builder,
      DataSourceV2Relation relation,
      OpenLineageContext context,
      DatasetFactory factory,
      OpenLineage openLineage) {
    OpenLineage.DatasetFacetsBuilder datasetFacetsBuilder =
        mock(OpenLineage.DatasetFacetsBuilder.class);
    List<OpenLineage.InputDataset> datasets = mock(List.class);

    when(openLineage.newDatasetFacetsBuilder()).thenReturn(datasetFacetsBuilder);
    when(context.getOpenLineage()).thenReturn(openLineage);

    try (MockedStatic planUtils3MockedStatic = mockStatic(PlanUtils3.class)) {
      try (MockedStatic facetUtilsMockedStatic =
          mockStatic(DatasetVersionDatasetFacetUtils.class)) {
        when(PlanUtils3.fromDataSourceV2Relation(factory, context, relation, datasetFacetsBuilder))
            .thenReturn(datasets);

        if (builder instanceof DataSourceV2RelationOutputDatasetBuilder) {
          assertEquals(
              datasets,
              ((DataSourceV2RelationOutputDatasetBuilder) builder)
                  .apply(new SparkListenerSQLExecutionEnd(1L, 1L), relation));
        } else {
          assertEquals(datasets, builder.apply(relation));
        }

        facetUtilsMockedStatic.verify(
            () ->
                DatasetVersionDatasetFacetUtils.includeDatasetVersion(
                    context, datasetFacetsBuilder, relation),
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
