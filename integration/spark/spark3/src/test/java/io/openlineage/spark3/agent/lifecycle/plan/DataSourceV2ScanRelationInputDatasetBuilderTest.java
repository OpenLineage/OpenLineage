/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import io.openlineage.spark3.agent.utils.DataSourceV2RelationDatasetExtractor;
import io.openlineage.spark3.agent.utils.DatasetVersionDatasetFacetUtils;
import java.util.List;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class DataSourceV2ScanRelationInputDatasetBuilderTest {

  OpenLineage openLineage = mock(OpenLineage.class);
  OpenLineageContext context = mock(OpenLineageContext.class);
  DatasetFactory factory = mock(DatasetFactory.class);
  DataSourceV2ScanRelationInputDatasetBuilder builder;

  @BeforeEach
  void setUp() {
    when(context.getOpenLineageConfig()).thenReturn(new SparkOpenLineageConfig());
    builder = new DataSourceV2ScanRelationInputDatasetBuilder(context, factory);
  }

  @Test
  void testIsDefinedAt() {
    Assertions.assertFalse(builder.isDefinedAtLogicalPlan(mock(LogicalPlan.class)));
    Assertions.assertTrue(builder.isDefinedAtLogicalPlan(mock(DataSourceV2ScanRelation.class)));
  }

  @Test
  void testApply() {
    OpenLineage.DatasetFacetsBuilder datasetFacetsBuilder =
        mock(OpenLineage.DatasetFacetsBuilder.class);
    List<OpenLineage.InputDataset> datasets = mock(List.class);
    DataSourceV2ScanRelation scanRelation = mock(DataSourceV2ScanRelation.class);
    DataSourceV2Relation relation = mock(DataSourceV2Relation.class);

    when(openLineage.newDatasetFacetsBuilder()).thenReturn(datasetFacetsBuilder);
    when(context.getOpenLineage()).thenReturn(openLineage);
    when(scanRelation.relation()).thenReturn(relation);

    try (MockedStatic planUtils3MockedStatic =
        mockStatic(DataSourceV2RelationDatasetExtractor.class)) {
      try (MockedStatic facetUtilsMockedStatic =
          mockStatic(DatasetVersionDatasetFacetUtils.class)) {
        when(DataSourceV2RelationDatasetExtractor.extract(
                factory, context, relation, datasetFacetsBuilder))
            .thenReturn(datasets);

        Assertions.assertEquals(datasets, builder.apply(scanRelation));

        facetUtilsMockedStatic.verify(
            () ->
                DatasetVersionDatasetFacetUtils.includeDatasetVersion(
                    context, datasetFacetsBuilder, relation),
            times(1));
      }
    }
  }
}
