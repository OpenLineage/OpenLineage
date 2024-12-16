/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.dataset.DatasetCompositeFacetsBuilder;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.lifecycle.SparkOpenLineageExtensionVisitorWrapper;
import io.openlineage.spark.agent.util.DatasetVersionUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.DataSourceV2RelationDatasetExtractor;
import io.openlineage.spark3.agent.utils.DatasetVersionDatasetFacetUtils;
import java.util.List;
import java.util.Optional;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.scheduler.SparkListenerTaskStart;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class DataSourceV2ScanRelationOnStartInputDatasetBuilderTest {

  private final OpenLineage openLineage = mock(OpenLineage.class);
  private final OpenLineageContext context = mock(OpenLineageContext.class);
  private final DatasetFactory<OpenLineage.InputDataset> factory = mock(DatasetFactory.class);
  private final DataSourceV2ScanRelationOnStartInputDatasetBuilder builder =
      new DataSourceV2ScanRelationOnStartInputDatasetBuilder(context, factory);
  private final DatasetCompositeFacetsBuilder datasetFacetsBuilder =
      new DatasetCompositeFacetsBuilder(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI));

  @BeforeEach
  public void setup() {
    when(factory.createCompositeFacetBuilder()).thenReturn(datasetFacetsBuilder);
  }

  @Test
  void testIsDefinedAt() {
    Assertions.assertThat(builder)
        .returns(true, b -> b.isDefinedAt(mock(SparkListenerSQLExecutionStart.class)))
        .returns(false, b -> b.isDefinedAt(mock(SparkListenerSQLExecutionEnd.class)))
        .returns(false, b -> b.isDefinedAt(mock(SparkListenerApplicationStart.class)))
        .returns(false, b -> b.isDefinedAt(mock(SparkListenerApplicationEnd.class)))
        .returns(false, b -> b.isDefinedAt(mock(SparkListenerStageSubmitted.class)))
        .returns(false, b -> b.isDefinedAt(mock(SparkListenerStageCompleted.class)))
        .returns(false, b -> b.isDefinedAt(mock(SparkListenerTaskStart.class)))
        .returns(false, b -> b.isDefinedAt(mock(SparkListenerTaskEnd.class)))
        .returns(false, b -> b.isDefinedAt(mock(SparkListenerJobStart.class)))
        .returns(false, b -> b.isDefinedAt(mock(SparkListenerJobEnd.class)));
  }

  @Test
  void testIsDefinedAtLogicalPlan() {
    Assertions.assertThat(builder)
        .returns(false, b -> b.isDefinedAtLogicalPlan(mock(LogicalPlan.class)))
        .returns(true, b -> b.isDefinedAtLogicalPlan(mock(DataSourceV2ScanRelation.class)));
  }

  @Test
  void testApply() {
    List<OpenLineage.InputDataset> datasets = mock(List.class);
    DataSourceV2ScanRelation scanRelation = mock(DataSourceV2ScanRelation.class);
    DataSourceV2Relation relation = mock(DataSourceV2Relation.class);

    when(context.getOpenLineage()).thenReturn(openLineage);
    when(scanRelation.relation()).thenReturn(relation);
    when(context.getSparkExtensionVisitorWrapper())
        .thenReturn(mock(SparkOpenLineageExtensionVisitorWrapper.class));

    try (MockedStatic<DataSourceV2RelationDatasetExtractor> ignored =
        mockStatic(DataSourceV2RelationDatasetExtractor.class)) {
      try (MockedStatic<DatasetVersionDatasetFacetUtils> facetUtilsMockedStatic =
          mockStatic(DatasetVersionDatasetFacetUtils.class)) {
        try (MockedStatic<DatasetVersionUtils> versionUtils =
            mockStatic(DatasetVersionUtils.class)) {
          when(DatasetVersionDatasetFacetUtils.extractVersionFromDataSourceV2Relation(
                  context, relation))
              .thenReturn(Optional.of("v2"));
          when(DataSourceV2RelationDatasetExtractor.extract(
                  factory, context, relation, datasetFacetsBuilder))
              .thenReturn(datasets);

          Assertions.assertThat(builder.apply(scanRelation)).isEqualTo(datasets);

          versionUtils.verify(
              () ->
                  DatasetVersionUtils.buildVersionFacets(
                      eq(context), eq(datasetFacetsBuilder), any()),
              times(1));
        }
      }
    }
  }
}
