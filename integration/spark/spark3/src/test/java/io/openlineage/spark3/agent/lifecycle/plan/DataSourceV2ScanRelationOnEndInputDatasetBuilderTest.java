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
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.DataSourceV2RelationDatasetExtractor;
import io.openlineage.spark3.agent.utils.DatasetVersionDatasetFacetUtils;
import java.util.List;
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
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class DataSourceV2ScanRelationOnEndInputDatasetBuilderTest {

  private final OpenLineage openLineage = mock(OpenLineage.class);
  private final OpenLineageContext context = mock(OpenLineageContext.class);
  private final DatasetFactory<InputDataset> factory = mock(DatasetFactory.class);
  private final DataSourceV2ScanRelationOnEndInputDatasetBuilder builder =
      new DataSourceV2ScanRelationOnEndInputDatasetBuilder(context, factory);

  @Test
  void testIsDefinedAt() {
    Assertions.assertThat(builder)
        .returns(true, b -> b.isDefinedAt(mock(SparkListenerSQLExecutionEnd.class)))
        .returns(false, b -> b.isDefinedAt(mock(SparkListenerSQLExecutionStart.class)))
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
    OpenLineage.DatasetFacetsBuilder datasetFacetsBuilder =
        mock(OpenLineage.DatasetFacetsBuilder.class);
    List<InputDataset> datasets = mock(List.class);
    DataSourceV2ScanRelation scanRelation = mock(DataSourceV2ScanRelation.class);
    DataSourceV2Relation relation = mock(DataSourceV2Relation.class);

    when(openLineage.newDatasetFacetsBuilder()).thenReturn(datasetFacetsBuilder);
    when(context.getOpenLineage()).thenReturn(openLineage);
    when(scanRelation.relation()).thenReturn(relation);

    try (MockedStatic<DataSourceV2RelationDatasetExtractor> ignored =
        mockStatic(DataSourceV2RelationDatasetExtractor.class)) {
      try (MockedStatic<DatasetVersionDatasetFacetUtils> facetUtilsMockedStatic =
          mockStatic(DatasetVersionDatasetFacetUtils.class)) {
        when(DataSourceV2RelationDatasetExtractor.extract(
                factory, context, relation, datasetFacetsBuilder))
            .thenReturn(datasets);

        Assertions.assertThat(builder.apply(scanRelation)).isEqualTo(datasets);

        facetUtilsMockedStatic.verify(
            () ->
                DatasetVersionDatasetFacetUtils.includeDatasetVersion(
                    context, datasetFacetsBuilder, relation),
            times(0));
      }
    }
  }
}
