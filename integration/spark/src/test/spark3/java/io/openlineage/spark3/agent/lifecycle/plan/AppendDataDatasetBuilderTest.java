package io.openlineage.spark3.agent.lifecycle.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.EventEmitter;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelLineageUtils;
import io.openlineage.spark3.agent.utils.DatasetVersionDatasetFacetUtils;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NamedRelation;
import org.apache.spark.sql.catalyst.plans.logical.AppendData;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class AppendDataDatasetBuilderTest {

  OpenLineageContext context =
      OpenLineageContext.builder()
          .sparkSession(Optional.of(mock(SparkSession.class)))
          .sparkContext(mock(SparkContext.class))
          .openLineage(new OpenLineage(EventEmitter.OPEN_LINEAGE_PRODUCER_URI))
          .build();
  DatasetFactory<OpenLineage.OutputDataset> factory = mock(DatasetFactory.class);
  AppendDataDatasetBuilder builder = new AppendDataDatasetBuilder(context, factory);
  AppendData appendData = mock(AppendData.class);
  StructType schema = mock(StructType.class);
  DataSourceV2Relation relation = mock(DataSourceV2Relation.class);
  OpenLineage.OutputDataset dataset = mock(OpenLineage.OutputDataset.class);
  OpenLineage.OutputDataset datasetWithColumnLineage = mock(OpenLineage.OutputDataset.class);
  OpenLineage.ColumnLineageDatasetFacet columnLineageDatasetFacet =
      mock(OpenLineage.ColumnLineageDatasetFacet.class);

  @Test
  public void isDefinedAtLogicalPlan() {
    assertTrue(builder.isDefinedAtLogicalPlan(mock(AppendData.class)));
    assertFalse(builder.isDefinedAtLogicalPlan(mock(LogicalPlan.class)));
  }

  @Test
  public void testApplyWithDataSourceV2RelationAndColumLineagePresent() {
    when(relation.schema()).thenReturn(schema);
    when(appendData.table()).thenReturn(relation);

    try (MockedStatic mockedPlanUtils3 = mockStatic(PlanUtils3.class)) {
      try (MockedStatic mockedFacetUtils = mockStatic(DatasetVersionDatasetFacetUtils.class)) {
        try (MockedStatic mockedColumnLineage = mockStatic(ColumnLevelLineageUtils.class)) {
          when(DatasetVersionDatasetFacetUtils.extractVersionFromDataSourceV2Relation(relation))
              .thenReturn(Optional.of("v2"));
          when(PlanUtils3.fromDataSourceV2Relation(eq(factory), eq(context), eq(relation), any()))
              .thenReturn(Collections.singletonList(dataset));
          when(ColumnLevelLineageUtils.buildColumnLineageDatasetFacet(context, schema))
              .thenReturn(Optional.of(columnLineageDatasetFacet));
          when(ColumnLevelLineageUtils.rewriteOutputDataset(dataset, columnLineageDatasetFacet))
              .thenReturn(datasetWithColumnLineage);

          List<OpenLineage.OutputDataset> datasets = builder.apply(appendData);

          assertEquals(1, datasets.size());
          assertEquals(datasetWithColumnLineage, datasets.get(0));
        }
      }
    }
  }

  @Test
  public void testApplyWithDataSourceV2RelationAndWithoutColumLineage() {
    when(relation.schema()).thenReturn(schema);
    when(appendData.table()).thenReturn(relation);
    OpenLineage.ColumnLineageDatasetFacet columnLineageDatasetFacet = null;

    try (MockedStatic mockedPlanUtils3 = mockStatic(PlanUtils3.class)) {
      try (MockedStatic mockedFacetUtils = mockStatic(DatasetVersionDatasetFacetUtils.class)) {
        try (MockedStatic mockedColumnLineage = mockStatic(ColumnLevelLineageUtils.class)) {
          when(DatasetVersionDatasetFacetUtils.extractVersionFromDataSourceV2Relation(relation))
              .thenReturn(Optional.of("v2"));
          when(PlanUtils3.fromDataSourceV2Relation(eq(factory), eq(context), eq(relation), any()))
              .thenReturn(Collections.singletonList(dataset));
          when(ColumnLevelLineageUtils.buildColumnLineageDatasetFacet(context, schema))
              .thenReturn(Optional.ofNullable(columnLineageDatasetFacet));

          List<OpenLineage.OutputDataset> datasets = builder.apply(appendData);

          assertEquals(1, datasets.size());
          assertEquals(dataset, datasets.get(0));
        }
      }
    }
  }

  @Test
  public void testApplyWithoutDataSourceV2Relation() {
    AppendData appendData = mock(AppendData.class);
    when(appendData.table())
        .thenReturn(
            (NamedRelation)
                mock(LogicalPlan.class, withSettings().extraInterfaces(NamedRelation.class)));

    assertEquals(0, builder.apply(appendData).size());
  }
}
