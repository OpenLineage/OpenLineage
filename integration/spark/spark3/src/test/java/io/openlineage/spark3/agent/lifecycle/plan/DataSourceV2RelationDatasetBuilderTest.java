package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.shared.api.AbstractQueryPlanDatasetBuilder;
import io.openlineage.spark.shared.api.DatasetFactory;
import io.openlineage.spark.shared.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.DatasetVersionDatasetFacetUtils;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

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

        assertEquals(datasets, builder.apply(relation));

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

  @ParameterizedTest
  @MethodSource("provideBuildersWithSparkListeners")
  public void testIsDefinedStSparkListenerEvent(
      AbstractQueryPlanDatasetBuilder builder, SparkListenerEvent event, boolean expectedResult) {
    assertEquals(expectedResult, builder.isDefinedAt(event));
  }

  private static Stream<Arguments> provideBuildersWithSparkListeners() {
    OpenLineageContext context = mock(OpenLineageContext.class);
    DatasetFactory factory = mock(DatasetFactory.class);

    return Stream.of(
        Arguments.of(
            new DataSourceV2RelationInputDatasetBuilder(context, factory),
            mock(SparkListenerJobStart.class),
            true),
        Arguments.of(
            new DataSourceV2RelationInputDatasetBuilder(context, factory),
            mock(SparkListenerSQLExecutionStart.class),
            true),
        Arguments.of(
            new DataSourceV2RelationInputDatasetBuilder(context, factory),
            mock(SparkListenerJobEnd.class),
            false),
        Arguments.of(
            new DataSourceV2RelationInputDatasetBuilder(context, factory),
            mock(SparkListenerSQLExecutionEnd.class),
            false),
        Arguments.of(
            new DataSourceV2RelationOutputDatasetBuilder(context, factory),
            mock(SparkListenerJobStart.class),
            false),
        Arguments.of(
            new DataSourceV2RelationOutputDatasetBuilder(context, factory),
            mock(SparkListenerSQLExecutionStart.class),
            false),
        Arguments.of(
            new DataSourceV2RelationOutputDatasetBuilder(context, factory),
            mock(SparkListenerJobEnd.class),
            true),
        Arguments.of(
            new DataSourceV2RelationOutputDatasetBuilder(context, factory),
            mock(SparkListenerSQLExecutionEnd.class),
            true));
  }
}
