/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.InsertIntoDataSourceCommand;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.PartialFunction;

class InsertIntoDataSourceDatasetBuilderTest {

  private static final String NAMESPACE = "namespace";
  private static final String NAME = "table";

  private final OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
  private final OpenLineageContext context =
      OpenLineageContext.builder()
          .sparkSession(mock(SparkSession.class))
          .sparkContext(mock(SparkContext.class))
          .openLineage(openLineage)
          .openLineageConfig(new SparkOpenLineageConfig())
          .meterRegistry(new SimpleMeterRegistry())
          .build();
  private final LogicalRelation relation = mock(LogicalRelation.class);
  private final InsertIntoDataSourceCommand command = mock(InsertIntoDataSourceCommand.class);
  private final SparkListenerEvent event = mock(SparkListenerEvent.class);
  private final OpenLineage.OutputDataset relationDataset =
      openLineage
          .newOutputDatasetBuilder()
          .namespace(NAMESPACE)
          .name(NAME)
          .facets(
              openLineage
                  .newDatasetFacetsBuilder()
                  .schema(openLineage.newSchemaDatasetFacet(Collections.emptyList()))
                  .build())
          .outputFacets(
              openLineage
                  .newOutputDatasetOutputFacetsBuilder()
                  .outputStatistics(openLineage.newOutputStatisticsOutputDatasetFacet(1L, 2L, 3L))
                  .build())
          .build();

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    when(command.logicalRelation()).thenReturn(relation);
    AbstractQueryPlanOutputDatasetBuilder<LogicalRelation> relationBuilder =
        new AbstractQueryPlanOutputDatasetBuilder<LogicalRelation>(context, false) {
          @Override
          public boolean isDefinedAtLogicalPlan(LogicalPlan plan) {
            return relation.equals(plan);
          }

          @Override
          protected List<OpenLineage.OutputDataset> apply(
              SparkListenerEvent event, LogicalRelation plan) {
            return Collections.singletonList(relationDataset);
          }
        };
    context
        .getOutputDatasetBuilders()
        .add(
            (PartialFunction<Object, Collection<OpenLineage.OutputDataset>>)
                (PartialFunction<?, ?>) relationBuilder);
  }

  @Test
  void testApplyReturnsRelationDataset() {
    when(command.overwrite()).thenReturn(false);

    List<OpenLineage.OutputDataset> datasets =
        new InsertIntoDataSourceDatasetBuilder(context).apply(event, command);

    assertThat(datasets).containsExactly(relationDataset);
  }

  @Test
  void testApplyMarksOverwrittenRelationDataset() {
    when(command.overwrite()).thenReturn(true);

    List<OpenLineage.OutputDataset> datasets =
        new InsertIntoDataSourceDatasetBuilder(context).apply(event, command);

    assertThat(datasets).hasSize(1);
    OpenLineage.OutputDataset dataset = datasets.get(0);
    assertThat(dataset.getNamespace()).isEqualTo(NAMESPACE);
    assertThat(dataset.getName()).isEqualTo(NAME);
    assertThat(dataset.getFacets().getSchema()).isNotNull();
    assertThat(dataset.getFacets().getLifecycleStateChange().getLifecycleStateChange())
        .isEqualTo(LifecycleStateChange.OVERWRITE);
    assertThat(dataset.getOutputFacets().getOutputStatistics().getRowCount()).isEqualTo(1L);
  }
}
