/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.AppendData;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.OneRowRelation;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.hudi.command.InsertIntoHoodieTableCommand;
import org.junit.jupiter.api.Test;
import scala.PartialFunction;
import scala.runtime.AbstractPartialFunction;

class InsertIntoHoodieTableCommandDatasetBuilderTest {

  private static final OpenLineage OPEN_LINEAGE =
      new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
  private static final String NAMESPACE = "file";
  private static final String TARGET_NAME = "/tmp/hudi/target";

  OpenLineageContext context = mock(OpenLineageContext.class);
  InsertIntoHoodieTableCommandOutputDatasetBuilder outputBuilder =
      new InsertIntoHoodieTableCommandOutputDatasetBuilder(context);
  InsertIntoHoodieTableCommandInputDatasetBuilder inputBuilder =
      new InsertIntoHoodieTableCommandInputDatasetBuilder(context);
  SparkListenerEvent event = new SparkListenerSQLExecutionEnd(1L, 1L);

  @Test
  void testIsDefinedOnlyForInsertIntoHoodieTableCommand() {
    LogicalPlan command = mock(InsertIntoHoodieTableCommand.class);

    assertThat(outputBuilder.isDefinedAtLogicalPlan(command)).isTrue();
    assertThat(inputBuilder.isDefinedAtLogicalPlan(command)).isTrue();
    assertThat(outputBuilder.isDefinedAtLogicalPlan(mock(AppendData.class))).isFalse();
    assertThat(inputBuilder.isDefinedAtLogicalPlan(mock(LogicalPlan.class))).isFalse();
  }

  @Test
  void testOutputIsDelegatedToTargetRelation() {
    LogicalRelation target = mock(LogicalRelation.class);
    InsertIntoHoodieTableCommand command = mock(InsertIntoHoodieTableCommand.class);
    when(command.logicalRelation()).thenReturn(target);

    when(context.getOutputDatasetQueryPlanVisitors())
        .thenReturn(
            Collections.singletonList(
                visitor(
                    LogicalRelation.class,
                    p ->
                        OPEN_LINEAGE
                            .newOutputDatasetBuilder()
                            .namespace(NAMESPACE)
                            .name(TARGET_NAME)
                            .build())));
    when(context.getOutputDatasetBuilders()).thenReturn(Collections.emptyList());

    List<OutputDataset> outputs = outputBuilder.apply(event, command);

    assertThat(outputs)
        .singleElement()
        .hasFieldOrPropertyWithValue("namespace", NAMESPACE)
        .hasFieldOrPropertyWithValue("name", TARGET_NAME);
  }

  @Test
  void testInsertOverwriteAddsOverwriteLifecycleStateChange() {
    LogicalRelation target = mock(LogicalRelation.class);
    InsertIntoHoodieTableCommand command = mock(InsertIntoHoodieTableCommand.class);
    when(command.logicalRelation()).thenReturn(target);
    when(command.overwrite()).thenReturn(true);
    when(context.getOpenLineage()).thenReturn(OPEN_LINEAGE);

    when(context.getOutputDatasetQueryPlanVisitors())
        .thenReturn(
            Collections.singletonList(
                visitor(
                    LogicalRelation.class,
                    p ->
                        OPEN_LINEAGE
                            .newOutputDatasetBuilder()
                            .namespace(NAMESPACE)
                            .name(TARGET_NAME)
                            .facets(
                                OPEN_LINEAGE
                                    .newDatasetFacetsBuilder()
                                    .schema(OPEN_LINEAGE.newSchemaDatasetFacetBuilder().build())
                                    .build())
                            .build())));
    when(context.getOutputDatasetBuilders()).thenReturn(Collections.emptyList());

    List<OutputDataset> outputs = outputBuilder.apply(event, command);

    assertThat(outputs).hasSize(1);
    OutputDataset output = outputs.get(0);
    assertThat(output.getNamespace()).isEqualTo(NAMESPACE);
    assertThat(output.getName()).isEqualTo(TARGET_NAME);
    assertThat(output.getFacets().getSchema()).isNotNull();
    assertThat(output.getFacets().getLifecycleStateChange().getLifecycleStateChange())
        .isEqualTo(LifecycleStateChange.OVERWRITE);
  }

  @Test
  void testInputsAreCollectedFromQuery() {
    InsertIntoHoodieTableCommand command = mock(InsertIntoHoodieTableCommand.class);
    when(command.query()).thenReturn(new OneRowRelation());

    when(context.getInputDatasetQueryPlanVisitors())
        .thenReturn(
            Collections.singletonList(
                visitor(
                    OneRowRelation.class,
                    p ->
                        OPEN_LINEAGE
                            .newInputDatasetBuilder()
                            .namespace(NAMESPACE)
                            .name("/tmp/hudi/input")
                            .build())));
    when(context.getInputDatasetBuilders()).thenReturn(Collections.emptyList());

    List<InputDataset> inputs = inputBuilder.apply(event, command);

    assertThat(inputs)
        .singleElement()
        .hasFieldOrPropertyWithValue("namespace", NAMESPACE)
        .hasFieldOrPropertyWithValue("name", "/tmp/hudi/input");
  }

  private static <D> PartialFunction<LogicalPlan, List<D>> visitor(
      Class<? extends LogicalPlan> planClass, Function<LogicalPlan, D> datasetFn) {
    return new AbstractPartialFunction<LogicalPlan, List<D>>() {
      @Override
      public boolean isDefinedAt(LogicalPlan x) {
        return planClass.isInstance(x);
      }

      @Override
      public List<D> apply(LogicalPlan x) {
        return Collections.singletonList(datasetFn.apply(x));
      }
    };
  }
}
