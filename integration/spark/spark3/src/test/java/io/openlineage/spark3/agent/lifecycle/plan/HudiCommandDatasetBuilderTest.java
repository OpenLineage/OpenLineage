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
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.DeleteFromTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.MergeIntoTable;
import org.apache.spark.sql.catalyst.plans.logical.OneRowRelation;
import org.apache.spark.sql.catalyst.plans.logical.UpdateTable;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.hudi.command.CreateHoodieTableAsSelectCommand;
import org.apache.spark.sql.hudi.command.DeleteHoodieTableCommand;
import org.apache.spark.sql.hudi.command.InsertIntoHoodieTableCommand;
import org.apache.spark.sql.hudi.command.MergeIntoHoodieTableCommand;
import org.apache.spark.sql.hudi.command.UpdateHoodieTableCommand;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Option;
import scala.PartialFunction;
import scala.runtime.AbstractPartialFunction;

class HudiCommandDatasetBuilderTest {

  private static final OpenLineage OPEN_LINEAGE =
      new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
  private static final String NAMESPACE = "file";
  private static final String TARGET_NAME = "/tmp/hudi/target";
  private static final String SOURCE_NAME = "/tmp/hudi/source";

  OpenLineageContext context = mock(OpenLineageContext.class);
  HudiCommandOutputDatasetBuilder outputBuilder = new HudiCommandOutputDatasetBuilder(context);
  HudiCommandInputDatasetBuilder inputBuilder = new HudiCommandInputDatasetBuilder(context);
  SparkListenerEvent event = new SparkListenerSQLExecutionEnd(1L, 1L);
  LogicalRelation target = mock(LogicalRelation.class);

  @BeforeEach
  void setup() {
    when(target.collectLeaves()).thenReturn(ScalaConversionUtils.fromList(planList(target)));
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
    when(context.getInputDatasetQueryPlanVisitors())
        .thenReturn(
            Collections.singletonList(
                visitor(
                    LogicalPlan.class,
                    p ->
                        OPEN_LINEAGE
                            .newInputDatasetBuilder()
                            .namespace(NAMESPACE)
                            .name(p instanceof OneRowRelation ? SOURCE_NAME : TARGET_NAME)
                            .build())));
    when(context.getOutputDatasetBuilders()).thenReturn(Collections.emptyList());
    when(context.getInputDatasetBuilders()).thenReturn(Collections.emptyList());
  }

  @Test
  void testIsDefinedForHudiMergeUpdateDeleteAndCtas() {
    for (Class<? extends LogicalPlan> command :
        Arrays.asList(
            MergeIntoHoodieTableCommand.class,
            UpdateHoodieTableCommand.class,
            DeleteHoodieTableCommand.class,
            CreateHoodieTableAsSelectCommand.class)) {
      assertThat(outputBuilder.isDefinedAtLogicalPlan(mock(command)))
          .as(command.getName())
          .isTrue();
      assertThat(inputBuilder.isDefinedAtLogicalPlan(mock(command))).as(command.getName()).isTrue();
    }
    // INSERT has builders of its own
    assertThat(outputBuilder.isDefinedAtLogicalPlan(mock(InsertIntoHoodieTableCommand.class)))
        .isFalse();
    assertThat(inputBuilder.isDefinedAtLogicalPlan(mock(LogicalPlan.class))).isFalse();
  }

  @Test
  void testMergeReadsSourceAndTargetAndWritesTarget() {
    MergeIntoTable merge = mock(MergeIntoTable.class);
    when(merge.targetTable()).thenReturn(target);
    when(merge.sourceTable()).thenReturn(new OneRowRelation());
    MergeIntoHoodieTableCommand command = mock(MergeIntoHoodieTableCommand.class);
    when(command.mergeInto()).thenReturn(merge);

    assertThat(names(outputBuilder.apply(event, command))).containsExactly(TARGET_NAME);
    assertThat(inputNames(inputBuilder.apply(event, command)))
        .containsExactly(TARGET_NAME, SOURCE_NAME);
  }

  @Test
  void testUpdateReadsAndWritesTarget() {
    UpdateTable update = mock(UpdateTable.class);
    when(update.table()).thenReturn(target);
    UpdateHoodieTableCommand command = mock(UpdateHoodieTableCommand.class);
    when(command.ut()).thenReturn(update);

    assertThat(names(outputBuilder.apply(event, command))).containsExactly(TARGET_NAME);
    assertThat(inputNames(inputBuilder.apply(event, command))).containsExactly(TARGET_NAME);
  }

  @Test
  void testDeleteReadsAndWritesTarget() {
    DeleteFromTable delete = mock(DeleteFromTable.class);
    when(delete.table()).thenReturn(target);
    DeleteHoodieTableCommand command = mock(DeleteHoodieTableCommand.class);
    when(command.dft()).thenReturn(delete);

    assertThat(names(outputBuilder.apply(event, command))).containsExactly(TARGET_NAME);
    assertThat(inputNames(inputBuilder.apply(event, command))).containsExactly(TARGET_NAME);
  }

  @Test
  void testCtasReadsQueryAndIsNamedAfterCreatedTable() {
    CatalogTable table = mock(CatalogTable.class);
    when(table.identifier()).thenReturn(new TableIdentifier("orders", Option.apply("db")));
    CreateHoodieTableAsSelectCommand command = mock(CreateHoodieTableAsSelectCommand.class);
    when(command.table()).thenReturn(table);
    when(command.query()).thenReturn(new OneRowRelation());

    assertThat(inputNames(inputBuilder.apply(event, command))).containsExactly(SOURCE_NAME);
    assertThat(outputBuilder.jobNameSuffix(command)).hasValue("db_orders");
  }

  private static List<LogicalPlan> planList(LogicalPlan plan) {
    return Collections.singletonList(plan);
  }

  private static List<String> names(List<OutputDataset> datasets) {
    return datasets.stream().map(OutputDataset::getName).collect(Collectors.toList());
  }

  private static List<String> inputNames(List<InputDataset> datasets) {
    return datasets.stream().map(InputDataset::getName).collect(Collectors.toList());
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
