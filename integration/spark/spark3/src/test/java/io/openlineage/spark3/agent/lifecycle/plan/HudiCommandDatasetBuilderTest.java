/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable;
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
import org.mockito.MockedStatic;
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
    noSubqueries(target);
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
  void testIsDefinedForHudiCommands() {
    for (Class<? extends LogicalPlan> command :
        Arrays.asList(
            InsertIntoHoodieTableCommand.class,
            MergeIntoHoodieTableCommand.class,
            UpdateHoodieTableCommand.class,
            DeleteHoodieTableCommand.class,
            CreateHoodieTableAsSelectCommand.class)) {
      assertThat(outputBuilder.isDefinedAtLogicalPlan(mock(command)))
          .as(command.getName())
          .isTrue();
      assertThat(inputBuilder.isDefinedAtLogicalPlan(mock(command))).as(command.getName()).isTrue();
    }
    assertThat(outputBuilder.isDefinedAtLogicalPlan(mock(LogicalPlan.class))).isFalse();
    assertThat(inputBuilder.isDefinedAtLogicalPlan(mock(LogicalPlan.class))).isFalse();
  }

  @Test
  void testMergeReadsSourceAndTargetAndWritesTarget() {
    MergeIntoTable merge = mock(MergeIntoTable.class);
    when(merge.targetTable()).thenReturn(target);
    when(merge.sourceTable()).thenReturn(new OneRowRelation());
    noSubqueries(merge);
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
    noSubqueries(update);
    UpdateHoodieTableCommand command = mock(UpdateHoodieTableCommand.class);
    when(command.ut()).thenReturn(update);

    assertThat(names(outputBuilder.apply(event, command))).containsExactly(TARGET_NAME);
    assertThat(inputNames(inputBuilder.apply(event, command))).containsExactly(TARGET_NAME);
  }

  @Test
  void testUpdateReadsTablesOfSubqueries() {
    // UPDATE target SET name = (SELECT max(name) FROM source)
    UpdateTable update = mock(UpdateTable.class);
    when(update.table()).thenReturn(target);
    when(update.subqueries())
        .thenReturn(ScalaConversionUtils.fromList(planList(new OneRowRelation())));
    when(update.children()).thenReturn(ScalaConversionUtils.fromList(planList(target)));
    UpdateHoodieTableCommand command = mock(UpdateHoodieTableCommand.class);
    when(command.ut()).thenReturn(update);

    assertThat(inputNames(inputBuilder.apply(event, command)))
        .containsExactly(TARGET_NAME, SOURCE_NAME);
  }

  @Test
  void testDeleteReadsTablesOfSubqueries() {
    // DELETE FROM target WHERE id IN (SELECT id FROM source)
    DeleteFromTable delete = mock(DeleteFromTable.class);
    when(delete.table()).thenReturn(target);
    when(delete.subqueries())
        .thenReturn(ScalaConversionUtils.fromList(planList(new OneRowRelation())));
    when(delete.children()).thenReturn(ScalaConversionUtils.fromList(planList(target)));
    DeleteHoodieTableCommand command = mock(DeleteHoodieTableCommand.class);
    when(command.dft()).thenReturn(delete);

    assertThat(inputNames(inputBuilder.apply(event, command)))
        .containsExactly(TARGET_NAME, SOURCE_NAME);
  }

  @Test
  void testDeleteReadsAndWritesTarget() {
    DeleteFromTable delete = mock(DeleteFromTable.class);
    when(delete.table()).thenReturn(target);
    noSubqueries(delete);
    DeleteHoodieTableCommand command = mock(DeleteHoodieTableCommand.class);
    when(command.dft()).thenReturn(delete);

    assertThat(names(outputBuilder.apply(event, command))).containsExactly(TARGET_NAME);
    assertThat(inputNames(inputBuilder.apply(event, command))).containsExactly(TARGET_NAME);
  }

  @Test
  void testPrunedHudi1DeleteIsNamedAfterCatalogTable() {
    // Hudi 1.x keeps the catalog table and the query; WHERE false prunes the query to no relation
    CatalogTable table = mock(CatalogTable.class);
    HoodieCatalogTable hoodieTable = mock(HoodieCatalogTable.class);
    when(hoodieTable.table()).thenReturn(table);
    DeleteHoodieTableCommand command =
        mock(DeleteHoodieTableCommand.class, withSettings().extraInterfaces(Hudi1Delete.class));
    when(((Hudi1Delete) command).catalogTable()).thenReturn(hoodieTable);
    when(((Hudi1Delete) command).query()).thenReturn(new OneRowRelation());
    SparkSession session = mock(SparkSession.class);
    when(context.getSparkSession()).thenReturn(Optional.of(session));
    when(context.getOpenLineageConfig()).thenReturn(new SparkOpenLineageConfig());

    try (MockedStatic<PathUtils> pathUtils = mockStatic(PathUtils.class)) {
      pathUtils
          .when(() -> PathUtils.fromCatalogTable(table, session))
          .thenReturn(new DatasetIdentifier(TARGET_NAME, NAMESPACE));

      assertThat(outputBuilder.jobNameSuffix(command)).hasValue("hudi_target");
    }
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

  @Test
  void testCtasLifecycleFollowsSaveMode() {
    CreateHoodieTableAsSelectCommand command = mock(CreateHoodieTableAsSelectCommand.class);

    when(command.mode()).thenReturn(SaveMode.ErrorIfExists);
    assertThat(HudiCommandOutputDatasetBuilder.ctasLifecycle(command))
        .hasValue(LifecycleStateChange.CREATE);
    when(command.mode()).thenReturn(SaveMode.Ignore);
    assertThat(HudiCommandOutputDatasetBuilder.ctasLifecycle(command))
        .hasValue(LifecycleStateChange.CREATE);
    when(command.mode()).thenReturn(SaveMode.Overwrite);
    assertThat(HudiCommandOutputDatasetBuilder.ctasLifecycle(command))
        .hasValue(LifecycleStateChange.OVERWRITE);
    when(command.mode()).thenReturn(SaveMode.Append);
    assertThat(HudiCommandOutputDatasetBuilder.ctasLifecycle(command)).isEmpty();
  }

  @Test
  void testInsertReadsQueryAndWritesTarget() {
    InsertIntoHoodieTableCommand command = mock(InsertIntoHoodieTableCommand.class);
    when(command.logicalRelation()).thenReturn(target);
    when(command.query()).thenReturn(new OneRowRelation());

    assertThat(names(outputBuilder.apply(event, command))).containsExactly(TARGET_NAME);
    assertThat(inputNames(inputBuilder.apply(event, command))).containsExactly(SOURCE_NAME);
  }

  @Test
  void testInsertOverwriteAddsOverwriteLifecycleStateChange() {
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
                                    .datasetType(
                                        OPEN_LINEAGE.newDatasetTypeDatasetFacet(
                                            "TABLE", "COPY_ON_WRITE"))
                                    .build())
                            .build())));

    List<OutputDataset> outputs = outputBuilder.apply(event, command);

    assertThat(outputs).hasSize(1);
    OutputDataset output = outputs.get(0);
    assertThat(output.getName()).isEqualTo(TARGET_NAME);
    assertThat(output.getFacets().getSchema()).isNotNull();
    assertThat(output.getFacets().getDatasetType().getSubType()).isEqualTo("COPY_ON_WRITE");
    assertThat(output.getFacets().getLifecycleStateChange().getLifecycleStateChange())
        .isEqualTo(LifecycleStateChange.OVERWRITE);
  }

  @Test
  void testInsertJobNameSuffixIsTargetTable() {
    CatalogTable catalogTable = mock(CatalogTable.class);
    when(target.catalogTable()).thenReturn(Option.apply(catalogTable));
    InsertIntoHoodieTableCommand command = mock(InsertIntoHoodieTableCommand.class);
    when(command.logicalRelation()).thenReturn(target);
    SparkSession session = mock(SparkSession.class);
    when(context.getSparkSession()).thenReturn(Optional.of(session));
    when(context.getOpenLineageConfig()).thenReturn(new SparkOpenLineageConfig());

    try (MockedStatic<PathUtils> pathUtils = mockStatic(PathUtils.class)) {
      pathUtils
          .when(() -> PathUtils.fromCatalogTable(catalogTable, session))
          .thenReturn(new DatasetIdentifier(TARGET_NAME, NAMESPACE));

      assertThat(outputBuilder.jobNameSuffix(command)).hasValue("hudi_target");
    }
  }

  @Test
  void testInsertJobNameSuffixIsEmptyWithoutCatalogTable() {
    when(target.catalogTable()).thenReturn(Option.empty());
    InsertIntoHoodieTableCommand command = mock(InsertIntoHoodieTableCommand.class);
    when(command.logicalRelation()).thenReturn(target);

    assertThat(outputBuilder.jobNameSuffix(command)).isEmpty();
  }

  private static void noSubqueries(LogicalPlan plan) {
    when(plan.subqueries()).thenReturn(ScalaConversionUtils.fromList(Collections.emptyList()));
    when(plan.children()).thenReturn(ScalaConversionUtils.fromList(Collections.emptyList()));
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

  /** The methods of Hudi 1.x's {@code DeleteHoodieTableCommand}, which keeps no {@code dft}. */
  public interface Hudi1Delete {
    HoodieCatalogTable catalogTable();

    LogicalPlan query();
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
