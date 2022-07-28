/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark33.agent.lifecycle.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.CatalogUtils3;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import java.util.List;
import java.util.Optional;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.ResolvedDBObjectName;
import org.apache.spark.sql.catalyst.plans.logical.CreateTable;
import org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelect;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceTable;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceTableAsSelect;
import org.apache.spark.sql.catalyst.plans.logical.TableSpec;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import scala.collection.Seq;
import scala.collection.Seq$;
import scala.collection.immutable.HashMap;
import scala.collection.immutable.Map;

public class CreateReplaceVisitorDatasetBuilderTest {

  private static final String TABLE = "table";
  OpenLineageContext openLineageContext =
      OpenLineageContext.builder()
          .sparkSession(Optional.of(mock(SparkSession.class)))
          .sparkContext(mock(SparkContext.class))
          .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
          .build();

  CreateReplaceDatasetBuilder builder = new CreateReplaceDatasetBuilder(openLineageContext);

  TableCatalog catalog = mock(TableCatalog.class);
  StructType schema = new StructType();
  Map<String, String> commandProperties = new HashMap<>();
  Identifier tableName = Identifier.of(new String[] {"db"}, TABLE);

  ResolvedDBObjectName namePlan =
      new ResolvedDBObjectName(catalog, (Seq<String>) Seq$.MODULE$.empty());
  TableSpec tableSpec = mock(TableSpec.class);

  @BeforeEach
  public void setup() {
    when(tableSpec.properties()).thenReturn(commandProperties);
  }

  @Test
  void testIsDefined() {
    assertTrue(builder.isDefinedAtLogicalPlan(mock(CreateTableAsSelect.class)));
    assertTrue(builder.isDefinedAtLogicalPlan(mock(ReplaceTableAsSelect.class)));
    assertTrue(builder.isDefinedAtLogicalPlan(mock(ReplaceTable.class)));
    assertTrue(builder.isDefinedAtLogicalPlan(mock(CreateTable.class)));
    assertFalse(builder.isDefinedAtLogicalPlan(mock(LogicalPlan.class)));
  }

  @Test
  void testApplyForCreateTableAsSelect() {
    CreateTableAsSelect logicalPlan = mock(CreateTableAsSelect.class);
    when(logicalPlan.name()).thenReturn(namePlan);
    when(logicalPlan.tableName()).thenReturn(tableName);
    when(logicalPlan.tableSpec()).thenReturn(tableSpec);
    when(logicalPlan.tableSchema()).thenReturn(schema);
    verifyApply(
        (LogicalPlan) logicalPlan,
        commandProperties,
        OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.CREATE);
  }

  @Test
  void testApplyForReplaceTable() {
    ReplaceTable logicalPlan = mock(ReplaceTable.class);
    when(logicalPlan.name()).thenReturn(namePlan);
    when(logicalPlan.tableName()).thenReturn(tableName);
    when(logicalPlan.tableSpec()).thenReturn(tableSpec);
    when(logicalPlan.tableSchema()).thenReturn(schema);
    verifyApply(
        (LogicalPlan) logicalPlan,
        commandProperties,
        OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE);
  }

  @Test
  void testApplyForReplaceTableAsSelect() {
    ReplaceTableAsSelect logicalPlan = mock(ReplaceTableAsSelect.class);
    when(logicalPlan.name()).thenReturn(namePlan);
    when(logicalPlan.tableName()).thenReturn(tableName);
    when(logicalPlan.tableSpec()).thenReturn(tableSpec);
    when(logicalPlan.tableSchema()).thenReturn(schema);
    verifyApply(
        (LogicalPlan) logicalPlan,
        commandProperties,
        OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE);
  }

  @Test
  void testApplyForCreateTable() {
    CreateTable logicalPlan = mock(CreateTable.class);
    when(logicalPlan.name()).thenReturn(namePlan);
    when(logicalPlan.tableName()).thenReturn(tableName);
    when(logicalPlan.tableSpec()).thenReturn(tableSpec);
    when(logicalPlan.tableSchema()).thenReturn(schema);
    verifyApply(
        (LogicalPlan) logicalPlan,
        commandProperties,
        OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.CREATE);
  }

  @Test
  void testApplyDatasetVersionIncluded() {
    ReplaceTable logicalPlan = mock(ReplaceTable.class);
    when(logicalPlan.name()).thenReturn(namePlan);
    when(logicalPlan.tableName()).thenReturn(tableName);
    when(logicalPlan.tableSpec()).thenReturn(tableSpec);
    when(logicalPlan.tableSchema()).thenReturn(schema);

    DatasetIdentifier di = new DatasetIdentifier(TABLE, "db");
    try (MockedStatic mocked = mockStatic(PlanUtils3.class)) {
      try (MockedStatic mockedCatalog = mockStatic(CatalogUtils3.class)) {
        when(CatalogUtils3.getDatasetVersion(
                openLineageContext,
                catalog,
                Identifier.of(new String[] {"db"}, TABLE),
                ScalaConversionUtils.<String, String>fromMap(commandProperties)))
            .thenReturn(Optional.of("v2"));

        when(PlanUtils3.getDatasetIdentifier(
                openLineageContext,
                catalog,
                tableName,
                ScalaConversionUtils.<String, String>fromMap(commandProperties)))
            .thenReturn(Optional.of(di));

        List<OpenLineage.OutputDataset> outputDatasets =
            builder.apply(new SparkListenerSQLExecutionEnd(1L, 1L), logicalPlan);

        assertEquals(1, outputDatasets.size());
        assertEquals("v2", outputDatasets.get(0).getFacets().getVersion().getDatasetVersion());
      }
    }
  }

  @Test
  void testApplyDatasetVersionMissing() {
    ReplaceTable logicalPlan = mock(ReplaceTable.class);
    when(logicalPlan.name()).thenReturn(namePlan);
    when(logicalPlan.tableName()).thenReturn(tableName);
    when(logicalPlan.tableSpec()).thenReturn(tableSpec);
    when(logicalPlan.tableSchema()).thenReturn(schema);

    DatasetIdentifier di = new DatasetIdentifier(TABLE, "db");
    try (MockedStatic mocked = mockStatic(PlanUtils3.class)) {
      try (MockedStatic mockedCatalog = mockStatic(CatalogUtils3.class)) {
        when(CatalogUtils3.getDatasetVersion(
                openLineageContext,
                catalog,
                Identifier.of(new String[] {"db"}, TABLE),
                ScalaConversionUtils.<String, String>fromMap(commandProperties)))
            .thenReturn(Optional.empty());

        when(PlanUtils3.getDatasetIdentifier(
                openLineageContext,
                catalog,
                tableName,
                ScalaConversionUtils.<String, String>fromMap(commandProperties)))
            .thenReturn(Optional.of(di));

        List<OpenLineage.OutputDataset> outputDatasets =
            builder.apply(new SparkListenerSQLExecutionEnd(1L, 1L), logicalPlan);

        assertEquals(1, outputDatasets.size());
        assertEquals(null, outputDatasets.get(0).getFacets().getVersion());
      }
    }
  }

  private void verifyApply(
      LogicalPlan logicalPlan,
      Map<String, String> tableProperties,
      OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange lifecycleStateChange) {
    DatasetIdentifier di = new DatasetIdentifier(TABLE, "db");
    try (MockedStatic mocked = mockStatic(PlanUtils3.class)) {
      when(PlanUtils3.getDatasetIdentifier(
              openLineageContext,
              catalog,
              tableName,
              ScalaConversionUtils.<String, String>fromMap(tableProperties)))
          .thenReturn(Optional.of(di));

      List<OpenLineage.OutputDataset> outputDatasets =
          builder.apply(new SparkListenerSQLExecutionEnd(1L, 1L), logicalPlan);

      assertEquals(1, outputDatasets.size());
      assertEquals(
          lifecycleStateChange,
          outputDatasets.get(0).getFacets().getLifecycleStateChange().getLifecycleStateChange());
      assertEquals(TABLE, outputDatasets.get(0).getName());
      assertEquals("db", outputDatasets.get(0).getNamespace());
    }
  }

  @Test
  void testApplyWhenNoDatasetIdentifierReturned() {
    CreateTableAsSelect logicalPlan = mock(CreateTableAsSelect.class);
    try (MockedStatic mocked = mockStatic(PlanUtils3.class)) {
      when(logicalPlan.name()).thenReturn(namePlan);
      when(logicalPlan.tableName()).thenReturn(tableName);
      when(logicalPlan.tableSpec()).thenReturn(tableSpec);
      when(logicalPlan.tableSchema()).thenReturn(schema);

      when(PlanUtils3.getDatasetIdentifier(
              openLineageContext,
              catalog,
              tableName,
              ScalaConversionUtils.<String, String>fromMap(tableSpec.properties())))
          .thenReturn(Optional.empty());

      List<OpenLineage.OutputDataset> outputDatasets =
          builder.apply(new SparkListenerSQLExecutionEnd(1L, 1L), logicalPlan);
      assertEquals(0, outputDatasets.size());
    }
  }
}
