/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark32.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark32.agent.lifecycle.plan.catalog.CatalogUtils3;
import io.openlineage.spark32.agent.utils.PlanUtils3;
import lombok.SneakyThrows;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import org.apache.spark.sql.catalyst.analysis.ResolvedTable;
import org.apache.spark.sql.catalyst.plans.logical.AlterTableCommand;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import scala.collection.immutable.HashMap;
import scala.collection.immutable.Map;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

class AlterTableDatasetBuilderTest {

  OpenLineageContext openLineageContext =
      OpenLineageContext.builder()
          .sparkSession(Optional.of(mock(SparkSession.class)))
          .sparkContext(mock(SparkContext.class))
          .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
          .build();

  TableCatalog tableCatalog = mock(TableCatalog.class);
  StructType schema = new StructType();
  Table table = mock(Table.class);
  Map<String, String> tableProperties = new HashMap<>();
  DatasetIdentifier di = new DatasetIdentifier("table", "db");
  Identifier identifier = mock(Identifier.class);
  AlterTableCommand alterTable = mock(AlterTableCommand.class);

  ResolvedTable resolvedTable = mock(ResolvedTable.class);

  AlterTableDatasetBuilder builder = new AlterTableDatasetBuilder(openLineageContext);

  @BeforeEach
  public void setUp() {
    when(alterTable.table()).thenReturn(resolvedTable);
    when(resolvedTable.catalog()).thenReturn(tableCatalog);
    when(resolvedTable.identifier()).thenReturn(identifier);
    when(resolvedTable.schema()).thenReturn(schema);
  }

  @Test
  @SneakyThrows
  void testApplyWhenTableNotFound() {
    when(tableCatalog.loadTable(identifier)).thenThrow(mock(NoSuchTableException.class));
    List<OpenLineage.OutputDataset> outputDatasets =
        builder.apply(new SparkListenerSQLExecutionEnd(1L, 1L), alterTable);
    assertEquals(0, outputDatasets.size());
  }

  @Test
  @SneakyThrows
  void testApplyWhenNoDatasetIdentifier() {
    when(tableCatalog.loadTable(identifier)).thenReturn(table);
    try (MockedStatic mocked = mockStatic(PlanUtils3.class)) {
      when(PlanUtils3.getDatasetIdentifier(
              openLineageContext,
              tableCatalog,
              identifier,
              ScalaConversionUtils.<String, String>fromMap(tableProperties)))
          .thenReturn(Optional.empty());

      List<OpenLineage.OutputDataset> outputDatasets =
          builder.apply(new SparkListenerSQLExecutionEnd(1L, 1L), alterTable);
      assertEquals(0, outputDatasets.size());
    }
  }

  @Test
  @SneakyThrows
  void testApply() {
    when(tableCatalog.loadTable(identifier)).thenReturn(table);
    try (MockedStatic mocked = mockStatic(PlanUtils3.class)) {
      when(PlanUtils3.getDatasetIdentifier(
              openLineageContext,
              tableCatalog,
              identifier,
              ScalaConversionUtils.<String, String>fromMap(tableProperties)))
          .thenReturn(Optional.of(di));

      List<OpenLineage.OutputDataset> outputDatasets =
          builder.apply(new SparkListenerSQLExecutionEnd(1L, 1L), alterTable);

      assertEquals(1, outputDatasets.size());
      assertEquals("table", outputDatasets.get(0).getName());
      assertEquals("db", outputDatasets.get(0).getNamespace());
    }
  }

  @Test
  @SneakyThrows
  void testApplyDatasetVersionIncluded() {
    when(tableCatalog.loadTable(identifier)).thenReturn(table);
    try (MockedStatic mocked = mockStatic(PlanUtils3.class)) {
      try (MockedStatic mockCatalog = mockStatic(CatalogUtils3.class)) {
        when(CatalogUtils3.getDatasetVersion(
                tableCatalog,
                identifier,
                ScalaConversionUtils.<String, String>fromMap(tableProperties)))
            .thenReturn(Optional.of("v2"));

        when(PlanUtils3.getDatasetIdentifier(
                openLineageContext,
                tableCatalog,
                identifier,
                ScalaConversionUtils.<String, String>fromMap(tableProperties)))
            .thenReturn(Optional.of(di));

        List<OpenLineage.OutputDataset> outputDatasets =
            builder.apply(new SparkListenerSQLExecutionEnd(1L, 1L), alterTable);

        assertEquals(1, outputDatasets.size());
        assertEquals("v2", outputDatasets.get(0).getFacets().getVersion().getDatasetVersion());
      }
    }
  }
}
