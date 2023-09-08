/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.CatalogUtils3;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.UnsupportedCatalogException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import scala.Option;

class PlanUtils3Test {

  OpenLineageContext openLineageContext = mock(OpenLineageContext.class);
  SparkSession sparkSession = mock(SparkSession.class);
  DatasetFactory<OpenLineage.Dataset> datasetFactory = mock(DatasetFactory.class);
  DataSourceV2Relation dataSourceV2Relation = mock(DataSourceV2Relation.class);
  OpenLineage.DatasetFacetsBuilder datasetFacetsBuilder =
      mock(OpenLineage.DatasetFacetsBuilder.class);
  TableCatalog tableCatalog = mock(TableCatalog.class);
  Identifier identifier = mock(Identifier.class);
  StructType schema = mock(StructType.class);
  Table table = mock(Table.class);
  Map<String, String> tableProperties;
  OpenLineage openLineage = mock(OpenLineage.class);

  @BeforeEach
  void setUp() {
    tableProperties = new HashMap<>();
    when(openLineageContext.getSparkSession()).thenReturn(Optional.of(sparkSession));
    when(openLineageContext.getOpenLineage()).thenReturn(openLineage);
    when(openLineage.newDatasetFacetsBuilder()).thenReturn(datasetFacetsBuilder);
    when(dataSourceV2Relation.catalog()).thenReturn(Option.apply(tableCatalog));
    when(dataSourceV2Relation.identifier()).thenReturn(Option.apply(identifier));
    when(dataSourceV2Relation.schema()).thenReturn(schema);
    when(dataSourceV2Relation.table()).thenReturn(table);
    when(table.properties()).thenReturn(tableProperties);
  }

  @Test
  void testFromDataSourceV2Relation() {
    try (MockedStatic<CatalogUtils3> mocked = mockStatic(CatalogUtils3.class)) {
      try (MockedStatic<PlanUtils> mockedPlanUtils = mockStatic(PlanUtils.class)) {
        DatasetIdentifier di = mock(DatasetIdentifier.class);
        when(di.getNamespace()).thenReturn("file://tmp");
        when(di.getName()).thenReturn("name");

        OpenLineage.DatasetFacets datasetFacets = mock(OpenLineage.DatasetFacets.class);
        OpenLineage.Dataset dataset = mock(OpenLineage.Dataset.class);
        OpenLineage.SchemaDatasetFacet schemaDatasetFacet =
            mock(OpenLineage.SchemaDatasetFacet.class);
        OpenLineage.DatasourceDatasetFacet datasourceDatasetFacet =
            mock(OpenLineage.DatasourceDatasetFacet.class);
        when(PlanUtils.schemaFacet(openLineage, schema)).thenReturn(schemaDatasetFacet);
        when(PlanUtils.datasourceFacet(openLineage, di.getNamespace()))
            .thenReturn(datasourceDatasetFacet);
        when(datasetFacetsBuilder.schema(schemaDatasetFacet)).thenReturn(datasetFacetsBuilder);
        when(datasetFacetsBuilder.dataSource(datasourceDatasetFacet))
            .thenReturn(datasetFacetsBuilder);
        when(datasetFacetsBuilder.build()).thenReturn(datasetFacets);

        when(CatalogUtils3.getDatasetIdentifier(
                openLineageContext, tableCatalog, identifier, tableProperties))
            .thenReturn(di);
        when(datasetFactory.getDataset(di, datasetFacetsBuilder)).thenReturn(dataset);

        assertEquals(
            Collections.singletonList(dataset),
            PlanUtils3.fromDataSourceV2Relation(
                datasetFactory, openLineageContext, dataSourceV2Relation));
      }
    }
  }

  @Test
  void testFromDataSourceV2RelationWhenDatasetIdentifierEmpty() {
    try (MockedStatic<CatalogUtils3> mocked = mockStatic(CatalogUtils3.class)) {
      DatasetIdentifier di = mock(DatasetIdentifier.class);
      OpenLineage.Dataset dataset = mock(OpenLineage.Dataset.class);

      when(CatalogUtils3.getDatasetIdentifier(
              openLineageContext, tableCatalog, identifier, tableProperties))
          .thenThrow(new UnsupportedCatalogException("exception"));
      when(datasetFactory.getDataset(di, schema)).thenReturn(dataset);

      assertEquals(
          Collections.emptyList(),
          PlanUtils3.fromDataSourceV2Relation(
              datasetFactory, openLineageContext, dataSourceV2Relation));
    }
  }

  @Test
  void testFromDataSourceV2RelationWhenIdentifierEmpty() {
    when(dataSourceV2Relation.identifier()).thenReturn(Option.empty());
    final List<OpenLineage.Dataset> result =
        PlanUtils3.fromDataSourceV2Relation(
            datasetFactory, openLineageContext, dataSourceV2Relation);
    assertEquals(0, result.size());
  }

  @Test
  void testFromDataSourceV2RelationWhenCatalogEmpty() {
    when(dataSourceV2Relation.identifier()).thenReturn(Option.apply(mock(Identifier.class)));
    when(dataSourceV2Relation.catalog()).thenReturn(Option.empty());
    final List<OpenLineage.Dataset> result =
        PlanUtils3.fromDataSourceV2Relation(
            datasetFactory, openLineageContext, dataSourceV2Relation);
    assertEquals(0, result.size());
  }

  @Test
  void testGetDatasetIdentifier() {
    DatasetIdentifier di = mock(DatasetIdentifier.class);
    try (MockedStatic<CatalogUtils3> mocked = mockStatic(CatalogUtils3.class)) {
      when(CatalogUtils3.getDatasetIdentifier(
              openLineageContext, tableCatalog, identifier, tableProperties))
          .thenReturn(di);

      assertEquals(
          di,
          PlanUtils3.getDatasetIdentifier(
                  openLineageContext, tableCatalog, identifier, tableProperties)
              .get());
    }
  }

  @Test
  void testGetDatasetIdentifierWhenCatalogUnsupported() {
    try (MockedStatic<CatalogUtils3> mocked = mockStatic(CatalogUtils3.class)) {
      when(CatalogUtils3.getDatasetIdentifier(
              openLineageContext, tableCatalog, identifier, tableProperties))
          .thenThrow(new UnsupportedCatalogException("exception"));

      assertEquals(
          Optional.empty(),
          PlanUtils3.getDatasetIdentifier(
              openLineageContext, tableCatalog, identifier, tableProperties));
    }
  }

  @Test
  void testGetDatasetIdentifierWhenNoSparkSession() {
    when(openLineageContext.getSparkSession()).thenReturn(Optional.empty());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            PlanUtils3.getDatasetIdentifier(
                openLineageContext, tableCatalog, identifier, tableProperties));
  }

  @Test
  void testGetDatasetIdentifierFromV2Relation() {
    DatasetIdentifier di = mock(DatasetIdentifier.class);
    try (MockedStatic<CatalogUtils3> mocked = mockStatic(CatalogUtils3.class)) {
      when(CatalogUtils3.getDatasetIdentifier(
              openLineageContext, tableCatalog, identifier, tableProperties))
          .thenReturn(di);
      assertEquals(
          di, PlanUtils3.getDatasetIdentifier(openLineageContext, dataSourceV2Relation).get());
    }
  }

  @Test
  void testGetDatasetIdentifierFromV2RelationWithMissingIdentifier() {
    when(dataSourceV2Relation.identifier()).thenReturn(null).thenReturn(Option.empty());
    assertEquals(
        Optional.empty(),
        PlanUtils3.getDatasetIdentifier(openLineageContext, dataSourceV2Relation));
    assertEquals(
        Optional.empty(),
        PlanUtils3.getDatasetIdentifier(openLineageContext, dataSourceV2Relation));
  }

  @Test
  void testGetDatasetIdentifierFromV2RelationWithMissingCatalog() {
    when(dataSourceV2Relation.catalog())
        .thenReturn(null)
        .thenReturn(Option.empty())
        .thenReturn(Option.apply(mock(CatalogPlugin.class)));

    assertEquals(
        Optional.empty(),
        PlanUtils3.getDatasetIdentifier(openLineageContext, dataSourceV2Relation));
    assertEquals(
        Optional.empty(),
        PlanUtils3.getDatasetIdentifier(openLineageContext, dataSourceV2Relation));
    assertEquals(
        Optional.empty(),
        PlanUtils3.getDatasetIdentifier(openLineageContext, dataSourceV2Relation));
  }
}
