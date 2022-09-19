/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.CatalogUtils3;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.files.TahoeLogFileIndex;
import org.apache.spark.sql.execution.datasources.FileIndex;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.sources.BaseRelation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import scala.Option;

class DatasetVersionDatasetFacetUtilsTest {

  DataSourceV2Relation v2Relation = mock(DataSourceV2Relation.class);
  Identifier identifier = mock(Identifier.class);
  TableCatalog tableCatalog = mock(TableCatalog.class);
  Table table = mock(Table.class);
  Map<String, String> tableProperties = new HashMap<>();

  LogicalRelation logicalRelation = mock(LogicalRelation.class);
  CatalogTable catalogTable = mock(CatalogTable.class);
  HadoopFsRelation fsRelation = mock(HadoopFsRelation.class);
  TahoeLogFileIndex tahoeLogFileIndex = mock(TahoeLogFileIndex.class);
  Snapshot snapshot = mock(Snapshot.class);
  OpenLineage openLineage = mock(OpenLineage.class);
  OpenLineageContext openLineageContext = mock(OpenLineageContext.class);

  @BeforeEach
  void setUp() {
    when(logicalRelation.relation()).thenReturn(fsRelation);
    when(logicalRelation.catalogTable()).thenReturn(Option.apply(catalogTable));
    when(catalogTable.provider()).thenReturn(Option.apply("delta"));
    when(fsRelation.location()).thenReturn(tahoeLogFileIndex);
    when(tahoeLogFileIndex.getSnapshot()).thenReturn(snapshot);
    when(openLineageContext.getOpenLineage()).thenReturn(openLineage);
  }

  @Test
  void testExtractVersionFromDataSourceV2RelationWhenNoIdentifier() {
    when(v2Relation.identifier()).thenReturn(Option.empty());
    assertEquals(
        Optional.empty(),
        DatasetVersionDatasetFacetUtils.extractVersionFromDataSourceV2Relation(
            openLineageContext, v2Relation));
  }

  @Test
  void testExtractVersionFromDataSourceV2RelationWhenNoCatalog() {
    when(v2Relation.identifier()).thenReturn(Option.apply(identifier));
    when(v2Relation.catalog()).thenReturn(Option.empty());
    assertEquals(
        Optional.empty(),
        DatasetVersionDatasetFacetUtils.extractVersionFromDataSourceV2Relation(
            openLineageContext, v2Relation));
  }

  @Test
  void testExtractVersionFromDataSourceV2RelationWhenCatalogIsNotTableCatalog() {
    when(v2Relation.identifier()).thenReturn(Option.apply(identifier));
    when(v2Relation.catalog()).thenReturn(Option.apply(mock(CatalogPlugin.class)));
    assertEquals(
        Optional.empty(),
        DatasetVersionDatasetFacetUtils.extractVersionFromDataSourceV2Relation(
            openLineageContext, v2Relation));
  }

  @Test
  void testExtractVersionFromDataSourceV2Relation() {
    when(v2Relation.identifier()).thenReturn(Option.apply(identifier));
    when(v2Relation.catalog()).thenReturn(Option.apply(tableCatalog));
    when(v2Relation.table()).thenReturn(table);
    when(table.properties()).thenReturn(tableProperties);

    try (MockedStatic<CatalogUtils3> mocked = mockStatic(CatalogUtils3.class)) {
      when(CatalogUtils3.getDatasetVersion(
              openLineageContext, tableCatalog, identifier, tableProperties))
          .thenReturn(Optional.of("some-version"));
      assertEquals(
          Optional.of("some-version"),
          DatasetVersionDatasetFacetUtils.extractVersionFromDataSourceV2Relation(
              openLineageContext, v2Relation));
    }
  }

  @Test
  void testExtractVersionFromLogicalRelationWhenNotHadoopFsRelation() {
    when(logicalRelation.relation()).thenReturn(mock(BaseRelation.class));
    assertEquals(
        Optional.empty(),
        DatasetVersionDatasetFacetUtils.extractVersionFromLogicalRelation(logicalRelation));
  }

  @Test
  void testExtractVersionFromLogicalRelationWhenCatalogTableNotDefined() {
    when(logicalRelation.catalogTable()).thenReturn(Option.empty());
    assertEquals(
        Optional.empty(),
        DatasetVersionDatasetFacetUtils.extractVersionFromLogicalRelation(logicalRelation));
  }

  @Test
  void testExtractVersionFromLogicalRelationWhenProviderNotDefined() {
    when(catalogTable.provider()).thenReturn(Option.empty());
    assertEquals(
        Optional.empty(),
        DatasetVersionDatasetFacetUtils.extractVersionFromLogicalRelation(logicalRelation));
  }

  @Test
  void testExtractVersionFromLogicalRelationWhenProviderNotDelta() {
    when(catalogTable.provider()).thenReturn(Option.apply("non-delta"));
    assertEquals(
        Optional.empty(),
        DatasetVersionDatasetFacetUtils.extractVersionFromLogicalRelation(logicalRelation));
  }

  @Test
  void testExtractVersionFromLogicalRelationWhenNoDeltaClasses() {
    try (MockedStatic mocked =
        mockStatic(DatasetVersionDatasetFacetUtils.class, Mockito.CALLS_REAL_METHODS)) {
      when(DatasetVersionDatasetFacetUtils.hasDeltaClasses()).thenReturn(false);
      assertEquals(
          Optional.empty(),
          DatasetVersionDatasetFacetUtils.extractVersionFromLogicalRelation(logicalRelation));
    }
  }

  @Test
  void testExtractVersionFromLogicalRelationWhenLocationNotTahoeLogFileIndex() {
    when(fsRelation.location()).thenReturn(mock(FileIndex.class));
    assertEquals(
        Optional.empty(),
        DatasetVersionDatasetFacetUtils.extractVersionFromLogicalRelation(logicalRelation));
  }

  @Test
  void testExtractVersionFromLogicalRelation() {
    when(snapshot.version()).thenReturn(1L);
    assertEquals(
        Optional.of("1"),
        DatasetVersionDatasetFacetUtils.extractVersionFromLogicalRelation(logicalRelation));
  }

  @Test
  void testIncludeDatasetVersion() {
    OpenLineage.DatasetFacetsBuilder datasetFacetsBuilder = new OpenLineage.DatasetFacetsBuilder();
    DataSourceV2Relation relation = mock(DataSourceV2Relation.class);

    OpenLineage.DatasetVersionDatasetFacet datasetVersionDatasetFacet =
        mock(OpenLineage.DatasetVersionDatasetFacet.class);
    when(datasetVersionDatasetFacet.getDatasetVersion()).thenReturn("v2");
    when(openLineage.newDatasetVersionDatasetFacet("v2")).thenReturn(datasetVersionDatasetFacet);
    when(relation.identifier()).thenReturn(Option.apply(identifier));
    when(relation.catalog()).thenReturn(Option.apply(tableCatalog));
    when(relation.table()).thenReturn(table);
    when(table.properties()).thenReturn(tableProperties);

    try (MockedStatic mocked = mockStatic(CatalogUtils3.class)) {
      when(CatalogUtils3.getDatasetVersion(
              openLineageContext, tableCatalog, identifier, tableProperties))
          .thenReturn(Optional.of("v2"));
      DatasetVersionDatasetFacetUtils.includeDatasetVersion(
          openLineageContext, datasetFacetsBuilder, relation);
      assertEquals("v2", datasetFacetsBuilder.build().getVersion().getDatasetVersion());
    }
  }

  @Test
  void testIncludeDatasetVersionWhenNoDatasetVersion() {
    OpenLineage.DatasetFacetsBuilder datasetFacetsBuilder = new OpenLineage.DatasetFacetsBuilder();
    DataSourceV2Relation relation = mock(DataSourceV2Relation.class);

    when(relation.identifier()).thenReturn(Option.apply(identifier));
    when(relation.catalog()).thenReturn(Option.apply(tableCatalog));
    when(relation.table()).thenReturn(table);
    when(table.properties()).thenReturn(tableProperties);

    try (MockedStatic mocked = mockStatic(CatalogUtils3.class)) {
      when(CatalogUtils3.getDatasetVersion(
              openLineageContext, tableCatalog, identifier, tableProperties))
          .thenReturn(Optional.empty());
      DatasetVersionDatasetFacetUtils.includeDatasetVersion(
          openLineageContext, datasetFacetsBuilder, relation);
      assertEquals(null, datasetFacetsBuilder.build().getVersion());
    }
  }
}
