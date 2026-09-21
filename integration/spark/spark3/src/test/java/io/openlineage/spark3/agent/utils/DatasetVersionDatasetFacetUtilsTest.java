/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.DatasetBuilderFactory;
import io.openlineage.spark.agent.lifecycle.plan.catalog.CatalogHandler;
import io.openlineage.spark.agent.lifecycle.plan.catalog.CatalogUtils;
import io.openlineage.spark.agent.lifecycle.plan.catalog.RelationHandler;
import io.openlineage.spark.api.OpenLineageContext;
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
    // The owning-catalog fallback asks the factory for relation handlers, so it has to be a real
    // factory rather than an unstubbed mock returning null - otherwise the fallback returns empty
    // via its exception handler instead of via "no handler matched".
    when(openLineageContext.getDatasetBuilderFactory()).thenReturn(DatasetBuilderFactory.EMPTY);
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

    try (MockedStatic<CatalogUtils> mocked = mockStatic(CatalogUtils.class)) {
      when(CatalogUtils.getDatasetVersion(
              openLineageContext, tableCatalog, identifier, tableProperties))
          .thenReturn(Optional.of("some-version"));
      assertEquals(
          Optional.of("some-version"),
          DatasetVersionDatasetFacetUtils.extractVersionFromDataSourceV2Relation(
              openLineageContext, v2Relation));
    }
  }

  /**
   * A supported catalog reporting no version has none to report - an Iceberg table with no snapshot
   * yet, which is the normal state on a START event. The owning catalog recovered from the table
   * name is that same catalog, so falling back would only repeat the table load.
   */
  @Test
  void testExtractVersionDoesNotFallBackWhenCatalogIsSupported() {
    when(v2Relation.identifier()).thenReturn(Option.apply(identifier));
    when(v2Relation.catalog()).thenReturn(Option.apply(tableCatalog));
    when(v2Relation.table()).thenReturn(table);
    when(table.properties()).thenReturn(tableProperties);

    try (MockedStatic<CatalogUtils> mocked = mockStatic(CatalogUtils.class)) {
      when(CatalogUtils.getDatasetVersion(
              openLineageContext, tableCatalog, identifier, tableProperties))
          .thenReturn(Optional.empty());
      when(CatalogUtils.getCatalogHandler(openLineageContext, tableCatalog))
          .thenReturn(Optional.of(mock(CatalogHandler.class)));

      assertEquals(
          Optional.empty(),
          DatasetVersionDatasetFacetUtils.extractVersionFromDataSourceV2Relation(
              openLineageContext, v2Relation));

      mocked.verify(
          () -> CatalogUtils.getOwningCatalogFromRelation(openLineageContext, v2Relation), never());
    }
  }

  /**
   * The cached-catalog case: no handler supports the relation's own catalog, so the version comes
   * from the catalog that owns the table, resolved through a relation handler.
   */
  @Test
  void testExtractVersionFallsBackToOwningCatalogWhenCatalogUnsupported() {
    when(v2Relation.identifier()).thenReturn(Option.apply(identifier));
    when(v2Relation.catalog()).thenReturn(Option.apply(tableCatalog));
    when(v2Relation.table()).thenReturn(table);
    when(table.properties()).thenReturn(tableProperties);

    TableCatalog owningCatalog = mock(TableCatalog.class);
    Identifier owningIdentifier = mock(Identifier.class);

    try (MockedStatic<CatalogUtils> mocked = mockStatic(CatalogUtils.class)) {
      when(CatalogUtils.getDatasetVersion(
              openLineageContext, tableCatalog, identifier, tableProperties))
          .thenReturn(Optional.empty());
      when(CatalogUtils.getCatalogHandler(openLineageContext, tableCatalog))
          .thenReturn(Optional.empty());
      when(CatalogUtils.getOwningCatalogFromRelation(openLineageContext, v2Relation))
          .thenReturn(
              Optional.of(RelationHandler.OwningCatalog.of(owningCatalog, owningIdentifier)));
      when(CatalogUtils.getDatasetVersion(
              openLineageContext, owningCatalog, owningIdentifier, tableProperties))
          .thenReturn(Optional.of("owning-version"));

      assertEquals(
          Optional.of("owning-version"),
          DatasetVersionDatasetFacetUtils.extractVersionFromDataSourceV2Relation(
              openLineageContext, v2Relation));
    }
  }

  /** A linkage error from the relation handler must not escape as a version lookup failure. */
  @Test
  void testExtractVersionSwallowsFailureOfOwningCatalogLookup() {
    when(v2Relation.identifier()).thenReturn(Option.apply(identifier));
    when(v2Relation.catalog()).thenReturn(Option.apply(tableCatalog));
    when(v2Relation.table()).thenReturn(table);
    when(table.properties()).thenReturn(tableProperties);

    try (MockedStatic<CatalogUtils> mocked = mockStatic(CatalogUtils.class)) {
      when(CatalogUtils.getDatasetVersion(
              openLineageContext, tableCatalog, identifier, tableProperties))
          .thenReturn(Optional.empty());
      when(CatalogUtils.getCatalogHandler(openLineageContext, tableCatalog))
          .thenReturn(Optional.empty());
      when(CatalogUtils.getOwningCatalogFromRelation(openLineageContext, v2Relation))
          .thenThrow(new NoSuchMethodError("SparkTable.table()"));

      assertEquals(
          Optional.empty(),
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
}
