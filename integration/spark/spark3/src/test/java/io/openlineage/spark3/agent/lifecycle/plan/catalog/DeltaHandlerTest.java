/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.api.OpenLineageContext;
import java.net.URI;
import java.util.Collections;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.V1Table;
import org.apache.spark.sql.delta.catalog.DeltaCatalog;
import org.apache.spark.sql.delta.catalog.DeltaTableV2;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import scala.Option;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class DeltaHandlerTest {
  DeltaCatalog deltaCatalog = mock(DeltaCatalog.class);
  DeltaHandler deltaHandler = new DeltaHandler(mock(OpenLineageContext.class));
  SparkSession sparkSession = mock(SparkSession.class);
  SparkContext sparkContext = mock(SparkContext.class);

  @BeforeEach
  void beforeEach() {
    SparkConf sparkConf = new SparkConf();
    sparkConf.set("spark.sql.warehouse.dir", "file:/tmp/warehouse");
    Configuration hadoopConf = new Configuration();
    when(sparkContext.getConf()).thenReturn(sparkConf);
    when(sparkContext.hadoopConfiguration()).thenReturn(hadoopConf);
    when(sparkSession.sparkContext()).thenReturn(sparkContext);
  }

  @Test
  void testGetVersionString() {
    Identifier identifier = Identifier.of(new String[] {"database", "schema"}, "table");
    DeltaTableV2 deltaTable = Mockito.mock(DeltaTableV2.class, RETURNS_DEEP_STUBS);
    when(deltaCatalog.loadTable(identifier)).thenReturn(deltaTable);
    when(deltaTable.snapshot().version()).thenReturn(2L);

    Optional<String> version =
        deltaHandler.getDatasetVersion(deltaCatalog, identifier, Collections.emptyMap());

    assertThat(version.isPresent()).isTrue();
    assertThat(version.get()).isEqualTo("2");
  }

  @Test
  @SneakyThrows
  void testGetIdentifierForPath() {
    Identifier identifier = Identifier.of(new String[] {}, "/some/location");
    when(deltaCatalog.isPathIdentifier(identifier)).thenReturn(true);

    DatasetIdentifier datasetIdentifier =
        deltaHandler.getDatasetIdentifier(
            sparkSession, deltaCatalog, identifier, Collections.emptyMap());

    assertThat(datasetIdentifier)
        .hasFieldOrPropertyWithValue("namespace", "file")
        .hasFieldOrPropertyWithValue("name", "/some/location");

    assertThat(datasetIdentifier.getSymlinks()).hasSize(0);
  }

  @Test
  @SneakyThrows
  void testGetidentifierForDeltaTableWithDefaultLocation() {
    Identifier identifier = Identifier.of(new String[] {"database", "schema"}, "table");
    TableIdentifier tableIdentifier = mock(TableIdentifier.class);
    when(tableIdentifier.database()).thenReturn(Option.apply("schema"));
    when(tableIdentifier.table()).thenReturn("table");

    CatalogTable catalogTable = mock(CatalogTable.class);
    when(catalogTable.identifier()).thenReturn(tableIdentifier);

    CatalogStorageFormat catalogStorageFormat = mock(CatalogStorageFormat.class);
    when(catalogTable.storage()).thenReturn(catalogStorageFormat);
    when(catalogTable.provider()).thenReturn(Option.empty());
    when(catalogStorageFormat.locationUri())
        .thenReturn(Option.apply(new URI("/tmp/warehouse/schema.db/table")));

    DeltaTableV2 deltaTable = Mockito.mock(DeltaTableV2.class);
    when(deltaTable.catalogTable()).thenReturn(Option.apply(catalogTable));
    when(deltaCatalog.loadTable(identifier)).thenReturn(deltaTable);

    DatasetIdentifier datasetIdentifier =
        deltaHandler.getDatasetIdentifier(
            sparkSession, deltaCatalog, identifier, Collections.emptyMap());

    assertThat(datasetIdentifier)
        .hasFieldOrPropertyWithValue("namespace", "file")
        .hasFieldOrPropertyWithValue("name", "/tmp/warehouse/schema.db/table");

    assertThat(datasetIdentifier.getSymlinks())
        .singleElement()
        .hasFieldOrPropertyWithValue("namespace", "file:/tmp/warehouse")
        .hasFieldOrPropertyWithValue("name", "schema.table")
        .hasFieldOrPropertyWithValue("type", DatasetIdentifier.SymlinkType.TABLE);
  }

  @Test
  @SneakyThrows
  void testGetidentifierForV1TableWithDefaultLocation() {
    Identifier identifier = Identifier.of(new String[] {"database", "schema"}, "table");
    TableIdentifier tableIdentifier = mock(TableIdentifier.class);
    when(tableIdentifier.database()).thenReturn(Option.apply("schema"));
    when(tableIdentifier.table()).thenReturn("table");

    CatalogTable catalogTable = mock(CatalogTable.class);
    when(catalogTable.identifier()).thenReturn(tableIdentifier);

    CatalogStorageFormat catalogStorageFormat = mock(CatalogStorageFormat.class);
    when(catalogTable.storage()).thenReturn(catalogStorageFormat);
    when(catalogTable.provider()).thenReturn(Option.empty());
    when(catalogStorageFormat.locationUri())
        .thenReturn(Option.apply(new URI("/tmp/warehouse/schema.db/table")));

    V1Table v1Table = Mockito.mock(V1Table.class);
    when(v1Table.catalogTable()).thenReturn(catalogTable);
    when(deltaCatalog.loadTable(identifier)).thenReturn(v1Table);

    DatasetIdentifier datasetIdentifier =
        deltaHandler.getDatasetIdentifier(
            sparkSession, deltaCatalog, identifier, Collections.emptyMap());

    assertThat(datasetIdentifier)
        .hasFieldOrPropertyWithValue("namespace", "file")
        .hasFieldOrPropertyWithValue("name", "/tmp/warehouse/schema.db/table");

    assertThat(datasetIdentifier.getSymlinks())
        .singleElement()
        .hasFieldOrPropertyWithValue("namespace", "file:/tmp/warehouse")
        .hasFieldOrPropertyWithValue("name", "schema.table")
        .hasFieldOrPropertyWithValue("type", DatasetIdentifier.SymlinkType.TABLE);
  }

  @Test
  @SneakyThrows
  void testGetidentifierForDeltaTableWithCustomLocation() {
    Identifier identifier = Identifier.of(new String[] {"database", "schema"}, "table");
    TableIdentifier tableIdentifier = mock(TableIdentifier.class);
    when(tableIdentifier.database()).thenReturn(Option.apply("schema"));
    when(tableIdentifier.table()).thenReturn("table");

    CatalogTable catalogTable = mock(CatalogTable.class);
    when(catalogTable.identifier()).thenReturn(tableIdentifier);

    CatalogStorageFormat catalogStorageFormat = mock(CatalogStorageFormat.class);
    when(catalogTable.storage()).thenReturn(catalogStorageFormat);
    when(catalogTable.provider()).thenReturn(Option.empty());
    when(catalogStorageFormat.locationUri()).thenReturn(Option.apply(new URI("/some/location")));

    DeltaTableV2 deltaTable = Mockito.mock(DeltaTableV2.class);
    when(deltaTable.catalogTable()).thenReturn(Option.apply(catalogTable));
    when(deltaCatalog.loadTable(identifier)).thenReturn(deltaTable);

    DatasetIdentifier datasetIdentifier =
        deltaHandler.getDatasetIdentifier(
            sparkSession, deltaCatalog, identifier, Collections.emptyMap());

    assertThat(datasetIdentifier)
        .hasFieldOrPropertyWithValue("namespace", "file")
        .hasFieldOrPropertyWithValue("name", "/some/location");

    assertThat(datasetIdentifier.getSymlinks()).hasSize(0);
  }

  @Test
  @SneakyThrows
  void testGetidentifierForV1TableWithCustomLocation() {
    Identifier identifier = Identifier.of(new String[] {"database", "schema"}, "table");
    TableIdentifier tableIdentifier = mock(TableIdentifier.class);
    when(tableIdentifier.database()).thenReturn(Option.apply("schema"));
    when(tableIdentifier.table()).thenReturn("table");

    CatalogTable catalogTable = mock(CatalogTable.class);
    when(catalogTable.identifier()).thenReturn(tableIdentifier);

    CatalogStorageFormat catalogStorageFormat = mock(CatalogStorageFormat.class);
    when(catalogTable.storage()).thenReturn(catalogStorageFormat);
    when(catalogTable.provider()).thenReturn(Option.empty());
    when(catalogStorageFormat.locationUri()).thenReturn(Option.apply(new URI("/some/location")));

    V1Table v1Table = Mockito.mock(V1Table.class);
    when(v1Table.catalogTable()).thenReturn(catalogTable);
    when(deltaCatalog.loadTable(identifier)).thenReturn(v1Table);

    DatasetIdentifier datasetIdentifier =
        deltaHandler.getDatasetIdentifier(
            sparkSession, deltaCatalog, identifier, Collections.emptyMap());

    assertThat(datasetIdentifier)
        .hasFieldOrPropertyWithValue("namespace", "file")
        .hasFieldOrPropertyWithValue("name", "/some/location");

    assertThat(datasetIdentifier.getSymlinks()).hasSize(0);
  }
}
