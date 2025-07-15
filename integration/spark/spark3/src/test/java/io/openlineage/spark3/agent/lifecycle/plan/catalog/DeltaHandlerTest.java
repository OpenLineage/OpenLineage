/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.net.URI;
import java.util.Collections;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.RuntimeConfig;
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
  OpenLineageContext context = mock(OpenLineageContext.class);
  DeltaCatalog deltaCatalog = mock(DeltaCatalog.class);
  DeltaHandler deltaHandler = new DeltaHandler(context);
  SparkSession sparkSession = mock(SparkSession.class);
  SparkContext sparkContext = mock(SparkContext.class);
  RuntimeConfig runtimeConfig = mock(RuntimeConfig.class);

  @BeforeEach
  void beforeEach() {
    SparkConf sparkConf = new SparkConf();
    sparkConf.set("spark.sql.warehouse.dir", "file:/tmp/warehouse");
    Configuration hadoopConf = new Configuration();
    when(sparkContext.getConf()).thenReturn(sparkConf);
    when(sparkContext.hadoopConfiguration()).thenReturn(hadoopConf);
    when(sparkSession.sparkContext()).thenReturn(sparkContext);
    when(sparkSession.conf()).thenReturn(runtimeConfig);
    when(runtimeConfig.getAll())
        .thenReturn(ScalaConversionUtils.fromJavaMap(Collections.emptyMap()));
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
  void testGetIdentifierForDeltaTableWithoutCatalogTable() {
    Identifier identifier = Identifier.of(new String[] {"database", "schema"}, "table");
    TableIdentifier tableIdentifier = mock(TableIdentifier.class);
    when(tableIdentifier.database()).thenReturn(Option.apply("schema"));
    when(tableIdentifier.table()).thenReturn("table");

    when(deltaCatalog.isPathIdentifier(identifier)).thenReturn(false);

    DeltaTableV2 deltaTable = Mockito.mock(DeltaTableV2.class);
    when(deltaTable.catalogTable()).thenReturn(Option.empty());
    when(deltaTable.path()).thenReturn(new Path("/some/location"));
    when(deltaCatalog.loadTable(identifier)).thenReturn(deltaTable);

    when(runtimeConfig.getAll())
        .thenReturn(
            ScalaConversionUtils.fromJavaMap(
                Collections.singletonMap("spark.sql.warehouse.dir", "/tmp/warehouse")));

    DatasetIdentifier datasetIdentifier =
        deltaHandler.getDatasetIdentifier(
            sparkSession, deltaCatalog, identifier, Collections.emptyMap());

    assertThat(datasetIdentifier)
        .hasFieldOrPropertyWithValue("namespace", "file")
        .hasFieldOrPropertyWithValue("name", "/some/location");

    assertThat(datasetIdentifier.getSymlinks()).hasSize(1);
    assertThat(datasetIdentifier.getSymlinks().get(0))
        .hasFieldOrPropertyWithValue("namespace", "/tmp/warehouse")
        .hasFieldOrPropertyWithValue("name", "database.schema.table")
        .hasFieldOrPropertyWithValue("type", DatasetIdentifier.SymlinkType.TABLE);
  }

  @Test
  @SneakyThrows
  void testGetIdentifierForDeltaTableWithDefaultLocation() {
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
  void testGetIdentifierForV1TableWithDefaultLocation() {
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
  void testGetIdentifierForDeltaTableWithCustomLocation() {
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
  void testGetIdentifierForV1TableWithCustomLocation() {
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

  @Test
  @SneakyThrows
  void testGetCatalogDatasetFacet() {
    when(context.getOpenLineage()).thenReturn(new OpenLineage(URI.create("http://localhost")));
    when(deltaCatalog.name()).thenReturn("name");

    Optional<CatalogHandler.CatalogWithAdditionalFacets> catalogDatasetFacet =
        deltaHandler.getCatalogDatasetFacet(deltaCatalog, Collections.emptyMap());
    assertThat(catalogDatasetFacet.isPresent()).isTrue();

    OpenLineage.CatalogDatasetFacet facet = catalogDatasetFacet.get().getCatalogDatasetFacet();
    assertThat(facet.getName()).isEqualTo("name");
    assertThat(facet.getFramework()).isEqualTo("delta");
    assertThat(facet.getType()).isEqualTo("delta");
  }
}
