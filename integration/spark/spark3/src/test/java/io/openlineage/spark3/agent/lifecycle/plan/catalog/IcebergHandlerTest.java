/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg.IcebergHandler;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junitpioneer.jupiter.SetEnvironmentVariable;
import scala.collection.immutable.Map;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class IcebergHandlerTest {

  private OpenLineageContext context = mock(OpenLineageContext.class);
  private IcebergHandler icebergHandler = new IcebergHandler(context);
  private SparkSession sparkSession = mock(SparkSession.class);
  private SparkContext sparkContext = mock(SparkContext.class);
  private SparkConf sparkConf = new SparkConf();
  private Configuration hadoopConf = new Configuration();
  private RuntimeConfig runtimeConfig = mock(RuntimeConfig.class);

  @ParameterizedTest
  @CsvSource({
    "hdfs://namenode:8020/tmp/warehouse,hdfs://namenode:8020/tmp/warehouse,hdfs://namenode:8020,/tmp/warehouse/database/table",
    "/tmp/warehouse,file:/tmp/warehouse,file,/tmp/warehouse/database/table"
  })
  @SneakyThrows
  void testGetDatasetIdentifierForHadoop(
      String warehouseConf, String warehouseLocation, String namespace, String name) {
    when(sparkSession.conf()).thenReturn(runtimeConfig);
    when(runtimeConfig.getAll())
        .thenReturn(
            new Map.Map2<>(
                "spark.sql.catalog.test.type",
                "hadoop",
                "spark.sql.catalog.test.warehouse",
                warehouseConf));

    SparkCatalog sparkCatalog = mock(SparkCatalog.class);
    SparkTable sparkTable = mock(SparkTable.class, RETURNS_DEEP_STUBS);
    Identifier identifier = Identifier.of(new String[] {"database"}, "table");

    when(sparkCatalog.name()).thenReturn("test");
    when(sparkCatalog.loadTable(identifier)).thenReturn(sparkTable);
    when(sparkTable.table().location()).thenReturn(warehouseLocation + "/database/table");

    DatasetIdentifier datasetIdentifier =
        icebergHandler.getDatasetIdentifier(
            sparkSession, sparkCatalog, identifier, new HashMap<>());

    assertThat(datasetIdentifier)
        .hasFieldOrPropertyWithValue("namespace", namespace)
        .hasFieldOrPropertyWithValue("name", name);

    assertThat(datasetIdentifier.getSymlinks())
        .singleElement()
        .hasFieldOrPropertyWithValue("namespace", warehouseLocation)
        .hasFieldOrPropertyWithValue("name", "database.table")
        .hasFieldOrPropertyWithValue("type", DatasetIdentifier.SymlinkType.TABLE);
  }

  @Test
  @SneakyThrows
  void testGetDatasetIdentifierForHive() {
    when(sparkSession.conf()).thenReturn(runtimeConfig);
    when(runtimeConfig.getAll())
        .thenReturn(
            new Map.Map3<>(
                "spark.sql.catalog.test.type",
                "hive",
                "spark.sql.catalog.test.uri",
                "thrift://metastore-host:10001",
                "spark.sql.catalog.test.warehouse",
                "/tmp/warehouse"));

    SparkCatalog sparkCatalog = mock(SparkCatalog.class);
    SparkTable sparkTable = mock(SparkTable.class, RETURNS_DEEP_STUBS);
    Identifier identifier = Identifier.of(new String[] {"database"}, "table");

    when(sparkCatalog.name()).thenReturn("test");
    when(sparkCatalog.loadTable(identifier)).thenReturn(sparkTable);
    when(sparkTable.table().location()).thenReturn("file:/tmp/warehouse/database/table");

    DatasetIdentifier datasetIdentifier =
        icebergHandler.getDatasetIdentifier(
            sparkSession,
            sparkCatalog,
            Identifier.of(new String[] {"database"}, "table"),
            new HashMap<>());

    assertThat(datasetIdentifier)
        .hasFieldOrPropertyWithValue("namespace", "file")
        .hasFieldOrPropertyWithValue("name", "/tmp/warehouse/database/table");

    assertThat(datasetIdentifier.getSymlinks())
        .singleElement()
        .hasFieldOrPropertyWithValue("namespace", "hive://metastore-host:10001")
        .hasFieldOrPropertyWithValue("name", "database.table")
        .hasFieldOrPropertyWithValue("type", DatasetIdentifier.SymlinkType.TABLE);
  }

  @Test
  @SneakyThrows
  void testGetDatasetIdentifierForRest() {
    when(sparkSession.conf()).thenReturn(runtimeConfig);
    when(runtimeConfig.getAll())
        .thenReturn(
            new Map.Map3<>(
                "spark.sql.catalog.test.type",
                "rest",
                "spark.sql.catalog.test.uri",
                "http://lakehouse-host:8080",
                "spark.sql.catalog.test.warehouse",
                "s3a://lakehouse/warehouse"));

    SparkCatalog sparkCatalog = mock(SparkCatalog.class);
    SparkTable sparkTable = mock(SparkTable.class, RETURNS_DEEP_STUBS);
    Identifier identifier = Identifier.of(new String[] {"database"}, "table");

    when(sparkCatalog.name()).thenReturn("test");
    when(sparkCatalog.loadTable(identifier)).thenReturn(sparkTable);
    when(sparkTable.table().location()).thenReturn("s3a://lakehouse/warehouse/database/table");

    DatasetIdentifier datasetIdentifier =
        icebergHandler.getDatasetIdentifier(
            sparkSession,
            sparkCatalog,
            Identifier.of(new String[] {"database"}, "table"),
            new HashMap<>());

    assertThat(datasetIdentifier)
        .hasFieldOrPropertyWithValue("namespace", "s3://lakehouse")
        .hasFieldOrPropertyWithValue("name", "warehouse/database/table");

    assertThat(datasetIdentifier.getSymlinks())
        .singleElement()
        .hasFieldOrPropertyWithValue("namespace", "http://lakehouse-host:8080")
        .hasFieldOrPropertyWithValue("name", "database.table")
        .hasFieldOrPropertyWithValue("type", DatasetIdentifier.SymlinkType.TABLE);
  }

  @Test
  @SneakyThrows
  void testGetDatasetIdentifierForRestWithoutLocation() {
    when(sparkSession.conf()).thenReturn(runtimeConfig);
    when(runtimeConfig.getAll())
        .thenReturn(
            new Map.Map3<>(
                "spark.sql.catalog.rest",
                "org.apache.iceberg.spark.SparkCatalog",
                "spark.sql.catalog.rest.catalog-impl",
                "org.apache.iceberg.rest.RESTCatalog",
                "spark.sql.catalog.rest.uri",
                "http://lakehouse-host:8080"));

    SparkCatalog sparkCatalog = mock(SparkCatalog.class);
    Identifier identifier = Identifier.of(new String[] {"database"}, "table");

    when(sparkCatalog.name()).thenReturn("rest");
    when(sparkCatalog.loadTable(identifier)).thenThrow(new NoSuchTableException(identifier));

    assertThrows(
        MissingDatasetIdentifierCatalogException.class,
        () ->
            icebergHandler.getDatasetIdentifier(
                sparkSession,
                sparkCatalog,
                Identifier.of(new String[] {"database"}, "table"),
                new HashMap<>()));
  }

  @Test
  @SneakyThrows
  @SetEnvironmentVariable(key = "AWS_DEFAULT_REGION", value = "us-west-2")
  void testGetDatasetIdentifierForGlue() {
    when(sparkSession.conf()).thenReturn(runtimeConfig);
    sparkConf.set("spark.glue.accountId", "1122334455");
    when(sparkContext.getConf()).thenReturn(sparkConf);
    hadoopConf.set(
        "hive.metastore.client.factory.class",
        "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory");
    when(sparkContext.hadoopConfiguration()).thenReturn(hadoopConf);
    when(sparkSession.sparkContext()).thenReturn(sparkContext);
    when(runtimeConfig.getAll())
        .thenReturn(
            new Map.Map2<>(
                "spark.sql.catalog.test.catalog-impl",
                "org.apache.iceberg.aws.glue.GlueCatalog",
                "spark.sql.catalog.test.warehouse",
                "/tmp/warehouse"));

    SparkCatalog sparkCatalog = mock(SparkCatalog.class);
    SparkTable sparkTable = mock(SparkTable.class, RETURNS_DEEP_STUBS);
    Identifier identifier = Identifier.of(new String[] {"database"}, "table");

    when(sparkCatalog.name()).thenReturn("test");
    when(sparkCatalog.loadTable(identifier)).thenReturn(sparkTable);
    when(sparkTable.table().location()).thenReturn("file:/tmp/warehouse/database/table");

    DatasetIdentifier datasetIdentifier =
        icebergHandler.getDatasetIdentifier(
            sparkSession,
            sparkCatalog,
            Identifier.of(new String[] {"database"}, "table"),
            new HashMap<>());

    assertThat(datasetIdentifier)
        .hasFieldOrPropertyWithValue("namespace", "file")
        .hasFieldOrPropertyWithValue("name", "/tmp/warehouse/database/table");

    assertThat(datasetIdentifier.getSymlinks())
        .singleElement()
        .hasFieldOrPropertyWithValue("namespace", "arn:aws:glue:us-west-2:1122334455")
        .hasFieldOrPropertyWithValue("name", "table/database/table")
        .hasFieldOrPropertyWithValue("type", DatasetIdentifier.SymlinkType.TABLE);
  }

  private static Stream<Arguments> missingTableOptions() {
    return Stream.of(
        Arguments.of(Identifier.of(new String[] {}, "table"), "table", "/tmp/iceberg/table"),
        Arguments.of(
            Identifier.of(new String[] {"database"}, "table"),
            "database.table",
            "/tmp/iceberg/database/table"),
        Arguments.of(
            Identifier.of(new String[] {"nested", "namespace"}, "table"),
            "nested.namespace.table",
            "/tmp/iceberg/nested/namespace/table"));
  }

  @ParameterizedTest
  @MethodSource("missingTableOptions")
  @SneakyThrows
  void testGetDatasetIdentifierMissingSparkCatalogTable(
      Identifier identifier, String name, String location) {
    when(sparkSession.conf()).thenReturn(runtimeConfig);
    when(runtimeConfig.getAll())
        .thenReturn(
            new Map.Map2<>(
                "spark.sql.catalog.test.type",
                "hadoop",
                "spark.sql.catalog.test.warehouse",
                "file:/tmp/iceberg"));

    SparkCatalog sparkCatalog = mock(SparkCatalog.class);

    when(sparkCatalog.name()).thenReturn("test");
    when(sparkCatalog.loadTable(identifier)).thenThrow(new NoSuchTableException(identifier));

    DatasetIdentifier datasetIdentifier =
        icebergHandler.getDatasetIdentifier(
            sparkSession, sparkCatalog, identifier, new HashMap<>());

    assertThat(datasetIdentifier)
        .hasFieldOrPropertyWithValue("namespace", "file")
        .hasFieldOrPropertyWithValue("name", location);

    assertThat(datasetIdentifier.getSymlinks())
        .singleElement()
        .hasFieldOrPropertyWithValue("namespace", "file:/tmp/iceberg")
        .hasFieldOrPropertyWithValue("name", name)
        .hasFieldOrPropertyWithValue("type", DatasetIdentifier.SymlinkType.TABLE);
  }

  @Test
  @SneakyThrows
  void testGetDatasetIdentifierForIcebergTable() {
    when(sparkSession.conf()).thenReturn(runtimeConfig);
    when(runtimeConfig.getAll())
        .thenReturn(
            new Map.Map3<>(
                "spark.sql.catalog.test",
                "org.apache.iceberg.spark.SparkSessionCatalog",
                "spark.sql.catalog.test.type",
                "hadoop",
                "spark.sql.catalog.test.warehouse",
                "file:/tmp/warehouse"));

    SparkSessionCatalog sparkCatalog = mock(SparkSessionCatalog.class);
    when(sparkCatalog.name()).thenReturn("test");

    Catalog icebergCatalog = mock(Catalog.class);
    when(sparkCatalog.icebergCatalog()).thenReturn(icebergCatalog);

    TableIdentifier tableIdentifier = TableIdentifier.of("database", "table");
    Table icebergTable = mock(Table.class, RETURNS_DEEP_STUBS);
    when(icebergCatalog.loadTable(tableIdentifier)).thenReturn(icebergTable);
    when(icebergTable.location()).thenReturn("file:/tmp/warehouse/database/table");

    Identifier identifier = Identifier.of(new String[] {"database"}, "table");
    DatasetIdentifier datasetIdentifier =
        icebergHandler.getDatasetIdentifier(
            sparkSession, sparkCatalog, identifier, new HashMap<>());

    assertThat(datasetIdentifier)
        .hasFieldOrPropertyWithValue("namespace", "file")
        .hasFieldOrPropertyWithValue("name", "/tmp/warehouse/database/table");

    assertThat(datasetIdentifier.getSymlinks())
        .singleElement()
        .hasFieldOrPropertyWithValue("namespace", "file:/tmp/warehouse")
        .hasFieldOrPropertyWithValue("name", "database.table")
        .hasFieldOrPropertyWithValue("type", DatasetIdentifier.SymlinkType.TABLE);
  }

  @ParameterizedTest
  @MethodSource("missingTableOptions")
  @SneakyThrows
  void testGetDatasetIdentifierMissingIcebergCatalogTable(
      Identifier identifier, String name, String location) {
    when(sparkSession.conf()).thenReturn(runtimeConfig);
    when(runtimeConfig.getAll())
        .thenReturn(
            new Map.Map3<>(
                "spark.sql.catalog.test",
                "org.apache.iceberg.spark.SparkSessionCatalog",
                "spark.sql.catalog.test.type",
                "hadoop",
                "spark.sql.catalog.test.warehouse",
                "file:/tmp/iceberg"));

    SparkSessionCatalog sparkCatalog = mock(SparkSessionCatalog.class);
    when(sparkCatalog.name()).thenReturn("test");

    Catalog icebergCatalog = mock(Catalog.class);
    when(sparkCatalog.icebergCatalog()).thenReturn(icebergCatalog);

    TableIdentifier tableIdentifier = TableIdentifier.parse(identifier.toString());
    when(icebergCatalog.loadTable(tableIdentifier))
        .thenThrow(new org.apache.iceberg.exceptions.NoSuchTableException(identifier.toString()));

    DatasetIdentifier datasetIdentifier =
        icebergHandler.getDatasetIdentifier(
            sparkSession, sparkCatalog, identifier, new HashMap<>());

    assertThat(datasetIdentifier)
        .hasFieldOrPropertyWithValue("namespace", "file")
        .hasFieldOrPropertyWithValue("name", location);

    assertThat(datasetIdentifier.getSymlinks())
        .singleElement()
        .hasFieldOrPropertyWithValue("namespace", "file:/tmp/iceberg")
        .hasFieldOrPropertyWithValue("name", name)
        .hasFieldOrPropertyWithValue("type", DatasetIdentifier.SymlinkType.TABLE);
  }

  @Test
  @SneakyThrows
  void testGetStorageDatasetFacet() {
    when(context.getOpenLineage()).thenReturn(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI));
    Optional<OpenLineage.StorageDatasetFacet> storageDatasetFacet =
        icebergHandler.getStorageDatasetFacet(
            Collections.singletonMap("format", "iceberg/parquet"));

    assertThat(storageDatasetFacet.get())
        .hasFieldOrPropertyWithValue("storageLayer", "iceberg")
        .hasFieldOrPropertyWithValue("fileFormat", "parquet");
  }

  @Test
  @SneakyThrows
  void testStorageDatasetFacetWhenFormatNotProvided() {
    when(context.getOpenLineage()).thenReturn(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI));
    Optional<OpenLineage.StorageDatasetFacet> storageDatasetFacet =
        icebergHandler.getStorageDatasetFacet(new HashMap<>());

    assertThat(storageDatasetFacet.get())
        .hasFieldOrPropertyWithValue("storageLayer", "iceberg")
        .hasFieldOrPropertyWithValue("fileFormat", "");
  }

  @Test
  @SneakyThrows
  void testGetVersionString() {
    SparkCatalog sparkCatalog = mock(SparkCatalog.class);
    SparkTable sparkTable = mock(SparkTable.class, RETURNS_DEEP_STUBS);
    Identifier identifier = Identifier.of(new String[] {"database", "schema"}, "table");

    when(sparkCatalog.loadTable(identifier)).thenReturn(sparkTable);
    when(sparkTable.table().currentSnapshot().snapshotId()).thenReturn(1500100900L);

    Optional<String> version =
        icebergHandler.getDatasetVersion(sparkCatalog, identifier, Collections.emptyMap());

    assertThat(version.isPresent()).isTrue();
    assertThat(version.get()).isEqualTo("1500100900");
  }

  @Test
  void testGetHadoopCatalogData() {
    SparkCatalog sparkCatalog = mock(SparkCatalog.class);
    when(context.getSparkSession()).thenReturn(Optional.of(sparkSession));
    when(context.getOpenLineage()).thenReturn(new OpenLineage(URI.create("http://localhost")));
    when(sparkSession.conf()).thenReturn(runtimeConfig);
    when(sparkCatalog.name()).thenReturn("test");
    when(runtimeConfig.getAll())
        .thenReturn(
            new Map.Map2(
                "spark.sql.catalog.test.type",
                "hadoop",
                "spark.sql.catalog.test.warehouse",
                "hdfs://namenode:9000/path/to/warehouse"));

    Optional<CatalogHandler.CatalogWithAdditionalFacets> catalogDatasetFacet =
        icebergHandler.getCatalogDatasetFacet(sparkCatalog, new HashMap<>());
    assertTrue(catalogDatasetFacet.isPresent());

    OpenLineage.CatalogDatasetFacet facet = catalogDatasetFacet.get().getCatalogDatasetFacet();

    assertEquals("test", facet.getName());
    assertEquals("hadoop", facet.getType());
    assertEquals("iceberg", facet.getFramework());
    assertEquals("hdfs://namenode:9000/path/to/warehouse", facet.getWarehouseUri());
    assertNull(facet.getMetadataUri());
  }

  @Test
  void testGetGlueCatalogData() {
    SparkCatalog sparkCatalog = mock(SparkCatalog.class);
    when(context.getSparkSession()).thenReturn(Optional.of(sparkSession));
    when(context.getOpenLineage()).thenReturn(new OpenLineage(URI.create("http://localhost")));
    when(sparkSession.conf()).thenReturn(runtimeConfig);
    when(sparkCatalog.name()).thenReturn("test");
    when(runtimeConfig.getAll()).thenReturn(new Map.Map1("spark.sql.catalog.test.type", "hadoop"));

    Optional<CatalogHandler.CatalogWithAdditionalFacets> catalogDatasetFacet =
        icebergHandler.getCatalogDatasetFacet(sparkCatalog, new HashMap<>());
    assertTrue(catalogDatasetFacet.isPresent());

    OpenLineage.CatalogDatasetFacet facet = catalogDatasetFacet.get().getCatalogDatasetFacet();

    assertEquals("test", facet.getName());
    assertEquals("hadoop", facet.getType());
    assertEquals("iceberg", facet.getFramework());
  }

  @Test
  void testGetJdbcCatalogData() {
    SparkCatalog sparkCatalog = mock(SparkCatalog.class);
    when(context.getSparkSession()).thenReturn(Optional.of(sparkSession));
    when(context.getOpenLineage()).thenReturn(new OpenLineage(URI.create("http://localhost")));
    when(sparkSession.conf()).thenReturn(runtimeConfig);
    when(sparkCatalog.name()).thenReturn("test");
    when(runtimeConfig.getAll())
        .thenReturn(
            new Map.Map3(
                "spark.sql.catalog.test.type",
                "jdbc",
                "spark.sql.catalog.test.uri",
                "jdbc:mysql://test.1234567890.us-west-2.rds.amazonaws.com:3306/default",
                "spark.sql.catalog.test.warehouse",
                "s3://bucket/path/to/iceberg/warehouse"));

    Optional<CatalogHandler.CatalogWithAdditionalFacets> catalogDatasetFacet =
        icebergHandler.getCatalogDatasetFacet(sparkCatalog, new HashMap<>());
    assertTrue(catalogDatasetFacet.isPresent());

    OpenLineage.CatalogDatasetFacet facet = catalogDatasetFacet.get().getCatalogDatasetFacet();

    assertEquals("test", facet.getName());
    assertEquals("jdbc", facet.getType());
    assertEquals(
        "jdbc:mysql://test.1234567890.us-west-2.rds.amazonaws.com:3306/default",
        facet.getMetadataUri());
    assertEquals("s3://bucket/path/to/iceberg/warehouse", facet.getWarehouseUri());
    assertEquals("iceberg", facet.getFramework());
  }

  @Test
  void testGetBigQueryMetastoreCatalogData() {
    SparkCatalog sparkCatalog = mock(SparkCatalog.class);
    when(context.getSparkSession()).thenReturn(Optional.of(sparkSession));
    when(context.getOpenLineage()).thenReturn(new OpenLineage(URI.create("http://localhost")));
    when(sparkSession.conf()).thenReturn(runtimeConfig);
    when(sparkCatalog.name()).thenReturn("bq_metastore_catalog");
    when(runtimeConfig.getAll())
        .thenReturn(
            new Map.Map2(
                "spark.sql.catalog.bq_metastore_catalog.catalog-impl",
                "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog",
                "spark.sql.catalog.bq_metastore_catalog.warehouse",
                "gcs://bucket/path/to/iceberg/warehouse"));

    Optional<CatalogHandler.CatalogWithAdditionalFacets> catalogDatasetFacet =
        icebergHandler.getCatalogDatasetFacet(sparkCatalog, new HashMap<>());
    assertTrue(catalogDatasetFacet.isPresent());

    OpenLineage.CatalogDatasetFacet facet = catalogDatasetFacet.get().getCatalogDatasetFacet();

    assertEquals("bq_metastore_catalog", facet.getName());
    assertEquals("bigquerymetastore", facet.getType());
    assertEquals("gcs://bucket/path/to/iceberg/warehouse", facet.getWarehouseUri());
    assertEquals("iceberg", facet.getFramework());
  }

  @Test
  @SneakyThrows
  void testGetDatasetIdentifierMissingBigQueryMetastoreCatalogTable() {
    when(sparkSession.conf()).thenReturn(runtimeConfig);
    when(runtimeConfig.getAll())
        .thenReturn(
            new Map.Map3<>(
                "spark.sql.catalog.test",
                "org.apache.iceberg.spark.SparkSessionCatalog",
                "spark.sql.catalog.test.catalog-impl",
                "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog",
                "spark.sql.catalog.test.warehouse",
                "gcs://bucket/path/to/iceberg/warehouse"));

    SparkSessionCatalog sparkCatalog = mock(SparkSessionCatalog.class);
    when(sparkCatalog.name()).thenReturn("test");

    Catalog icebergCatalog = mock(Catalog.class);
    when(sparkCatalog.icebergCatalog()).thenReturn(icebergCatalog);

    TableIdentifier tableIdentifier =
        TableIdentifier.parse(Identifier.of(new String[] {"database"}, "table").toString());
    when(icebergCatalog.loadTable(tableIdentifier))
        .thenThrow(
            new org.apache.iceberg.exceptions.NoSuchTableException(
                Identifier.of(new String[] {"database"}, "table").toString()));

    DatasetIdentifier datasetIdentifier =
        icebergHandler.getDatasetIdentifier(
            sparkSession,
            sparkCatalog,
            Identifier.of(new String[] {"database"}, "table"),
            new HashMap<>());

    assertThat(datasetIdentifier)
        .hasFieldOrPropertyWithValue("namespace", "gcs://bucket")
        .hasFieldOrPropertyWithValue("name", "/path/to/iceberg/warehouse/database.db/table");

    assertThat(datasetIdentifier.getSymlinks())
        .singleElement()
        .hasFieldOrPropertyWithValue("namespace", "gcs://bucket/path/to/iceberg/warehouse")
        .hasFieldOrPropertyWithValue("name", "database.table")
        .hasFieldOrPropertyWithValue("type", DatasetIdentifier.SymlinkType.TABLE);
  }
}
