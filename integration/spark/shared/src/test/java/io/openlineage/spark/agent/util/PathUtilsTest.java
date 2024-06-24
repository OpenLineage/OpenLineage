/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static io.openlineage.client.utils.DatasetIdentifier.SymlinkType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.DatasetIdentifier;
import java.net.URI;
import java.net.URISyntaxException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.apache.spark.sql.internal.SessionState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;
import scala.Option;

@Slf4j
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class PathUtilsTest {

  SparkSession sparkSession;
  SparkContext sparkContext;
  SparkConf sparkConf;
  Configuration hadoopConf;
  CatalogTable catalogTable;
  CatalogStorageFormat catalogStorageFormat;
  SessionState sessionState;
  SessionCatalog sessionCatalog;

  @BeforeEach
  void setConf() {
    sparkSession = mock(SparkSession.class);
    sparkContext = mock(SparkContext.class);
    sparkConf = new SparkConf();
    hadoopConf = new Configuration();
    catalogTable = mock(CatalogTable.class);
    catalogStorageFormat = mock(CatalogStorageFormat.class);
    sessionState = mock(SessionState.class);
    sessionCatalog = mock(SessionCatalog.class);

    when(sparkContext.getConf()).thenReturn(sparkConf);
    when(sparkContext.hadoopConfiguration()).thenReturn(hadoopConf);
    when(sparkSession.sparkContext()).thenReturn(sparkContext);

    when(catalogTable.storage()).thenReturn(catalogStorageFormat);
    when(catalogTable.provider()).thenReturn(Option.empty());

    when(sparkSession.sessionState()).thenReturn(sessionState);
    when(sessionState.catalog()).thenReturn(sessionCatalog);
  }

  @Test
  void testFromPathWithoutSchema() {
    assertThat(PathUtils.fromPath(new Path("/home/test")))
        .hasFieldOrPropertyWithValue("name", "/home/test")
        .hasFieldOrPropertyWithValue("namespace", "file");

    assertThat(PathUtils.fromPath(new Path("home/test")))
        .hasFieldOrPropertyWithValue("name", "home/test")
        .hasFieldOrPropertyWithValue("namespace", "file");
  }

  @Test
  void testFromPathWithSchema() {
    assertThat(PathUtils.fromPath(new Path("file:/home/test")))
        .hasFieldOrPropertyWithValue("name", "/home/test")
        .hasFieldOrPropertyWithValue("namespace", "file");

    assertThat(PathUtils.fromPath(new Path("hdfs://namenode:8020/home/test")))
        .hasFieldOrPropertyWithValue("name", "/home/test")
        .hasFieldOrPropertyWithValue("namespace", "hdfs://namenode:8020");
  }

  @Test
  void testFromURI() throws URISyntaxException {
    assertThat(PathUtils.fromURI(new URI("/home/test")))
        .hasFieldOrPropertyWithValue("name", "/home/test")
        .hasFieldOrPropertyWithValue("namespace", "file");

    assertThat(PathUtils.fromURI(new URI("file:///home/test")))
        .hasFieldOrPropertyWithValue("name", "/home/test")
        .hasFieldOrPropertyWithValue("namespace", "file");

    assertThat(PathUtils.fromURI(new URI("hdfs://localhost:8020/home/test")))
        .hasFieldOrPropertyWithValue("name", "/home/test")
        .hasFieldOrPropertyWithValue("namespace", "hdfs://localhost:8020");

    assertThat(PathUtils.fromURI(new URI("s3://data-bucket/home/test")))
        .hasFieldOrPropertyWithValue("name", "home/test")
        .hasFieldOrPropertyWithValue("namespace", "s3://data-bucket");

    assertThat(PathUtils.fromURI(new URI("gs://gs-bucket/test.csv")))
        .hasFieldOrPropertyWithValue("name", "test.csv")
        .hasFieldOrPropertyWithValue("namespace", "gs://gs-bucket");
  }

  @Test
  void testFromCatalogTableWithHiveMetastore() throws URISyntaxException {
    sparkConf.set("spark.sql.catalogImplementation", "hive");
    sparkConf.set("spark.sql.hive.metastore.uris", "thrift://10.1.0.1:9083");
    TableIdentifier tableIdentifier = mock(TableIdentifier.class);
    when(catalogTable.identifier()).thenReturn(tableIdentifier);
    when(tableIdentifier.database()).thenReturn(Option.apply("database"));
    when(tableIdentifier.table()).thenReturn("table");
    when(catalogStorageFormat.locationUri())
        .thenReturn(Option.apply(new URI("/tmp/warehouse/database.db/table")));

    DatasetIdentifier datasetIdentifier = PathUtils.fromCatalogTable(catalogTable, sparkSession);
    assertThat(datasetIdentifier)
        .hasFieldOrPropertyWithValue("name", "/tmp/warehouse/database.db/table")
        .hasFieldOrPropertyWithValue("namespace", "file");
    assertThat(datasetIdentifier.getSymlinks()).hasSize(1);
    assertThat(datasetIdentifier.getSymlinks().get(0))
        .hasFieldOrPropertyWithValue("name", "database.table")
        .hasFieldOrPropertyWithValue("namespace", "hive://10.1.0.1:9083")
        .hasFieldOrPropertyWithValue("type", SymlinkType.TABLE);

    sparkConf.set(
        "spark.sql.hive.metastore.uris", "anotherprotocol://127.0.0.1:1010,yetanother://something");
    datasetIdentifier = PathUtils.fromCatalogTable(catalogTable, sparkSession);
    assertThat(datasetIdentifier)
        .hasFieldOrPropertyWithValue("name", "/tmp/warehouse/database.db/table")
        .hasFieldOrPropertyWithValue("namespace", "file");
    assertThat(datasetIdentifier.getSymlinks()).hasSize(1);
    assertThat(datasetIdentifier.getSymlinks().get(0))
        .hasFieldOrPropertyWithValue("name", "database.table")
        .hasFieldOrPropertyWithValue("namespace", "hive://127.0.0.1:1010")
        .hasFieldOrPropertyWithValue("type", SymlinkType.TABLE);

    sparkConf.remove("spark.sql.hive.metastore.uris");
    hadoopConf.set("hive.metastore.uris", "thrift://10.1.0.1:9084");
    datasetIdentifier = PathUtils.fromCatalogTable(catalogTable, sparkSession);
    assertThat(datasetIdentifier)
        .hasFieldOrPropertyWithValue("name", "/tmp/warehouse/database.db/table")
        .hasFieldOrPropertyWithValue("namespace", "file");
    assertThat(datasetIdentifier.getSymlinks()).hasSize(1);
    assertThat(datasetIdentifier.getSymlinks().get(0))
        .hasFieldOrPropertyWithValue("name", "database.table")
        .hasFieldOrPropertyWithValue("namespace", "hive://10.1.0.1:9084")
        .hasFieldOrPropertyWithValue("type", SymlinkType.TABLE);
  }

  @Test
  @SetEnvironmentVariable(key = "AWS_DEFAULT_REGION", value = "us-west-2")
  void testFromCatalogTableWithGlue() throws URISyntaxException {
    hadoopConf.set(
        "hive.metastore.client.factory.class",
        "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory");
    sparkConf.set("spark.sql.catalogImplementation", "hive");
    sparkConf.set("spark.glue.accountId", "123456789");

    when(catalogTable.provider()).thenReturn(Option.apply("hive"));
    when(catalogTable.storage()).thenReturn(catalogStorageFormat);
    TableIdentifier tableIdentifier = mock(TableIdentifier.class);
    when(catalogTable.identifier()).thenReturn(tableIdentifier);
    when(tableIdentifier.database()).thenReturn(Option.apply("database"));
    when(tableIdentifier.table()).thenReturn("mytable");
    when(catalogStorageFormat.locationUri())
        .thenReturn(Option.apply(new URI("s3://bucket/warehouse/mytable")));

    DatasetIdentifier datasetIdentifier = PathUtils.fromCatalogTable(catalogTable, sparkSession);
    assertThat(datasetIdentifier)
        .hasFieldOrPropertyWithValue("name", "warehouse/mytable")
        .hasFieldOrPropertyWithValue("namespace", "s3://bucket");
    assertThat(datasetIdentifier.getSymlinks()).hasSize(1);
    assertThat(datasetIdentifier.getSymlinks().get(0))
        .hasFieldOrPropertyWithValue("name", "table/database/mytable")
        .hasFieldOrPropertyWithValue("namespace", "arn:aws:glue:us-west-2:123456789")
        .hasFieldOrPropertyWithValue("type", SymlinkType.TABLE);
  }

  @Test
  void testFromCatalogWithDefaultStorage() throws URISyntaxException {
    when(catalogStorageFormat.locationUri())
        .thenReturn(Option.apply(new URI("hdfs://namenode:8020/warehouse/database.db/table")));
    sparkConf.set("spark.sql.warehouse.dir", "hdfs://namenode:8020/warehouse");

    TableIdentifier tableIdentifier = mock(TableIdentifier.class);
    when(catalogTable.identifier()).thenReturn(tableIdentifier);
    when(tableIdentifier.database()).thenReturn(Option.apply("database"));
    when(tableIdentifier.table()).thenReturn("table");

    DatasetIdentifier datasetIdentifier = PathUtils.fromCatalogTable(catalogTable, sparkSession);
    assertThat(datasetIdentifier)
        .hasFieldOrPropertyWithValue("name", "/warehouse/database.db/table")
        .hasFieldOrPropertyWithValue("namespace", "hdfs://namenode:8020");

    assertThat(datasetIdentifier.getSymlinks()).hasSize(1);
    assertThat(datasetIdentifier.getSymlinks().get(0))
        .hasFieldOrPropertyWithValue("name", "database.table")
        .hasFieldOrPropertyWithValue("namespace", "hdfs://namenode:8020/warehouse")
        .hasFieldOrPropertyWithValue("type", SymlinkType.TABLE);

    sparkConf.remove("spark.sql.warehouse.dir");
    hadoopConf.set("hive.metastore.warehouse.dir", "hdfs://namenode:8020/warehouse");
    datasetIdentifier = PathUtils.fromCatalogTable(catalogTable, sparkSession);
    assertThat(datasetIdentifier)
        .hasFieldOrPropertyWithValue("name", "/warehouse/database.db/table")
        .hasFieldOrPropertyWithValue("namespace", "hdfs://namenode:8020");

    assertThat(datasetIdentifier.getSymlinks()).hasSize(1);
    assertThat(datasetIdentifier.getSymlinks().get(0))
        .hasFieldOrPropertyWithValue("name", "database.table")
        .hasFieldOrPropertyWithValue("namespace", "hdfs://namenode:8020/warehouse")
        .hasFieldOrPropertyWithValue("type", SymlinkType.TABLE);
  }

  @Test
  void testFromCatalogWithDefaultStorageAndNoWarehouse() throws URISyntaxException {
    when(catalogStorageFormat.locationUri())
        .thenReturn(Option.apply(new URI("s3://bucket/warehouse/database.db/table")));

    TableIdentifier tableIdentifier = mock(TableIdentifier.class);
    when(catalogTable.identifier()).thenReturn(tableIdentifier);
    when(tableIdentifier.database()).thenReturn(Option.apply("database"));
    when(tableIdentifier.table()).thenReturn("table");

    DatasetIdentifier datasetIdentifier = PathUtils.fromCatalogTable(catalogTable, sparkSession);
    assertThat(datasetIdentifier)
        .hasFieldOrPropertyWithValue("name", "warehouse/database.db/table")
        .hasFieldOrPropertyWithValue("namespace", "s3://bucket");

    // without warehouse other Spark sessions can access this table only by location
    assertThat(datasetIdentifier.getSymlinks()).hasSize(0);
  }

  @Test
  void testFromCatalogWithDefaultStorageAndCustomLocation() throws URISyntaxException {
    when(catalogStorageFormat.locationUri())
        .thenReturn(Option.apply(new URI("hdfs://namenode:8020/custom/path/table")));
    sparkConf.set("spark.sql.warehouse.dir", "hdfs://namenode:8020/warehouse");

    TableIdentifier tableIdentifier = mock(TableIdentifier.class);
    when(catalogTable.identifier()).thenReturn(tableIdentifier);
    when(tableIdentifier.database()).thenReturn(Option.apply("database"));
    when(tableIdentifier.table()).thenReturn("table");

    DatasetIdentifier datasetIdentifier = PathUtils.fromCatalogTable(catalogTable, sparkSession);
    assertThat(datasetIdentifier)
        .hasFieldOrPropertyWithValue("name", "/custom/path/table")
        .hasFieldOrPropertyWithValue("namespace", "hdfs://namenode:8020");

    // without metastore other Spark sessions can access this table only by custom location, not by
    // name
    assertThat(datasetIdentifier.getSymlinks()).hasSize(0);
  }
}
