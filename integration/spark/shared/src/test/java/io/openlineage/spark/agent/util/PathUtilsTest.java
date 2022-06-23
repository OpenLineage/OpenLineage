/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static io.openlineage.spark.agent.util.PathUtils.enrichHiveMetastoreURIWithTableName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.apache.spark.sql.internal.SessionState;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import scala.Option;

@Slf4j
public class PathUtilsTest {

  @Test
  void testPathSeparation() {
    Path path = new Path("scheme:/asdf/fdsa");
    assertThat(path.toUri().getScheme()).isEqualTo("scheme");
    assertThat(path.toUri().getAuthority()).isEqualTo(null);
    assertThat(path.toUri().getPath()).isEqualTo("/asdf/fdsa");

    path = new Path("scheme://asdf/fdsa");
    assertThat(path.toUri().getScheme()).isEqualTo("scheme");
    assertThat(path.toUri().getAuthority()).isEqualTo("asdf");
    assertThat(path.toUri().getPath()).isEqualTo("/fdsa");

    path = new Path("scheme:///asdf/fdsa");
    assertThat(path.toUri().getScheme()).isEqualTo("scheme");
    assertThat(path.toUri().getAuthority()).isEqualTo(null);
    assertThat(path.toUri().getPath()).isEqualTo("/asdf/fdsa");

    path = new Path("scheme:////asdf/fdsa");
    assertThat(path.toUri().getScheme()).isEqualTo("scheme");
    assertThat(path.toUri().getAuthority()).isEqualTo(null);
    assertThat(path.toUri().getPath()).isEqualTo("/asdf/fdsa");
  }

  @Test
  void testFromPathWithoutSchema() {
    DatasetIdentifier di = PathUtils.fromPath(new Path("/home/test"));
    assertThat(di.getName()).isEqualTo("/home/test");
    assertThat(di.getNamespace()).isEqualTo("file");

    di = PathUtils.fromPath(new Path("/home/test"), "hive");
    assertThat(di.getName()).isEqualTo("/home/test");
    assertThat(di.getNamespace()).isEqualTo("hive");

    di = PathUtils.fromPath(new Path("home/test"));
    assertThat(di.getName()).isEqualTo("home/test");
    assertThat(di.getNamespace()).isEqualTo("file");
  }

  @Test
  void testFromPathWithSchema() {
    DatasetIdentifier di = PathUtils.fromPath(new Path("file:/home/test"));
    assertThat(di.getName()).isEqualTo("/home/test");
    assertThat(di.getNamespace()).isEqualTo("file");

    di = PathUtils.fromPath(new Path("hdfs://namenode:8020/home/test"));
    assertThat(di.getName()).isEqualTo("/home/test");
    assertThat(di.getNamespace()).isEqualTo("hdfs://namenode:8020");
  }

  @Test
  void testFromURI() throws URISyntaxException {
    DatasetIdentifier di = PathUtils.fromURI(new URI("file:///home/test"), null);
    assertThat(di.getName()).isEqualTo("/home/test");
    assertThat(di.getNamespace()).isEqualTo("file");

    di = PathUtils.fromURI(new URI(null, null, "/home/test", null), "file");
    assertThat(di.getName()).isEqualTo("/home/test");
    assertThat(di.getNamespace()).isEqualTo("file");

    di =
        PathUtils.fromURI(
            new URI("hdfs", null, "localhost", 8020, "/home/test", null, null), "file");
    assertThat(di.getName()).isEqualTo("/home/test");
    assertThat(di.getNamespace()).isEqualTo("hdfs://localhost:8020");

    di = PathUtils.fromURI(new URI("s3://data-bucket/path"), "file");
    assertThat(di.getName()).isEqualTo("path");
    assertThat(di.getNamespace()).isEqualTo("s3://data-bucket");
  }

  @Test
  void testFromCatalogTableWithHiveTables() throws URISyntaxException {
    SparkSession sparkSession = mock(SparkSession.class);
    SparkContext sparkContext = mock(SparkContext.class);
    SparkConf sparkConf = new SparkConf();
    sparkConf.set("spark.sql.catalogImplementation", "hive");
    sparkConf.set("spark.sql.hive.metastore.uris", "thrift://10.1.0.1:9083");
    when(sparkContext.getConf()).thenReturn(sparkConf);
    when(sparkSession.sparkContext()).thenReturn(sparkContext);

    CatalogTable catalogTable = mock(CatalogTable.class);
    when(catalogTable.qualifiedName()).thenReturn("table");

    DatasetIdentifier di = PathUtils.fromCatalogTable(catalogTable, Optional.of(sparkConf));
    assertThat(di.getName()).isEqualTo("table");
    assertThat(di.getNamespace()).isEqualTo("hive://10.1.0.1:9083");

    sparkConf.set(
        "spark.sql.hive.metastore.uris", "anotherprotocol://127.0.0.1:1010,yetanother://something");
    di = PathUtils.fromCatalogTable(catalogTable, Optional.of(sparkConf));
    assertThat(di.getName()).isEqualTo("table");
    assertThat(di.getNamespace()).isEqualTo("hive://127.0.0.1:1010");

    sparkConf.remove("spark.sql.hive.metastore.uris");
    sparkConf.set("spark.hadoop.hive.metastore.uris", "thrift://10.1.0.1:9083");
    di = PathUtils.fromCatalogTable(catalogTable, Optional.of(sparkConf));
    assertThat(di.getName()).isEqualTo("table");
    assertThat(di.getNamespace()).isEqualTo("hive://10.1.0.1:9083");
  }

  @Test
  void testFromCatalogWithNoHiveMetastoreAndHdfsLocation() throws URISyntaxException {
    CatalogTable catalogTable = mock(CatalogTable.class);
    CatalogStorageFormat catalogStorageFormat = mock(CatalogStorageFormat.class);
    SparkConf sparkConf = new SparkConf();

    sparkConf.remove("spark.hadoop.hive.metastore.uris");
    when(catalogTable.storage()).thenReturn(catalogStorageFormat);
    when(catalogStorageFormat.locationUri())
        .thenReturn(Option.apply(new URI("hdfs://namenode:8020/warehouse/table")));
    DatasetIdentifier di = PathUtils.fromCatalogTable(catalogTable, Optional.of(sparkConf));
    assertThat(di.getName()).isEqualTo("/warehouse/table");
    assertThat(di.getNamespace()).isEqualTo("hdfs://namenode:8020");
  }

  @Test
  void testFromCatalogExceptionIsThrownWhenUnableToExtractDatasetIdentifier() {
    CatalogTable catalogTable = mock(CatalogTable.class);
    try (MockedStatic mocked = mockStatic(SparkSession.class)) {
      mocked.when(SparkSession::active).thenThrow(new IllegalStateException("DUPA DUPA"));
      mocked.when(SparkSession::getActiveSession).thenReturn(Option.empty());
      assertThrows(IllegalArgumentException.class, () -> PathUtils.fromCatalogTable(catalogTable));
    }

    //    assertThrows(IllegalArgumentException.class, () ->
    // PathUtils.fromCatalogTable(catalogTable));
  }

  @Test
  void testEnrichMetastoreUriWithTableName() throws URISyntaxException {
    assertThat(enrichHiveMetastoreURIWithTableName(new URI("thrift://10.1.0.1:9083"), "/db/table"))
        .isEqualTo(new URI("hive://10.1.0.1:9083/db/table"));
  }

  @Test
  void testFromCatalogFromDefaultTablePath() throws URISyntaxException {
    SparkSession sparkSession = mock(SparkSession.class);
    SessionState sessionState = mock(SessionState.class);
    SessionCatalog sessionCatalog = mock(SessionCatalog.class);

    when(sparkSession.sessionState()).thenReturn(sessionState);
    when(sessionState.catalog()).thenReturn(sessionCatalog);
    when(sessionCatalog.defaultTablePath(any())).thenReturn(new URI("/warehouse/table"));

    try (MockedStatic mocked = mockStatic(SparkSession.class)) {
      mocked.when(SparkSession::active).thenReturn(sparkSession);
      DatasetIdentifier di =
          PathUtils.fromCatalogTable(mock(CatalogTable.class), Optional.of(new SparkConf()));

      assertThat(di.getName()).isEqualTo("/warehouse/table");
      assertThat(di.getNamespace()).isEqualTo("file");
    }
  }
}
