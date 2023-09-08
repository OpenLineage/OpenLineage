/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static io.openlineage.spark.agent.util.PathUtils.enrichHiveMetastoreURIWithTableName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.DatasetIdentifier;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import scala.Option;

@Slf4j
class PathUtilsTest {

  private static final String HOME_TEST = "/home/test";
  private static final String SCHEME = "scheme";
  private static final String FILE = "file";
  private static final String TABLE = "table";

  SparkSession sparkSession = mock(SparkSession.class);
  SparkContext sparkContext = mock(SparkContext.class);
  SparkConf sparkConf = new SparkConf();
  CatalogTable catalogTable = mock(CatalogTable.class);
  CatalogStorageFormat catalogStorageFormat = mock(CatalogStorageFormat.class);

  @Test
  void testPathSeparation() {
    Path path = new Path("scheme:/asdf/fdsa");
    assertThat(path.toUri().getScheme()).isEqualTo(SCHEME);
    assertThat(path.toUri().getAuthority()).isEqualTo(null);
    assertThat(path.toUri().getPath()).isEqualTo("/asdf/fdsa");

    path = new Path("scheme://asdf/fdsa");
    assertThat(path.toUri().getScheme()).isEqualTo(SCHEME);
    assertThat(path.toUri().getAuthority()).isEqualTo("asdf");
    assertThat(path.toUri().getPath()).isEqualTo("/fdsa");
  }

  @Test
  void testPathSeparationWithNullAuthority() {
    Path path = new Path("scheme:///asdf/fdsa");
    assertThat(path.toUri().getScheme()).isEqualTo(SCHEME);
    assertThat(path.toUri().getAuthority()).isEqualTo(null);
    assertThat(path.toUri().getPath()).isEqualTo("/asdf/fdsa");

    path = new Path("scheme:////asdf/fdsa");
    assertThat(path.toUri().getScheme()).isEqualTo(SCHEME);
    assertThat(path.toUri().getAuthority()).isEqualTo(null);
    assertThat(path.toUri().getPath()).isEqualTo("/asdf/fdsa");
  }

  @Test
  void testFromPathWithoutSchema() {
    DatasetIdentifier di = PathUtils.fromPath(new Path(HOME_TEST));
    assertThat(di.getName()).isEqualTo(HOME_TEST);
    assertThat(di.getNamespace()).isEqualTo(FILE);

    di = PathUtils.fromPath(new Path(HOME_TEST), "hive");
    assertThat(di.getName()).isEqualTo(HOME_TEST);
    assertThat(di.getNamespace()).isEqualTo("hive");

    di = PathUtils.fromPath(new Path("home/test"));
    assertThat(di.getName()).isEqualTo("home/test");
    assertThat(di.getNamespace()).isEqualTo(FILE);
  }

  @Test
  void testFromPathWithSchema() {
    DatasetIdentifier di = PathUtils.fromPath(new Path("file:/home/test"));
    assertThat(di.getName()).isEqualTo(HOME_TEST);
    assertThat(di.getNamespace()).isEqualTo(FILE);

    di = PathUtils.fromPath(new Path("hdfs://namenode:8020/home/test"));
    assertThat(di.getName()).isEqualTo(HOME_TEST);
    assertThat(di.getNamespace()).isEqualTo("hdfs://namenode:8020");
  }

  @Test
  void testFromURI() throws URISyntaxException {
    DatasetIdentifier di = PathUtils.fromURI(new URI("file:///home/test"), null);
    assertThat(di.getName()).isEqualTo(HOME_TEST);
    assertThat(di.getNamespace()).isEqualTo(FILE);

    di = PathUtils.fromURI(new URI(null, null, HOME_TEST, null), FILE);
    assertThat(di.getName()).isEqualTo(HOME_TEST);
    assertThat(di.getNamespace()).isEqualTo(FILE);

    di = PathUtils.fromURI(new URI("hdfs", null, "localhost", 8020, HOME_TEST, null, null), FILE);
    assertThat(di.getName()).isEqualTo(HOME_TEST);
    assertThat(di.getNamespace()).isEqualTo("hdfs://localhost:8020");

    di = PathUtils.fromURI(new URI("s3://data-bucket/path"), FILE);
    assertThat(di.getName()).isEqualTo("path");
    assertThat(di.getNamespace()).isEqualTo("s3://data-bucket");
  }

  @Test
  void testFromCatalogTableWithStorage() throws URISyntaxException {
    sparkConf.set("spark.sql.catalogImplementation", "hive");
    sparkConf.set("spark.sql.hive.metastore.uris", "thrift://10.1.0.1:9083");
    when(sparkContext.getConf()).thenReturn(sparkConf);
    when(sparkSession.sparkContext()).thenReturn(sparkContext);

    when(catalogTable.storage()).thenReturn(catalogStorageFormat);
    when(catalogTable.identifier()).thenReturn(TableIdentifier.apply(TABLE));
    when(catalogStorageFormat.locationUri()).thenReturn(Option.apply(new URI("/tmp/warehouse")));

    DatasetIdentifier di = PathUtils.fromCatalogTable(catalogTable, Optional.of(sparkConf));
    assertThat(di.getName()).isEqualTo("/tmp/warehouse");
    assertThat(di.getNamespace()).isEqualTo("file");
    assertThat(di.getSymlinks().size()).isEqualTo(1);
    assertThat(di.getSymlinks().get(0).getName()).isEqualTo(TABLE);
    assertThat(di.getSymlinks().get(0).getNamespace()).isEqualTo("hive://10.1.0.1:9083");

    sparkConf.set(
        "spark.sql.hive.metastore.uris", "anotherprotocol://127.0.0.1:1010,yetanother://something");
    di = PathUtils.fromCatalogTable(catalogTable, Optional.of(sparkConf));
    assertThat(di.getSymlinks().get(0).getName()).isEqualTo(TABLE);
    assertThat(di.getSymlinks().get(0).getNamespace()).isEqualTo("hive://127.0.0.1:1010");

    sparkConf.remove("spark.sql.hive.metastore.uris");
    sparkConf.set("spark.hadoop.hive.metastore.uris", "thrift://10.1.0.1:9083");
    di = PathUtils.fromCatalogTable(catalogTable, Optional.of(sparkConf));
    assertThat(di.getSymlinks().get(0).getName()).isEqualTo(TABLE);
    assertThat(di.getSymlinks().get(0).getNamespace()).isEqualTo("hive://10.1.0.1:9083");
  }

  @Test
  void testFromCatalogWithDefaultStorage() throws URISyntaxException {
    sparkConf.remove("spark.hadoop.hive.metastore.uris");
    when(catalogTable.storage()).thenReturn(catalogStorageFormat);
    when(catalogStorageFormat.locationUri())
        .thenReturn(Option.apply(new URI("hdfs://namenode:8020/warehouse/table")));
    TableIdentifier tableIdentifier = mock(TableIdentifier.class);
    when(catalogTable.identifier()).thenReturn(tableIdentifier);
    when(tableIdentifier.database()).thenReturn(Option.apply("db"));
    when(tableIdentifier.table()).thenReturn("table");

    DatasetIdentifier di = PathUtils.fromCatalogTable(catalogTable, Optional.of(sparkConf));
    assertThat(di.getName()).isEqualTo("/warehouse/table");
    assertThat(di.getNamespace()).isEqualTo("hdfs://namenode:8020");
    assertThat(di.getSymlinks().size()).isEqualTo(1);
    assertThat(di.getSymlinks().get(0).getName()).isEqualTo("db.table");
    assertThat(di.getSymlinks().get(0).getNamespace()).isEqualTo("/warehouse");
  }

  @Test
  void testFromCatalogExceptionIsThrownWhenUnableToExtractDatasetIdentifier() {
    try (MockedStatic mocked = mockStatic(SparkSession.class)) {
      mocked.when(SparkSession::active).thenThrow(new IllegalStateException("some message"));
      mocked.when(SparkSession::getDefaultSession).thenReturn(Option.empty());
      assertThrows(IllegalArgumentException.class, () -> PathUtils.fromCatalogTable(catalogTable));
    }
  }

  @Test
  void testEnrichMetastoreUriWithTableName() throws URISyntaxException {
    assertThat(enrichHiveMetastoreURIWithTableName(new URI("thrift://10.1.0.1:9083"), "/db/table"))
        .isEqualTo(new URI("hive://10.1.0.1:9083/db/table"));
  }

  @Test
  void testDatasetNameReplaceNamePattern() throws URISyntaxException {
    DatasetIdentifier di;
    try (MockedStatic mocked = mockStatic(SparkSession.class)) {
      mocked.when(SparkSession::getDefaultSession).thenReturn(Option.apply(sparkSession));
      when(sparkSession.sparkContext()).thenReturn(sparkContext);
      when(sparkContext.getConf()).thenReturn(sparkConf);

      sparkConf.set(
          "spark.openlineage.dataset.removePath.pattern", "(.*)(?<remove>\\/.*\\/.*\\/.*)");
      di = PathUtils.fromURI(new URI("s3:///my-whatever-path/year=2023/month=04/day=24"), null);
      assertThat(di.getName()).isEqualTo("/my-whatever-path");

      sparkConf.set(
          "spark.openlineage.dataset.removePath.pattern", "(.*)(?<nonValidGroup>\\/.*\\/.*\\/.*)");
      di = PathUtils.fromURI(new URI("s3:///my-whatever-path/year=2023/month=04/day=24"), null);
      assertThat(di.getName()).isEqualTo("/my-whatever-path/year=2023/month=04/day=24");

      sparkConf.set(
          "spark.openlineage.dataset.removePath.pattern", "(.*)(?<remove>\\/.*\\/.*\\/.*)");
      di = PathUtils.fromURI(new URI("s3:///path-without-group/file"), null);
      assertThat(di.getName()).isEqualTo("/path-without-group/file");
    }
  }
}
