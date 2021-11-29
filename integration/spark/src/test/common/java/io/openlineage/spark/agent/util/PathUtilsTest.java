package io.openlineage.spark.agent.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.net.URISyntaxException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier$;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.junit.jupiter.api.Test;
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
  void testFromCatalogTable() {
    CatalogTable catalogTable = mock(CatalogTable.class);
    when(catalogTable.identifier()).thenReturn(TableIdentifier$.MODULE$.apply("table"));
    when(catalogTable.qualifiedName()).thenReturn("table");
    given(catalogTable.location())
        .willAnswer(
            invocation -> {
              throw new AnalysisException(
                  "", Option.empty(), Option.empty(), Option.empty(), Option.empty());
            });

    DatasetIdentifier di = PathUtils.fromCatalogTable(catalogTable, "10.1.0.1:9083");
    assertThat(catalogTable.qualifiedName()).isEqualTo("table");
    assertThat(di.getName()).isEqualTo("table");
    assertThat(di.getNamespace()).isEqualTo("hive://10.1.0.1:9083");
  }

  @Test
  void testFromHiveTable() throws URISyntaxException {
    SparkSession sparkSession = mock(SparkSession.class);
    SparkContext sparkContext = mock(SparkContext.class);
    SparkConf sparkConf = new SparkConf();
    sparkConf.set("spark.sql.hive.metastore.uris", "thrift://10.1.0.1:9083");
    when(sparkContext.getConf()).thenReturn(sparkConf);
    when(sparkSession.sparkContext()).thenReturn(sparkContext);

    CatalogTable catalogTable = mock(CatalogTable.class);
    when(catalogTable.qualifiedName()).thenReturn("table");

    DatasetIdentifier di = PathUtils.fromHiveTable(sparkSession, catalogTable);
    assertThat(di.getName()).isEqualTo("table");
    assertThat(di.getNamespace()).isEqualTo("hive://10.1.0.1:9083");

    sparkConf.set(
        "spark.sql.hive.metastore.uris", "anotherprotocol://127.0.0.1:1010,yetanother://something");
    di = PathUtils.fromHiveTable(sparkSession, catalogTable);
    assertThat(di.getName()).isEqualTo("table");
    assertThat(di.getNamespace()).isEqualTo("hive://127.0.0.1:1010");

    sparkConf.remove("spark.sql.hive.metastore.uris");
    sparkConf.set("spark.hadoop.hive.metastore.uris", "thrift://10.1.0.1:9083");
    di = PathUtils.fromHiveTable(sparkSession, catalogTable);
    assertThat(di.getName()).isEqualTo("table");
    assertThat(di.getNamespace()).isEqualTo("hive://10.1.0.1:9083");

    sparkConf.remove("spark.hadoop.hive.metastore.uris");
    when(catalogTable.location()).thenReturn(new URI("hdfs://namenode:8020/warehouse/table"));
    di = PathUtils.fromHiveTable(sparkSession, catalogTable);
    assertThat(di.getName()).isEqualTo("/warehouse/table");
    assertThat(di.getNamespace()).isEqualTo("hdfs://namenode:8020");
  }
}
