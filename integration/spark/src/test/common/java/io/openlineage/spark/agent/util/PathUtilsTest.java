package io.openlineage.spark.agent.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.URISyntaxException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

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
  }
}
