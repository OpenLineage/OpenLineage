/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import io.openlineage.utils.DatasetIdentifierUtils;
import java.net.URI;
import java.net.URISyntaxException;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class DatasetIdentifierUtilsTest {

  private static final String HOME_TEST = "/home/test";
  private static final String FILE = "file";

  @Test
  void testFromURI() throws URISyntaxException {
    URI uri1 = new URI("file:///home/test");
    assertThat(DatasetIdentifierUtils.nameFromURI(uri1)).isEqualTo(HOME_TEST);
    assertThat(DatasetIdentifierUtils.namespaceFromURI(uri1)).isEqualTo(FILE);

    URI uri2 = new URI(null, null, HOME_TEST, null);
    assertThat(DatasetIdentifierUtils.nameFromURI(uri2, FILE)).isEqualTo(HOME_TEST);
    assertThat(DatasetIdentifierUtils.namespaceFromURI(uri2, FILE)).isEqualTo(FILE);

    URI uri3 = new URI("hdfs", null, "localhost", 8020, HOME_TEST, null, null);
    assertThat(DatasetIdentifierUtils.nameFromURI(uri3, FILE)).isEqualTo(HOME_TEST);
    assertThat(DatasetIdentifierUtils.namespaceFromURI(uri3, FILE))
        .isEqualTo("hdfs://localhost:8020");

    URI uri4 = new URI("s3://data-bucket/path");
    assertThat(DatasetIdentifierUtils.nameFromURI(uri4, FILE)).isEqualTo("path");
    assertThat(DatasetIdentifierUtils.namespaceFromURI(uri4, FILE)).isEqualTo("s3://data-bucket");

    URI uri5 = new URI("file:/home/test");
    assertThat(DatasetIdentifierUtils.nameFromURI(uri5)).isEqualTo(HOME_TEST);
    assertThat(DatasetIdentifierUtils.namespaceFromURI(uri5)).isEqualTo(FILE);

    URI uri6 = new URI("hdfs://namenode:8020/home/test");
    assertThat(DatasetIdentifierUtils.nameFromURI(uri6)).isEqualTo(HOME_TEST);
    assertThat(DatasetIdentifierUtils.namespaceFromURI(uri6)).isEqualTo("hdfs://namenode:8020");

    URI uri7 = new URI("home/test");
    assertThat(DatasetIdentifierUtils.nameFromURI(uri7)).isEqualTo("home/test");
    assertThat(DatasetIdentifierUtils.namespaceFromURI(uri7)).isEqualTo(FILE);
  }

  @Test
  @SneakyThrows
  void testFromURIWithoutSchema() {
    URI uri = new URI(HOME_TEST);
    assertThat(DatasetIdentifierUtils.nameFromURI(uri)).isEqualTo(HOME_TEST);
    assertThat(DatasetIdentifierUtils.namespaceFromURI(uri)).isEqualTo(FILE);

    assertThat(DatasetIdentifierUtils.nameFromURI(uri, "hive")).isEqualTo(HOME_TEST);
    assertThat(DatasetIdentifierUtils.namespaceFromURI(uri, "hive")).isEqualTo("hive");
  }

  @Test
  @SneakyThrows
  void testUriForAbsolutePathAndNoSchemaNorAuthority() {
    URI uri2 = Mockito.mock(URI.class);
    when(uri2.getScheme()).thenReturn(null);
    when(uri2.getAuthority()).thenReturn(null);
    when(uri2.getPath()).thenReturn("C:/home/test");

    assertThat(DatasetIdentifierUtils.nameFromURI(uri2)).isEqualTo("C:/home/test");
    assertThat(DatasetIdentifierUtils.namespaceFromURI(uri2)).isEqualTo(FILE);
  }

  @Test
  @SneakyThrows
  void testUriWithSlashAtTheEnd() {
    URI uri = new URI("s3://bucket/table/");

    assertThat(DatasetIdentifierUtils.nameFromURI(uri)).isEqualTo("table");
    assertThat(DatasetIdentifierUtils.namespaceFromURI(uri)).isEqualTo("s3://bucket");
  }
}
