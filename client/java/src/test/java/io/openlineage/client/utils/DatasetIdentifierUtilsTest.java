/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

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
    assertThat(DatasetIdentifierUtils.fromURI(uri1))
        .hasFieldOrPropertyWithValue("name", HOME_TEST)
        .hasFieldOrPropertyWithValue("namespace", FILE);

    URI uri2 = new URI(null, null, HOME_TEST, null);
    assertThat(DatasetIdentifierUtils.fromURI(uri2))
        .hasFieldOrPropertyWithValue("name", HOME_TEST)
        .hasFieldOrPropertyWithValue("namespace", FILE);

    URI uri3 = new URI("hdfs", null, "localhost", 8020, HOME_TEST, null, null);
    assertThat(DatasetIdentifierUtils.fromURI(uri3))
        .hasFieldOrPropertyWithValue("name", HOME_TEST)
        .hasFieldOrPropertyWithValue("namespace", "hdfs://localhost:8020");

    URI uri4 = new URI("s3://data-bucket/path");
    assertThat(DatasetIdentifierUtils.fromURI(uri4))
        .hasFieldOrPropertyWithValue("name", "path")
        .hasFieldOrPropertyWithValue("namespace", "s3://data-bucket");

    URI uri5 = new URI("file:/home/test");
    assertThat(DatasetIdentifierUtils.fromURI(uri5))
        .hasFieldOrPropertyWithValue("name", HOME_TEST)
        .hasFieldOrPropertyWithValue("namespace", FILE);

    URI uri6 = new URI("hdfs://namenode:8020/home/test");
    assertThat(DatasetIdentifierUtils.fromURI(uri6))
        .hasFieldOrPropertyWithValue("name", HOME_TEST)
        .hasFieldOrPropertyWithValue("namespace", "hdfs://namenode:8020");

    URI uri7 = new URI("home/test");
    assertThat(DatasetIdentifierUtils.fromURI(uri7))
        .hasFieldOrPropertyWithValue("name", "home/test")
        .hasFieldOrPropertyWithValue("namespace", FILE);
  }

  @Test
  @SneakyThrows
  void testFromURIWithSchema() {
    URI uri = new URI(HOME_TEST);
    assertThat(DatasetIdentifierUtils.fromURI(uri))
        .hasFieldOrPropertyWithValue("name", HOME_TEST)
        .hasFieldOrPropertyWithValue("namespace", FILE);

    assertThat(DatasetIdentifierUtils.fromURI(uri, "hive"))
        .hasFieldOrPropertyWithValue("name", HOME_TEST)
        .hasFieldOrPropertyWithValue("namespace", "hive");
  }

  @Test
  @SneakyThrows
  void testUriForAbsolutePathAndNoSchemaNorAuthority() {
    URI uri = Mockito.mock(URI.class);
    when(uri.getScheme()).thenReturn(null);
    when(uri.getAuthority()).thenReturn(null);
    when(uri.getPath()).thenReturn("C:/home/test");

    assertThat(DatasetIdentifierUtils.fromURI(uri))
        .hasFieldOrPropertyWithValue("name", "C:/home/test")
        .hasFieldOrPropertyWithValue("namespace", FILE);
  }

  @Test
  @SneakyThrows
  void testUriWithSlashAtTheEnd() {
    URI uri = new URI("s3://bucket/table/");

    assertThat(DatasetIdentifierUtils.fromURI(uri))
        .hasFieldOrPropertyWithValue("name", "table")
        .hasFieldOrPropertyWithValue("namespace", "s3://bucket");
  }
}
