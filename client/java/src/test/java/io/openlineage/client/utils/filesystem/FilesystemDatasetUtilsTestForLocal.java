/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.filesystem;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.DatasetIdentifier;
import java.net.URI;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class FilesystemDatasetUtilsTestForLocal {
  @Test
  @SneakyThrows
  void testFromLocation() {
    assertThat(FilesystemDatasetUtils.fromLocation(new URI("file:/warehouse/location")))
        .hasFieldOrPropertyWithValue("namespace", "file")
        .hasFieldOrPropertyWithValue("name", "/warehouse/location");

    assertThat(FilesystemDatasetUtils.fromLocation(new URI("file:/warehouse/location/")))
        .hasFieldOrPropertyWithValue("namespace", "file")
        .hasFieldOrPropertyWithValue("name", "/warehouse/location");

    assertThat(FilesystemDatasetUtils.fromLocation(new URI("file:///warehouse/location")))
        .hasFieldOrPropertyWithValue("namespace", "file")
        .hasFieldOrPropertyWithValue("name", "/warehouse/location");
  }

  @Test
  @SneakyThrows
  void testFromLocationUnixPath() {
    assertThat(FilesystemDatasetUtils.fromLocation(new URI("/warehouse")))
        .hasFieldOrPropertyWithValue("namespace", "file")
        .hasFieldOrPropertyWithValue("name", "/warehouse");

    assertThat(FilesystemDatasetUtils.fromLocation(new URI("/warehouse/location")))
        .hasFieldOrPropertyWithValue("namespace", "file")
        .hasFieldOrPropertyWithValue("name", "/warehouse/location");

    assertThat(FilesystemDatasetUtils.fromLocation(new URI("warehouse/location")))
        .hasFieldOrPropertyWithValue("namespace", "file")
        .hasFieldOrPropertyWithValue("name", "warehouse/location");
  }

  @Test
  @SneakyThrows
  void testFromLocationWindowsPath() {
    URI uri = Mockito.mock(URI.class);
    when(uri.getScheme()).thenReturn(null);
    when(uri.getAuthority()).thenReturn(null);
    when(uri.getPath()).thenReturn("C:/home/test");

    assertThat(FilesystemDatasetUtils.fromLocation(uri))
        .hasFieldOrPropertyWithValue("namespace", "file")
        .hasFieldOrPropertyWithValue("name", "C:/home/test");
  }

  @Test
  @SneakyThrows
  void testFromLocationAndName() {
    assertThat(FilesystemDatasetUtils.fromLocationAndName(new URI(""), "default.table"))
        .hasFieldOrPropertyWithValue("namespace", "file")
        .hasFieldOrPropertyWithValue("name", "default.table");

    assertThat(FilesystemDatasetUtils.fromLocationAndName(new URI("/"), "default.table"))
        .hasFieldOrPropertyWithValue("namespace", "file")
        .hasFieldOrPropertyWithValue("name", "default.table");

    assertThat(FilesystemDatasetUtils.fromLocationAndName(new URI("/warehouse"), "default.table"))
        .hasFieldOrPropertyWithValue("namespace", "file:/warehouse")
        .hasFieldOrPropertyWithValue("name", "default.table");

    assertThat(
            FilesystemDatasetUtils.fromLocationAndName(
                new URI("/warehouse/location"), "default.table"))
        .hasFieldOrPropertyWithValue("namespace", "file:/warehouse/location")
        .hasFieldOrPropertyWithValue("name", "default.table");

    assertThat(
            FilesystemDatasetUtils.fromLocationAndName(
                new URI("file:/warehouse/location"), "default.table"))
        .hasFieldOrPropertyWithValue("namespace", "file:/warehouse/location")
        .hasFieldOrPropertyWithValue("name", "default.table");

    assertThat(
            FilesystemDatasetUtils.fromLocationAndName(
                new URI("file:/warehouse/location/"), "default.table"))
        .hasFieldOrPropertyWithValue("namespace", "file:/warehouse/location")
        .hasFieldOrPropertyWithValue("name", "default.table");

    assertThat(
            FilesystemDatasetUtils.fromLocationAndName(
                new URI("file:///warehouse/location"), "default.table"))
        .hasFieldOrPropertyWithValue("namespace", "file:/warehouse/location")
        .hasFieldOrPropertyWithValue("name", "default.table");
  }

  @Test
  @SneakyThrows
  void testFromLocationAndNameWindowsPath() {
    URI uri = Mockito.mock(URI.class);
    when(uri.getScheme()).thenReturn(null);
    when(uri.getAuthority()).thenReturn(null);
    when(uri.getPath()).thenReturn("C:/home/test");

    assertThat(FilesystemDatasetUtils.fromLocationAndName(uri, "default.table"))
        .hasFieldOrPropertyWithValue("namespace", "file:C:/home/test")
        .hasFieldOrPropertyWithValue("name", "default.table");
  }

  @Test
  @SneakyThrows
  void toLocation() {
    assertThat(FilesystemDatasetUtils.toLocation(new DatasetIdentifier("/", "file")))
        .isEqualTo(new URI("file:/"));

    assertThat(
            FilesystemDatasetUtils.toLocation(new DatasetIdentifier("/warehouse/location", "file")))
        .isEqualTo(new URI("file:/warehouse/location"));

    assertThat(
            FilesystemDatasetUtils.toLocation(
                new DatasetIdentifier("default.table", "file:/warehouse/location")))
        .isEqualTo(new URI("file:/warehouse/location/default.table"));
  }
}
