/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.filesystem;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.utils.DatasetIdentifier;
import java.net.URI;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

class FilesystemDatasetUtilsTestForDBFS {
  @Test
  @SneakyThrows
  void testFromLocation() {
    assertThat(FilesystemDatasetUtils.fromLocation(new URI("dbfs:/warehouse/location")))
        .hasFieldOrPropertyWithValue("namespace", "dbfs")
        .hasFieldOrPropertyWithValue("name", "/warehouse/location");

    assertThat(FilesystemDatasetUtils.fromLocation(new URI("dbfs:/warehouse/location/")))
        .hasFieldOrPropertyWithValue("namespace", "dbfs")
        .hasFieldOrPropertyWithValue("name", "/warehouse/location");

    assertThat(FilesystemDatasetUtils.fromLocation(new URI("dbfs:///warehouse/location")))
        .hasFieldOrPropertyWithValue("namespace", "dbfs")
        .hasFieldOrPropertyWithValue("name", "/warehouse/location");
  }

  @Test
  @SneakyThrows
  void testFromLocationAndName() {
    assertThat(
            FilesystemDatasetUtils.fromLocationAndName(
                new URI("dbfs:/warehouse/location"), "default.table"))
        .hasFieldOrPropertyWithValue("namespace", "dbfs:/warehouse/location")
        .hasFieldOrPropertyWithValue("name", "default.table");

    assertThat(
            FilesystemDatasetUtils.fromLocationAndName(
                new URI("dbfs:/warehouse/location/"), "default.table"))
        .hasFieldOrPropertyWithValue("namespace", "dbfs:/warehouse/location")
        .hasFieldOrPropertyWithValue("name", "default.table");
  }

  @Test
  @SneakyThrows
  void toLocation() {
    assertThat(FilesystemDatasetUtils.toLocation(new DatasetIdentifier("/", "dbfs")))
        .isEqualTo(new URI("dbfs:/"));

    assertThat(
            FilesystemDatasetUtils.toLocation(new DatasetIdentifier("/warehouse/location", "dbfs")))
        .isEqualTo(new URI("dbfs:/warehouse/location"));

    assertThat(
            FilesystemDatasetUtils.toLocation(
                new DatasetIdentifier("default.table", "dbfs:/warehouse/location")))
        .isEqualTo(new URI("dbfs:/warehouse/location/default.table"));
  }
}
