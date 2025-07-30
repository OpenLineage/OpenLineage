/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.filesystem;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.utils.DatasetIdentifier;
import java.net.URI;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

class FilesystemDatasetUtilsTestForWASBS {
  @Test
  @SneakyThrows
  void testFromLocation() {
    assertThat(
            FilesystemDatasetUtils.fromLocation(
                new URI("wasbs://container@bucket.dfs.core.windows.net/warehouse")))
        .hasFieldOrPropertyWithValue("namespace", "wasbs://container@bucket.dfs.core.windows.net")
        .hasFieldOrPropertyWithValue("name", "warehouse");

    assertThat(
            FilesystemDatasetUtils.fromLocation(
                new URI("wasbs://container@bucket.dfs.core.windows.net/warehouse/location")))
        .hasFieldOrPropertyWithValue("namespace", "wasbs://container@bucket.dfs.core.windows.net")
        .hasFieldOrPropertyWithValue("name", "warehouse/location");

    assertThat(
            FilesystemDatasetUtils.fromLocation(
                new URI("wasbs://container@bucket.dfs.core.windows.net/warehouse/location")))
        .hasFieldOrPropertyWithValue("namespace", "wasbs://container@bucket.dfs.core.windows.net")
        .hasFieldOrPropertyWithValue("name", "warehouse/location");
  }

  @Test
  @SneakyThrows
  void testFromLocationAndName() {
    assertThat(
            FilesystemDatasetUtils.fromLocationAndName(
                new URI("wasbs://container@bucket.dfs.core.windows.net/warehouse"),
                "default.table"))
        .hasFieldOrPropertyWithValue(
            "namespace", "wasbs://container@bucket.dfs.core.windows.net/warehouse")
        .hasFieldOrPropertyWithValue("name", "default.table");

    assertThat(
            FilesystemDatasetUtils.fromLocationAndName(
                new URI("wasbs://container@bucket.dfs.core.windows.net/warehouse/location"), ""))
        .hasFieldOrPropertyWithValue(
            "namespace", "wasbs://container@bucket.dfs.core.windows.net/warehouse/location")
        .hasFieldOrPropertyWithValue("name", "/");
  }

  @Test
  @SneakyThrows
  void toLocation() {
    assertThat(
            FilesystemDatasetUtils.toLocation(
                new DatasetIdentifier("/", "wasbs://container@bucket")))
        .isEqualTo(new URI("wasbs://container@bucket"));

    assertThat(
            FilesystemDatasetUtils.toLocation(
                new DatasetIdentifier("warehouse", "wasbs://container@bucket")))
        .isEqualTo(new URI("wasbs://container@bucket/warehouse"));

    assertThat(
            FilesystemDatasetUtils.toLocation(
                new DatasetIdentifier("warehouse/location", "wasbs://container@bucket")))
        .isEqualTo(new URI("wasbs://container@bucket/warehouse/location"));

    assertThat(
            FilesystemDatasetUtils.toLocation(
                new DatasetIdentifier(
                    "default.table", "wasbs://container@bucket/warehouse/location")))
        .isEqualTo(new URI("wasbs://container@bucket/warehouse/location/default.table"));
  }
}
