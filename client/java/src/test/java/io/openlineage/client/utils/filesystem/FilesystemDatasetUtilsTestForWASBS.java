/*
/* Copyright 2018-2024 contributors to the OpenLineage project
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
    assertThat(FilesystemDatasetUtils.fromLocation(new URI("wasbs://container@bucket")))
        .hasFieldOrPropertyWithValue("namespace", "wasbs://container@bucket")
        .hasFieldOrPropertyWithValue("name", "/");

    assertThat(FilesystemDatasetUtils.fromLocation(new URI("wasbs://container@bucket/warehouse")))
        .hasFieldOrPropertyWithValue("namespace", "wasbs://container@bucket")
        .hasFieldOrPropertyWithValue("name", "warehouse");

    assertThat(
            FilesystemDatasetUtils.fromLocation(
                new URI("wasbs://container@bucket/warehouse/location")))
        .hasFieldOrPropertyWithValue("namespace", "wasbs://container@bucket")
        .hasFieldOrPropertyWithValue("name", "warehouse/location");

    assertThat(
            FilesystemDatasetUtils.fromLocation(
                new URI("wasbs://container@bucket/warehouse/location/")))
        .hasFieldOrPropertyWithValue("namespace", "wasbs://container@bucket")
        .hasFieldOrPropertyWithValue("name", "warehouse/location");
  }

  @Test
  @SneakyThrows
  void testFromLocationAndName() {
    assertThat(
            FilesystemDatasetUtils.fromLocationAndName(
                new URI("wasbs://container@bucket/warehouse"), "default.table"))
        .hasFieldOrPropertyWithValue("namespace", "wasbs://container@bucket/warehouse")
        .hasFieldOrPropertyWithValue("name", "default.table");

    assertThat(
            FilesystemDatasetUtils.fromLocationAndName(
                new URI("wasbs://container@bucket/warehouse/location"), "default.table"))
        .hasFieldOrPropertyWithValue("namespace", "wasbs://container@bucket/warehouse/location")
        .hasFieldOrPropertyWithValue("name", "default.table");

    assertThat(
            FilesystemDatasetUtils.fromLocationAndName(
                new URI("wasbs://container@bucket/warehouse"), ""))
        .hasFieldOrPropertyWithValue("namespace", "wasbs://container@bucket/warehouse")
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
