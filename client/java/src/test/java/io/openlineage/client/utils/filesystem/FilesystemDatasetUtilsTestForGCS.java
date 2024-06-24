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

class FilesystemDatasetUtilsTestForGCS {
  @Test
  @SneakyThrows
  void testFromLocation() {
    assertThat(FilesystemDatasetUtils.fromLocation(new URI("gs://bucket")))
        .hasFieldOrPropertyWithValue("namespace", "gs://bucket")
        .hasFieldOrPropertyWithValue("name", "/");

    assertThat(FilesystemDatasetUtils.fromLocation(new URI("gs://bucket/warehouse")))
        .hasFieldOrPropertyWithValue("namespace", "gs://bucket")
        .hasFieldOrPropertyWithValue("name", "warehouse");

    assertThat(FilesystemDatasetUtils.fromLocation(new URI("gs://bucket/warehouse/location")))
        .hasFieldOrPropertyWithValue("namespace", "gs://bucket")
        .hasFieldOrPropertyWithValue("name", "warehouse/location");

    assertThat(FilesystemDatasetUtils.fromLocation(new URI("gs://bucket/warehouse/location/")))
        .hasFieldOrPropertyWithValue("namespace", "gs://bucket")
        .hasFieldOrPropertyWithValue("name", "warehouse/location");
  }

  @Test
  @SneakyThrows
  void testFromLocationAndName() {
    assertThat(
            FilesystemDatasetUtils.fromLocationAndName(
                new URI("gs://bucket/warehouse"), "default.table"))
        .hasFieldOrPropertyWithValue("namespace", "gs://bucket/warehouse")
        .hasFieldOrPropertyWithValue("name", "default.table");

    assertThat(
            FilesystemDatasetUtils.fromLocationAndName(
                new URI("gs://bucket/warehouse/location"), "default.table"))
        .hasFieldOrPropertyWithValue("namespace", "gs://bucket/warehouse/location")
        .hasFieldOrPropertyWithValue("name", "default.table");

    assertThat(FilesystemDatasetUtils.fromLocationAndName(new URI("gs://bucket/warehouse"), ""))
        .hasFieldOrPropertyWithValue("namespace", "gs://bucket/warehouse")
        .hasFieldOrPropertyWithValue("name", "/");
  }

  @Test
  @SneakyThrows
  void toLocation() {
    assertThat(FilesystemDatasetUtils.toLocation(new DatasetIdentifier("/", "gs://bucket")))
        .isEqualTo(new URI("gs://bucket"));

    assertThat(FilesystemDatasetUtils.toLocation(new DatasetIdentifier("warehouse", "gs://bucket")))
        .isEqualTo(new URI("gs://bucket/warehouse"));

    assertThat(
            FilesystemDatasetUtils.toLocation(
                new DatasetIdentifier("warehouse/location", "gs://bucket")))
        .isEqualTo(new URI("gs://bucket/warehouse/location"));

    assertThat(
            FilesystemDatasetUtils.toLocation(
                new DatasetIdentifier("default.table", "gs://bucket/warehouse/location")))
        .isEqualTo(new URI("gs://bucket/warehouse/location/default.table"));
  }
}
