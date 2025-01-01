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

class FilesystemDatasetUtilsTestForS3 {
  @Test
  @SneakyThrows
  void testFromLocation() {
    assertThat(FilesystemDatasetUtils.fromLocation(new URI("s3://bucket")))
        .hasFieldOrPropertyWithValue("namespace", "s3://bucket")
        .hasFieldOrPropertyWithValue("name", "/");

    assertThat(FilesystemDatasetUtils.fromLocation(new URI("s3://bucket/warehouse")))
        .hasFieldOrPropertyWithValue("namespace", "s3://bucket")
        .hasFieldOrPropertyWithValue("name", "warehouse");

    assertThat(FilesystemDatasetUtils.fromLocation(new URI("s3://bucket/warehouse/location")))
        .hasFieldOrPropertyWithValue("namespace", "s3://bucket")
        .hasFieldOrPropertyWithValue("name", "warehouse/location");

    assertThat(FilesystemDatasetUtils.fromLocation(new URI("s3://bucket/warehouse/location/")))
        .hasFieldOrPropertyWithValue("namespace", "s3://bucket")
        .hasFieldOrPropertyWithValue("name", "warehouse/location");
  }

  @Test
  @SneakyThrows
  void testFromLocationWithDifferentProtocol() {
    assertThat(FilesystemDatasetUtils.fromLocation(new URI("s3a://bucket/warehouse/location/")))
        .hasFieldOrPropertyWithValue("namespace", "s3://bucket")
        .hasFieldOrPropertyWithValue("name", "warehouse/location");

    assertThat(FilesystemDatasetUtils.fromLocation(new URI("s3n://bucket/warehouse/location/")))
        .hasFieldOrPropertyWithValue("namespace", "s3://bucket")
        .hasFieldOrPropertyWithValue("name", "warehouse/location");
  }

  @Test
  @SneakyThrows
  void testFromLocationAndName() {
    assertThat(
            FilesystemDatasetUtils.fromLocationAndName(
                new URI("s3://bucket/warehouse"), "default.table"))
        .hasFieldOrPropertyWithValue("namespace", "s3://bucket/warehouse")
        .hasFieldOrPropertyWithValue("name", "default.table");

    assertThat(
            FilesystemDatasetUtils.fromLocationAndName(
                new URI("s3://bucket/warehouse/location"), "default.table"))
        .hasFieldOrPropertyWithValue("namespace", "s3://bucket/warehouse/location")
        .hasFieldOrPropertyWithValue("name", "default.table");

    assertThat(FilesystemDatasetUtils.fromLocationAndName(new URI("s3://bucket/warehouse"), ""))
        .hasFieldOrPropertyWithValue("namespace", "s3://bucket/warehouse")
        .hasFieldOrPropertyWithValue("name", "/");
  }

  @Test
  @SneakyThrows
  void testFromLocationAndNameWithDifferentProtocol() {
    assertThat(
            FilesystemDatasetUtils.fromLocationAndName(
                new URI("s3a://bucket/warehouse/location"), "default.table"))
        .hasFieldOrPropertyWithValue("namespace", "s3://bucket/warehouse/location")
        .hasFieldOrPropertyWithValue("name", "default.table");

    assertThat(
            FilesystemDatasetUtils.fromLocationAndName(
                new URI("s3n://bucket/warehouse/location"), "default.table"))
        .hasFieldOrPropertyWithValue("namespace", "s3://bucket/warehouse/location")
        .hasFieldOrPropertyWithValue("name", "default.table");
  }

  @Test
  @SneakyThrows
  void toLocation() {
    assertThat(FilesystemDatasetUtils.toLocation(new DatasetIdentifier("/", "s3://bucket")))
        .isEqualTo(new URI("s3://bucket"));

    assertThat(FilesystemDatasetUtils.toLocation(new DatasetIdentifier("warehouse", "s3://bucket")))
        .isEqualTo(new URI("s3://bucket/warehouse"));

    assertThat(
            FilesystemDatasetUtils.toLocation(
                new DatasetIdentifier("warehouse/location", "s3://bucket")))
        .isEqualTo(new URI("s3://bucket/warehouse/location"));

    assertThat(
            FilesystemDatasetUtils.toLocation(
                new DatasetIdentifier("default.table", "s3://bucket/warehouse/location")))
        .isEqualTo(new URI("s3://bucket/warehouse/location/default.table"));
  }
}
