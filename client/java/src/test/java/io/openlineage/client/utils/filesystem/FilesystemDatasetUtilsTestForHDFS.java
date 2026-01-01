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

class FilesystemDatasetUtilsTestForHDFS {
  @Test
  @SneakyThrows
  void testFromLocation() {
    assertThat(FilesystemDatasetUtils.fromLocation(new URI("hdfs://namenode:8020")))
        .hasFieldOrPropertyWithValue("namespace", "hdfs://namenode:8020")
        .hasFieldOrPropertyWithValue("name", "/");

    assertThat(FilesystemDatasetUtils.fromLocation(new URI("hdfs://namenode:8020/warehouse")))
        .hasFieldOrPropertyWithValue("namespace", "hdfs://namenode:8020")
        .hasFieldOrPropertyWithValue("name", "/warehouse");

    assertThat(
            FilesystemDatasetUtils.fromLocation(new URI("hdfs://namenode:8020/warehouse/location")))
        .hasFieldOrPropertyWithValue("namespace", "hdfs://namenode:8020")
        .hasFieldOrPropertyWithValue("name", "/warehouse/location");

    assertThat(
            FilesystemDatasetUtils.fromLocation(
                new URI("hdfs://namenode:8020/warehouse/location/")))
        .hasFieldOrPropertyWithValue("namespace", "hdfs://namenode:8020")
        .hasFieldOrPropertyWithValue("name", "/warehouse/location");
  }

  @Test
  @SneakyThrows
  void testFromLocationAndName() {
    assertThat(
            FilesystemDatasetUtils.fromLocationAndName(
                new URI("hdfs://namenode:8020/warehouse"), "default.table"))
        .hasFieldOrPropertyWithValue("namespace", "hdfs://namenode:8020/warehouse")
        .hasFieldOrPropertyWithValue("name", "default.table");

    assertThat(
            FilesystemDatasetUtils.fromLocationAndName(
                new URI("hdfs://namenode:8020/warehouse/location"), "default.table"))
        .hasFieldOrPropertyWithValue("namespace", "hdfs://namenode:8020/warehouse/location")
        .hasFieldOrPropertyWithValue("name", "default.table");

    assertThat(
            FilesystemDatasetUtils.fromLocationAndName(
                new URI("hdfs://namenode:8020/warehouse"), ""))
        .hasFieldOrPropertyWithValue("namespace", "hdfs://namenode:8020/warehouse")
        .hasFieldOrPropertyWithValue("name", "/");
  }

  @Test
  @SneakyThrows
  void toLocation() {
    assertThat(
            FilesystemDatasetUtils.toLocation(new DatasetIdentifier("/", "hdfs://namenode:8020")))
        .isEqualTo(new URI("hdfs://namenode:8020"));

    assertThat(
            FilesystemDatasetUtils.toLocation(
                new DatasetIdentifier("/warehouse", "hdfs://namenode:8020")))
        .isEqualTo(new URI("hdfs://namenode:8020/warehouse"));

    assertThat(
            FilesystemDatasetUtils.toLocation(
                new DatasetIdentifier("/warehouse/location", "hdfs://namenode:8020")))
        .isEqualTo(new URI("hdfs://namenode:8020/warehouse/location"));

    assertThat(
            FilesystemDatasetUtils.toLocation(
                new DatasetIdentifier("default.table", "hdfs://namenode:8020/warehouse/location")))
        .isEqualTo(new URI("hdfs://namenode:8020/warehouse/location/default.table"));
  }
}
