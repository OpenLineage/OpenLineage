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

class FilesystemDatasetUtilsTestForABFSS {
  @Test
  @SneakyThrows
  void testFromLocation() {
    assertThat(
            FilesystemDatasetUtils.fromLocation(
                new URI("abfss://container@account.dfs.core.windows.net")))
        .hasFieldOrPropertyWithValue("namespace", "abfss://container@account.dfs.core.windows.net")
        .hasFieldOrPropertyWithValue("name", "/");

    assertThat(
            FilesystemDatasetUtils.fromLocation(
                new URI("abfss://container@account.dfs.core.windows.net/warehouse")))
        .hasFieldOrPropertyWithValue("namespace", "abfss://container@account.dfs.core.windows.net")
        .hasFieldOrPropertyWithValue("name", "warehouse");

    assertThat(
            FilesystemDatasetUtils.fromLocation(
                new URI("abfss://container@account.dfs.core.windows.net/warehouse/location")))
        .hasFieldOrPropertyWithValue("namespace", "abfss://container@account.dfs.core.windows.net")
        .hasFieldOrPropertyWithValue("name", "warehouse/location");

    assertThat(
            FilesystemDatasetUtils.fromLocation(
                new URI("abfss://container@account.dfs.core.windows.net/warehouse/location/")))
        .hasFieldOrPropertyWithValue("namespace", "abfss://container@account.dfs.core.windows.net")
        .hasFieldOrPropertyWithValue("name", "warehouse/location");
  }

  /**
   * The non-TLS scheme {@code abfs} differs from {@code abfss} only in transport security and
   * addresses the same data, so it is normalized to the canonical {@code abfss} scheme defined by
   * the OpenLineage naming spec
   */
  @Test
  @SneakyThrows
  void testFromLocationWithDifferentProtocol() {
    assertThat(
            FilesystemDatasetUtils.fromLocation(
                new URI("abfs://container@account.dfs.core.windows.net/warehouse")))
        .hasFieldOrPropertyWithValue("namespace", "abfss://container@account.dfs.core.windows.net")
        .hasFieldOrPropertyWithValue("name", "warehouse");
  }

  @Test
  @SneakyThrows
  void testFromLocationAndNameWithDifferentProtocol() {
    assertThat(
            FilesystemDatasetUtils.fromLocationAndName(
                new URI("abfs://container@account.dfs.core.windows.net/warehouse"),
                "default.table"))
        .hasFieldOrPropertyWithValue(
            "namespace", "abfss://container@account.dfs.core.windows.net/warehouse")
        .hasFieldOrPropertyWithValue("name", "default.table");
  }

  @Test
  @SneakyThrows
  void testFromLocationAndName() {
    assertThat(
            FilesystemDatasetUtils.fromLocationAndName(
                new URI("abfss://container@account.dfs.core.windows.net/warehouse"),
                "default.table"))
        .hasFieldOrPropertyWithValue(
            "namespace", "abfss://container@account.dfs.core.windows.net/warehouse")
        .hasFieldOrPropertyWithValue("name", "default.table");

    assertThat(
            FilesystemDatasetUtils.fromLocationAndName(
                new URI("abfss://container@account.dfs.core.windows.net/warehouse"), ""))
        .hasFieldOrPropertyWithValue(
            "namespace", "abfss://container@account.dfs.core.windows.net/warehouse")
        .hasFieldOrPropertyWithValue("name", "/");
  }

  @Test
  @SneakyThrows
  void toLocation() {
    assertThat(
            FilesystemDatasetUtils.toLocation(
                new DatasetIdentifier("/", "abfss://container@account.dfs.core.windows.net")))
        .isEqualTo(new URI("abfss://container@account.dfs.core.windows.net"));

    assertThat(
            FilesystemDatasetUtils.toLocation(
                new DatasetIdentifier(
                    "warehouse", "abfss://container@account.dfs.core.windows.net")))
        .isEqualTo(new URI("abfss://container@account.dfs.core.windows.net/warehouse"));

    assertThat(
            FilesystemDatasetUtils.toLocation(
                new DatasetIdentifier(
                    "default.table",
                    "abfss://container@account.dfs.core.windows.net/warehouse/location")))
        .isEqualTo(
            new URI(
                "abfss://container@account.dfs.core.windows.net/warehouse/location/default.table"));
  }
}
