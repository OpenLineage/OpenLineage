/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.utils;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class PathUtilsTest {
  @Test
  void trimSlashesTest() {
    assertThat(PathUtils.trimSlashesInName("/a/")).isEqualTo("a");
    assertThat(PathUtils.trimSlashesInName("a/")).isEqualTo("a");
    assertThat(PathUtils.trimSlashesInName("/a")).isEqualTo("a");
    assertThat(PathUtils.trimSlashesInName("a")).isEqualTo("a");
    assertThat(PathUtils.trimSlashesInName("/   a   /")).isEqualTo("   a   ");
    assertThat(PathUtils.trimSlashesInName("/\uD83D\uDE02/")).isEqualTo("\uD83D\uDE02");
    assertThat(PathUtils.trimSlashesInName("\uD83D\uDE02/")).isEqualTo("\uD83D\uDE02");
    assertThat(PathUtils.trimSlashesInName("/\uD83D\uDE02")).isEqualTo("\uD83D\uDE02");
    assertThat(PathUtils.trimSlashesInName("\uD83D\uDE02/")).isEqualTo("\uD83D\uDE02");
    assertThat(PathUtils.trimSlashesInName("\uD83D\uDE02\uD83D\uDE02"))
        .isEqualTo("\uD83D\uDE02\uD83D\uDE02");
  }
}
