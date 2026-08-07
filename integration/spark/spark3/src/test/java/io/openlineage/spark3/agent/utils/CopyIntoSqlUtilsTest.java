/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.utils;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class CopyIntoSqlUtilsTest {

  private static final String SQL =
      "COPY INTO copy_into_table\n"
          + "FROM '/Volumes/sangeeta_catalog/default/sangeeta_vol/input/csv_input'\n"
          + "FILEFORMAT = CSV\n"
          + "FORMAT_OPTIONS (\n"
          + "    'header' = 'true',\n"
          + "    'inferSchema' = 'true'\n"
          + ")";

  @Test
  void testParsesTargetTable() {
    assertThat(CopyIntoSqlUtils.targetTable(SQL)).contains("copy_into_table");
  }

  @Test
  void testParsesSourcePath() {
    assertThat(CopyIntoSqlUtils.sourcePath(SQL))
        .contains("/Volumes/sangeeta_catalog/default/sangeeta_vol/input/csv_input");
  }

  @Test
  void testDetectsCopyIntoStatement() {
    assertThat(CopyIntoSqlUtils.isCopyIntoStatement(SQL)).isTrue();
    assertThat(CopyIntoSqlUtils.isCopyIntoStatement("SELECT 1")).isFalse();
  }
}
