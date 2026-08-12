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

  @Test
  void testParsesCatalogQualifiedTargetTable() {
    String sql = "COPY INTO catalog.schema.copy_into_table FROM '/path/to/source'";

    assertThat(CopyIntoSqlUtils.targetTable(sql)).contains("catalog.schema.copy_into_table");
  }

  @Test
  void testParsesDoubleQuotedSourcePath() {
    String sql = "COPY INTO copy_into_table FROM \"/Volumes/catalog/schema/vol/input\"";

    assertThat(CopyIntoSqlUtils.sourcePath(sql)).contains("/Volumes/catalog/schema/vol/input");
  }

  @Test
  void testParsesBacktickQuotedTargetTable() {
    String sql = "COPY INTO `catalog`.`schema`.`copy_into_table` FROM '/path/to/source'";

    assertThat(CopyIntoSqlUtils.targetTable(sql)).contains("catalog.schema.copy_into_table");
  }

  @Test
  void testParsesBacktickQuotedTargetTableWithHyphen() {
    String sql = "COPY INTO `sales-data` FROM '/path/to/source'";

    assertThat(CopyIntoSqlUtils.targetTable(sql)).contains("sales-data");
  }

  @Test
  void testParsesBacktickQuotedTargetTableWithSpace() {
    String sql = "COPY INTO `sales data` FROM '/path/to/source'";

    assertThat(CopyIntoSqlUtils.targetTable(sql)).contains("sales data");
  }

  @Test
  void testParsesBacktickQuotedTargetTableWithEscapedBacktick() {
    String sql = "COPY INTO `sales``data` FROM '/path/to/source'";

    assertThat(CopyIntoSqlUtils.targetTable(sql)).contains("sales`data");
  }

  @Test
  void testDetectsValidateStatement() {
    String sql =
        "COPY INTO copy_into_table FROM '/path/to/source' FILEFORMAT = CSV VALIDATE 15 ROWS";

    assertThat(CopyIntoSqlUtils.isValidateStatement(sql)).isTrue();
    assertThat(CopyIntoSqlUtils.isValidateStatement("COPY INTO t FROM '/p' FILEFORMAT = CSV"))
        .isFalse();
  }

  @Test
  void testDetectsCaseInsensitiveCopyInto() {
    String sql = "copy into my_table from '/path/to/source'";

    assertThat(CopyIntoSqlUtils.isCopyIntoStatement(sql)).isTrue();
    assertThat(CopyIntoSqlUtils.targetTable(sql)).contains("my_table");
    assertThat(CopyIntoSqlUtils.sourcePath(sql)).contains("/path/to/source");
  }

  @Test
  void testReturnsEmptyForBlankSql() {
    assertThat(CopyIntoSqlUtils.targetTable(null)).isEmpty();
    assertThat(CopyIntoSqlUtils.targetTable("   ")).isEmpty();
    assertThat(CopyIntoSqlUtils.sourcePath(null)).isEmpty();
    assertThat(CopyIntoSqlUtils.isCopyIntoStatement(null)).isFalse();
  }
}
