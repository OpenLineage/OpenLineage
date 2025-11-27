/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.snowflake;

import static io.openlineage.client.utils.SnowflakeUtils.stripQuotes;

import io.openlineage.sql.DbTableMeta;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SnowflakeTable {
  public static String getQualifiedName(
      String defaultDatabase, String defaultSchema, DbTableMeta table) {
    String tableDatabase = table.database() != null ? table.database() : defaultDatabase;
    String tableSchema = table.schema() != null ? table.schema() : defaultSchema;
    return getQualifiedName(tableDatabase, tableSchema, table.name());
  }

  public static String getQualifiedName(String database, String schema, String table) {
    return String.format(
        "%s.%s.%s", stripQuotes(database), stripQuotes(schema), stripQuotes(table));
  }
}
