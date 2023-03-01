/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.sql.DbTableMeta;
import io.openlineage.sql.OpenLineageSql;
import io.openlineage.sql.SqlMeta;
import java.util.Collections;
import java.util.Optional;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;

public class JdbcUtils {
  /**
   * JdbcUrl can contain username and password this method clean-up credentials from jdbcUrl and
   * strip the jdbc prefix from the url
   */
  public static String sanitizeJdbcUrl(String jdbcUrl) {
    String jdbcUrlCroppedPrefix = jdbcUrl.substring(5);
    return jdbcUrlCroppedPrefix
        .replaceAll(PlanUtils.SLASH_DELIMITER_USER_PASSWORD_REGEX, "@")
        .replaceAll(PlanUtils.COLON_DELIMITER_USER_PASSWORD_REGEX, "$1")
        .replaceAll("(?<=[?,;&:)=])\\(?(?i)(?:user|username|password)=[^;&,)]+(?:[;&;)]|$)", "");
  }

  public static Optional<SqlMeta> extractQueryFromSpark(JDBCRelation relation) {
    String tableOrQuery = relation.jdbcOptions().tableOrQuery();
    if (tableOrQuery.contains(") SPARK_GEN_SUBQ_")) {
      String query =
          tableOrQuery.replaceFirst("\\(", "").replaceAll("\\) SPARK_GEN_SUBQ_[0-9]+", "");
      return OpenLineageSql.parse(Collections.singletonList(query));
    } else {
      return Optional.of(
          new SqlMeta(
              Collections.singletonList(new DbTableMeta(null, null, tableOrQuery)),
              Collections.emptyList(),
              Collections.emptyList(),
              Collections.emptyList()));
    }
  }
}
