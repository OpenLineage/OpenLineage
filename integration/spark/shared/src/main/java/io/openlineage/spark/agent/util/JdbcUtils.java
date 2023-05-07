/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.sql.DbTableMeta;
import io.openlineage.sql.ExtractionError;
import io.openlineage.sql.OpenLineageSql;
import io.openlineage.sql.SqlMeta;
import java.util.Collections;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;

@Slf4j
public class JdbcUtils {
  /**
   * JdbcUrl can contain username and password this method clean-up credentials from jdbcUrl and
   * strip the jdbc prefix from the url
   */
  public static String sanitizeJdbcUrl(String jdbcUrl) {
    String jdbcUrlCroppedPrefix = jdbcUrl.substring(5);
    return jdbcUrlCroppedPrefix
        .replaceFirst("^jdbc:", "")
        .replaceFirst("^postgresql:", "postgres:")
        .replaceAll(PlanUtils.SLASH_DELIMITER_USER_PASSWORD_REGEX, "@")
        .replaceAll(PlanUtils.COLON_DELIMITER_USER_PASSWORD_REGEX, "$1")
        .replaceAll("(?<=[?,;&:)=])\\(?(?i)(?:user|username|password)=[^;&,)]+(?:[;&;)]|$)", "")
        .replaceAll("\\?.+$", "");
  }

  public static Optional<SqlMeta> extractQueryFromSpark(JDBCRelation relation) {
    String tableOrQuery = relation.jdbcOptions().tableOrQuery();
    if (!tableOrQuery.trim().startsWith("(")) {
      return Optional.of(
          new SqlMeta(
              Collections.singletonList(new DbTableMeta(null, null, tableOrQuery)),
              Collections.emptyList(),
              Collections.emptyList(),
              Collections.emptyList()));
    } else {
      String query =
          tableOrQuery.substring(0, tableOrQuery.lastIndexOf(")")).replaceFirst("\\(", "");

      String dialect = extractDialectFromJdbcUrl(relation.jdbcOptions().url());
      SqlMeta sqlMeta = OpenLineageSql.parse(Collections.singletonList(query), dialect).get();

      if (!sqlMeta.errors().isEmpty()) { // error return nothing
        log.error(
            String.format(
                "error while parsing query: %s",
                sqlMeta.errors().stream()
                    .map(ExtractionError::toString)
                    .collect(Collectors.joining(","))));
        return Optional.empty();
      } else if (sqlMeta.inTables().isEmpty()) {
        log.error("no tables defined in query, this should not happen");
        return Optional.empty();
      }
      return Optional.of(sqlMeta);
    }
  }

  private static String extractDialectFromJdbcUrl(String jdbcUrl) {
    Pattern pattern = Pattern.compile("^jdbc:([^:]+):.*");
    Matcher matcher = pattern.matcher(jdbcUrl);

    if (matcher.find()) {
      return matcher.group(1);
    } else {
      return null;
    }
  }
}
