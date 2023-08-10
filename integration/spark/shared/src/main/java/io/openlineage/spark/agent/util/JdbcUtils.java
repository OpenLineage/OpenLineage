/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import com.google.common.base.CharMatcher;
import io.openlineage.sql.DbTableMeta;
import io.openlineage.sql.ExtractionError;
import io.openlineage.sql.OpenLineageSql;
import io.openlineage.sql.SqlMeta;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
    return jdbcUrl
        .replaceFirst("^jdbc:", "")
        .replaceFirst("^postgresql:", "postgres:")
        .replaceAll(PlanUtils.SLASH_DELIMITER_USER_PASSWORD_REGEX, "@")
        .replaceAll(PlanUtils.COLON_DELIMITER_USER_PASSWORD_REGEX, "$1")
        .replaceAll("(?<=[?,;&:)=])\\(?(?i)(?:user|username|password)=[^;&,)]+(?:[;&;)]|$)", "")
        .replaceAll("\\?.+$", "");
  }

  public static DatasetIdentifier getDatasetIdentifierFromJdbcUrl(String jdbcUrl, String name) {
    List<String> parts = Arrays.stream(name.split("\\.")).collect(Collectors.toList());
    return getDatasetIdentifierFromJdbcUrl(jdbcUrl, parts);
  }

  /**
   * The algorithm for this method is as follows. First we parse URI and check if it includes path
   * part of URI. If yes, then we check if it contains database. Database is the first part after
   * slash in URI - the "db" in something like postgres://host:5432/db. If it does contain it, and
   * provided parts list has less than three elements, then we use it as database part of name -
   * this indicates that database is the default one in this context. Otherwise, we take database
   * from parts list.
   *
   * @param jdbcUrl String URI we want to take dataset identifier from
   * @param parts Provided list of delimited parts of table qualified name parts. Can include
   *     database name.
   * @return DatasetIdentifier
   */
  public static DatasetIdentifier getDatasetIdentifierFromJdbcUrl(
      String jdbcUrl, List<String> parts) {
    jdbcUrl = sanitizeJdbcUrl(jdbcUrl);
    String namespace = jdbcUrl;
    String urlDatabase = null;

    try {
      URI uri = new URI(jdbcUrl);
      String path = uri.getPath();
      if (path != null) {
        namespace = String.format("%s://%s", uri.getScheme(), uri.getAuthority());

        if (path.startsWith("/")) {
          path = path.substring(1);
        }

        if (path.length() > 1
            && CharMatcher.forPredicate(Character::isAlphabetic).matchesAllOf(path)) {
          urlDatabase = path;
        }
      }
    } catch (URISyntaxException ignored) {
      // If URI parsing fails, we can't do anything smart - let's return provided URI
      // as a dataset namespace
    }

    if (urlDatabase != null && parts.size() <= 3) {
      parts.add(0, urlDatabase);
    }

    String name = String.join(".", parts);

    return new DatasetIdentifier(name, namespace);
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
