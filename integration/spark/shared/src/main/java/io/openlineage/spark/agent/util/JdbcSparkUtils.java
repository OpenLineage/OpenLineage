/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.jdbc.JdbcDatasetUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.sql.ColumnLineage;
import io.openlineage.sql.ColumnMeta;
import io.openlineage.sql.DbTableMeta;
import io.openlineage.sql.ExtractionError;
import io.openlineage.sql.OpenLineageSql;
import io.openlineage.sql.SqlMeta;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions$;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.apache.spark.sql.types.StructType;

@Slf4j
public class JdbcSparkUtils {
  private static final char OPEN_PAREN = '(';
  private static final char CLOSE_PAREN = ')';
  private static final char OPEN_BRACKET = '[';
  private static final char CLOSE_BRACKET = ']';
  private static final String BLOCK_COMMENT_START = "/*";
  private static final String BLOCK_COMMENT_END = "*/";
  private static final String LINE_COMMENT_START = "--";
  private static final Pattern SUBQUERY_ALIAS =
      Pattern.compile("(?i)\\s*(?:(?:AS\\s+)?(?:\\w+|\"[^\"]+\"|`[^`]+`|\\[[^\\]]+\\]))?\\s*");

  public static <D extends OpenLineage.Dataset> List<D> getDatasets(
      DatasetFactory<D> datasetFactory, SqlMeta meta, JDBCRelation relation) {

    StructType schema = relation.schema();
    String jdbcUrl = relation.jdbcOptions().url();
    Properties jdbcProperties = relation.jdbcOptions().asConnectionProperties();

    return SqlUtils.createDatasets(
        datasetFactory,
        meta,
        schema,
        dbtm ->
            JdbcDatasetUtils.getDatasetIdentifier(jdbcUrl, dbtm.qualifiedName(), jdbcProperties));
  }

  public static Optional<SqlMeta> extractQueryFromSpark(JDBCRelation relation) {
    Optional<String> dbtable =
        ScalaConversionUtils.asJavaOptional(
            relation.jdbcOptions().parameters().get(JDBCOptions$.MODULE$.JDBC_TABLE_NAME()));

    // Anything that is valid in a FROM clause of a SQL query can be used in dbtable.
    // It could be a subquery in parentheses, an alias or even multiple tables with a join.
    // Examples of valid dbtable:
    // `(SELECT col1, col2 FROM table_name WHERE some='filter')`
    // `table_name AS t`
    // `table_name t JOIN another_table a ON t.id = a.t_id`
    // `table_name`
    // `schema_name.table_name`
    // https://spark.apache.org/docs/3.5.6/sql-data-sources-jdbc.html#data-source-option
    if (dbtable.isPresent() && dbtableIsJustATableName(dbtable.get())) {
      DbTableMeta origin = new DbTableMeta(null, null, dbtable.get());
      return Optional.of(
          new SqlMeta(
              Collections.singletonList(origin),
              Collections.emptyList(),
              Arrays.stream(relation.schema().fields())
                  .map(
                      field ->
                          new ColumnLineage(
                              new ColumnMeta(null, field.name()),
                              Collections.singletonList(new ColumnMeta(origin, field.name()))))
                  .collect(Collectors.toList()),
              Collections.emptyList()));
    }

    String query =
        dbtable
            .map(fromClause -> unwrapSubquery(fromClause).orElse("select * from " + fromClause))
            .orElseGet(() -> queryStringFromJdbcOptions(relation.jdbcOptions()));

    String dialect = extractDialectFromJdbcUrl(relation.jdbcOptions().url());
    Optional<SqlMeta> sqlMeta = OpenLineageSql.parse(Collections.singletonList(query), dialect);

    if (!sqlMeta.isPresent()) { // missing JNI library
      return sqlMeta;
    }
    if (!sqlMeta.get().errors().isEmpty()) { // error return nothing
      log.error(
          String.format(
              "error while parsing query: %s",
              sqlMeta.get().errors().stream()
                  .map(ExtractionError::toString)
                  .collect(Collectors.joining(","))));
      return Optional.empty();
    }
    if (sqlMeta.get().inTables().isEmpty()) {
      log.error("no tables defined in query, this should not happen");
      return Optional.empty();
    }
    return sqlMeta;
  }

  public static String queryStringFromJdbcOptions(JDBCOptions options) {
    String tableOrQuery = options.tableOrQuery();
    return tableOrQuery.substring(0, tableOrQuery.lastIndexOf(")")).replaceFirst("\\(", "");
  }

  /**
   * Returns the inner query if {@code dbtable} is a single parenthesized subquery with an optional
   * alias, e.g. {@code (SELECT ...) AS t}. Returns empty for anything else, e.g. {@code (SELECT
   * ...) a JOIN b ON ...}. Parentheses inside quotes, bracket-quoted identifiers and SQL comments
   * are ignored.
   */
  private static Optional<String> unwrapSubquery(String dbtable) {
    String trimmed = dbtable.trim();
    if (!trimmed.startsWith("(")) {
      return Optional.empty();
    }
    int depth = 0;
    char quote = 0;
    int i = 0;
    while (i < trimmed.length()) {
      char c = trimmed.charAt(i);
      int next = i + 1;
      if (quote != 0) {
        if (c == quote && next < trimmed.length() && trimmed.charAt(next) == quote) {
          next++; // doubled closing quote is an escaped quote, e.g. 'it''s' or [a]]b]
        } else if (c == quote) {
          quote = 0;
        }
      } else if (trimmed.startsWith(BLOCK_COMMENT_START, i)) {
        next = skipPast(trimmed, BLOCK_COMMENT_END, i + BLOCK_COMMENT_START.length());
      } else if (trimmed.startsWith(LINE_COMMENT_START, i)) {
        next = skipPast(trimmed, "\n", i + LINE_COMMENT_START.length());
      } else if (c == '\'' || c == '"' || c == '`') {
        quote = c;
      } else if (c == OPEN_BRACKET) {
        quote = CLOSE_BRACKET; // bracket-quoted identifier, e.g. [a)b]
      } else if (c == OPEN_PAREN) {
        depth++;
      } else if (c == CLOSE_PAREN) {
        depth--;
        if (depth == 0) {
          return SUBQUERY_ALIAS.matcher(trimmed.substring(i + 1)).matches()
              ? Optional.of(trimmed.substring(1, i))
              : Optional.empty();
        }
      }
      i = next;
    }
    return Optional.empty();
  }

  /** Returns the index just after the next {@code end} at or after {@code from}, or the length. */
  private static int skipPast(String s, String end, int from) {
    int idx = s.indexOf(end, from);
    return idx < 0 ? s.length() : idx + end.length();
  }

  private static boolean dbtableIsJustATableName(String dbtable) {
    // If there are no whitespaces between characters, we can assume this is a table name
    // in form of `table_name` or `schema_name.table_name`
    return !dbtable.matches("(?s).*\\S\\s+\\S.*");
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
