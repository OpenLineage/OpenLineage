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
import java.util.ArrayList;
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
  private static final char DOLLAR = '$';
  private static final String BLOCK_COMMENT_START = "/*";
  private static final String BLOCK_COMMENT_END = "*/";
  private static final String LINE_COMMENT_START = "--";
  private static final String AS = "AS";
  // opening delimiter of a PostgreSQL dollar-quoted string, e.g. $$ or $tag$
  private static final Pattern DOLLAR_QUOTE_DELIMITER =
      Pattern.compile("\\$(?:[\\p{L}_][\\p{L}\\p{N}_]*)?\\$");

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
   * ...) a JOIN b ON ...}. Parentheses inside quoted strings and identifiers, dollar-quoted strings
   * and comments are ignored.
   */
  private static Optional<String> unwrapSubquery(String dbtable) {
    String sql = dbtable.trim();
    if (!sql.startsWith("(")) {
      return Optional.empty();
    }
    int depth = 0;
    for (int start = 0; start < sql.length(); start = tokenEnd(sql, start)) {
      char c = sql.charAt(start);
      if (c == OPEN_PAREN) {
        depth++;
      } else if (c == CLOSE_PAREN) {
        depth--;
        if (depth == 0) {
          return isOptionalAlias(tokens(sql, start + 1))
              ? Optional.of(sql.substring(1, start))
              : Optional.empty();
        }
      }
    }
    return Optional.empty();
  }

  /**
   * Returns whether {@code tokens} are just an optional alias: nothing, {@code t} or {@code AS t}.
   */
  private static boolean isOptionalAlias(List<String> tokens) {
    switch (tokens.size()) {
      case 0:
        return true;
      case 1:
        return isIdentifier(tokens.get(0));
      case 2:
        return AS.equalsIgnoreCase(tokens.get(0)) && isIdentifier(tokens.get(1));
      default:
        return false;
    }
  }

  /** Splits {@code sql} from {@code from} on into tokens, leaving out whitespace and comments. */
  private static List<String> tokens(String sql, int from) {
    List<String> tokens = new ArrayList<>();
    int start = from;
    while (start < sql.length()) {
      int end = tokenEnd(sql, start);
      String token = sql.substring(start, end);
      if (!Character.isWhitespace(token.charAt(0))
          && !token.startsWith(LINE_COMMENT_START)
          && !token.startsWith(BLOCK_COMMENT_START)) {
        tokens.add(token);
      }
      start = end;
    }
    return tokens;
  }

  /**
   * Returns the end of the SQL token starting at {@code start}: a comment, a quoted string or
   * identifier, a dollar-quoted string, a word, or else a single character. An unterminated token
   * runs to the end of {@code sql}.
   */
  private static int tokenEnd(String sql, int start) {
    char c = sql.charAt(start);
    if (sql.startsWith(LINE_COMMENT_START, start)) {
      int newline = sql.indexOf('\n', start);
      return newline < 0 ? sql.length() : newline + 1;
    } else if (sql.startsWith(BLOCK_COMMENT_START, start)) {
      return blockCommentEnd(sql, start);
    } else if (c == '\'' || c == '"' || c == '`') {
      return quotedEnd(sql, start, c);
    } else if (c == OPEN_BRACKET) {
      return quotedEnd(sql, start, CLOSE_BRACKET); // bracket-quoted identifier, e.g. [a)b]
    } else if (c == DOLLAR) {
      return dollarQuotedEnd(sql, start);
    } else if (isWordPart(c)) {
      int end = start + 1;
      while (end < sql.length() && isWordPart(sql.charAt(end))) {
        end++;
      }
      return end;
    }
    return start + 1;
  }

  /** {@code $} can be part of a word, e.g. the alias {@code t$1}, but doesn't start one. */
  private static boolean isWordPart(char c) {
    return Character.isLetterOrDigit(c) || c == '_' || c == DOLLAR;
  }

  private static boolean isIdentifier(String token) {
    char c = token.charAt(0);
    return Character.isLetter(c) || c == '_' || c == '"' || c == '`' || c == OPEN_BRACKET;
  }

  /** Returns the end of the quoted token at {@code start}; a doubled closing quote is an escape. */
  private static int quotedEnd(String sql, int start, char closingQuote) {
    int close = sql.indexOf(closingQuote, start + 1);
    while (close >= 0 && close + 1 < sql.length() && sql.charAt(close + 1) == closingQuote) {
      close = sql.indexOf(closingQuote, close + 2); // e.g. 'it''s' or [a]]b]
    }
    return close < 0 ? sql.length() : close + 1;
  }

  /**
   * Returns the end of the PostgreSQL dollar-quoted string at {@code start}, e.g. {@code $$a)b$$}
   * or {@code $tag$a)b$tag$}, or {@code start + 1} if the {@code $} doesn't open one, e.g. {@code
   * $1}.
   */
  private static int dollarQuotedEnd(String sql, int start) {
    Matcher delimiter = DOLLAR_QUOTE_DELIMITER.matcher(sql).region(start, sql.length());
    if (!delimiter.lookingAt()) {
      return start + 1;
    }
    int close = sql.indexOf(delimiter.group(), delimiter.end());
    return close < 0 ? sql.length() : close + delimiter.group().length();
  }

  /**
   * Returns the end of the block comment at {@code start}; block comments nest, as in PostgreSQL.
   */
  private static int blockCommentEnd(String sql, int start) {
    int depth = 0;
    int i = start;
    while (i < sql.length()) {
      if (sql.startsWith(BLOCK_COMMENT_START, i)) {
        depth++;
        i += BLOCK_COMMENT_START.length();
      } else if (sql.startsWith(BLOCK_COMMENT_END, i)) {
        depth--;
        i += BLOCK_COMMENT_END.length();
        if (depth == 0) {
          return i;
        }
      } else {
        i++;
      }
    }
    return sql.length();
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
