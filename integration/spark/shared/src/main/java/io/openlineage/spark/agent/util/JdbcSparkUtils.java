/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
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
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions$;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.apache.spark.sql.types.StructType;

@Slf4j
public class JdbcSparkUtils {
  // SQL Server's [brackets], ANSI "double quotes" and MySQL's `backticks`
  private static final Pattern QUOTED_NAME = Pattern.compile("[\\[\\]\"`]");

  public static <D extends OpenLineage.Dataset> List<D> getDatasets(
      DatasetFactory<D> datasetFactory, SqlMeta meta, JDBCRelation relation) {

    StructType schema = relation.schema();
    JDBCOptions jdbcOptions = relation.jdbcOptions();

    return SqlUtils.createDatasets(
        datasetFactory, meta, schema, table -> getDatasetIdentifier(jdbcOptions, table));
  }

  /**
   * Builds the dataset identifier of a table referenced by a JDBC relation. Input datasets and
   * column-level lineage inputs must both be named through this method, so that column lineage
   * input fields point to the same datasets as the inputs of the event.
   *
   * @param jdbcOptions options of the JDBC relation
   * @param table table extracted from the relation's query
   * @return dataset identifier, before namespace resolution
   */
  public static DatasetIdentifier getDatasetIdentifier(JDBCOptions jdbcOptions, DbTableMeta table) {
    // Connectors built on JDBC, such as Microsoft's Spark connector for Fabric, pass their own
    // options in the connection properties, so the client's extractors can use them
    return JdbcDatasetUtils.getDatasetIdentifier(
        jdbcOptions.url(), nameParts(table), jdbcOptions.asConnectionProperties());
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
      DbTableMeta origin = tableName(dbtable.get(), relation.jdbcOptions().url());
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

    String dialect = extractDialectFromJdbcUrl(relation.jdbcOptions().url());
    Optional<SqlMeta> sqlMeta =
        dbtable
            .map(fromClause -> parseDbtable(fromClause, dialect))
            .orElseGet(() -> parse(queryStringFromJdbcOptions(relation.jdbcOptions()), dialect));

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

  /**
   * Parses {@code dbtable} as {@code select * from <dbtable>}. That has no column lineage, so a
   * single subquery like {@code (SELECT ...) t} is parsed on its own instead: the text between its
   * first {@code (} and last {@code )}. A dbtable that only starts with a subquery is told apart by
   * the parser: that text then fails to parse, e.g. for {@code (SELECT ...) a JOIN (SELECT ...) b
   * ON ...}, or reads fewer tables than the whole dbtable, e.g. for {@code (SELECT ...) a JOIN b ON
   * ...}. Leaving quoting and comments to the parser keeps every dialect's lexical rules.
   */
  private static Optional<SqlMeta> parseDbtable(String dbtable, String dialect) {
    Optional<SqlMeta> wholeTable = parse("select * from " + dbtable, dialect);
    if (!wholeTable.isPresent() || !dbtableIsASubquery(dbtable)) {
      return wholeTable;
    }
    List<DbTableMeta> tables = wholeTable.get().inTables();
    Optional<SqlMeta> subquery =
        parse(unwrapParentheses(dbtable), dialect)
            .filter(meta -> meta.errors().isEmpty() && meta.inTables().containsAll(tables));
    return subquery.isPresent() ? subquery : wholeTable;
  }

  private static Optional<SqlMeta> parse(String query, String dialect) {
    return OpenLineageSql.parse(Collections.singletonList(query), dialect);
  }

  public static String queryStringFromJdbcOptions(JDBCOptions options) {
    return unwrapParentheses(options.tableOrQuery());
  }

  /** Returns the text between the first {@code (} and the last {@code )}. */
  private static String unwrapParentheses(String sql) {
    return sql.substring(0, sql.lastIndexOf(")")).replaceFirst("\\(", "");
  }

  private static boolean dbtableIsASubquery(String dbtable) {
    return dbtable.startsWith("(");
  }

  /**
   * The table a {@code dbtable} option names, when it's just a table name.
   *
   * <p>A quoted name, such as {@code [raw].[orders]} or {@code "raw"."orders"}, goes through the
   * SQL parser, as tables in a query do, so the quotes are removed and the table gets the same name
   * however it was read. A plain name is used as written. So is a quoted one when the parser isn't
   * available or can't read it.
   */
  private static DbTableMeta tableName(String dbtable, String url) {
    if (QUOTED_NAME.matcher(dbtable).find()) {
      Optional<DbTableMeta> parsed =
          parse("select * from " + dbtable, extractDialectFromJdbcUrl(url))
              .filter(meta -> meta.errors().isEmpty() && meta.inTables().size() == 1)
              .map(meta -> meta.inTables().get(0));
      if (parsed.isPresent()) {
        return parsed.get();
      }
    }
    return new DbTableMeta(null, null, dbtable);
  }

  /**
   * The parts of a table's name: database, schema and table, whichever are known. A table the
   * parser read is passed as its parts, so a quoted part containing a dot, such as {@code
   * [my.orders]}, stays one part. A plain name that wasn't parsed is split on dots.
   */
  private static List<String> nameParts(DbTableMeta table) {
    if (table.database() == null && table.schema() == null) {
      return new ArrayList<>(Arrays.asList(table.name().split("\\.")));
    }
    return Stream.of(table.database(), table.schema(), table.name())
        .filter(Objects::nonNull)
        .collect(Collectors.toCollection(ArrayList::new));
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
