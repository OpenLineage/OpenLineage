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

  public static <D extends OpenLineage.Dataset> List<D> getDatasets(
      DatasetFactory<D> datasetFactory, SqlMeta meta, JDBCRelation relation) {
    return getDatasets(datasetFactory, meta, relation, false);
  }

  /**
   * Builds the datasets of the tables read by a JDBC relation.
   *
   * @param datasetFactory factory of the datasets
   * @param meta tables and column lineage extracted with {@link
   *     #extractQueryFromSpark(JDBCRelation, boolean)}
   * @param relation JDBC relation
   * @param applyDefaultSchema whether to apply the database's default schema, see {@link
   *     JdbcDefaultSchema}
   * @return input datasets
   */
  public static <D extends OpenLineage.Dataset> List<D> getDatasets(
      DatasetFactory<D> datasetFactory,
      SqlMeta meta,
      JDBCRelation relation,
      boolean applyDefaultSchema) {

    StructType schema = relation.schema();
    JDBCOptions jdbcOptions = relation.jdbcOptions();

    return SqlUtils.createDatasets(
        datasetFactory,
        meta,
        schema,
        table -> getDatasetIdentifier(jdbcOptions, table, applyDefaultSchema));
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
    return getDatasetIdentifier(jdbcOptions, table, false);
  }

  /**
   * Builds the dataset identifier of a table referenced by a JDBC relation. Input datasets and
   * column-level lineage inputs must both be named through this method, so that column lineage
   * input fields point to the same datasets as the inputs of the event.
   *
   * @param jdbcOptions options of the JDBC relation
   * @param table table extracted with {@link #extractQueryFromSpark(JDBCRelation, boolean)}
   * @param applyDefaultSchema whether to apply the database's default schema, see {@link
   *     JdbcDefaultSchema}
   * @return dataset identifier, before namespace resolution
   */
  public static DatasetIdentifier getDatasetIdentifier(
      JDBCOptions jdbcOptions, DbTableMeta table, boolean applyDefaultSchema) {
    return getDatasetIdentifier(
        jdbcOptions.url(), table, jdbcOptions.asConnectionProperties(), applyDefaultSchema);
  }

  /**
   * Builds the dataset identifier of a table of a JDBC database.
   *
   * <p>When {@code applyDefaultSchema} is set and the database has a default schema (see {@link
   * JdbcDefaultSchema}), the name always follows {@code {database}.{schema}.{table}}: a table
   * without a schema gets the default one, and a table that names its database keeps it instead of
   * getting the database of the URL prepended. Otherwise the table's qualified name is appended to
   * the database of the URL.
   *
   * @param jdbcUrl JDBC URL
   * @param table table, with its name split into database, schema and table name
   * @param properties JDBC connection properties
   * @param applyDefaultSchema whether to apply the database's default schema
   * @return dataset identifier, before namespace resolution
   */
  public static DatasetIdentifier getDatasetIdentifier(
      String jdbcUrl, DbTableMeta table, Properties properties, boolean applyDefaultSchema) {
    Optional<String> defaultSchema =
        applyDefaultSchema ? JdbcDefaultSchema.resolve(jdbcUrl, properties) : Optional.empty();
    if (!defaultSchema.isPresent()) {
      return JdbcDatasetUtils.getDatasetIdentifier(jdbcUrl, table.qualifiedName(), properties);
    }

    String schema = Optional.ofNullable(table.schema()).orElse(defaultSchema.get());
    List<String> parts = new ArrayList<>(Arrays.asList(schema, table.name()));
    DatasetIdentifier identifier =
        JdbcDatasetUtils.getDatasetIdentifier(jdbcUrl, parts, properties);
    if (table.database() == null) {
      return identifier;
    }
    return new DatasetIdentifier(
        String.join(".", table.database(), schema, table.name()), identifier.getNamespace());
  }

  /**
   * Splits a table name written by a user, e.g. the {@code dbtable} option of a JDBC write, into
   * database, schema and table name.
   *
   * <p>When {@code applyDefaultSchema} is set and the database has a default schema, the name is
   * parsed with the SQL parser, the same way as the tables of JDBC reads. Otherwise, the name is
   * kept as it is.
   *
   * @param jdbcUrl JDBC URL
   * @param tableName table name, optionally qualified with a schema and a database
   * @param properties JDBC connection properties
   * @param applyDefaultSchema whether the default schema is going to be applied
   * @return table meta to be named with {@link #getDatasetIdentifier(String, DbTableMeta,
   *     Properties, boolean)}
   */
  public static DbTableMeta parseTableName(
      String jdbcUrl, String tableName, Properties properties, boolean applyDefaultSchema) {
    if (!applyDefaultSchema || !JdbcDefaultSchema.resolve(jdbcUrl, properties).isPresent()) {
      return new DbTableMeta(null, null, tableName);
    }
    return OpenLineageSql.parse(
            Collections.singletonList("select * from " + tableName),
            extractDialectFromJdbcUrl(jdbcUrl))
        .filter(meta -> meta.errors().isEmpty() && meta.inTables().size() == 1)
        .map(meta -> meta.inTables().get(0))
        .orElseGet(() -> splitTableName(tableName));
  }

  @SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
  private static DbTableMeta splitTableName(String tableName) {
    String[] parts = tableName.split("\\.");
    if (parts.length == 2) {
      return new DbTableMeta(null, parts[0], parts[1]);
    } else if (parts.length == 3) {
      return new DbTableMeta(parts[0], parts[1], parts[2]);
    }
    return new DbTableMeta(null, null, tableName);
  }

  public static Optional<SqlMeta> extractQueryFromSpark(JDBCRelation relation) {
    return extractQueryFromSpark(relation, false);
  }

  /**
   * Extracts the tables read by a JDBC relation, and the column lineage between them and the
   * relation's output.
   *
   * @param relation JDBC relation
   * @param applyDefaultSchema whether the database's default schema is going to be applied to the
   *     dataset names. In that case, a plain table name in {@code dbtable} is parsed, so that it
   *     gets split into database, schema and table name the same way as the tables of a query.
   * @return tables and column lineage, empty if the query can't be parsed
   */
  public static Optional<SqlMeta> extractQueryFromSpark(
      JDBCRelation relation, boolean applyDefaultSchema) {
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
      DbTableMeta origin =
          parseTableName(
              relation.jdbcOptions().url(),
              dbtable.get(),
              relation.jdbcOptions().asConnectionProperties(),
              applyDefaultSchema);
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
            .filter(t -> !dbtableIsASubquery(t))
            .map(fromClause -> "select * from " + fromClause)
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

  private static boolean dbtableIsASubquery(String dbtable) {
    return dbtable.startsWith("(");
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
