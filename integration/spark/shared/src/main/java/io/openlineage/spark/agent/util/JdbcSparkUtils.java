/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static io.openlineage.client.utils.JdbcUtils.sanitizeJdbcUrl;

import com.google.common.base.CharMatcher;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.JdbcUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.sql.ColumnLineage;
import io.openlineage.sql.ColumnMeta;
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
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions$;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

@Slf4j
public class JdbcSparkUtils {

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

  public static <D extends OpenLineage.Dataset> List<D> getDatasets(
      DatasetFactory<D> datasetFactory, SqlMeta meta, StructType schema, String url) {
    if (meta.columnLineage().isEmpty()) {
      DatasetIdentifier di =
          JdbcUtils.getDatasetIdentifierFromJdbcUrl(url, meta.inTables().get(0).qualifiedName());
      return Collections.singletonList(
          datasetFactory.getDataset(di.getName(), di.getNamespace(), schema));
    }
    return meta.inTables().stream()
        .map(
            dbtm -> {
              DatasetIdentifier di =
                  JdbcUtils.getDatasetIdentifierFromJdbcUrl(url, dbtm.qualifiedName());
              return datasetFactory.getDataset(
                  di.getName(), di.getNamespace(), generateJDBCSchema(dbtm, schema, meta));
            })
        .collect(Collectors.toList());
  }

  private static StructType generateJDBCSchema(
      DbTableMeta origin, StructType schema, SqlMeta sqlMeta) {
    StructType originSchema = new StructType();
    for (StructField f : schema.fields()) {
      List<ColumnMeta> fields =
          sqlMeta.columnLineage().stream()
              .filter(cl -> cl.descendant().name().equals(f.name()))
              .flatMap(
                  cl ->
                      cl.lineage().stream()
                          .filter(
                              cm -> cm.origin().isPresent() && cm.origin().get().equals(origin)))
              .collect(Collectors.toList());
      for (ColumnMeta cm : fields) {
        originSchema = originSchema.add(cm.name(), f.dataType());
      }
    }
    return originSchema;
  }

  public static Optional<SqlMeta> extractQueryFromSpark(JDBCRelation relation) {
    Optional<String> table =
        ScalaConversionUtils.asJavaOptional(
            relation.jdbcOptions().parameters().get(JDBCOptions$.MODULE$.JDBC_TABLE_NAME()));
    // in some cases table value can be "(SELECT col1, col2 FROM table_name WHERE some='filter')
    // ALIAS"
    if (table.isPresent() && !table.get().startsWith("(")) {
      DbTableMeta origin = new DbTableMeta(null, null, table.get());
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

    String tableOrQuery = relation.jdbcOptions().tableOrQuery();
    String query = tableOrQuery.substring(0, tableOrQuery.lastIndexOf(")")).replaceFirst("\\(", "");

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
