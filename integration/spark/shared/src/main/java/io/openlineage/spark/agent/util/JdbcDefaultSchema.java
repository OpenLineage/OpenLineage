/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.spark.api.OpenLineageContext;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Locale;
import java.util.Optional;
import java.util.Properties;
import org.apache.spark.SparkContext;

/**
 * Resolves the schema that a JDBC database uses for table names that do not specify one.
 *
 * <p>The OpenLineage naming spec names PostgreSQL and SQL Server tables {@code
 * {database}.{schema}.{table}}. When a Spark job references a table without a schema (e.g. {@code
 * dbtable=orders} or {@code select * from orders}), the database resolves it against a default
 * schema, which is applied to the dataset name when {@value #ENABLED_CONFIG_KEY} is {@code true}:
 *
 * <ul>
 *   <li>PostgreSQL: the first entry of the {@code currentSchema} connection property (taken from
 *       the JDBC URL, then from the connection properties), otherwise {@code public}.
 *   <li>SQL Server: {@code dbo}.
 *   <li>Other databases: none, dataset names are left unchanged.
 * </ul>
 */
public final class JdbcDefaultSchema {

  public static final String ENABLED_CONFIG_KEY =
      "spark.openlineage.dataset.jdbc.defaultSchema.enabled";

  static final String POSTGRES_DEFAULT_SCHEMA = "public";
  static final String SQL_SERVER_DEFAULT_SCHEMA = "dbo";

  private static final String POSTGRES_CURRENT_SCHEMA_PROPERTY = "currentSchema";
  private static final String POSTGRES_USER_SCHEMA = "$user";

  private JdbcDefaultSchema() {}

  /**
   * Checks whether the default schema should be applied to JDBC dataset names.
   *
   * @param context OpenLineage context of the current event
   * @return value of {@value #ENABLED_CONFIG_KEY}, {@code false} when not set
   */
  public static boolean isEnabled(OpenLineageContext context) {
    if (context == null) {
      return false;
    }
    Optional<SparkContext> sparkContext = context.getSparkContext();
    if (sparkContext == null) {
      return false;
    }
    return sparkContext
        .map(SparkContext::conf)
        .map(conf -> conf.get(ENABLED_CONFIG_KEY, "false"))
        .map(value -> Boolean.parseBoolean(value.trim()))
        .orElse(false);
  }

  /**
   * Resolves the default schema of the database a JDBC URL points to.
   *
   * @param jdbcUrl JDBC URL, with or without the {@code jdbc:} prefix
   * @param properties JDBC connection properties, may be {@code null}
   * @return schema of unqualified table names, empty if the database has no schema level or it is
   *     not known
   */
  public static Optional<String> resolve(String jdbcUrl, Properties properties) {
    if (jdbcUrl == null) {
      return Optional.empty();
    }
    String uri = jdbcUrl.replaceFirst("^(?i)jdbc:", "").toLowerCase(Locale.ROOT);
    if (uri.startsWith("postgres")) {
      return Optional.of(
          postgresCurrentSchema(jdbcUrl, properties).orElse(POSTGRES_DEFAULT_SCHEMA));
    }
    if (uri.startsWith("sqlserver") || uri.startsWith("jtds:sqlserver")) {
      return Optional.of(SQL_SERVER_DEFAULT_SCHEMA);
    }
    return Optional.empty();
  }

  private static Optional<String> postgresCurrentSchema(String jdbcUrl, Properties properties) {
    // As in the PostgreSQL JDBC driver, URL parameters take precedence over properties
    Optional<String> fromUrl = postgresCurrentSchemaFromUrl(jdbcUrl);
    if (fromUrl.isPresent()) {
      return fromUrl.flatMap(JdbcDefaultSchema::firstSearchPathEntry);
    }
    return postgresCurrentSchemaFromProperties(properties)
        .flatMap(JdbcDefaultSchema::firstSearchPathEntry);
  }

  private static Optional<String> postgresCurrentSchemaFromUrl(String jdbcUrl) {
    int queryStart = jdbcUrl.indexOf('?');
    if (queryStart < 0) {
      return Optional.empty();
    }
    for (String parameter : jdbcUrl.substring(queryStart + 1).split("&")) {
      int separator = parameter.indexOf('=');
      if (separator > 0
          && POSTGRES_CURRENT_SCHEMA_PROPERTY.equalsIgnoreCase(parameter.substring(0, separator))) {
        return Optional.of(urlDecode(parameter.substring(separator + 1)));
      }
    }
    return Optional.empty();
  }

  private static Optional<String> postgresCurrentSchemaFromProperties(Properties properties) {
    if (properties == null) {
      return Optional.empty();
    }
    return properties.stringPropertyNames().stream()
        .filter(POSTGRES_CURRENT_SCHEMA_PROPERTY::equalsIgnoreCase)
        .findFirst()
        .map(properties::getProperty);
  }

  /**
   * {@code currentSchema} holds a search path: a comma separated list of schemas. Unqualified
   * tables are created in, and in most cases read from, its first schema. {@code $user} can't be
   * resolved without a connection, so it is skipped.
   */
  private static Optional<String> firstSearchPathEntry(String searchPath) {
    for (String entry : searchPath.split(",")) {
      String schema = entry.trim();
      if (schema.length() > 1 && schema.startsWith("\"") && schema.endsWith("\"")) {
        schema = schema.substring(1, schema.length() - 1);
      }
      if (!schema.isEmpty() && !POSTGRES_USER_SCHEMA.equals(schema)) {
        return Optional.of(schema);
      }
    }
    return Optional.empty();
  }

  private static String urlDecode(String value) {
    try {
      return URLDecoder.decode(value, "UTF-8");
    } catch (UnsupportedEncodingException | IllegalArgumentException e) {
      return value;
    }
  }
}
