/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.spark.api.OpenLineageContext;
import java.util.Optional;
import java.util.Properties;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class JdbcDefaultSchemaTest {

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "jdbc:postgresql://db.example:5432/app|public",
        "postgres://db.example/app|public",
        "jdbc:postgresql://db.example:5432/app?currentSchema=sales|sales",
        "jdbc:postgresql://db.example:5432/app?ssl=true&currentSchema=sales,public|sales",
        "jdbc:postgresql://db.example:5432/app?currentschema=%22Sales%22|Sales",
        "jdbc:postgresql://db.example:5432/app?currentSchema=$user,sales|sales",
        "jdbc:sqlserver://db.example:1433;databaseName=app|dbo",
        "jdbc:jtds:sqlserver://db.example:1433/app|dbo",
      })
  /** Checks the default schema resolved for PostgreSQL and SQL Server URLs. */
  void testResolve(String url, String expectedSchema) {
    assertThat(JdbcDefaultSchema.resolve(url, new Properties())).contains(expectedSchema);
  }

  @ParameterizedTest
  @CsvSource({
    "jdbc:mysql://db.example:3306/app",
    "jdbc:mariadb://db.example:3306/app",
    "jdbc:oracle:thin:@db.example:1521:app",
    "jdbc:sqlite::memory:"
  })
  /** Checks that databases without a schema level resolve no default schema. */
  void testResolveWithoutSchemaLevel(String url) {
    assertThat(JdbcDefaultSchema.resolve(url, new Properties())).isEmpty();
    assertThat(JdbcDefaultSchema.resolve(url, null)).isEmpty();
  }

  /**
   * Checks that currentSchema is read from the connection properties, ignoring the case of the key.
   */
  @Test
  void testResolvePostgresCurrentSchemaFromProperties() {
    Properties properties = new Properties();
    properties.setProperty("currentSchema", " sales , public");

    assertThat(JdbcDefaultSchema.resolve("jdbc:postgresql://db.example/app", properties))
        .contains("sales");
    assertThat(JdbcDefaultSchema.resolve("jdbc:postgresql://db.example/app", null))
        .contains("public");
  }

  /** Checks that currentSchema in the URL wins over the connection properties, as in the driver. */
  @Test
  void testResolvePostgresUrlTakesPrecedenceOverProperties() {
    Properties properties = new Properties();
    properties.setProperty("currentSchema", "from_properties");

    assertThat(
            JdbcDefaultSchema.resolve(
                "jdbc:postgresql://db.example/app?currentSchema=from_url", properties))
        .contains("from_url");
  }

  /** Checks that the flag is read from the Spark configuration and is off by default. */
  @Test
  void testIsEnabled() {
    assertThat(JdbcDefaultSchema.isEnabled(null)).isFalse();
    assertThat(JdbcDefaultSchema.isEnabled(contextWithConf(new SparkConf()))).isFalse();
    assertThat(
            JdbcDefaultSchema.isEnabled(
                contextWithConf(
                    new SparkConf().set(JdbcDefaultSchema.ENABLED_CONFIG_KEY, "false"))))
        .isFalse();
    assertThat(
            JdbcDefaultSchema.isEnabled(
                contextWithConf(new SparkConf().set(JdbcDefaultSchema.ENABLED_CONFIG_KEY, "true"))))
        .isTrue();

    OpenLineageContext withoutSparkContext = mock(OpenLineageContext.class);
    when(withoutSparkContext.getSparkContext()).thenReturn(Optional.empty());
    assertThat(JdbcDefaultSchema.isEnabled(withoutSparkContext)).isFalse();
  }

  /** Creates a context whose Spark context has the given configuration. */
  static OpenLineageContext contextWithConf(SparkConf conf) {
    SparkContext sparkContext = mock(SparkContext.class);
    when(sparkContext.conf()).thenReturn(conf);
    OpenLineageContext context = mock(OpenLineageContext.class);
    when(context.getSparkContext()).thenReturn(Optional.of(sparkContext));
    return context;
  }
}
