/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.sql.ColumnLineage;
import io.openlineage.sql.ColumnMeta;
import io.openlineage.sql.DbTableMeta;
import io.openlineage.sql.SqlMeta;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Singular;
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap$;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions$;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class JdbcSparkUtilsTest {
  private static final String DEFAULT_URL = "jdbc:postgresql://localhost:5432/testdb";
  private final JDBCRelation relation = mock(JDBCRelation.class);

  @ParameterizedTest
  @MethodSource("testCases")
  void testExtractQueryFromSpark(TestCase testCase) {
    givenJdbcOptions(
        JdbcOptions.builder()
            .url(DEFAULT_URL)
            .dbtable(testCase.dbtable)
            .query(testCase.query)
            .build());
    givenSchema(testCase.schema);

    Optional<SqlMeta> result = JdbcSparkUtils.extractQueryFromSpark(relation);

    assertInputTables(result, testCase.expectedInputTables);
    assertColumnLineage(result, testCase.expectedColumnLineages);
  }

  public static Collection<TestCase> testCases() {
    return Arrays.asList(
        TestCase.builder()
            .dbtable("users")
            .schema(
                new StructType().add("id", DataTypes.IntegerType).add("name", DataTypes.StringType))
            .expectedInputTable("users")
            .expectedColumnLineage(columnLineage("users.id"))
            .expectedColumnLineage(columnLineage("users.name"))
            .build(),
        TestCase.builder()
            .dbtable("(SELECT id FROM users WHERE active = 1) AS t")
            .schema(
                new StructType().add("id", DataTypes.IntegerType).add("name", DataTypes.StringType))
            .expectedInputTable("users")
            .expectedColumnLineage(columnLineage("users.id"))
            .build(),
        TestCase.builder()
            .dbtable("(SELECT id FROM public.users WHERE active = 1) AS t")
            .schema(
                new StructType().add("id", DataTypes.IntegerType).add("name", DataTypes.StringType))
            .expectedInputTable("public.users")
            .expectedColumnLineage(columnLineage("public.users.id"))
            .build(),
        TestCase.builder()
            .dbtable(
                "(SELECT u.id, u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id) AS t")
            .schema(
                new StructType().add("id", DataTypes.IntegerType).add("name", DataTypes.StringType))
            .expectedInputTable("users")
            .expectedInputTable("orders")
            .expectedColumnLineage(columnLineage("users.id"))
            .expectedColumnLineage(columnLineage("users.name"))
            .expectedColumnLineage(columnLineage("orders.total"))
            .build(),
        TestCase.builder()
            .dbtable("users AS u")
            .schema(
                new StructType().add("id", DataTypes.IntegerType).add("name", DataTypes.StringType))
            .expectedInputTable("users")
            .build(),
        TestCase.builder()
            .dbtable("users u JOIN orders o ON u.id = o.user_id")
            .schema(
                new StructType()
                    .add("id", DataTypes.IntegerType) // users.id
                    .add("name", DataTypes.StringType) // users.name
                    .add("user_id", DataTypes.IntegerType) // orders.user_id
                    .add("total", DataTypes.IntegerType)) // orders.total
            .expectedInputTable("users")
            .expectedInputTable("orders")
            .build(),
        TestCase.builder()
            .dbtable("public.users")
            .schema(
                new StructType().add("id", DataTypes.IntegerType).add("name", DataTypes.StringType))
            .expectedInputTable("public.users")
            .expectedColumnLineage(columnLineage("public.users.id"))
            .expectedColumnLineage(columnLineage("public.users.name"))
            .build(),
        TestCase.builder()
            .query("SELECT * FROM users WHERE active = 1")
            .expectedInputTable("users")
            .build(),
        TestCase.builder()
            .query("SELECT id FROM users")
            .expectedInputTable("users")
            .expectedColumnLineage(columnLineage("users.id"))
            .build(),
        TestCase.builder()
            .query("SELECT id FROM public.users")
            .expectedInputTable("public.users")
            .expectedColumnLineage(columnLineage("public.users.id"))
            .build(),
        TestCase.builder()
            .query("SELECT u.id, u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id")
            .expectedInputTable("users")
            .expectedInputTable("orders")
            .expectedColumnLineage(columnLineage("users.id"))
            .expectedColumnLineage(columnLineage("users.name"))
            .expectedColumnLineage(columnLineage("orders.total"))
            .build());
  }

  private void givenSchema(StructType schema) {
    if (schema != null) {
      when(relation.schema()).thenReturn(schema);
    }
  }

  private void givenJdbcOptions(JDBCOptions options) {
    when(relation.jdbcOptions()).thenReturn(options);
  }

  private void assertInputTables(Optional<SqlMeta> result, List<String> expectedNames) {
    assertTrue(result.isPresent());
    assertEquals(expectedNames.size(), result.get().inTables().size());

    List<String> actualNames =
        result.get().inTables().stream()
            .map(DbTableMeta::qualifiedName)
            .collect(Collectors.toList());

    expectedNames.forEach(
        expectedName ->
            assertTrue(
                actualNames.contains(expectedName),
                "Expected input table '"
                    + expectedName
                    + "' not found. Actual input tables: "
                    + actualNames));
  }

  private void assertColumnLineage(Optional<SqlMeta> result, List<ColumnLineage> expectedLineages) {
    assertTrue(result.isPresent());
    assertEquals(expectedLineages.size(), result.get().columnLineage().size());

    List<ColumnLineage> actualLineages = result.get().columnLineage();

    expectedLineages.forEach(
        expectedLineage ->
            assertTrue(
                actualLineages.contains(expectedLineage),
                "Expected lineage: "
                    + expectedLineage
                    + "\nActual lineage list: "
                    + actualLineages));
  }

  private static ColumnLineage columnLineage(String columnUri) {
    int dotIndex = columnUri.lastIndexOf('.');
    String sourceTable = columnUri.substring(0, dotIndex);
    String column = columnUri.substring(dotIndex + 1);
    return new ColumnLineage(
        new ColumnMeta(null, column),
        Collections.singletonList(
            new ColumnMeta(new DbTableMeta(null, null, sourceTable), column)));
  }

  @Builder
  public static class TestCase {
    private final String dbtable;
    private final String query;
    private final StructType schema;
    @Singular private final List<String> expectedInputTables;
    @Singular private final List<ColumnLineage> expectedColumnLineages;
  }

  private static class JdbcOptions {
    private final Map<String, String> paramsMap = new HashMap<>();

    public static JdbcOptions builder() {
      return new JdbcOptions();
    }

    public JdbcOptions url(String url) {
      paramsMap.put(JDBCOptions$.MODULE$.JDBC_URL(), url);
      return this;
    }

    public JdbcOptions dbtable(String tableName) {
      if (tableName != null) {
        paramsMap.put(JDBCOptions$.MODULE$.JDBC_TABLE_NAME(), tableName);
      }
      return this;
    }

    public JdbcOptions query(String query) {
      if (query != null) {
        paramsMap.put(JDBCOptions$.MODULE$.JDBC_QUERY_STRING(), query);
      }
      return this;
    }

    public JDBCOptions build() {
      return new JDBCOptions(
          CaseInsensitiveMap$.MODULE$.apply(ScalaConversionUtils.fromJavaMap(paramsMap)));
    }
  }
}
