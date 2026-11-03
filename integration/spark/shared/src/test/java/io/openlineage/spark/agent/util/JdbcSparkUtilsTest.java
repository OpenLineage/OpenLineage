/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.DatasetIdentifier;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class JdbcSparkUtilsTest {
  private static final String DEFAULT_URL = "jdbc:postgresql://localhost:5432/testdb";
  private static final String SQLSERVER_URL = "jdbc:sqlserver://localhost:1433;databaseName=testdb";
  private static final String MYSQL_URL = "jdbc:mysql://localhost:3306/testdb";
  private static final String FABRIC_HOST =
      "6obaxtbopqfublwdicr5oicqlm-ac4ezc2eudhehd3wgnlpocnuna.datawarehouse.fabric.microsoft.com";
  // the URL the Fabric connector builds when it looks up the address itself, with ".z8b"
  private static final String FABRIC_URL =
      "jdbc:sqlserver://;servername="
          + FABRIC_HOST.replace(".datawarehouse", ".z8b.datawarehouse")
          + ";encrypt=true;databaseName=wh_primary";
  private static final String FABRIC_NAMESPACE = "fabric-warehouse://" + FABRIC_HOST;
  private static final String FABRIC_WAREHOUSE_ID = "0fd9a0c2-ee93-46af-aaea-a45c8649ed9d";
  private final JDBCRelation relation = mock(JDBCRelation.class);

  @ParameterizedTest
  @MethodSource("testCases")
  void testExtractQueryFromSpark(TestCase testCase) {
    givenJdbcOptions(
        JdbcOptions.builder()
            .url(testCase.url)
            // explicit driver, so URLs used only to select the SQL dialect need no driver jar
            .driver("org.postgresql.Driver")
            .dbtable(testCase.dbtable)
            .query(testCase.query)
            .build());
    givenSchema(testCase.schema);

    Optional<SqlMeta> result = JdbcSparkUtils.extractQueryFromSpark(relation);

    assertInputTables(result, testCase.expectedInputTables);
    assertColumnLineage(result, testCase.expectedColumnLineages);
  }

  @Test
  void testGetDatasetIdentifierUsesQualifiedTableName() {
    JDBCOptions options = JdbcOptions.builder().url(DEFAULT_URL).dbtable("public.users").build();

    DatasetIdentifier identifier =
        JdbcSparkUtils.getDatasetIdentifier(options, new DbTableMeta(null, "public", "users"));

    assertEquals("testdb.public.users", identifier.getName());
    assertEquals("postgres://localhost:5432", identifier.getNamespace());
  }

  @Test
  void testFabricWarehouseConnectorQuery() {
    // the query the Fabric connector sends for spark.read.synapsesql("wh_primary.raw.orders")
    givenJdbcOptions(
        JdbcOptions.builder()
            .url(FABRIC_URL)
            .driver("org.postgresql.Driver")
            .dbtable("(SELECT [a],[b] FROM [wh_primary].[raw].orders) as Qry0123abcdAlias")
            .build());
    givenSchema(new StructType().add("a", DataTypes.IntegerType).add("b", DataTypes.StringType));

    Optional<SqlMeta> result = JdbcSparkUtils.extractQueryFromSpark(relation);

    assertInputTables(result, Collections.singletonList("wh_primary.raw.orders"));
    assertColumnLineage(
        result,
        Arrays.asList(
            columnLineage("wh_primary.raw.orders.a"), columnLineage("wh_primary.raw.orders.b")));
    DatasetIdentifier identifier =
        JdbcSparkUtils.getDatasetIdentifier(relation.jdbcOptions(), result.get().inTables().get(0));
    assertEquals(FABRIC_NAMESPACE, identifier.getNamespace());
    assertEquals("wh_primary.raw.orders", identifier.getName());
    assertTrue(identifier.getSymlinks().isEmpty());
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      nullValues = "null",
      value = {
        // the WorkspaceId option wins over the workspace in the address
        "WorkspaceId|bcc59eac-e270-40a8-9deb-f8f5994596a5|DatawarehouseId|bcc59eac-e270-40a8-9deb-f8f5994596a5",
        // without WorkspaceId, the workspace ID is read from the address
        "null|null|DatawarehouseId|8b4cb800-a044-43ce-8f76-3356f709b468",
        // option names can be in any case
        "workspaceid|bcc59eac-e270-40a8-9deb-f8f5994596a5|DATAWAREHOUSEID|bcc59eac-e270-40a8-9deb-f8f5994596a5",
      })
  void testFabricWarehouseOneLakeLocation(
      String workspaceKey, String workspaceId, String warehouseKey, String expectedWorkspace) {
    JdbcOptions builder =
        JdbcOptions.builder()
            .url(FABRIC_URL)
            .driver("org.postgresql.Driver")
            .dbtable("raw.orders")
            .option(warehouseKey, FABRIC_WAREHOUSE_ID);
    if (workspaceKey != null) {
      builder.option(workspaceKey, workspaceId);
    }

    DatasetIdentifier identifier =
        JdbcSparkUtils.getDatasetIdentifier(
            builder.build(), new DbTableMeta(null, "raw", "orders"));

    assertEquals(FABRIC_NAMESPACE, identifier.getNamespace());
    assertEquals("wh_primary.raw.orders", identifier.getName());
    assertEquals(
        Collections.singletonList(
            new DatasetIdentifier.Symlink(
                FABRIC_WAREHOUSE_ID + "/Tables/raw/orders",
                "abfss://" + expectedWorkspace + "@onelake.dfs.fabric.microsoft.com",
                DatasetIdentifier.SymlinkType.LOCATION)),
        identifier.getSymlinks());
  }

  @Test
  void testOneLakeLocationOnlyForDatabaseNameWarehouse() {
    // a query sent with the connector's database_name option; the URL names no database
    JdbcOptions options =
        JdbcOptions.builder()
            .url(
                "jdbc:sqlserver://;servername="
                    + FABRIC_HOST.replace(".datawarehouse", ".z8b.datawarehouse")
                    + ";encrypt=true")
            .option("database_name", "wh_primary");

    assertLocationOnlyForPrimaryWarehouse(options);
  }

  @Test
  void testOneLakeLocationOnlyForUrlDatabaseWarehouse() {
    // without the database_name option, the database in the URL is used
    assertLocationOnlyForPrimaryWarehouse(JdbcOptions.builder().url(FABRIC_URL));
  }

  // A query in wh_primary that also reads a table from wh_secondary, in the same workspace. The
  // DatawarehouseId option is wh_primary's ID, so only wh_primary's table gets a OneLake location.
  private void assertLocationOnlyForPrimaryWarehouse(JdbcOptions options) {
    givenJdbcOptions(
        options
            .driver("org.postgresql.Driver")
            .dbtable(
                "(SELECT TOP 5 o.order_id, x.v FROM wh_primary.raw.orders o"
                    + " CROSS JOIN wh_secondary.probe_pr.l9_other x) as Qry0123abcdAlias")
            .option("WorkspaceId", "8b4cb800-a044-43ce-8f76-3356f709b468")
            .option("DatawarehouseId", FABRIC_WAREHOUSE_ID)
            .build());
    givenSchema(
        new StructType().add("order_id", DataTypes.IntegerType).add("v", DataTypes.StringType));

    Map<String, List<DatasetIdentifier.Symlink>> symlinks =
        JdbcSparkUtils.extractQueryFromSpark(relation).get().inTables().stream()
            .map(table -> JdbcSparkUtils.getDatasetIdentifier(relation.jdbcOptions(), table))
            .collect(Collectors.toMap(DatasetIdentifier::getName, DatasetIdentifier::getSymlinks));

    assertEquals(2, symlinks.size());
    assertEquals(
        Collections.singletonList(
            new DatasetIdentifier.Symlink(
                FABRIC_WAREHOUSE_ID + "/Tables/raw/orders",
                "abfss://8b4cb800-a044-43ce-8f76-3356f709b468@onelake.dfs.fabric.microsoft.com",
                DatasetIdentifier.SymlinkType.LOCATION)),
        symlinks.get("wh_primary.raw.orders"));
    assertTrue(symlinks.get("wh_secondary.probe_pr.l9_other").isEmpty());
  }

  @Test
  void testDatawarehouseIdIgnoredForNonFabricUrls() {
    JDBCOptions options =
        JdbcOptions.builder()
            .url(SQLSERVER_URL)
            .driver("org.postgresql.Driver")
            .dbtable("dbo.orders")
            .option("DatawarehouseId", FABRIC_WAREHOUSE_ID)
            .build();

    DatasetIdentifier identifier =
        JdbcSparkUtils.getDatasetIdentifier(options, new DbTableMeta(null, "dbo", "orders"));

    assertEquals("sqlserver://localhost:1433", identifier.getNamespace());
    assertTrue(identifier.getSymlinks().isEmpty());
  }

  private static Collection<TestCase> testCases() {
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
            .dbtable(
                "(SELECT id\n"
                    + "FROM users\n"
                    + "WHERE active = 1)\n"
                    + "AS t") // multiline dbtable
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
            .dbtable("(SELECT id FROM users WHERE name <> ')') t")
            .schema(new StructType().add("id", DataTypes.IntegerType))
            .expectedInputTable("users")
            .expectedColumnLineage(columnLineage("users.id"))
            .build(),
        TestCase.builder()
            .dbtable("(SELECT id /* ) */ FROM users) t")
            .schema(new StructType().add("id", DataTypes.IntegerType))
            .expectedInputTable("users")
            .expectedColumnLineage(columnLineage("users.id"))
            .build(),
        TestCase.builder()
            .dbtable("(SELECT id -- )\nFROM users) t")
            .schema(new StructType().add("id", DataTypes.IntegerType))
            .expectedInputTable("users")
            .expectedColumnLineage(columnLineage("users.id"))
            .build(),
        TestCase.builder()
            .url(SQLSERVER_URL)
            .dbtable("(SELECT [id] FROM users WHERE [x)y] = 1) t")
            .schema(new StructType().add("id", DataTypes.IntegerType))
            .expectedInputTable("users")
            .expectedColumnLineage(columnLineage("users.id"))
            .build(),
        TestCase.builder()
            .url(SQLSERVER_URL)
            .dbtable("(SELECT [id] FROM users WHERE [x]])y] = 1) t") // escaped bracket
            .schema(new StructType().add("id", DataTypes.IntegerType))
            .expectedInputTable("users")
            .expectedColumnLineage(columnLineage("users.id"))
            .build(),
        TestCase.builder()
            .dbtable("(SELECT id FROM users) AS \"u\"\"x\"") // escaped quote in the alias
            .schema(new StructType().add("id", DataTypes.IntegerType))
            .expectedInputTable("users")
            .expectedColumnLineage(columnLineage("users.id"))
            .build(),
        TestCase.builder()
            .url(SQLSERVER_URL)
            .dbtable("(SELECT [id] FROM users) AS [u]]x]") // escaped bracket in the alias
            .schema(new StructType().add("id", DataTypes.IntegerType))
            .expectedInputTable("users")
            .expectedColumnLineage(columnLineage("users.id"))
            .build(),
        TestCase.builder()
            .dbtable("(SELECT id FROM users) /* a */ AS /* b */ t /* c */ -- d")
            .schema(new StructType().add("id", DataTypes.IntegerType))
            .expectedInputTable("users")
            .expectedColumnLineage(columnLineage("users.id"))
            .build(),
        TestCase.builder()
            .dbtable("(SELECT id AS user_id FROM users) t$1") // $ in an unquoted alias
            .schema(new StructType().add("user_id", DataTypes.IntegerType))
            .expectedInputTable("users")
            .expectedColumnLineage(columnLineage("user_id", "users.id"))
            .build(),
        TestCase.builder()
            .dbtable("(SELECT id AS user_id FROM users WHERE name <> $$)$$) t")
            .schema(new StructType().add("user_id", DataTypes.IntegerType))
            .expectedInputTable("users")
            .expectedColumnLineage(columnLineage("user_id", "users.id"))
            .build(),
        TestCase.builder()
            .dbtable("(SELECT id AS user_id FROM users WHERE name <> $tag$)$$$tag$) t")
            .schema(new StructType().add("user_id", DataTypes.IntegerType))
            .expectedInputTable("users")
            .expectedColumnLineage(columnLineage("user_id", "users.id"))
            .build(),
        TestCase.builder()
            .dbtable("(SELECT id AS user_id /* outer /* inner */ ) */ FROM users) t")
            .schema(new StructType().add("user_id", DataTypes.IntegerType))
            .expectedInputTable("users")
            .expectedColumnLineage(columnLineage("user_id", "users.id"))
            .build(),
        TestCase.builder()
            .dbtable(
                "(SELECT id AS user_id FROM users WHERE id IN ($1, $2)) t") // not dollar quotes
            .schema(new StructType().add("user_id", DataTypes.IntegerType))
            .expectedInputTable("users")
            .expectedColumnLineage(columnLineage("user_id", "users.id"))
            .build(),
        TestCase.builder()
            .url(MYSQL_URL)
            .dbtable("(SELECT id AS user_id FROM users WHERE name <> 'a\\')') t") // \' escape
            .schema(new StructType().add("user_id", DataTypes.IntegerType))
            .expectedInputTable("users")
            .expectedColumnLineage(columnLineage("user_id", "users.id"))
            .build(),
        TestCase.builder()
            .dbtable("(SELECT id AS user_id FROM users WHERE name <> E'a\\')') t")
            .schema(new StructType().add("user_id", DataTypes.IntegerType))
            .expectedInputTable("users")
            .expectedColumnLineage(columnLineage("user_id", "users.id"))
            .build(),
        TestCase.builder()
            .dbtable("(SELECT id FROM users) u CROSS JOIN (SELECT user_id FROM orders) o")
            .schema(new StructType().add("id", DataTypes.IntegerType))
            .expectedInputTable("users")
            .expectedInputTable("orders")
            .build(),
        TestCase.builder()
            .dbtable("(SELECT id FROM users) t$1 JOIN orders o ON t$1.id = o.user_id")
            .schema(new StructType().add("id", DataTypes.IntegerType))
            .expectedInputTable("users")
            .expectedInputTable("orders")
            .build(),
        TestCase.builder()
            .dbtable(
                "(SELECT id FROM users WHERE name <> $$)$$) u JOIN orders o ON u.id = o.user_id")
            .schema(new StructType().add("id", DataTypes.IntegerType))
            .expectedInputTable("users")
            .expectedInputTable("orders")
            .build(),
        TestCase.builder()
            .dbtable("(SELECT id FROM users) u JOIN orders o ON u.id = o.user_id")
            .schema(new StructType().add("id", DataTypes.IntegerType))
            .expectedInputTable("users")
            .expectedInputTable("orders")
            .build(),
        TestCase.builder()
            .dbtable(
                "(SELECT id FROM users) u JOIN (SELECT user_id FROM orders) o ON u.id = o.user_id")
            .schema(new StructType().add("id", DataTypes.IntegerType))
            .expectedInputTable("users")
            .expectedInputTable("orders")
            .build(),
        TestCase.builder()
            .dbtable("(SELECT id FROM users WHERE name <> ')') u JOIN orders o ON u.id = o.user_id")
            .schema(new StructType().add("id", DataTypes.IntegerType))
            .expectedInputTable("users")
            .expectedInputTable("orders")
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
    return columnLineage(columnUri.substring(columnUri.lastIndexOf('.') + 1), columnUri);
  }

  private static ColumnLineage columnLineage(String outputColumn, String sourceColumnUri) {
    int dotIndex = sourceColumnUri.lastIndexOf('.');
    String sourceTable = sourceColumnUri.substring(0, dotIndex);
    String sourceColumn = sourceColumnUri.substring(dotIndex + 1);
    return new ColumnLineage(
        new ColumnMeta(null, outputColumn),
        Collections.singletonList(
            new ColumnMeta(new DbTableMeta(null, null, sourceTable), sourceColumn)));
  }

  @Builder
  public static class TestCase {
    @Builder.Default private final String url = DEFAULT_URL;
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

    public JdbcOptions option(String key, String value) {
      paramsMap.put(key, value);
      return this;
    }

    public JdbcOptions driver(String driverClass) {
      paramsMap.put(JDBCOptions$.MODULE$.JDBC_DRIVER_CLASS(), driverClass);
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
