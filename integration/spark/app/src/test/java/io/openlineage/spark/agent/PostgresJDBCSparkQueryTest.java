/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.SparkTestUtils.createHttpServer;
import static io.openlineage.spark.agent.SparkTestUtils.createSparkSession;
import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.DatasetFacets;
import io.openlineage.client.OpenLineage.InputField;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.spark.agent.SparkTestUtils.OpenLineageEndpointHandler;
import io.openlineage.spark.agent.SparkTestUtils.PostgreSQLTestContainer;
import io.openlineage.spark.agent.SparkTestUtils.SchemaRecord;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Tag("integration-test")
@Testcontainers
@Slf4j
class PostgresJDBCSparkQueryTest {
  private static final OpenLineageEndpointHandler handler = new OpenLineageEndpointHandler();

  @Test
  void testPostgresWhenOnlyOneTableIsLoadedDirectly() throws IOException, InterruptedException {
    HttpServer server = createHttpServer(handler);
    PostgreSQLTestContainer postgres = startPostgresContainer();

    SparkSession spark =
        createSparkSession(
            server.getAddress().getPort(), "testPostgresWhenOnlyOneTableIsLoadedDirectly");

    spark
        .read()
        .format("jdbc")
        .option("url", postgres.getPostgres().getJdbcUrl())
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", "authors")
        .option("user", postgres.getPostgres().getUsername())
        .option("password", postgres.getPostgres().getPassword())
        .load()
        .show();

    postgres.stop();
    spark.stop();

    List<OpenLineage.RunEvent> runEvents =
        handler.getEvents("test_postgres_when_only_one_table_is_loaded_directly").stream()
            .filter(event -> !event.getInputs().isEmpty())
            .collect(Collectors.toList());

    assertThat(runEvents).hasSizeGreaterThanOrEqualTo(4);

    List<SchemaRecord> expectedSchema =
        Arrays.asList(
            new SchemaRecord("author_name", "string"), new SchemaRecord("author_id", "integer"));

    runEvents.forEach(
        event -> {
          List<OpenLineage.InputDataset> inputs = event.getInputs();
          assertThat(inputs).hasSize(1);
          OpenLineage.InputDataset input = inputs.get(0);
          assertThat(input.getName()).isEqualTo("openlineage.authors");

          List<SchemaRecord> schema =
              SparkTestUtils.mapToSchemaRecord(input.getFacets().getSchema());

          assertThat(schema).containsExactlyInAnyOrderElementsOf(expectedSchema);
        });
  }

  @Test
  void testPostgresWhenOnlyOneTableIsLoadedWithQuery() throws IOException, InterruptedException {
    String query = loadResourceFileAsString("/queries/with_one_table.sql");
    HttpServer server = createHttpServer(handler);
    PostgreSQLTestContainer postgres = startPostgresContainer();

    SparkSession spark =
        createSparkSession(
            server.getAddress().getPort(), "testPostgresWhenOnlyOneTableIsLoadedWithQuery");

    loadSqlQuery(spark, postgres, query);

    postgres.stop();
    spark.stop();

    List<OpenLineage.RunEvent> runEvents =
        handler.getEvents("test_postgres_when_only_one_table_is_loaded_with_query").stream()
            .filter(event -> !event.getInputs().isEmpty())
            .collect(Collectors.toList());

    assertThat(runEvents).hasSizeGreaterThanOrEqualTo(4);

    List<SchemaRecord> expectedSchema =
        Arrays.asList(
            new SchemaRecord("author_name", "string"), new SchemaRecord("author_id", "integer"));

    runEvents.forEach(
        event -> {
          List<OpenLineage.InputDataset> inputs = event.getInputs();
          assertThat(inputs).hasSize(1);
          OpenLineage.InputDataset input = inputs.get(0);
          assertThat(input.getName()).isEqualTo("openlineage.authors");

          List<SchemaRecord> schema =
              SparkTestUtils.mapToSchemaRecord(input.getFacets().getSchema());

          assertThat(schema).containsExactlyInAnyOrderElementsOf(expectedSchema);
        });
  }

  @Test
  void testPostgresQueryWhenMultipleTablesAreInTheQuery() throws IOException, InterruptedException {
    String query = loadResourceFileAsString("/queries/with_multiple_tables.sql");
    HttpServer server = createHttpServer(handler);
    PostgreSQLTestContainer postgres = startPostgresContainer();

    SparkSession spark =
        createSparkSession(
            server.getAddress().getPort(), "testPostgresQueryWhenMultipleTablesAreInTheQuery");

    loadSqlQuery(spark, postgres, query);

    postgres.stop();
    spark.stop();

    List<OpenLineage.RunEvent> runEvents =
        handler.getEvents("test_postgres_query_when_multiple_tables_are_in_the_query").stream()
            .filter(event -> !event.getInputs().isEmpty())
            .collect(Collectors.toList());

    assertThat(runEvents).hasSizeGreaterThanOrEqualTo(4);

    runEvents.stream()
        .map(OpenLineage.RunEvent::getInputs)
        .forEach(
            inputs -> {
              assertThat(inputs).hasSize(2);
              List<String> names =
                  inputs.stream().map(OpenLineage.Dataset::getName).collect(Collectors.toList());

              List<OpenLineage.SchemaDatasetFacet> schemas =
                  inputs.stream()
                      .map(OpenLineage.Dataset::getFacets)
                      .map(OpenLineage.DatasetFacets::getSchema)
                      .filter(Objects::nonNull)
                      .collect(Collectors.toList());

              assertThat(schemas.isEmpty()).isTrue();

              assertThat(names)
                  .containsExactlyInAnyOrder("openlineage.books", "openlineage.authors");
            });
  }

  @EnabledIfSystemProperty(named = "spark.version", matches = "([34].*)")
  @SneakyThrows
  @ParameterizedTest
  @CsvSource(
      value = {
        "query;select * from authors",
        "query;select author_id, author_name from authors",
        "dbtable;authors"
      },
      delimiter = ';')
  void testColumnLevelLineageWhenLoadingJDBC(String option, String value) {
    HttpServer server = createHttpServer(handler);
    PostgreSQLTestContainer postgres = startPostgresContainer();

    SparkSession spark =
        createSparkSession(server.getAddress().getPort(), "testColumnLevelLineageWhenLoadingJDBC");

    // Load authors with dbTable option set
    Dataset<Row> authors =
        spark
            .read()
            .format("jdbc")
            .option("url", postgres.getPostgres().getJdbcUrl())
            .option("driver", "org.postgresql.Driver")
            .option(option, value)
            .option("user", postgres.getPostgres().getUsername())
            .option("password", postgres.getPostgres().getPassword())
            .load();

    authors.write().format("parquet").saveAsTable("spark_authors");

    postgres.stop();
    spark.stop();

    // get column level lineage facet
    List<RunEvent> events = handler.getEvents("test_column_level_lineage_when_loading_jdbc");
    List<OpenLineage.ColumnLineageDatasetFacet> columnLineageFacets =
        events.stream()
            .filter(event -> !event.getInputs().isEmpty())
            .flatMap(e -> e.getOutputs().stream())
            .map(OutputDataset::getFacets)
            .map(DatasetFacets::getColumnLineage)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    assertThat(columnLineageFacets).hasSizeGreaterThanOrEqualTo(1);
    assertThat(columnLineageFacets.get(0).getFields().getAdditionalProperties())
        .containsOnlyKeys("author_id", "author_name");

    List<InputField> inputFields =
        columnLineageFacets
            .get(0)
            .getFields()
            .getAdditionalProperties()
            .get("author_id")
            .getInputFields();

    assertThat(inputFields).hasSize(1);
    assertThat(inputFields.get(0).getName()).isEqualTo("openlineage.authors");
    assertThat(inputFields.get(0).getField()).isEqualTo("author_id");
    assertThat(inputFields.get(0).getNamespace()).startsWith("postgres://");
  }

  private void loadSqlQuery(SparkSession spark, PostgreSQLTestContainer postgres, String query) {
    spark
        .read()
        .format("jdbc")
        .option("url", postgres.getPostgres().getJdbcUrl())
        .option("driver", "org.postgresql.Driver")
        .option("user", postgres.getPostgres().getUsername())
        .option("password", postgres.getPostgres().getPassword())
        .option("query", query)
        .load()
        .show();
  }

  private PostgreSQLTestContainer startPostgresContainer()
      throws IOException, InterruptedException {
    PostgreSQLContainer<?> postgres =
        new PostgreSQLContainer<>(DockerImageName.parse("postgres:13"))
            .withDatabaseName("openlineage")
            .withPassword("openlineage")
            .withUsername("openlineage");

    postgres.start();

    postgres.execInContainer(
        "psql",
        "-U",
        "openlineage",
        "-d",
        "openlineage",
        "-c",
        "CREATE TABLE books (author_id INT, book_id INT);");
    postgres.execInContainer(
        "psql",
        "-U",
        "openlineage",
        "-d",
        "openlineage",
        "-c",
        "CREATE TABLE authors (author_id INT, author_name TEXT);");
    postgres.execInContainer(
        "psql",
        "-U",
        "openlineage",
        "-d",
        "openlineage",
        "-c",
        "INSERT INTO books (author_id, book_id) VALUES (1, 10);");
    postgres.execInContainer(
        "psql",
        "-U",
        "openlineage",
        "-d",
        "openlineage",
        "-c",
        "INSERT INTO books (author_id, book_id) VALUES (2, 20);");
    postgres.execInContainer(
        "psql",
        "-U",
        "openlineage",
        "-d",
        "openlineage",
        "-c",
        "INSERT INTO books (author_id, book_id) VALUES (3, 30);");
    postgres.execInContainer(
        "psql",
        "-U",
        "openlineage",
        "-d",
        "openlineage",
        "-c",
        "INSERT INTO authors (author_id, author_name) VALUES (1, 'John Doe');");
    postgres.execInContainer(
        "psql",
        "-U",
        "openlineage",
        "-d",
        "openlineage",
        "-c",
        "INSERT INTO authors (author_id, author_name) VALUES (2, 'Jane Doe');");
    postgres.execInContainer(
        "psql",
        "-U",
        "openlineage",
        "-d",
        "openlineage",
        "-c",
        "INSERT INTO authors (author_id, author_name) VALUES (3, 'Alice Doe');");
    return new PostgreSQLTestContainer(postgres);
  }

  String loadResourceFileAsString(String fileName) {
    InputStream inputStream = getClass().getResourceAsStream(fileName);
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));

    List<String> list = reader.lines().collect(Collectors.toList());

    return String.join("\n", list);
  }
}
