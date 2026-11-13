/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.integration.identity;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import io.openlineage.client.OpenLineageClientUtils;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class DatasetIdentityNormalizerTest {
  private static final String REGISTRY =
      "{\"sources\":[{\"id\":\"hms-prod\",\"aliases\":[\"thrift://hms-prod:9083\","
          + "\"thrift://hms-alias:9083\"],\"namespace\":\"hive://hms-prod:9083\","
          + "\"managedTables\":[{\"name\":\"test_db.orders\",\"metalake\":\"spark_metalake\","
          + "\"catalog\":\"hive_catalog\"}]},{\"id\":\"hms-other\","
          + "\"aliases\":[\"thrift://hms-other:9083\"],\"namespace\":\"hive://hms-other:9083\"}]}";

  @Test
  void mapsSparkLocationUsingTableSymlink() throws Exception {
    String event =
        "{\"inputs\":[{\"namespace\":\"hdfs://namenode:9000\",\"name\":\"/warehouse/orders\","
            + "\"facets\":{\"symlinks\":{\"identifiers\":[{\"namespace\":\"thrift://hms-prod:9083\","
            + "\"name\":\"test_db.orders\",\"type\":\"TABLE\"}]}}}]}";
    JsonNode mapped = json(DatasetIdentityNormalizer.fromJson(REGISTRY).normalizeJson(event, null));
    assertThat(mapped.path("inputs").get(0).path("namespace").asText()).isEqualTo("spark_metalake");
    assertThat(mapped.path("inputs").get(0).path("name").asText())
        .isEqualTo("hive_catalog.test_db.orders");
  }

  @Test
  void mapsHiveAndFlinkToSameIdentity() throws Exception {
    String hive = "{\"inputs\":[{\"namespace\":\"hive\",\"name\":\"test_db.orders\"}]}";
    String flink =
        "{\"inputs\":[{\"namespace\":\"flink_catalog\",\"name\":\"test_db.orders\","
            + "\"facets\":{\"dataSource\":{\"uri\":\"thrift://hms-alias:9083\"}}}]}";
    DatasetIdentityNormalizer normalizer = DatasetIdentityNormalizer.fromJson(REGISTRY);
    JsonNode hiveTable =
        json(normalizer.normalizeJson(hive, "thrift://hms-prod:9083")).path("inputs").get(0);
    JsonNode flinkTable = json(normalizer.normalizeJson(flink, null)).path("inputs").get(0);
    assertThat(hiveTable.path("namespace")).isEqualTo(flinkTable.path("namespace"));
    assertThat(hiveTable.path("name")).isEqualTo(flinkTable.path("name"));
  }

  @Test
  void keepsOtherMetastoreSeparate() throws Exception {
    String event =
        "{\"inputs\":[{\"namespace\":\"hive://hms-other:9083\",\"name\":\"test_db.orders\"}]}";
    JsonNode mapped = json(DatasetIdentityNormalizer.fromJson(REGISTRY).normalizeJson(event, null));
    assertThat(mapped.path("inputs").get(0).path("namespace").asText())
        .isEqualTo("hive://hms-other:9083");
  }

  @Test
  void rejectsUnregisteredMetastore() {
    String event =
        "{\"inputs\":[{\"namespace\":\"hive://unknown:9083\",\"name\":\"test_db.orders\"}]}";
    assertThatThrownBy(
            () -> DatasetIdentityNormalizer.fromJson(REGISTRY).normalizeJson(event, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("No authoritative source");
  }

  @Test
  void keepsExternalTableAndKafkaTopicDistinct() throws Exception {
    String event =
        "{\"inputs\":[{\"namespace\":\"thrift://hms-other:9083\",\"name\":\"test_db.orders\"},"
            + "{\"namespace\":\"kafka://broker:9092\",\"name\":\"sales.orders\"}]}";
    JsonNode inputs =
        json(DatasetIdentityNormalizer.fromJson(REGISTRY).normalizeJson(event, null))
            .path("inputs");
    assertThat(inputs.get(0).path("namespace").asText()).isEqualTo("hive://hms-other:9083");
    assertThat(inputs.get(0).path("name").asText()).isEqualTo("test_db.orders");
    assertThat(inputs.get(1).path("namespace").asText()).isEqualTo("kafka://broker:9092");
    assertThat(inputs.get(1).path("name").asText()).isEqualTo("sales.orders");
  }

  @Test
  void rejectsConflictingPhysicalSources() {
    String event =
        "{\"inputs\":[{\"namespace\":\"thrift://hms-prod:9083\",\"name\":\"test_db.orders\","
            + "\"facets\":{\"dataSource\":{\"uri\":\"thrift://hms-other:9083\"}}}]}";
    assertThatThrownBy(
            () -> DatasetIdentityNormalizer.fromJson(REGISTRY).normalizeJson(event, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Conflicting data sources");
  }

  @Test
  void correctsPreMappedTableUsingPhysicalSource() throws Exception {
    String event =
        "{\"inputs\":[{\"namespace\":\"spark_metalake\","
            + "\"name\":\"hive_catalog.test_db.orders\",\"facets\":{\"dataSource\":{"
            + "\"uri\":\"thrift://hms-other:9083\"}}}]}";
    JsonNode table =
        json(DatasetIdentityNormalizer.fromJson(REGISTRY).normalizeJson(event, null))
            .path("inputs")
            .get(0);
    assertThat(table.path("namespace").asText()).isEqualTo("hive://hms-other:9083");
    assertThat(table.path("name").asText()).isEqualTo("test_db.orders");
  }

  @Test
  void rejectsPreMappedTableWithoutPhysicalSource() {
    String event =
        "{\"inputs\":[{\"namespace\":\"spark_metalake\","
            + "\"name\":\"hive_catalog.test_db.orders\"}]}";
    assertThatThrownBy(
            () -> DatasetIdentityNormalizer.fromJson(REGISTRY).normalizeJson(event, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("No authoritative source");
  }

  @Test
  void rejectsUnverifiedManagedColumnReference() {
    String event =
        "{\"outputs\":[{\"namespace\":\"thrift://hms-prod:9083\","
            + "\"name\":\"test_db.summary\",\"facets\":{\"columnLineage\":{\"fields\":{"
            + "\"id\":{\"inputFields\":[{\"namespace\":\"spark_metalake\","
            + "\"name\":\"hive_catalog.test_db.orders\",\"field\":\"id\"}]}}}}}]}";
    assertThatThrownBy(
            () -> DatasetIdentityNormalizer.fromJson(REGISTRY).normalizeJson(event, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unresolved column lineage source");
  }

  @Test
  void doesNotApplyHiveHintToAnotherCatalog() throws Exception {
    String event = "{\"inputs\":[{\"namespace\":\"flink_catalog\",\"name\":\"test_db.orders\"}]}";
    JsonNode input =
        json(DatasetIdentityNormalizer.fromJson(REGISTRY)
                .normalizeJson(event, "thrift://hms-prod:9083"))
            .path("inputs")
            .get(0);
    assertThat(input.path("namespace").asText()).isEqualTo("flink_catalog");
    assertThat(input.path("name").asText()).isEqualTo("test_db.orders");
  }

  @Test
  void keepsUnrelatedSchemeFreeDataset() throws Exception {
    String event = "{\"inputs\":[{\"namespace\":\"bigquery\",\"name\":\"p.d.t\"}]}";

    assertThat(json(DatasetIdentityNormalizer.fromJson(REGISTRY).normalizeJson(event, null)))
        .isEqualTo(json(event));
  }

  @Test
  void keepsUnmatchedSchemeFreeColumnLineageInput() throws Exception {
    String event =
        "{\"outputs\":[{\"namespace\":\"thrift://hms-other:9083\","
            + "\"name\":\"test_db.summary\",\"facets\":{\"columnLineage\":{\"fields\":{"
            + "\"id\":{\"inputFields\":[{\"namespace\":\"bigquery\","
            + "\"name\":\"p.d.t\",\"field\":\"id\"}]}}}}}]}";

    JsonNode output =
        json(DatasetIdentityNormalizer.fromJson(REGISTRY).normalizeJson(event, null))
            .path("outputs")
            .get(0);
    JsonNode field =
        output
            .path("facets")
            .path("columnLineage")
            .path("fields")
            .path("id")
            .path("inputFields")
            .get(0);
    assertThat(output.path("namespace").asText()).isEqualTo("hive://hms-other:9083");
    assertThat(field.path("namespace").asText()).isEqualTo("bigquery");
    assertThat(field.path("name").asText()).isEqualTo("p.d.t");
  }

  @Test
  void resolvesManagedTablesFromGravitinoWithoutTableEntries() throws Exception {
    try (FakeGravitino gravitino = new FakeGravitino()) {
      DatasetIdentityNormalizer normalizer =
          DatasetIdentityNormalizer.fromJson(dynamicRegistry(gravitino.url()));
      String event =
          "{\"inputs\":[{\"namespace\":\"thrift://hms-prod:9083\","
              + "\"name\":\"test_db.orders\"}]}";

      JsonNode managed = json(normalizer.normalizeJson(event, null)).path("inputs").get(0);
      assertThat(managed.path("namespace").asText()).isEqualTo("spark_metalake");
      assertThat(managed.path("name").asText()).isEqualTo("hive_catalog.test_db.orders");

      gravitino.tableStatus.set(404);
      JsonNode external = json(normalizer.normalizeJson(event, null)).path("inputs").get(0);
      assertThat(external.path("namespace").asText()).isEqualTo("hive://hms-prod:9083");
      assertThat(external.path("name").asText()).isEqualTo("test_db.orders");
      assertThat(gravitino.tableRequests.get()).isEqualTo(2);
    }
  }

  @Test
  void rejectsGravitinoFailureInsteadOfGuessingExternalIdentity() throws Exception {
    try (FakeGravitino gravitino = new FakeGravitino()) {
      gravitino.tableStatus.set(503);
      String event =
          "{\"inputs\":[{\"namespace\":\"thrift://hms-prod:9083\","
              + "\"name\":\"test_db.orders\"}]}";
      assertThatThrownBy(
              () ->
                  DatasetIdentityNormalizer.fromJson(dynamicRegistry(gravitino.url()))
                      .normalizeJson(event, null))
          .isInstanceOf(IOException.class)
          .hasMessageContaining("HTTP 503");
    }
  }

  @Test
  void rejectsMissingManagedCatalogInsteadOfTreatingTableAsExternal() throws Exception {
    try (FakeGravitino gravitino = new FakeGravitino()) {
      gravitino.tableStatus.set(404);
      gravitino.catalogStatus.set(404);
      String event =
          "{\"inputs\":[{\"namespace\":\"thrift://hms-prod:9083\","
              + "\"name\":\"test_db.orders\"}]}";
      assertThatThrownBy(
              () ->
                  DatasetIdentityNormalizer.fromJson(dynamicRegistry(gravitino.url()))
                      .normalizeJson(event, null))
          .isInstanceOf(IOException.class)
          .hasMessageContaining("managed catalog is missing");
    }
  }

  private static String dynamicRegistry(String url) {
    return "{\"sources\":[{\"id\":\"hms-prod\",\"aliases\":[\"thrift://hms-prod:9083\"],"
        + "\"namespace\":\"hive://hms-prod:9083\",\"managedCatalog\":{"
        + "\"metalake\":\"spark_metalake\",\"catalog\":\"hive_catalog\","
        + "\"gravitinoUrl\":\""
        + url
        + "\"}}]}";
  }

  private static final class FakeGravitino implements AutoCloseable {
    final HttpServer server;
    final AtomicInteger tableStatus = new AtomicInteger(200);
    final AtomicInteger catalogStatus = new AtomicInteger(200);
    final AtomicInteger tableRequests = new AtomicInteger();

    FakeGravitino() throws IOException {
      server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      server.createContext(
          "/api/metalakes/spark_metalake/catalogs/hive_catalog",
          exchange -> {
            boolean table = exchange.getRequestURI().getPath().contains("/tables/");
            int status = table ? tableStatus.get() : catalogStatus.get();
            if (table) tableRequests.incrementAndGet();
            byte[] response =
                (status == 200 ? "{\"code\":0}" : "{\"code\":1}").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, response.length);
            try (java.io.OutputStream output = exchange.getResponseBody()) {
              output.write(response);
            }
          });
      server.start();
    }

    String url() {
      return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @Override
    public void close() {
      server.stop(0);
    }
  }

  private static JsonNode json(String value) throws Exception {
    return OpenLineageClientUtils.newObjectMapper().readTree(value);
  }
}
