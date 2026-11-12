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
import io.openlineage.client.transports.HttpConfig;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class GravitinoDiscoveryTest {
  @Test
  void unifiesSparkHiveAndFlinkWithoutSourceFile() throws Exception {
    try (Server server = new Server()) {
      DatasetIdentityNormalizer normalizer = normalizer(server);
      String event =
          "{\"inputs\":[{\"namespace\":\"thrift://hms-alias:9083\","
              + "\"name\":\"db.orders\"},{\"namespace\":\"spark_metalake\","
              + "\"name\":\"hive_catalog.db.orders\",\"facets\":{\"symlinks\":{"
              + "\"identifiers\":[{\"namespace\":\"gravitino://spark_metalake/hive_catalog\","
              + "\"name\":\"db.orders\",\"type\":\"TABLE\"}]}}},{"
              + "\"namespace\":\"flink_catalog\",\"name\":\"db.orders\","
              + "\"facets\":{\"dataSource\":{\"uri\":\"thrift://hms-prod:9083\"}}}],"
              + "\"outputs\":[{\"namespace\":\"hive\",\"name\":\"db.orders\","
              + "\"facets\":{\"columnLineage\":{\"fields\":{\"id\":{\"inputFields\":[{"
              + "\"namespace\":\"thrift://hms-alias:9083\",\"name\":\"db.orders\","
              + "\"field\":\"id\"}]}}}}}]}";
      JsonNode result = json(normalizer.normalizeJson(event, "thrift://hms-prod:9083"));
      for (JsonNode dataset : result.path("inputs")) {
        assertThat(dataset.path("namespace").asText()).isEqualTo("spark_metalake");
        assertThat(dataset.path("name").asText()).isEqualTo("hive_catalog.db.orders");
      }
      JsonNode output = result.path("outputs").get(0);
      assertThat(output.path("namespace").asText()).isEqualTo("spark_metalake");
      assertThat(
              output
                  .path("facets")
                  .path("columnLineage")
                  .path("fields")
                  .path("id")
                  .path("inputFields")
                  .get(0)
                  .path("name")
                  .asText())
          .isEqualTo("hive_catalog.db.orders");
    }
  }

  @Test
  void keepsExternalSourcesSeparateAndRejectsMetadataFailure() throws Exception {
    try (Server server = new Server()) {
      DatasetIdentityNormalizer normalizer = normalizer(server);
      String external =
          "{\"inputs\":[{\"namespace\":\"thrift://hms-other:9083\"," + "\"name\":\"db.orders\"}]}";
      assertThat(
              json(normalizer.normalizeJson(external, null))
                  .path("inputs")
                  .get(0)
                  .path("namespace")
                  .asText())
          .isEqualTo("hive://hms-other:9083");
      server.tableStatus.set(503);
      String managed =
          "{\"inputs\":[{\"namespace\":\"thrift://hms-prod:9083\"," + "\"name\":\"db.orders\"}]}";
      assertThatThrownBy(() -> normalizer.normalizeJson(managed, null))
          .isInstanceOf(IOException.class)
          .hasMessageContaining("HTTP 503");
    }
  }

  @Test
  void keepsDroppedTableManagedAfterMetadataDisappears() throws Exception {
    try (Server server = new Server()) {
      server.tableStatus.set(404);
      String event =
          "{\"outputs\":[{\"namespace\":\"hive://hms-prod:9083\","
              + "\"name\":\"db.orders\",\"facets\":{\"dataSource\":{"
              + "\"name\":\"spark_metalake\",\"uri\":\"spark_metalake\"},"
              + "\"lifecycleStateChange\":{\"lifecycleStateChange\":\"DROP\"}}}]}";
      JsonNode output = json(normalizer(server).normalizeJson(event, null)).path("outputs").get(0);
      assertThat(output.path("namespace").asText()).isEqualTo("spark_metalake");
      assertThat(output.path("name").asText()).isEqualTo("hive_catalog.db.orders");
      assertThat(output.path("facets").path("dataSource").path("uri").asText())
          .isEqualTo("thrift://hms-alias:9083");
    }
  }

  @Test
  void acceptsVerifiedCanonicalColumnReferenceWithoutTopLevelInput() throws Exception {
    try (Server server = new Server()) {
      String event =
          "{\"outputs\":[{\"namespace\":\"hive://hms-prod:9083\","
              + "\"name\":\"db.orders\",\"facets\":{\"columnLineage\":{\"fields\":{"
              + "\"id\":{\"inputFields\":[{\"namespace\":\"spark_metalake\","
              + "\"name\":\"hive_catalog.db.orders\",\"field\":\"id\"}]}}}}}]}";
      JsonNode output = json(normalizer(server).normalizeJson(event, null)).path("outputs").get(0);
      JsonNode inputField =
          output
              .path("facets")
              .path("columnLineage")
              .path("fields")
              .path("id")
              .path("inputFields")
              .get(0);
      assertThat(inputField.path("namespace").asText()).isEqualTo("spark_metalake");
      assertThat(inputField.path("name").asText()).isEqualTo("hive_catalog.db.orders");
    }
  }

  @Test
  void keepsUnrelatedSchemeFreeDatasetAndColumnReference() throws Exception {
    try (Server server = new Server()) {
      String event =
          "{\"inputs\":[{\"namespace\":\"bigquery\",\"name\":\"p.d.t\"}],"
              + "\"outputs\":[{\"namespace\":\"hive://hms-prod:9083\","
              + "\"name\":\"db.orders\",\"facets\":{\"columnLineage\":{\"fields\":{"
              + "\"id\":{\"inputFields\":[{\"namespace\":\"bigquery\","
              + "\"name\":\"p.d.t\",\"field\":\"id\"}]}}}}}]}";

      JsonNode mapped = json(normalizer(server).normalizeJson(event, null));
      JsonNode input = mapped.path("inputs").get(0);
      JsonNode output = mapped.path("outputs").get(0);
      JsonNode field =
          output
              .path("facets")
              .path("columnLineage")
              .path("fields")
              .path("id")
              .path("inputFields")
              .get(0);
      assertThat(input.path("namespace").asText()).isEqualTo("bigquery");
      assertThat(input.path("name").asText()).isEqualTo("p.d.t");
      assertThat(output.path("namespace").asText()).isEqualTo("spark_metalake");
      assertThat(field.path("namespace").asText()).isEqualTo("bigquery");
      assertThat(field.path("name").asText()).isEqualTo("p.d.t");
    }
  }

  private static DatasetIdentityNormalizer normalizer(Server server) {
    HttpConfig transport = new HttpConfig();
    transport.setUrl(URI.create(server.url()));
    transport.setEndpoint("/api/lineage");
    return DatasetIdentityNormalizer.fromTransport(transport);
  }

  @Test
  void normalizesRegisteredRestSourceAndItsColumnReferences() throws Exception {
    try (Server server = new Server()) {
      String event =
          "{\"job\":{\"namespace\":\"spark-jobs\"},\"inputs\":[{"
              + "\"namespace\":\"http://iceberg-rest:9001/iceberg\",\"name\":\"db.orders\"}],"
              + "\"outputs\":[{\"namespace\":\"http://iceberg-rest:9001/iceberg\","
              + "\"name\":\"db.orders\",\"facets\":{\"columnLineage\":{\"fields\":{\"id\":{"
              + "\"inputFields\":[{\"namespace\":\"http://iceberg-rest:9001/iceberg\","
              + "\"name\":\"db.orders\",\"field\":\"id\"}]}}}}}]}";
      JsonNode result = json(normalizer(server).normalizeJson(event, null));
      assertThat(result.path("job").path("namespace").asText()).isEqualTo("spark-jobs");
      assertThat(result.path("inputs").get(0).path("name").asText())
          .isEqualTo("iceberg_catalog.db.orders");
      assertThat(
              result
                  .path("outputs")
                  .get(0)
                  .path("facets")
                  .path("columnLineage")
                  .path("fields")
                  .path("id")
                  .path("inputFields")
                  .get(0)
                  .path("namespace")
                  .asText())
          .isEqualTo("spark_metalake");
    }
  }

  @Test
  void usesCatalogMetadataAddressWithoutTreatingStorageAsTableIdentity() throws Exception {
    try (Server server = new Server()) {
      String event =
          "{\"inputs\":[{\"namespace\":\"spark_metalake\","
              + "\"name\":\"lance_catalog.db.orders\",\"facets\":{\"dataSource\":{"
              + "\"uri\":\"spark_metalake\",\"name\":\"spark_metalake\"},\"symlinks\":{"
              + "\"identifiers\":[{\"namespace\":\"gravitino://spark_metalake/lance_catalog\","
              + "\"name\":\"db.orders\",\"type\":\"TABLE\"}]}}}]}";
      JsonNode dataset = json(normalizer(server).normalizeJson(event, null)).path("inputs").get(0);
      assertThat(dataset.path("namespace").asText()).isEqualTo("spark_metalake");
      assertThat(dataset.path("name").asText()).isEqualTo("lance_catalog.db.orders");
      assertThat(dataset.path("facets").path("dataSource").path("uri").asText())
          .isEqualTo(server.url() + "/api/metalakes/spark_metalake/catalogs/lance_catalog");
    }
  }

  @Test
  void preservesFilePathsWithDotsAndUnregisteredHttpObjects() throws Exception {
    try (Server server = new Server()) {
      String event =
          "{\"inputs\":[{\"namespace\":\"file\","
              + "\"name\":\"/warehouse/spark_db.db/orders\"},{\"namespace\":\"http://other\","
              + "\"name\":\"db.orders\"}]}";
      assertThat(json(normalizer(server).normalizeJson(event, null))).isEqualTo(json(event));
    }
  }

  @Test
  void removesLocalFileTableAliasesAndPreservesPhysicalColumnReferences() throws Exception {
    try (Server server = new Server()) {
      String event =
          "{\"outputs\":[{\"namespace\":\"file\","
              + "\"name\":\"/warehouse/spark_db.db/orders\",\"facets\":{\"symlinks\":{"
              + "\"identifiers\":[{\"namespace\":\"file:/warehouse\",\"name\":\"db.orders\","
              + "\"type\":\"TABLE\"}]},\"columnLineage\":{\"fields\":{\"id\":{\"inputFields\":[{"
              + "\"namespace\":\"file\",\"name\":\"/warehouse/spark_db.db/source\","
              + "\"field\":\"id\"}]}}}}}]}";
      JsonNode expected = json(event);
      ((com.fasterxml.jackson.databind.node.ObjectNode)
              expected.path("outputs").get(0).path("facets"))
          .remove("symlinks");
      assertThat(json(normalizer(server).normalizeJson(event, null))).isEqualTo(expected);
    }
  }

  @Test
  void usesMetadataSymlinkWhenStorageSymlinkIsAlsoPresent() throws Exception {
    try (Server server = new Server()) {
      String event =
          "{\"inputs\":[{\"namespace\":\"file\","
              + "\"name\":\"/warehouse/spark_db.db/orders\",\"facets\":{\"symlinks\":{"
              + "\"identifiers\":[{\"namespace\":\"file:/warehouse\",\"name\":\"other.name\","
              + "\"type\":\"TABLE\"},{\"namespace\":\"gravitino://spark_metalake/hive_catalog\","
              + "\"name\":\"db.orders\",\"type\":\"TABLE\"}]}}}]}";
      JsonNode dataset = json(normalizer(server).normalizeJson(event, null)).path("inputs").get(0);
      assertThat(dataset.path("namespace").asText()).isEqualTo("spark_metalake");
      assertThat(dataset.path("name").asText()).isEqualTo("hive_catalog.db.orders");
      assertThat(dataset.path("facets").has("symlinks")).isFalse();
    }
  }

  private static JsonNode json(String value) throws IOException {
    return OpenLineageClientUtils.newObjectMapper().readTree(value);
  }

  private static final class Server implements AutoCloseable {
    final HttpServer server;
    final AtomicInteger tableStatus = new AtomicInteger(200);

    Server() throws IOException {
      server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      server.createContext(
          "/api/metalakes",
          exchange -> {
            String path = exchange.getRequestURI().getPath();
            if (path.equals("/api/metalakes")) {
              respond(exchange, 200, "{\"code\":0,\"metalakes\":[{\"name\":\"spark_metalake\"}]}");
            } else if (path.equals("/api/metalakes/spark_metalake/catalogs")) {
              respond(
                  exchange,
                  200,
                  "{\"code\":0,\"identifiers\":[{\"name\":\"hive_catalog\"},"
                      + "{\"name\":\"mysql_catalog\"},{\"name\":\"iceberg_catalog\"},"
                      + "{\"name\":\"lance_catalog\"}]}");
            } else if (path.equals("/api/metalakes/spark_metalake/catalogs/hive_catalog")) {
              respond(
                  exchange,
                  200,
                  "{\"code\":0,\"catalog\":{\"provider\":\"hive\","
                      + "\"properties\":{\"metastore.uris\":\"thrift://hms-prod:9083,"
                      + "thrift://hms-alias:9083\"}}}");
            } else if (path.equals("/api/metalakes/spark_metalake/catalogs/mysql_catalog")) {
              respond(
                  exchange,
                  200,
                  "{\"code\":0,\"catalog\":{\"provider\":\"mysql\","
                      + "\"properties\":{\"jdbc-url\":\"jdbc:mysql://mysql-prod:3306/"
                      + "?useSSL=false\"}}}");
            } else if (path.equals("/api/metalakes/spark_metalake/catalogs/iceberg_catalog")) {
              respond(
                  exchange,
                  200,
                  "{\"code\":0,\"catalog\":{\"provider\":\"lakehouse-iceberg\","
                      + "\"properties\":{\"uri\":\"http://iceberg-rest:9001/iceberg/\","
                      + "\"jdbc-url\":\"jdbc:h2:/tmp/iceberg/metadata;MODE=MYSQL\"}}}");
            } else if (path.equals("/api/metalakes/spark_metalake/catalogs/lance_catalog")) {
              respond(
                  exchange,
                  200,
                  "{\"code\":0,\"catalog\":{\"provider\":\"lakehouse-generic\","
                      + "\"properties\":{\"location\":\"s3://shared-bucket\"}}}");
            } else if (path.endsWith("/tables/orders")) {
              respond(exchange, tableStatus.get(), "{\"code\":0}");
            } else {
              respond(exchange, 404, "{\"code\":1}");
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

    private static void respond(
        com.sun.net.httpserver.HttpExchange exchange, int status, String body) throws IOException {
      byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(status, bytes.length);
      try (OutputStream output = exchange.getResponseBody()) {
        output.write(bytes);
      }
    }
  }
}
