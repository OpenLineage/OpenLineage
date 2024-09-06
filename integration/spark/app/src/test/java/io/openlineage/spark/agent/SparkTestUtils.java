/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Tag;
import org.testcontainers.containers.PostgreSQLContainer;

public class SparkTestUtils {
  static final String SPARK_3_OR_ABOVE = "^[3-9].*";
  static final String SPARK_VERSION = "spark.version";
  static final String SPARK_3_3_AND_ABOVE = "^3.[3-5].*|^[4-9].*";

  @Getter
  @EqualsAndHashCode
  static class SchemaRecord {
    private final String name;
    private final String type;

    public SchemaRecord(String name, String type) {
      this.name = name;
      this.type = type;
    }
  }

  @Getter
  public static class PostgreSQLTestContainer {
    final PostgreSQLContainer<?> postgres;

    public PostgreSQLTestContainer(PostgreSQLContainer<?> postgres) {
      this.postgres = postgres;
    }

    public void stop() {
      postgres.stop();
    }

    public String getNamespace() {
      return "postgres://" + postgres.getHost() + ":" + postgres.getMappedPort(5432).toString();
    }
  }

  public static HttpServer createHttpServer(HttpHandler handler) throws IOException {
    int randomPort = new Random().nextInt(1000) + 10000;

    HttpServer server = HttpServer.create(new InetSocketAddress(randomPort), 0);
    server.createContext("/api/v1/lineage", handler);
    server.setExecutor(null);
    server.start();

    return server;
  }

  static SparkSession createSparkSession(Integer httpServerPort, String appName) {
    String userDirProperty = System.getProperty("user.dir");
    Path userDirPath = Paths.get(userDirProperty);
    UUID testUuid = UUID.randomUUID();

    Path derbySystemHome = userDirPath.resolve("tmp").resolve("derby").resolve(testUuid.toString());
    Path sparkSqlWarehouse =
        userDirPath.resolve("tmp").resolve("spark-sql-warehouse").resolve(testUuid.toString());

    OpenLineageSparkListener.close();

    return SparkSession.builder()
        .appName(appName)
        .master("local[*]")
        .config("spark.extraListeners", OpenLineageSparkListener.class.getCanonicalName())
        .config("spark.driver.host", "localhost")
        .config("spark.driver.extraJavaOptions", "-Dderby.system.home=" + derbySystemHome)
        .config("spark.sql.warehouse.dir", sparkSqlWarehouse.toString())
        .config("spark.ui.enabled", false)
        .config("spark.openlineage.transport.type", "http")
        .config("spark.openlineage.transport.url", "http://localhost:" + httpServerPort)
        .config("spark.openlineage.facets.sparkUnknown.disabled", "true")
        .config("spark.openlineage.dataset.namespaceResolvers.prod-cluster.type", "hostList")
        .config("spark.openlineage.dataset.namespaceResolvers.prod-cluster.hosts", "[localhost]")
        .getOrCreate();
  }

  static List<SchemaRecord> mapToSchemaRecord(OpenLineage.SchemaDatasetFacet schema) {
    return schema.getFields().stream()
        .map(field -> new SchemaRecord(field.getName(), field.getType()))
        .collect(Collectors.toList());
  }

  @Tag("integration-test")
  static class OpenLineageEndpointHandler implements HttpHandler {
    List<String> eventsContainer = new ArrayList<>();

    Map<String, List<OpenLineage.RunEvent>> events = new HashMap<>();

    public OpenLineageEndpointHandler() {}

    List<OpenLineage.RunEvent> getEvents(String jobName) {
      return events.getOrDefault(jobName, Collections.emptyList());
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
      InputStreamReader isr =
          new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8);
      BufferedReader br = new BufferedReader(isr);
      String value = br.readLine();

      eventsContainer.add(value);

      OpenLineage.RunEvent runEvent = OpenLineageClientUtils.runEventFromJson(value);
      String jobName = runEvent.getJob().getName();

      Optional<String> jobNameShort = Arrays.stream(jobName.split("\\.")).findFirst();

      if (!jobNameShort.isPresent()) {
        return;
      }

      String jobNameShortString = jobNameShort.get();

      if (!events.containsKey(jobNameShortString)) {
        events.put(jobNameShortString, new ArrayList<>());
      }

      events.get(jobNameShortString).add(runEvent);

      exchange.sendResponseHeaders(200, 0);
      try (Writer writer =
          new OutputStreamWriter(exchange.getResponseBody(), StandardCharsets.UTF_8)) {
        writer.write("{}");
      }
    }
  }
}
