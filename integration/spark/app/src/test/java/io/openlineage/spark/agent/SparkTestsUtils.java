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
import io.openlineage.spark.agent.lifecycle.OpenLineageSparkSQLExtensions;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import org.apache.spark.sql.SparkSession;

public class SparkTestsUtils {

  static final String SPARK_3_OR_ABOVE = "^[3-9].*";
  static final String SPARK_VERSION = "spark.version";
  static final String SPARK_3_ONLY = "^3.*";
  static final String SPARK_3_3_5_EXCLUDED = "^3.[0-4].*";

  protected static HttpServer createHttpServer(HttpHandler handler) throws IOException {
    int randomPort = new Random().nextInt(1000) + 10000;

    HttpServer server = HttpServer.create(new InetSocketAddress(randomPort), 0);
    server.createContext("/api/v1/lineage", handler);
    server.setExecutor(null);
    server.start();

    return server;
  }

  protected static SparkSession createSparkSession(Integer httpServerPort, String appName) {
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
        .config("spark.openlineage.facets.disabled", "[spark_unknown;]")
        .config("spark.openlineage.dataset.namespaceResolvers.prod-cluster.type", "hostList")
        .config("spark.openlineage.dataset.namespaceResolvers.prod-cluster.hosts", "[localhost]")
        .config("spark.sql.extensions", OpenLineageSparkSQLExtensions.class.getCanonicalName())
        .getOrCreate();
  }

  protected static SparkSession createSparkSessionWithDeltaLake(
      Integer httpServerPort, String appName) throws IOException {
    System.setProperty("derby.system.home", "/tmp/delta/derby");
    OpenLineageSparkListener.close();

    SparkSession spark =
        SparkSession.builder()
            .appName(appName)
            .master("local[*]")
            .config("spark.extraListeners", OpenLineageSparkListener.class.getCanonicalName())
            .config("spark.driver.host", "localhost")
            .config("spark.ui.enabled", false)
            .config("spark.openlineage.transport.type", "http")
            .config("spark.openlineage.transport.url", "http://localhost:" + httpServerPort)
            .config("spark.openlineage.facets.disabled", "[spark_unknown;]")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config(
                "spark.sql.extensions",
                String.format(
                    "io.delta.sql.DeltaSparkSessionExtension,%s",
                    OpenLineageSparkSQLExtensions.class.getCanonicalName()))
            .getOrCreate();

    return spark;
  }

  protected static SparkSession createSparkSessionWithIceberg(
      Integer httpServerPort, String appName) {
    OpenLineageSparkListener.close();

    return SparkSession.builder()
        .appName(appName)
        .master("local[*]")
        .config("spark.extraListeners", OpenLineageSparkListener.class.getCanonicalName())
        .config("spark.driver.host", "localhost")
        .config("spark.ui.enabled", false)
        .config("spark.openlineage.transport.type", "http")
        .config("spark.openlineage.transport.url", "http://localhost:" + httpServerPort)
        .config("spark.openlineage.facets.disabled", "[spark_unknown;]")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hadoop")
        .config("spark.sql.catalog.spark_catalog.warehouse", "/tmp/iceberg")
        .config(
            "spark.sql.extensions",
            String.format(
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,%s",
                OpenLineageSparkSQLExtensions.class.getCanonicalName()))
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .withExtensions(new OpenLineageSparkSQLExtensions())
        .getOrCreate();
  }

  static class OpenLineageEndpointHandler implements HttpHandler {

    public Map<String, List<OpenLineage.RunEvent>> events = new HashMap<>();

    public OpenLineageEndpointHandler() {}

    @Override
    public void handle(HttpExchange exchange) throws IOException {
      InputStreamReader isr =
          new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8);
      BufferedReader br = new BufferedReader(isr);
      String value = br.readLine();

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
