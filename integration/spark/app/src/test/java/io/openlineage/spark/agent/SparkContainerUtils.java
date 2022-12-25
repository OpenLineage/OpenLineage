/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MockServerContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class SparkContainerUtils {

  static MockServerContainer makeMockServerContainer(Network network) {
    return new MockServerContainer(
            DockerImageName.parse("jamesdbloom/mockserver:mockserver-5.12.0"))
        .withNetwork(network)
        .withNetworkAliases("openlineageclient");
  }

  static PostgreSQLContainer<?> makeMetastoreContainer(Network network) {
    String basePath = "src/test/resources/metastore_psql/";
    return new PostgreSQLContainer<>(DockerImageName.parse("postgres:13.4-bullseye"))
        .withNetwork(network)
        .withNetworkAliases("metastore")
        .withUsername("admin")
        .withPassword("password")
        .withDatabaseName("test")
        .withFileSystemBind(basePath + "init-db.sh", "/docker-entrypoint-initdb.d/init-db.sh")
        .withFileSystemBind(basePath + "create-databases.sql", "/create-databases.sql")
        .withFileSystemBind(basePath + "metastore-2.3.0.sql", "/metastore-2.3.0.sql")
        .withFileSystemBind(basePath + "metastore-3.1.0.sql", "/metastore-3.1.0.sql")
        .withExposedPorts(5432);
  }

  private static GenericContainer<?> makePysparkContainer(
      Network network,
      String waitMessage,
      MockServerContainer mockServerContainer,
      String... command) {
    return new GenericContainer<>(
            DockerImageName.parse("bitnami/spark:" + System.getProperty("spark.version")))
        .withNetwork(network)
        .withNetworkAliases("spark")
        .withFileSystemBind("build/gcloud", "/opt/gcloud")
        .withFileSystemBind("src/test/resources/test_data", "/test_data")
        .withFileSystemBind("src/test/resources/spark_scripts", "/opt/spark_scripts")
        .withFileSystemBind("build/libs", "/opt/libs")
        .withFileSystemBind("build/dependencies", "/opt/dependencies")
        .withLogConsumer(SparkContainerUtils::consumeOutput)
        .waitingFor(Wait.forLogMessage(waitMessage, 1))
        .withStartupTimeout(Duration.of(10, ChronoUnit.MINUTES))
        .dependsOn(mockServerContainer)
        .withReuse(true)
        .withCommand(command);
  }

  static GenericContainer<?> makeKafkaContainer(Network network) {
    return new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.0.0"))
        .withNetworkAliases("kafka")
        .withNetwork(network);
  }

  static GenericContainer<?> makePysparkContainerWithDefaultConf(
      Network network,
      MockServerContainer mockServerContainer,
      String namespace,
      List<String> urlParams,
      List<String> sparkConfigParams,
      String... command) {
    return makePysparkContainerWithDefaultConf(
        network,
        "http://openlineageclient:1080",
        ".*ShutdownHookManager: Shutdown hook called.*",
        mockServerContainer,
        namespace,
        urlParams,
        sparkConfigParams,
        command);
  }

  static GenericContainer<?> makePysparkContainerWithDefaultConf(
      Network network,
      MockServerContainer mockServerContainer,
      String namespace,
      String... command) {
    return makePysparkContainerWithDefaultConf(
        network,
        mockServerContainer,
        namespace,
        Collections.emptyList(),
        Collections.emptyList(),
        command);
  }

  static GenericContainer<?> makePysparkContainerWithDefaultConf(
      Network network,
      String openlineageUrl,
      String waitMessage,
      MockServerContainer mockServerContainer,
      String namespace,
      List<String> urlParams,
      List<String> sparkConfigParams,
      String... command) {

    //    String urlParamsString = urlParams.isEmpty() ?
    String paramString = "";
    if (!urlParams.isEmpty()) {
      paramString = "?" + String.join("&", urlParams);
    }

    List<String> sparkConf = new ArrayList<>();
    sparkConfigParams.forEach(param -> addSparkConfig(sparkConf, param));
    addSparkConfig(sparkConf, "spark.openlineage.transport.type=http");
    addSparkConfig(
        sparkConf,
        "spark.openlineage.transport.url="
            + openlineageUrl
            + "/api/v1/namespaces/"
            + namespace
            + paramString);
    addSparkConfig(sparkConf, "spark.extraListeners=" + OpenLineageSparkListener.class.getName());
    addSparkConfig(sparkConf, "spark.sql.warehouse.dir=/tmp/warehouse");
    addSparkConfig(sparkConf, "spark.sql.shuffle.partitions=1");
    addSparkConfig(sparkConf, "spark.driver.extraJavaOptions=-Dderby.system.home=/tmp/derby");
    addSparkConfig(sparkConf, "spark.sql.warehouse.dir=/tmp/warehouse");
    addSparkConfig(sparkConf, "spark.jars.ivy=/tmp/.ivy2/");
    addSparkConfig(sparkConf, "spark.openlineage.facets.disabled=");

    List<String> sparkSubmit =
        new ArrayList(Arrays.asList("./bin/spark-submit", "--master", "local"));
    sparkSubmit.addAll(sparkConf);
    sparkSubmit.addAll(
        Arrays.asList(
            "--jars",
            "/opt/libs/"
                + System.getProperty("openlineage.spark.jar")
                + ",/opt/dependencies/spark-sql-kafka-*.jar"
                + ",/opt/dependencies/spark-bigquery-with-dependencies*.jar"
                + ",/opt/dependencies/gcs-connector-hadoop*.jar"
                + ",/opt/dependencies/google-http-client-*.jar"
                + ",/opt/dependencies/google-oauth-client-*.jar"
                + ",/opt/dependencies/kafka-*.jar"
                + ",/opt/dependencies/spark-token-provider-*.jar"
                + ",/opt/dependencies/commons-pool2-*.jar"));
    sparkSubmit.addAll(Arrays.asList(command));

    return makePysparkContainer(
        network, waitMessage, mockServerContainer, sparkSubmit.toArray(new String[0]));
  }

  static void addSparkConfig(List command, String value) {
    command.add("--conf");
    command.add(value);
  }

  static void runPysparkContainerWithDefaultConf(
      Network network,
      MockServerContainer mockServerContainer,
      String namespace,
      String pysparkFile) {
    runPysparkContainerWithDefaultConf(
        network,
        mockServerContainer,
        namespace,
        Collections.emptyList(),
        Collections.emptyList(),
        pysparkFile);
  }

  static void runPysparkContainerWithDefaultConf(
      Network network,
      MockServerContainer mockServerContainer,
      String namespace,
      List<String> urlParams,
      List<String> sparkConfigParams,
      String pysparkFile) {
    makePysparkContainerWithDefaultConf(
            network,
            mockServerContainer,
            namespace,
            urlParams,
            sparkConfigParams,
            "/opt/spark_scripts/" + pysparkFile)
        .start();
  }

  @SuppressWarnings("PMD")
  private static void consumeOutput(org.testcontainers.containers.output.OutputFrame of) {
    try {
      switch (of.getType()) {
        case STDOUT:
          System.out.write(of.getBytes());
          break;
        case STDERR:
          System.err.write(of.getBytes());
          break;
        case END:
          System.out.println(of.getUtf8String());
          break;
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
}
