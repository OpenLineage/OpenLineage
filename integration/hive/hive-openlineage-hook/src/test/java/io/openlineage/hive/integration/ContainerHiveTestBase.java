/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.integration;

import static org.mockserver.model.HttpRequest.request;

import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.hive.testutils.HiveContainerTestUtils;
import io.openlineage.hive.testutils.HiveJdbcClient;
import io.openlineage.hive.testutils.MockServerTestUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.mockserver.client.MockServerClient;
import org.mockserver.model.ClearType;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MockServerContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class ContainerHiveTestBase {

  private static final Network network = Network.newNetwork();
  private static String hiveDatabase = "test";

  @Container
  static MockServerContainer openLineageClientMockContainer =
      MockServerTestUtils.makeMockServerContainer(network);

  static MockServerClient mockServerClient;
  static GenericContainer<?> hiveContainer;
  static HiveJdbcClient client;

  @BeforeAll
  static void setup() {
    hiveContainer = HiveContainerTestUtils.makeHiveContainer(network);
    hiveContainer.start();
    mockServerClient =
        new MockServerClient(
            openLineageClientMockContainer.getHost(),
            openLineageClientMockContainer.getServerPort());
    mockServerClient
        .when(request("/api/v1/lineage"))
        .respond(org.mockserver.model.HttpResponse.response().withStatusCode(201));
    client = HiveJdbcClient.create(hiveContainer.getMappedPort(10000));
    Awaitility.await().until(openLineageClientMockContainer::isRunning);
  }

  @BeforeEach
  @SneakyThrows
  void beforeEach() {
    mockServerClient.clear(request("/api/v1/lineage"), ClearType.LOG);
    createHiveDatabase(hiveDatabase);
    useDatabase(hiveDatabase);
  }

  private void createHiveDatabase(String hiveDatabase) {
    client.execute("CREATE DATABASE IF NOT EXISTS " + hiveDatabase);
  }

  private void useDatabase(String hiveDatabase) {
    client.execute("USE " + hiveDatabase);
  }

  private void dropHiveDatabase(String hiveDatabase) {
    client.execute("DROP DATABASE IF EXISTS " + hiveDatabase + " CASCADE");
  }

  @AfterEach
  void afterEach() {
    dropHiveDatabase(hiveDatabase);
  }

  @AfterAll
  static void tearDown() {
    openLineageClientMockContainer.stop();
    client.close();
    hiveContainer.close();
    network.close();
  }

  public void createHiveTable(
      String tableName,
      String hiveDDL,
      boolean isExternal,
      String properties,
      String comment,
      String partitionColumn) {
    client.execute(
        String.join(
            "\n",
            "CREATE " + (isExternal ? "EXTERNAL" : "") + " TABLE " + tableName + " (",
            hiveDDL,
            ")",
            comment != null ? "COMMENT \"" + comment + "\"" : "",
            partitionColumn != null ? "PARTITIONED BY (" + partitionColumn + ")" : "",
            properties != null ? "TBLPROPERTIES (" + properties + ")" : ""));
  }

  public void createManagedHiveTable(String tableName, String hiveDDL) {
    createHiveTable(tableName, hiveDDL, false, null, null, null);
  }

  public void createPartitionedHiveTable(String tableName, String hiveDDL, String partitionColumn) {
    createHiveTable(tableName, hiveDDL, true, null, null, partitionColumn);
  }

  public void runHiveQuery(String queryTemplate) {
    // Remove the ';' character at the end if there is one
    String cleanedTemplate = StringUtils.stripEnd(queryTemplate, null);
    if (StringUtils.endsWith(queryTemplate, ";")) {
      cleanedTemplate = StringUtils.chop(cleanedTemplate);
    }
    client.execute(renderQueryTemplate(cleanedTemplate));
  }

  public String renderQueryTemplate(String queryTemplate) {
    Map<String, Object> params = new HashMap<>();
    return StrSubstitutor.replace(queryTemplate, params, "${", "}");
  }

  void verifyEvents(String... events) {
    MockServerTestUtils.verifyEvents(mockServerClient, events);
  }

  List<RunEvent> getEventsEmitted() {
    return MockServerTestUtils.getEventsEmitted(mockServerClient);
  }
}
