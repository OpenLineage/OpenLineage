/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.MockServerUtils.getEventsEmitted;
import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage.FabricSparkRunFacet;
import io.openlineage.client.OpenLineage.RunEvent;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockserver.integration.ClientAndServer;

/**
 * A Spark session with the settings Fabric sets when it starts a notebook's session, and no {@code
 * spark.openlineage.capturedProperties}: every event carries the {@code fabric_spark} run facet,
 * the application's START and COMPLETE included.
 */
@Tag("integration-test")
class SparkFabricSparkRunFacetIntegrationTest {
  @SuppressWarnings("PMD")
  private static final String LOCAL_IP = "127.0.0.1";

  private static final int MOCK_SERVER_PORT = 1090;
  private static final String WORKSPACE = "8b4cb800-a044-43ce-8f76-3356f709b468";
  private static final String NOTEBOOK = "af3e0877-abde-4457-8e7d-2f6026993a02";
  private static final String LIVY = "fa3ea1fc-c6bf-4b1d-a926-9be1bbf4851c";
  private static final String JOB_INSTANCE = "a266f2f5-8c88-4788-8915-cb4d4456b790";

  private static ClientAndServer mockServer;

  @BeforeAll
  static void beforeAll() {
    mockServer = MockServerUtils.createAndConfigureMockServer(MOCK_SERVER_PORT);
  }

  @AfterAll
  static void afterAll() {
    MockServerUtils.stopMockServer(mockServer);
  }

  @Test
  void everyEventOfAFabricSessionCarriesTheFacet() {
    SparkSession spark =
        SparkSession.builder()
            .master("local[1]")
            .appName("fabric_spark_facet")
            .config("spark.driver.host", LOCAL_IP)
            .config("spark.driver.bindAddress", LOCAL_IP)
            .config("spark.ui.enabled", false)
            .config("spark.sql.shuffle.partitions", 1)
            .config("spark.openlineage.transport.type", "http")
            .config(
                "spark.openlineage.transport.url",
                "http://localhost:" + mockServer.getPort() + "/api/v1/lineage")
            .config("spark.openlineage.namespace", "fabric-namespace")
            .config("spark.extraListeners", OpenLineageSparkListener.class.getName())
            .config(
                "spark.synapse.jobidentifier",
                "22dynamicdatacloudworkspace.datacloudpool1-4353128699960801747." + LIVY)
            .config("spark.hadoop.trident.artifact.workspace.id", WORKSPACE)
            .config("spark.hadoop.trident.artifact.id", NOTEBOOK)
            .config("spark.hadoop.trident.artifact.type", "SynapseNotebook")
            .config("spark.hadoop.trident.activity.id", JOB_INSTANCE)
            .getOrCreate();
    spark.range(10).write().mode("overwrite").parquet("/tmp/fabric_spark_facet_it/out");
    spark.stop();

    List<RunEvent> events = getEventsEmitted(mockServer);
    assertThat(events)
        .anySatisfy(e -> assertApplicationEvent(e, RunEvent.EventType.START))
        .anySatisfy(e -> assertApplicationEvent(e, RunEvent.EventType.COMPLETE))
        .anySatisfy(e -> assertThat(jobType(e)).isNotEqualTo("APPLICATION"));
    assertThat(events).allSatisfy(SparkFabricSparkRunFacetIntegrationTest::assertFabricSparkFacet);
  }

  private static void assertFabricSparkFacet(RunEvent event) {
    FabricSparkRunFacet facet = event.getRun().getFacets().getFabric_spark();
    assertThat(facet).isNotNull();
    assertThat(facet.getWorkspaceId()).isEqualTo(WORKSPACE);
    assertThat(facet.getItemId()).isEqualTo(NOTEBOOK);
    assertThat(facet.getItemType()).isEqualTo("SynapseNotebook");
    assertThat(facet.getLivyId()).isEqualTo(LIVY);
    assertThat(facet.getJobInstanceId()).isEqualTo(JOB_INSTANCE);
  }

  private static void assertApplicationEvent(RunEvent event, RunEvent.EventType type) {
    assertThat(event.getEventType()).isEqualTo(type);
    assertThat(jobType(event)).isEqualTo("APPLICATION");
  }

  private static String jobType(RunEvent event) {
    return event.getJob().getFacets().getJobType() == null
        ? null
        : event.getJob().getFacets().getJobType().getJobType();
  }
}
