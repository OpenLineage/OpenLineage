/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.agent.vendor.gcp.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockserver.configuration.Configuration;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Header;
import org.slf4j.event.Level;
import scala.Tuple2;

class GCPUtilsTest {

  private static String TEST_URI;
  public static final Header METADATA_HEADER = new Header("Metadata-Flavor", "Google");
  private static final String TEST_APP_NAME = "openlineage-gcp-utils-test";
  private static final String TEST_APP_ID = "application_12345";
  private static final String TEST_RESOURCE_UUID = "1q2w3e4r5t6y7u8i";
  private static final String TEST_CLUSTER_NAME = "openlineage-test-cluster";
  private static final String TEST_JOB_ID = "openlineage-test-job";
  private static final String TEST_BATCH_ID = "openlineage-test-batch";
  private static final String TEST_SESSION_ID = "openlineage-test-session";
  private static final String TEST_PROJECT_ID = "openlineage-gcp-project";
  private static final String TEST_REGION = "us-central1";
  private final SparkContext sparkContext = mock(SparkContext.class);
  private final SparkConf sparkConf = new SparkConf();
  private static ClientAndServer mockServer;

  private static final Map<String, Object> EXPECTED_FACET_DATAPROC_CLUSTER = new HashMap<>();

  private static final Map<String, Object> EXPECTED_FACET_DATAPROC_BATCH = new HashMap<>();

  private static final Map<String, Object> EXPECTED_FACET_DATAPROC_SESSION = new HashMap<>();

  @BeforeAll
  public static void setup() {
    Configuration config = Configuration.configuration();
    config.logLevel(Level.ERROR);
    mockServer = ClientAndServer.startClientAndServer(config);
    TEST_URI = String.format("http://localhost:%s", mockServer.getPort());
  }

  @BeforeEach
  public void beforeEach() {
    when(sparkContext.getConf()).thenReturn(sparkConf);

    sparkConf.set(GCPUtils.SPARK_APP_NAME, TEST_APP_NAME);
    sparkConf.set(GCPUtils.SPARK_APP_ID, TEST_APP_ID);
    sparkConf.set(GCPUtils.GOOGLE_METADATA_API, TEST_URI);

    mockServer
        .when(request(GCPUtils.DATAPROC_REGION_ENDPOINT).withHeader(METADATA_HEADER))
        .respond(response().withBody(TEST_REGION));
    mockServer
        .when(request(GCPUtils.PROJECT_ID_ENDPOINT).withHeader(METADATA_HEADER))
        .respond(response().withBody(TEST_PROJECT_ID));
  }

  @AfterEach
  public void afterEach() {
    mockServer.reset();

    Tuple2<String, String>[] configuration = sparkConf.getAll();
    Arrays.stream(configuration).forEach(tuple -> sparkConf.remove(tuple._1()));
  }

  @AfterAll
  public static void teardown() {
    mockServer.stop();
  }

  @Test
  void testForDataprocCluster() {
    sparkConf.set(
        GCPUtils.SPARK_DRIVER_HOST,
        String.format(
            "%s-m.%s-a.c.%s.google.com.internal", TEST_CLUSTER_NAME, TEST_REGION, TEST_PROJECT_ID));
    sparkConf.set(
        GCPUtils.SPARK_YARN_TAGS,
        String.format("dataproc_job_%s,dataproc_uuid_%s", TEST_JOB_ID, TEST_RESOURCE_UUID));
    sparkConf.set(GCPUtils.SPARK_MASTER, "yarn");

    mockServer
        .when(request(GCPUtils.CLUSTER_UUID_ENDPOINT).withHeader(METADATA_HEADER))
        .respond(response().withBody(TEST_RESOURCE_UUID));

    Map<String, Object> originFacet = GCPUtils.getOriginFacetMap(sparkContext);
    assertThat(originFacet.get("sourceType")).isEqualTo("DATAPROC");
    assertThat(originFacet.get("name"))
        .isEqualTo(
            String.format(
                "projects/%s/regions/%s/clusters/%s",
                TEST_PROJECT_ID, TEST_REGION, TEST_CLUSTER_NAME));

    Map<String, Object> dataprocRunFacet = GCPUtils.getDataprocRunFacetMap(sparkContext);
    assertThat(dataprocRunFacet).isEqualTo(EXPECTED_FACET_DATAPROC_CLUSTER);
  }

  @Test
  void testForDataprocBatch() {
    mockServer
        .when(request(GCPUtils.BATCH_ID_ENDPOINT).withHeader(METADATA_HEADER))
        .respond(response().withBody(TEST_BATCH_ID));
    mockServer
        .when(request(GCPUtils.BATCH_UUID_ENDPOINT).withHeader(METADATA_HEADER))
        .respond(response().withBody(TEST_RESOURCE_UUID));

    Map<String, Object> originFacet = GCPUtils.getOriginFacetMap(sparkContext);
    assertThat(originFacet.get("sourceType")).isEqualTo("DATAPROC");
    assertThat(originFacet.get("name"))
        .isEqualTo(
            String.format(
                "projects/%s/locations/%s/batches/%s",
                TEST_PROJECT_ID, TEST_REGION, TEST_BATCH_ID));

    Map<String, Object> dataprocRunFacet = GCPUtils.getDataprocRunFacetMap(sparkContext);
    assertThat(dataprocRunFacet).isEqualTo(EXPECTED_FACET_DATAPROC_BATCH);
  }

  @Test
  void testForDataprocSession() {
    mockServer
        .when(request(GCPUtils.SESSION_ID_ENDPOINT).withHeader(METADATA_HEADER))
        .respond(response().withBody(TEST_SESSION_ID));
    mockServer
        .when(request(GCPUtils.SESSION_UUID_ENDPOINT).withHeader(METADATA_HEADER))
        .respond(response().withBody(TEST_RESOURCE_UUID));

    Map<String, Object> originFacet = GCPUtils.getOriginFacetMap(sparkContext);
    assertThat(originFacet.get("sourceType")).isEqualTo("DATAPROC");
    assertThat(originFacet.get("name"))
        .isEqualTo(
            String.format(
                "projects/%s/locations/%s/sessions/%s",
                TEST_PROJECT_ID, TEST_REGION, TEST_SESSION_ID));

    Map<String, Object> dataprocRunFacet = GCPUtils.getDataprocRunFacetMap(sparkContext);
    assertThat(dataprocRunFacet).isEqualTo(EXPECTED_FACET_DATAPROC_SESSION);
  }

  static {
    EXPECTED_FACET_DATAPROC_CLUSTER.put("jobUuid", TEST_RESOURCE_UUID);
    EXPECTED_FACET_DATAPROC_CLUSTER.put("jobId", TEST_JOB_ID);
    EXPECTED_FACET_DATAPROC_CLUSTER.put("clusterUuid", TEST_RESOURCE_UUID);
    EXPECTED_FACET_DATAPROC_CLUSTER.put("clusterName", TEST_CLUSTER_NAME);
    EXPECTED_FACET_DATAPROC_CLUSTER.put("appName", TEST_APP_NAME);
    EXPECTED_FACET_DATAPROC_CLUSTER.put("appId", TEST_APP_ID);
    EXPECTED_FACET_DATAPROC_CLUSTER.put("projectId", TEST_PROJECT_ID);
    EXPECTED_FACET_DATAPROC_CLUSTER.put("jobType", "dataproc_job");

    EXPECTED_FACET_DATAPROC_BATCH.put("batchUuid", TEST_RESOURCE_UUID);
    EXPECTED_FACET_DATAPROC_BATCH.put("batchId", TEST_BATCH_ID);
    EXPECTED_FACET_DATAPROC_BATCH.put("appName", TEST_APP_NAME);
    EXPECTED_FACET_DATAPROC_BATCH.put("appId", TEST_APP_ID);
    EXPECTED_FACET_DATAPROC_BATCH.put("projectId", TEST_PROJECT_ID);
    EXPECTED_FACET_DATAPROC_BATCH.put("jobType", "batch");

    EXPECTED_FACET_DATAPROC_SESSION.put("sessionUuid", TEST_RESOURCE_UUID);
    EXPECTED_FACET_DATAPROC_SESSION.put("sessionId", TEST_SESSION_ID);
    EXPECTED_FACET_DATAPROC_SESSION.put("appName", TEST_APP_NAME);
    EXPECTED_FACET_DATAPROC_SESSION.put("appId", TEST_APP_ID);
    EXPECTED_FACET_DATAPROC_SESSION.put("projectId", TEST_PROJECT_ID);
    EXPECTED_FACET_DATAPROC_SESSION.put("jobType", "session");
  }
}
