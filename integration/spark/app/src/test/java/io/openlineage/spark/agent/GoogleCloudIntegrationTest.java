/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.MockServerUtils.getAvailablePort;
import static io.openlineage.spark.agent.MockServerUtils.verifyEvents;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockserver.model.HttpRequest.request;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import io.openlineage.client.OpenLineage.RunEvent;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession$;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mockserver.configuration.Configuration;
import org.mockserver.integration.ClientAndServer;
import org.slf4j.event.Level;

@Tag("integration-test")
@Tag("google-cloud")
@Slf4j
public class GoogleCloudIntegrationTest {

  private static final String LOCAL_IP = "127.0.0.1";
  private static final int MOCKSERVER_PORT = getAvailablePort();
  private static final String SPARK_3 = "(3.*)";
  private static final String SPARK_3_3 = "(3\\.[3-9].*)";
  private static final String SPARK_VERSION = "spark.version";
  private static ClientAndServer mockServer;
  static SparkSession spark;

  @BeforeAll
  @SneakyThrows
  public static void beforeAll() {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
    Configuration configuration = new Configuration();
    configuration.logLevel(Level.ERROR);
    mockServer = ClientAndServer.startClientAndServer(configuration, MOCKSERVER_PORT);
    mockServer
        .when(request("/api/v1/lineage"))
        .respond(org.mockserver.model.HttpResponse.response().withStatusCode(201));
  }

  @AfterAll
  @SneakyThrows
  public static void afterAll() {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
    mockServer.stop();
  }

  @BeforeEach
  @SneakyThrows
  public void beforeEach() {
    spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("GoogleCloudIntegrationTest")
            .config("spark.driver.host", LOCAL_IP)
            .config("spark.driver.bindAddress", LOCAL_IP)
            .config("spark.ui.enabled", false)
            .config("spark.sql.shuffle.partitions", 1)
            .config("spark.sql.warehouse.dir", "file:/tmp/iceberg/")
            .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/gctest")
            .config("spark.openlineage.transport.type", "http")
            .config(
                "spark.openlineage.transport.url",
                "http://localhost:" + mockServer.getPort() + "/api/v1/namespaces/gc-namespace")
            .config("spark.openlineage.facets.disabled", "spark_unknown;spark.logicalPlan")
            .config("spark.openlineage.debugFacet", "disabled")
            .config("parentProject", "openlineage-ci")
            .config("credentialsFile", "build/gcloud/gcloud-service-key.json")
            .config("temporaryGcsBucket", "openlineage-spark-bigquery-integration")
            .config(
                "spark.hadoop.fs.AbstractFileSystem.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
            .config(
                "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                "build/gcloud/gcloud-service-key.json")
            .config(
                "spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
            .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
            .config("spark.extraListeners", OpenLineageSparkListener.class.getName())
            .getOrCreate();

    spark.sparkContext().setLogLevel("WARN");
  }

  @Test
  @EnabledIfEnvironmentVariable(named = "CI", matches = "true")
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3_3) // Spark version >= 3.*
  void testReadAndWriteFromBigquery() {
    String PROJECT_ID = "openlineage-ci";
    String DATASET_ID = "airflow_integration";
    String versionName = System.getProperty(SPARK_VERSION).replace(".", "_");

    String source_table = String.format("%s.%s.%s_source", PROJECT_ID, DATASET_ID, versionName);
    String target_table = String.format("%s.%s.%s_target", PROJECT_ID, DATASET_ID, versionName);

    spark.sparkContext().setLogLevel("info");

    Dataset<Row> dataset =
        spark
            .createDataFrame(
                ImmutableList.of(RowFactory.create(1L, 2L), RowFactory.create(3L, 4L)),
                new StructType(
                    new StructField[] {
                      new StructField("a", LongType$.MODULE$, false, Metadata.empty()),
                      new StructField("b", LongType$.MODULE$, false, Metadata.empty())
                    }))
            .repartition(1);

    dataset.write().format("bigquery").option("table", source_table).mode("overwrite").save();

    Dataset<Row> first = spark.read().format("bigquery").option("table", source_table).load();

    first.write().format("bigquery").option("table", target_table).mode("overwrite").save();

    verifyEvents(
        mockServer,
        Collections.singletonMap(
            "{spark_version}", System.getProperty(SPARK_VERSION).replace(".", "_")),
        "pysparkBigquerySaveStart.json",
        "pysparkBigqueryInsertStart.json",
        "pysparkBigqueryInsertEnd.json",
        "pysparkBigquerySaveEnd.json");
  }

  @Test
  @EnabledIfEnvironmentVariable(named = "CI", matches = "true")
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3) // Spark version >= 3.*
  void testRddWriteToBucket() throws IOException {
    String sparkVersion = String.format("spark-%s", System.getProperty(SPARK_VERSION));
    String scalaVersion = String.format("scala-%s", System.getProperty("scala.binary.version"));
    URI buckertUri =
        URI.create(
            String.format(
                "gs://openlineage-spark-bigquery-integration/rdd-test/spark-%s/scala-%s",
                sparkVersion, scalaVersion));
    String pathPrefix = buckertUri.toString();

    URL url = Resources.getResource("test_data/data.txt");
    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
    JavaRDD<String> textFile = sc.textFile(url.getPath());

    // clear used locations - workaround save dataset to overwrite location
    spark
        .createDataFrame(
            ImmutableList.of(RowFactory.create(1L)),
            new StructType(
                new StructField[] {
                  new StructField("a", LongType$.MODULE$, false, Metadata.empty())
                }))
        .repartition(1)
        .write()
        .mode(SaveMode.Overwrite)
        .save(pathPrefix);

    // prepare some file in GS
    String inputPath = String.format("%s/input/data.csv", pathPrefix, pathPrefix);
    textFile.saveAsTextFile(inputPath);

    // read from GS and write to another location in GS
    spark
        .sparkContext()
        .textFile(inputPath, 1)
        .toJavaRDD()
        .map(t -> t + t) // RDD operation to make sure code goes through RDDExecutionContext
        .saveAsTextFile(String.format("%s/output/data.csv", pathPrefix));

    List<RunEvent> eventsEmitted = MockServerUtils.getEventsEmitted(mockServer);

    String uriPath = buckertUri.getPath();

    assertThat(eventsEmitted.get(eventsEmitted.size() - 1).getOutputs().get(0))
        .hasFieldOrPropertyWithValue("name", String.format("%s/output/data.csv", uriPath))
        .hasFieldOrPropertyWithValue("namespace", "gs://openlineage-spark-bigquery-integration");

    assertThat(eventsEmitted.get(eventsEmitted.size() - 2).getInputs().get(0))
        .hasFieldOrPropertyWithValue("name", String.format("%s/input/data.csv", uriPath))
        .hasFieldOrPropertyWithValue("namespace", "gs://openlineage-spark-bigquery-integration");
  }
}
