/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.MockServerUtils.verifyEvents;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import io.openlineage.client.OpenLineage.RunEvent;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
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
import org.mockserver.integration.ClientAndServer;

@Tag("integration-test")
@Tag("google-cloud")
@Slf4j
@EnabledIfEnvironmentVariable(named = "CI", matches = "true")
class GoogleCloudIntegrationTest {
  private static final String PROJECT_ID =
      Optional.ofNullable(System.getenv("GCLOUD_PROJECT_ID")).orElse("openlineage-ci");
  private static final String BUCKET_NAME =
      Optional.ofNullable(System.getenv("GCLOUD_BIGQUERY_BUCKET_NAME"))
          .orElse("openlineage-spark-bigquery-integration");
  private static final URI BUCKET_URI = URI.create("gs://" + BUCKET_NAME + "/");
  private static final String NAMESPACE = "google-cloud-namespace";
  private static final int MOCKSERVER_PORT = 3000;
  private static final String LOCAL_IP = "127.0.0.1";
  private static final String SPARK_3 = "(3.*)";
  private static final String SPARK_3_3 = "(3\\.[3-9].*)";
  private static final String SPARK_VERSION_PROPERTY = "spark.version";
  private static final String CREDENTIALS_FILE = "build/gcloud/gcloud-service-key.json";

  private static final String DATASET_ID = "airflow_integration";
  private static final String SPARK_VERSION =
      String.format("spark_%s", SparkContainerProperties.SPARK_VERSION).replace(".", "_");
  private static final String SCALA_VERSION =
      String.format("scala_%s", SparkContainerProperties.SCALA_BINARY_VERSION).replace(".", "_");
  private static final String VERSION_NAME = String.format("%s_%s", SPARK_VERSION, SCALA_VERSION);

  private static SparkSession spark;
  private static ClientAndServer mockServer;

  @BeforeAll
  @SneakyThrows
  public static void beforeAll() {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
    mockServer = new ClientAndServer(MOCKSERVER_PORT);
    MockServerUtils.configureStandardExpectation(mockServer);
  }

  @AfterAll
  @SneakyThrows
  public static void afterAll() {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
    MockServerUtils.stopMockServer(mockServer);
  }

  @BeforeEach
  @SneakyThrows
  public void beforeEach() {
    MockServerUtils.clearRequests(mockServer);
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
            .config("spark.extraListeners", OpenLineageSparkListener.class.getCanonicalName())
            .config("spark.openlineage.transport.type", "http")
            .config(
                "spark.openlineage.transport.url",
                "http://localhost:" + mockServer.getPort() + "/api/v1/lineage")
            .config("spark.openlineage.facets.disabled", "spark_unknown;spark.logicalPlan")
            .config("spark.openlineage.debugFacet", "disabled")
            .config("spark.openlineage.namespace", NAMESPACE)
            .config("parentProject", PROJECT_ID)
            .config("credentialsFile", CREDENTIALS_FILE)
            .config("temporaryGcsBucket", BUCKET_NAME)
            .config(
                "spark.hadoop.fs.AbstractFileSystem.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
            .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", CREDENTIALS_FILE)
            .config(
                "spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
            .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
            .getOrCreate();
  }

  @Test
  @EnabledIfSystemProperty(
      named = SPARK_VERSION_PROPERTY,
      matches = SPARK_3_3) // Spark version >= 3.*
  void testReadAndWriteFromBigquery() {
    String source_table = String.format("%s.%s.%s_source", PROJECT_ID, DATASET_ID, VERSION_NAME);
    String target_table = String.format("%s.%s.%s_target", PROJECT_ID, DATASET_ID, VERSION_NAME);
    log.info("Source Table: {}", source_table);
    log.info("Target Table: {}", target_table);

    Dataset<Row> dataset = getTestDataset();
    dataset.write().format("bigquery").option("table", source_table).mode("overwrite").save();

    Dataset<Row> first = spark.read().format("bigquery").option("table", source_table).load();

    first.write().format("bigquery").option("table", target_table).mode("overwrite").save();

    HashMap<String, String> replacements = new HashMap<>();
    replacements.put("{NAMESPACE}", NAMESPACE);
    replacements.put("{PROJECT_ID}", PROJECT_ID);
    replacements.put("{DATASET_ID}", DATASET_ID);
    replacements.put("{BUCKET_NAME}", BUCKET_NAME);
    replacements.put("{SPARK_VERSION}", SPARK_VERSION);
    replacements.put("{SCALA_VERSION}", SCALA_VERSION);

    if (log.isDebugEnabled()) {
      logRunEvents();
    }

    verifyEvents(
        mockServer,
        replacements,
        "pysparkBigquerySaveStart.json",
        "pysparkBigqueryInsertStart.json",
        "pysparkBigqueryInsertEnd.json",
        "pysparkBigquerySaveEnd.json");
  }

  @Test
  @EnabledIfSystemProperty(
      named = SPARK_VERSION_PROPERTY,
      matches = SPARK_3_3) // Spark version == 3.*
  void testReadAndWriteFromBigqueryUsingQuery() {
    String source_table =
        String.format("%s.%s.%s_source_query_test", PROJECT_ID, DATASET_ID, VERSION_NAME);
    String target_table =
        String.format("%s.%s.%s_target_query_test", PROJECT_ID, DATASET_ID, VERSION_NAME);
    String source_query = String.format("SELECT * FROM %s", source_table);
    log.info("Source Query: {}", source_query);
    log.info("Target Table: {}", target_table);

    Dataset<Row> dataset = getTestDataset();
    dataset.write().format("bigquery").option("table", source_table).mode("overwrite").save();

    Dataset<Row> first =
        spark
            .read()
            .format("bigquery")
            .option("viewMaterializationProject", PROJECT_ID)
            .option("viewMaterializationDataset", DATASET_ID)
            .option("viewsEnabled", "true")
            .option("query", source_query)
            .load();

    first.write().format("bigquery").option("table", target_table).mode("overwrite").save();

    HashMap<String, String> replacements = new HashMap<>();
    replacements.put("{NAMESPACE}", NAMESPACE);
    replacements.put("{PROJECT_ID}", PROJECT_ID);
    replacements.put("{DATASET_ID}", DATASET_ID);
    replacements.put("{BUCKET_NAME}", BUCKET_NAME);
    replacements.put("{SPARK_VERSION}", SPARK_VERSION);
    replacements.put("{SCALA_VERSION}", SCALA_VERSION);

    if (log.isDebugEnabled()) {
      logRunEvents();
    }

    verifyEvents(mockServer, replacements, "pysparkBigqueryQueryEnd.json");
  }

  private static void logRunEvents() {
    List<RunEvent> eventsEmitted = MockServerUtils.getEventsEmitted(mockServer);
    ObjectMapper om = new ObjectMapper().findAndRegisterModules();
    Function<RunEvent, String> serializer =
        event -> {
          try {
            return om.valueToTree(event).toString();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        };

    eventsEmitted.stream().map(serializer).forEach(log::info);
  }

  @Test
  @EnabledIfSystemProperty(
      named = SPARK_VERSION_PROPERTY,
      matches = SPARK_3) // Spark version >= 3.*
  void testRddWriteToBucket() throws IOException {
    String sparkVersion = String.format("spark-%s", SparkContainerProperties.SPARK_VERSION);
    String scalaVersion = String.format("scala-%s", SparkContainerProperties.SCALA_BINARY_VERSION);
    URI baseUri =
        BUCKET_URI.resolve("rdd-test/").resolve(sparkVersion + "/").resolve(scalaVersion + "/");

    log.info("This path will be used for this test: {}", baseUri);

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
        .save(baseUri.toString());

    // prepare some file in GS
    URI inputUri = baseUri.resolve("input/data.csv");
    textFile.saveAsTextFile(inputUri.toString());

    // read from GS and write to another location in GS
    URI outputUri = baseUri.resolve("output/data.csv");
    spark
        .sparkContext()
        .textFile(inputUri.toString(), 1)
        .toJavaRDD()
        .map(t -> t + t) // RDD operation to make sure code goes through RDDExecutionContext
        .saveAsTextFile(outputUri.toString());

    List<RunEvent> eventsEmitted = MockServerUtils.getEventsEmitted(mockServer);

    assertThat(eventsEmitted.get(eventsEmitted.size() - 1).getOutputs().get(0))
        .hasFieldOrPropertyWithValue("name", outputUri.getPath())
        .hasFieldOrPropertyWithValue(
            "namespace", BUCKET_URI.getScheme() + "://" + BUCKET_URI.getHost());

    assertThat(eventsEmitted.get(eventsEmitted.size() - 2).getInputs().get(0))
        .hasFieldOrPropertyWithValue("name", inputUri.getPath())
        .hasFieldOrPropertyWithValue(
            "namespace", BUCKET_URI.getScheme() + "://" + BUCKET_URI.getHost());
  }

  private static Dataset<Row> getTestDataset() {
    return spark
        .createDataFrame(
            ImmutableList.of(RowFactory.create(1L, 2L), RowFactory.create(3L, 4L)),
            new StructType(
                new StructField[] {
                  new StructField("a", LongType$.MODULE$, false, Metadata.empty()),
                  new StructField("b", LongType$.MODULE$, false, Metadata.empty())
                }))
        .repartition(1);
  }
}
