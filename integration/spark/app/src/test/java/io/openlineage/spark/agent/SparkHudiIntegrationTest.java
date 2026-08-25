/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.SparkTestUtils.SPARK_VERSION;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import io.openlineage.client.OpenLineage.RunEvent;
import java.io.File;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mockserver.integration.ClientAndServer;

@Tag("integration-test")
@Tag("hudi")
@EnabledIfSystemProperty(named = SPARK_VERSION, matches = "(4.*|3.5.*)")
class SparkHudiIntegrationTest {
  private static final int MOCK_SERVER_PORT = 1090;

  @SuppressWarnings("PMD")
  private static final String LOCAL_IP = "127.0.0.1";

  private static final String HUDI_BASE_PATH = "/tmp/hudi";

  private static SparkSession spark;
  private static ClientAndServer mockServer;

  @BeforeAll
  @SneakyThrows
  public static void beforeAll() {
    Spark4CompatUtils.cleanupAnyExistingSession();
    FileUtils.deleteDirectory(new File(HUDI_BASE_PATH));
    mockServer = MockServerUtils.createAndConfigureMockServer(MOCK_SERVER_PORT);
  }

  @AfterAll
  @SneakyThrows
  public static void afterAll() {
    Spark4CompatUtils.cleanupAnyExistingSession();
    MockServerUtils.stopMockServer(mockServer);
  }

  @BeforeEach
  @SneakyThrows
  public void beforeEach() {
    MockServerUtils.clearRequests(mockServer);
    FileUtils.deleteDirectory(new File(HUDI_BASE_PATH));
    spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("HudiIntegrationTest")
            .config("spark.driver.host", LOCAL_IP)
            .config("spark.driver.bindAddress", LOCAL_IP)
            .config("spark.ui.enabled", false)
            .config("spark.sql.shuffle.partitions", 1)
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.openlineage.transport.type", "composite")
            .config("spark.openlineage.transport.transports.http.type", "http")
            .config("spark.openlineage.transport.transports.console.type", "console")
            .config("spark.openlineage.transport.transports.file.type", "file")
            .config(
                "spark.openlineage.transport.transports.file.location",
                ".target/test-output/test-output.json")
            .config(
                "spark.openlineage.transport.transports.http.url",
                "http://localhost:" + mockServer.getPort() + "/api/v1/lineage")
            .config("spark.extraListeners", OpenLineageSparkListener.class.getName())
            .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
            .getOrCreate();
  }

  @Test
  void testHudiCopyOnWriteCapturesOutputDataset() {
    String outputPath = HUDI_BASE_PATH + "/cow_target";
    writeHudi("COPY_ON_WRITE", outputPath);

    RunEvent completeEvent = getLatestSaveCompleteEvent();
    assertThat(completeEvent.getOutputs()).isNotEmpty();
  }

  @Test
  void testHudiMergeOnReadCapturesOutputDataset() {
    String outputPath = HUDI_BASE_PATH + "/mor_target";
    writeHudi("MERGE_ON_READ", outputPath);

    RunEvent completeEvent = getLatestSaveCompleteEvent();
    assertThat(completeEvent.getOutputs()).isNotEmpty();
  }

  @Test
  void testHudiReadTransformAndWriteAgainCapturesOutputDataset() {
    String sourcePath = HUDI_BASE_PATH + "/roundtrip_source";
    String targetPath = HUDI_BASE_PATH + "/roundtrip_target";

    writeHudi("COPY_ON_WRITE", sourcePath);
    MockServerUtils.clearRequests(mockServer);

    Dataset<Row> transformed =
        spark
            .read()
            .format("hudi")
            .load(sourcePath)
            .selectExpr("entity_id", "upper(name) as name", "ts");

    writeHudiDataset("COPY_ON_WRITE", targetPath, transformed, "hudi_roundtrip_target");

    RunEvent completeEvent = getLatestSaveCompleteEvent();
    assertThat(completeEvent.getOutputs()).isNotEmpty();
  }

  @Test
  void testHudiMergeOnReadReadTransformAndWriteAgainCapturesOutputDataset() {
    String sourcePath = HUDI_BASE_PATH + "/mor_roundtrip_source";
    String targetPath = HUDI_BASE_PATH + "/mor_roundtrip_target";

    writeHudi("MERGE_ON_READ", sourcePath);
    MockServerUtils.clearRequests(mockServer);

    Dataset<Row> transformed =
        spark
            .read()
            .format("hudi")
            .load(sourcePath)
            .selectExpr("entity_id", "upper(name) as name", "ts");

    writeHudiDataset("MERGE_ON_READ", targetPath, transformed, "hudi_mor_roundtrip_target");

    RunEvent completeEvent = getLatestSaveCompleteEvent();
    assertThat(completeEvent.getOutputs()).isNotEmpty();
  }

  @Test
  void testHudiMergeOnReadIncrementalReadTransformAndWriteAgainCapturesOutputDataset() {
    String sourcePath = HUDI_BASE_PATH + "/mor_incremental_source";
    String targetPath = HUDI_BASE_PATH + "/mor_incremental_target";

    writeHudi("MERGE_ON_READ", sourcePath);
    MockServerUtils.clearRequests(mockServer);

    Dataset<Row> transformed =
        spark
            .read()
            .format("hudi")
            .option("hoodie.datasource.query.type", "incremental")
            .option("hoodie.datasource.read.begin.instanttime", "000")
            .load(sourcePath)
            .selectExpr("entity_id", "concat(name, '_inc') as name", "ts");

    writeHudiDataset(
        "MERGE_ON_READ", targetPath, transformed, "hudi_mor_incremental_roundtrip_target");

    RunEvent completeEvent = getLatestSaveCompleteEvent();
    assertThat(completeEvent.getOutputs()).isNotEmpty();
  }

  private RunEvent getLatestSaveCompleteEvent() {
    List<RunEvent> events = MockServerUtils.getEventsEmitted(mockServer);
    return events.stream()
        .filter(e -> e.getEventType() == RunEvent.EventType.COMPLETE)
        .filter(e -> e.getJob().getName().contains("save_into_data_source_command"))
        .reduce((previous, current) -> current)
        .orElseThrow(
            () ->
                new AssertionError(
                    "No COMPLETE event for save_into_data_source_command was emitted."));
  }

  private void writeHudi(String tableType, String outputPath) {
    String inputPath = HUDI_BASE_PATH + "/input";

    createInputDataset().write().mode(SaveMode.Overwrite).format("parquet").save(inputPath);

    MockServerUtils.clearRequests(mockServer);

    Dataset<Row> transformedInput =
        spark.read().parquet(inputPath).selectExpr("id as entity_id", "name", "ts");

    writeHudiDataset(tableType, outputPath, transformedInput, "hudi_" + tableType.toLowerCase());
  }

  private void writeHudiDataset(
      String tableType, String outputPath, Dataset<Row> dataset, String tableName) {
    dataset
        .write()
        .format("hudi")
        .mode(SaveMode.Overwrite)
        .option("hoodie.table.name", tableName)
        .option("hoodie.datasource.write.recordkey.field", "entity_id")
        .option("hoodie.datasource.write.precombine.field", "ts")
        .option("hoodie.datasource.write.partitionpath.field", "")
        .option(
            "hoodie.datasource.write.keygenerator.class",
            "org.apache.hudi.keygen.NonpartitionedKeyGenerator")
        .option("hoodie.datasource.write.table.type", tableType)
        .save(outputPath);
  }

  private Dataset<Row> createInputDataset() {
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("id", LongType$.MODULE$, false, Metadata.empty()),
              new StructField("name", StringType$.MODULE$, false, Metadata.empty()),
              new StructField("ts", LongType$.MODULE$, false, Metadata.empty())
            });

    return spark.createDataFrame(
        ImmutableList.of(
            RowFactory.create(1L, "alpha", 10L),
            RowFactory.create(2L, "beta", 20L),
            RowFactory.create(3L, "gamma", 30L)),
        schema);
  }
}
