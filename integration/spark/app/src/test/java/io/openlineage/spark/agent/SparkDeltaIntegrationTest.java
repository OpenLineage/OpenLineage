/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.MockServerUtils.verifyEvents;
import static io.openlineage.spark.agent.MockServerUtils.waitForJobComplete;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.JsonBody.json;

import com.google.common.collect.ImmutableList;
import java.time.Duration;
import java.util.Arrays;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession$;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.IntegerType$;
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
import org.junit.jupiter.api.condition.EnabledIf;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.matchers.MatchType;

@Tag("integration-test")
@Tag("delta")
@EnabledIf("isDeltaTestEnabled")
@Slf4j
public class SparkDeltaIntegrationTest {

  @SuppressWarnings("PMD")
  private static final String LOCAL_IP = "127.0.0.1";

  private static ClientAndServer mockServer;

  private static final String SPARK_VERSION = "spark.version";

  static SparkSession spark;

  @BeforeAll
  @SneakyThrows
  public static void beforeAll() {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
    mockServer = ClientAndServer.startClientAndServer(1080);
  }

  @AfterAll
  @SneakyThrows
  public static void afterAll() {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
  }

  @BeforeEach
  @SneakyThrows
  public void beforeEach() {
    mockServer.reset();
    mockServer
        .when(request("/api/v1/lineage"))
        .respond(org.mockserver.model.HttpResponse.response().withStatusCode(201));
    spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("DeltaIntegrationTest")
            .config("spark.driver.host", LOCAL_IP)
            .config("spark.driver.bindAddress", LOCAL_IP)
            .config("spark.sql.shuffle.partitions", 1)
            .config("spark.sql.warehouse.dir", "file:/tmp/delta/")
            .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/delta/derby")
            .config("spark.openlineage.transport.type", "http")
            .config(
                "spark.openlineage.transport.url",
                "http://localhost:" + mockServer.getPort() + "/api/v1/namespaces/delta-namespace")
            .config("spark.openlineage.facets.disabled", "")
            .config("spark.extraListeners", OpenLineageSparkListener.class.getName())
            .config("spark.jars.ivy", "/tmp/.ivy2/")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .enableHiveSupport()
            .getOrCreate();
    FileSystem.get(spark.sparkContext().hadoopConfiguration())
        .delete(new Path("/tmp/delta/"), true);
  }

  @Test
  void testCTASDelta() throws InterruptedException {
    clearTables("temp", "tbl");

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

    dataset.createOrReplaceTempView("temp");
    spark.sql("CREATE TABLE tbl USING delta LOCATION '/tmp/delta/tbl' AS SELECT * FROM temp");

    waitForJobComplete(mockServer, "delta_integration_test.atomic_create_table_as_select");
    verifyEvents(mockServer, "pysparkDeltaCTASComplete.json");
  }

  @Test
  void testFilteringDeltaEvents() throws InterruptedException {
    clearTables("temp", "t1", "t2", "tbl");
    mockServer.reset();
    mockServer
        .when(request("/api/v1/lineage"))
        .respond(org.mockserver.model.HttpResponse.response().withStatusCode(201));

    // 2 OL events expected
    spark.sql("CREATE TABLE t1 (a long, b long) USING delta LOCATION '/tmp/delta/t1'");
    Dataset<Row> dataset =
        spark
            .createDataFrame(
                ImmutableList.of(RowFactory.create(1L), RowFactory.create(2L)),
                new StructType(
                    new StructField[] {
                      new StructField("a", LongType$.MODULE$, false, Metadata.empty())
                    }))
            .repartition(1);

    // 2 OL events expected
    dataset.write().saveAsTable("temp");

    // 2 OL events expected
    spark.sql(
        "CREATE TABLE t2 USING delta LOCATION '/tmp/delta/t2' AS SELECT * FROM temp WHERE a > 1");

    // 2 OL events expected
    spark.sql("INSERT INTO t1 VALUES (3,4)");

    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () ->
                assertEquals(
                    8,
                    mockServer.retrieveRecordedRequests(request().withPath("/api/v1/lineage"))
                        .length));
  }

  @Test
  void testDeltaSaveAsTable() {
    clearTables("movies");
    Dataset<Row> dataset =
        spark
            .createDataFrame(
                ImmutableList.of(
                    RowFactory.create(
                        "{\"title\":\"Feeding Sea Lions\",\"year\":1900,\"cast\":[\"Paul Boyton\"],\"genres\":[]}"),
                    RowFactory.create(
                        "{\"title\":\"The Wonder, Ching Ling Foo\",\"year\":1900,\"cast\":[\"Ching Ling Foo\"],\"genres\":[\"Short\"]}")),
                new StructType(
                    new StructField[] {
                      new StructField("value", StringType$.MODULE$, false, Metadata.empty())
                    }))
            .repartition(1);

    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("title", StringType$.MODULE$, false, Metadata.empty()),
              new StructField("year", IntegerType$.MODULE$, false, Metadata.empty()),
              new StructField(
                  "cast", new ArrayType(StringType$.MODULE$, false), false, Metadata.empty()),
              new StructField(
                  "genres", new ArrayType(StringType$.MODULE$, false), false, Metadata.empty())
            });

    dataset
        .select(from_json(col("value"), schema).alias("parsed"))
        .select(col("parsed.*"))
        .write()
        .mode("overwrite")
        .format("parquet")
        .saveAsTable("movies");

    waitForJobComplete(
        mockServer, "delta_integration_test.execute_create_data_source_table_as_select_command");
    verifyEvents(mockServer, "pysparkDeltaSaveAsTableComplete.json");
  }

  @Test
  void testReplaceTable() {
    clearTables("tbl");
    spark.sql(
        "CREATE TABLE tbl (a string, b string) USING delta LOCATION '/tmp/delta/v2_replace_table'");
    spark.sql(
        "REPLACE TABLE tbl (c string, d string) USING delta LOCATION '/tmp/delta/v2_replace_table'");

    waitForJobComplete(mockServer, "delta_integration_test.atomic_replace_table");
    verifyEvents(
        mockServer,
        "pysparkV2ReplaceTableStartEvent.json",
        "pysparkV2ReplaceTableCompleteEvent.json");
  }

  @Test
  void testDeltaVersion() {
    clearTables("versioned_table", "versioned_input_table");

    // VERSION 1 of versioned_table
    spark.sql(
        "CREATE TABLE versioned_table (a long, b long) USING delta "
            + "LOCATION '/tmp/delta/versioned_table'");

    // VERSION 2 of versioned_table
    spark.sql("ALTER TABLE versioned_table ADD COLUMNS (c long)");

    Dataset<Row> dataset =
        spark
            .createDataFrame(
                ImmutableList.of(RowFactory.create(1L), RowFactory.create(2L)),
                new StructType(
                    new StructField[] {
                      new StructField("a", LongType$.MODULE$, false, Metadata.empty())
                    }))
            .repartition(1);
    dataset.createOrReplaceTempView("temp");

    // VERSION 1 of versioned_input_table
    spark.sql(
        "CREATE TABLE versioned_input_table USING delta LOCATION "
            + "'/tmp/delta/versioned_input_table' AS SELECT * FROM temp");

    // VERSION 2 of versioned_input_table
    spark.sql("ALTER TABLE versioned_input_table ADD COLUMNS (b long)");
    spark.sql("INSERT INTO versioned_input_table VALUES (3,4)");

    // VERSION 3 of versioned_input_table
    spark.sql("ALTER TABLE versioned_input_table ADD COLUMNS (c long)");
    spark.sql("INSERT INTO versioned_table SELECT * FROM versioned_input_table");

    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> {
              mockServer.verify(
                  request()
                      .withPath("/api/v1/lineage")
                      .withBody(
                          json(
                              "{\"eventType\": \"COMPLETE\", "
                                  + "\"inputs\": [{\"name\": \"/tmp/delta/versioned_input_table\"}]}"
                                  + "\"outputs\": [{\"name\": \"/tmp/delta/versioned_table\"}]}",
                              MatchType.ONLY_MATCHING_FIELDS)));
            });

    verifyEvents(
        mockServer,
        "pysparkWriteDeltaTableVersionStart.json",
        "pysparkWriteDeltaTableVersionEnd.json");
  }

  @Test
  void testSaveIntoDataSourceCommand() throws InterruptedException {
    Dataset<Row> dataset =
        spark
            .createDataFrame(
                ImmutableList.of(
                    RowFactory.create(1L, "bat"),
                    RowFactory.create(3L, "mouse"),
                    RowFactory.create(3L, "horse")),
                new StructType(
                    new StructField[] {
                      new StructField("a", LongType$.MODULE$, false, Metadata.empty()),
                      new StructField("b", StringType$.MODULE$, false, Metadata.empty())
                    }))
            .repartition(1);

    dataset
        .write()
        .mode("overwrite")
        .format("delta")
        .save("/tmp/delta/save_into_data_source_target/");

    waitForJobComplete(mockServer, "delta_integration_test.execute_save_into_data_source_command");
    verifyEvents(mockServer, "pysparkSaveIntoDatasourceCompleteEvent.json");
  }

  private void clearTables(String... tables) {
    Arrays.asList(tables).stream()
        .filter(t -> spark.catalog().tableExists(t))
        .forEach(t -> spark.sql("DROP TABLE " + t));
  }

  static boolean isDeltaTestEnabled() {
    if (System.getProperty(SPARK_VERSION).startsWith("2")) {
      // we don't run integration tests for delta and Spark 2.x
      return false;
    }

    return true;
  }
}
