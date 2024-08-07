/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.SparkTestsUtils.SPARK_3_3_5_EXCLUDED;
import static io.openlineage.spark.agent.SparkTestsUtils.SPARK_3_ONLY;
import static io.openlineage.spark.agent.SparkTestsUtils.SPARK_3_OR_ABOVE;
import static io.openlineage.spark.agent.SparkTestsUtils.SPARK_VERSION;
import static io.openlineage.spark.agent.SparkTestsUtils.createHttpServer;
import static io.openlineage.spark.agent.SparkTestsUtils.createSparkSession;
import static io.openlineage.spark.agent.SparkTestsUtils.createSparkSessionWithDeltaLake;
import static io.openlineage.spark.agent.SparkTestsUtils.createSparkSessionWithIceberg;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import io.openlineage.client.OpenLineage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

@Slf4j
@Tag("integration-test")
class SparkSqlQueryExtensionTest {

  private final String DeltaLakeLocation = "/tmp/delta-table/";

  private static final SparkTestsUtils.OpenLineageEndpointHandler handler =
      new SparkTestsUtils.OpenLineageEndpointHandler();

  @AfterEach
  void tearDown() throws IOException {
    FileUtils.deleteDirectory(new File(DeltaLakeLocation));
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3_OR_ABOVE)
  void testReadingSqlQuery() throws IOException {
    StructType structType = new StructType();
    structType = structType.add("A", StringType, false);
    structType = structType.add("B", StringType, false);

    List<Row> nums = new ArrayList<Row>();
    nums.add(RowFactory.create("value1", "value2"));

    HttpServer server = createHttpServer(handler);

    SparkSession spark = createSparkSession(server.getAddress().getPort(), "testReadingSqlQuery");

    spark
        .createDataFrame(nums, structType)
        .withColumn("data", expr("1").alias("data"))
        .select("data")
        .distinct()
        .createOrReplaceTempView("temp1");

    String queryA = "WITH table AS (" + "SELECT data FROM temp1" + ") " + "SELECT data FROM table";

    String queryB = "SELECT * FROM temp1";

    spark.sql(queryA).show();

    spark.sql(queryB).write().mode("overwrite").saveAsTable("temp2");

    List<OpenLineage.RunEvent> events =
        handler.events.getOrDefault("test_reading_sql_query", new ArrayList<>());

    // we have two queries so at least one should be properly recorded
    assertThat(events).hasSizeGreaterThanOrEqualTo(2);

    List<String> registeredQueries = filterToRecordedQueries(events);

    assertThat(registeredQueries).hasSize(2);

    assertThat(registeredQueries).contains(queryA);
    assertThat(registeredQueries).contains(queryB);
    spark.stop();
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3_3_5_EXCLUDED)
  void testWithDeltaLakeTurnedOn() throws IOException {
    HttpServer server = createHttpServer(handler);

    SparkSession spark =
        createSparkSessionWithDeltaLake(server.getAddress().getPort(), "testWithDeltaLakeTurnedOn");

    String createTableQuery =
        String.format(
            "CREATE TABLE delta.`%s` USING DELTA AS SELECT col1 as id FROM VALUES 0,1,2,3,4;",
            DeltaLakeLocation);
    String selectQuery = String.format("SELECT * FROM delta.`%s`;", DeltaLakeLocation);
    String insertQuery =
        String.format(
            "INSERT INTO delta.`%s` SELECT col1 as id FROM VALUES 5,6,7,8,9;", DeltaLakeLocation);
    String updateQuery =
        String.format("UPDATE delta.`%s` SET id = id + 100 WHERE id %% 2 == 0;", DeltaLakeLocation);
    String deleteQuery =
        String.format("DELETE FROM delta.`%s` WHERE id %% 2 == 0;", DeltaLakeLocation);
    String createTempViewQuery =
        String.format(
            "CREATE TEMP VIEW newData AS SELECT col1 AS id FROM VALUES 1,3,5,7,9,11,13,15,17,19;");
    String mergeIntoQuery =
        String.format(
            "MERGE INTO delta.`%1$s` USING newData ON delta.`%1$s`.id = newData.id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *;",
            DeltaLakeLocation);

    spark.sql(createTableQuery);
    spark.sql(selectQuery).show();
    spark.sql(insertQuery);
    spark.sql(updateQuery).show();
    spark.sql(deleteQuery).show();
    spark.sql(createTempViewQuery);
    spark.sql(mergeIntoQuery);
    spark.sql(selectQuery).show();

    List<OpenLineage.RunEvent> events =
        handler.events.getOrDefault("test_with_delta_lake_turned_on", new ArrayList<>());

    // at least 7 events should be recorded, one for each delta query
    assertThat(events).hasSizeGreaterThanOrEqualTo(8);

    List<String> recordedQueries = filterToRecordedQueries(events);

    long selectQueryCount = recordedQueries.stream().filter(q -> q.equals(selectQuery)).count();
    assertThat(selectQueryCount).isEqualTo(2);

    assertThat(recordedQueries).contains(createTableQuery);
    assertThat(recordedQueries).contains(selectQuery);
    assertThat(recordedQueries).contains(insertQuery);
    assertThat(recordedQueries).contains(updateQuery);
    assertThat(recordedQueries).contains(deleteQuery);
    assertThat(recordedQueries).contains(createTempViewQuery);
    assertThat(recordedQueries).contains(mergeIntoQuery);
    spark.stop();
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3_ONLY)
  void testWithIcebergTurnedOn() throws IOException {
    HttpServer server = createHttpServer(handler);

    SparkSession spark =
        createSparkSessionWithIceberg(server.getAddress().getPort(), "testWithIcebergTurnedOn");

    String dropTableIfExistsQuery = "DROP TABLE IF EXISTS test_table";
    String dropTableIfExistsNewDataQuery = "DROP TABLE IF EXISTS new_data";
    String createTableQuery =
        "CREATE TABLE test_table (id bigint, data string) USING iceberg PARTITIONED BY (id)";
    String insertQuery = "INSERT INTO test_table VALUES (1, 'value1'), (2, 'value2')";
    String selectQuery = "SELECT * FROM test_table";
    String selectQuery2 = "SELECT 3 as id, 'value3' as data";
    String tempTableQuery = String.format("CREATE TEMP VIEW new_data AS %s", selectQuery2);

    String mergeQuery =
        "MERGE INTO test_table USING new_data ON test_table.id = new_data.id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *";

    spark.sql(dropTableIfExistsQuery);
    spark.sql(dropTableIfExistsNewDataQuery);
    spark.sql(createTableQuery);
    spark.sql(insertQuery);
    spark.sql(selectQuery).show();
    spark.sql(tempTableQuery);
    spark.sql(mergeQuery);

    List<OpenLineage.RunEvent> events =
        handler.events.getOrDefault("test_with_iceberg_turned_on", new ArrayList<>());
    List<String> recordedSqlEvents = filterToRecordedQueries(events);

    assertThat(recordedSqlEvents).contains(dropTableIfExistsQuery);
    assertThat(recordedSqlEvents).contains(dropTableIfExistsNewDataQuery);
    assertThat(recordedSqlEvents).contains(createTableQuery);
    assertThat(recordedSqlEvents).contains(insertQuery);
    assertThat(recordedSqlEvents).contains(selectQuery);
    assertThat(recordedSqlEvents).contains(tempTableQuery);
    assertThat(recordedSqlEvents).contains(mergeQuery);

    spark.stop();
  }

  List<String> filterToRecordedQueries(List<OpenLineage.RunEvent> events) {
    List<String> registeredQueries =
        events.stream()
            .filter(
                e -> {
                  if (e.getRun() == null
                      || e.getRun().getFacets() == null
                      || e.getRun().getFacets().getExternalQuery() == null) {
                    return false;
                  }

                  return e.getRun()
                          .getFacets()
                          .getExternalQuery()
                          .getAdditionalProperties()
                          .get("query")
                      != null;
                })
            .map(
                e ->
                    e.getRun()
                        .getFacets()
                        .getExternalQuery()
                        .getAdditionalProperties()
                        .get("query")
                        .toString())
            .collect(Collectors.toList());
    return registeredQueries;
  }
}
