/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.SparkTestUtils.OpenLineageEndpointHandler;
import static io.openlineage.spark.agent.SparkTestUtils.SPARK_3_3_AND_ABOVE;
import static io.openlineage.spark.agent.SparkTestUtils.SPARK_VERSION;
import static io.openlineage.spark.agent.SparkTestUtils.createHttpServer;
import static io.openlineage.spark.agent.SparkTestUtils.createSparkSession;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.SparkTestUtils.OpenLineageEndpointHandler;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
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
class SparkSqlQueryRecordingTest {

  private final String DeltaLakeLocation = "/tmp/delta-table/";

  private static final OpenLineageEndpointHandler handler = new OpenLineageEndpointHandler();

  @AfterEach
  void tearDown() throws IOException {
    FileUtils.deleteDirectory(new File(DeltaLakeLocation));
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3_3_AND_ABOVE)
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

    spark.sql(queryA).write().mode("overwrite").saveAsTable("temp3");

    spark.sql(queryB).write().mode("overwrite").saveAsTable("temp2");
    spark.stop();

    List<OpenLineage.RunEvent> events =
        handler.events.getOrDefault("test_reading_sql_query", new ArrayList<>());

    // we have two queries so at least one should be properly recorded
    assertThat(events).hasSizeGreaterThanOrEqualTo(2);

    List<String> registeredQueries = filterToRecordedQueries(events);

    assertThat(registeredQueries).hasSizeGreaterThanOrEqualTo(2);

    assertThat(registeredQueries).contains(queryA);
    assertThat(registeredQueries).contains(queryB);
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3_3_AND_ABOVE)
  void testMultipartQuery() throws IOException {
    HttpServer server = createHttpServer(handler);

    SparkSession spark = createSparkSession(server.getAddress().getPort(), "testMultipartQuery");
    Dataset<Row> df = spark.sql("SELECT 1 as id");

    df.createOrReplaceTempView("table1");
    String queryA = "SELECT * FROM table1 UNION ALL SELECT 2 as id";

    spark.sql(queryA).createOrReplaceTempView("table2");

    // that query is recorded as it's the last in chain
    String groupByQuery = "SELECT id, count(*) as count FROM table2 GROUP BY id";

    spark.sql(groupByQuery).write().mode("overwrite").saveAsTable("temp3");

    // that should not be recorded
    spark.sql("SELECT 1").show();

    List<OpenLineage.RunEvent> events =
        handler.events.getOrDefault("test_multipart_query", new ArrayList<>());

    List<String> recordedSqlEvents = filterToRecordedQueries(events);

    assertThat(recordedSqlEvents).contains(groupByQuery);
    spark.stop();
  }

  List<String> filterToRecordedQueries(List<OpenLineage.RunEvent> events) {
    List<String> registeredQueries =
        events.stream()
            .filter(
                e -> {
                  if (e.getJob() == null
                      || e.getJob().getFacets() == null
                      || e.getJob().getFacets().getSql() == null) {
                    return false;
                  }

                  return e.getJob().getFacets().getSql().getQuery() != null;
                })
            .map(e -> e.getJob().getFacets().getSql().getQuery())
            .collect(Collectors.toList());
    return registeredQueries;
  }
}
