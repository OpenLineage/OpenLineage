/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.SparkTestUtils.createHttpServer;
import static io.openlineage.spark.agent.SparkTestUtils.createSparkSession;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.sun.net.httpserver.HttpServer;
import io.openlineage.client.OpenLineage;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
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

@Slf4j
@Tag("integration-test")
class NoOutputSparkActionTest {
  private static final SparkTestUtils.OpenLineageEndpointHandler handler =
      new SparkTestUtils.OpenLineageEndpointHandler();

  private final String ParquetFileLocation =
      Paths.get(System.getProperty("user.dir"), "parquet-file-path-1").toString();

  @AfterEach
  void tearDown() {
    try {
      FileUtils.deleteDirectory(new File(ParquetFileLocation.toString()));
    } catch (IOException e) {
      log.error("Failed to delete directory", e);
    }
  }

  @Test
  void testInvalidInputOutputWhenSavingToParquetAndThenShowingOnlySchema() throws IOException {
    HttpServer server = createHttpServer(handler);

    SparkSession spark =
        createSparkSession(
            server.getAddress().getPort(),
            "testInvalidInputOutputWhenSavingToParquetAndThenShowingOnlySchema");

    Dataset<Row> dataFrame = createDataFrame(spark);

    dataFrame.write().parquet(ParquetFileLocation);

    spark.read().parquet(ParquetFileLocation).printSchema();

    List<OpenLineage.RunEvent> events =
        handler.events.get(
            "test_invalid_input_output_when_saving_to_parquet_and_then_showing_only_schema");

    // there should not be any event with input dataset available
    events.forEach(
        event -> {
          assertEquals(0, event.getInputs().size());
        });

    List<OpenLineage.OutputDataset> nonEmptyOutputs =
        events.stream()
            .map(OpenLineage.RunEvent::getOutputs)
            .filter(outputs -> !outputs.isEmpty())
            .flatMap(List::stream)
            .collect(Collectors.toList());

    // also there should be at least one event with output dataset
    assertThat(nonEmptyOutputs.size()).isGreaterThan(0);

    assertThat(nonEmptyOutputs.get(0).getName()).isEqualTo(ParquetFileLocation);

    spark.stop();
  }

  private Dataset<Row> createDataFrame(SparkSession spark) {
    ArrayList<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create("Alice"));
    rows.add(RowFactory.create("Bob"));
    rows.add(RowFactory.create("Charlie"));

    StructType structType = new StructType().add("name", "string");
    return spark.createDataFrame(rows, structType);
  }
}
