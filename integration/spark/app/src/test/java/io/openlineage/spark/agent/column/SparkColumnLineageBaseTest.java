/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.column;

import static io.openlineage.spark.agent.MockServerUtils.getEventsEmitted;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.ColumnLineageDatasetFacet;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.spark.agent.MockServerUtils;
import io.openlineage.spark.agent.OpenLineageSparkListener;
import io.openlineage.spark.agent.Spark4CompatUtils;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
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
import org.mockserver.integration.ClientAndServer;

/**
 * Base class for column lineage integration tests. Provides common SparkSession setup, MockServer
 * configuration, and helper methods for creating test datasets and verifying column lineage.
 */
@Tag("integration-test")
@Slf4j
public abstract class SparkColumnLineageBaseTest {

  @SuppressWarnings("PMD")
  protected static final String LOCAL_IP = "127.0.0.1";

  protected static SparkSession spark;
  protected static ClientAndServer mockServer;

  /**
   * Subclasses should override this to provide a unique port number for the MockServer to avoid
   * port conflicts when running tests in parallel.
   */
  protected abstract int getMockServerPort();

  /** Subclasses should override this to provide a unique app name for the SparkSession. */
  protected abstract String getAppName();

  @BeforeAll
  @SneakyThrows
  public static void beforeAll() {
    Spark4CompatUtils.cleanupAnyExistingSession();
  }

  @AfterAll
  @SneakyThrows
  public static void afterAll() {
    Spark4CompatUtils.cleanupAnyExistingSession();
    if (mockServer != null) {
      MockServerUtils.stopMockServer(mockServer);
    }
  }

  @BeforeEach
  @SneakyThrows
  public void beforeEach() {
    if (mockServer == null) {
      mockServer = MockServerUtils.createAndConfigureMockServer(getMockServerPort());
    }

    MockServerUtils.clearRequests(mockServer);
    spark =
        SparkSession.builder()
            .master("local[*]")
            .appName(getAppName())
            .config("spark.driver.host", LOCAL_IP)
            .config("spark.driver.bindAddress", LOCAL_IP)
            .config("spark.sql.shuffle.partitions", 1)
            .config("spark.openlineage.transport.type", "http")
            .config(
                "spark.openlineage.transport.url",
                "http://localhost:" + mockServer.getPort() + "/api/v1/lineage")
            .config("spark.openlineage.facets.debug.disabled", "false")
            .config("spark.openlineage.namespace", "column-lineage-test")
            .config("spark.extraListeners", OpenLineageSparkListener.class.getName())
            .getOrCreate();
  }

  /**
   * Helper method to create test dataset with sequential values.
   *
   * @param rows number of rows to create
   * @param offset starting value offset
   */
  protected Dataset<Row> createTempDataset(int rows, int offset) {
    List<Row> rowList =
        Arrays.stream(IntStream.rangeClosed(1 + offset, rows + offset).toArray())
            .mapToObj(i -> RowFactory.create((long) i, (long) i * 10))
            .collect(Collectors.toList());

    return spark
        .createDataFrame(
            rowList,
            new StructType(
                new StructField[] {
                  new StructField("a", LongType$.MODULE$, false, Metadata.empty()),
                  new StructField("b", LongType$.MODULE$, false, Metadata.empty())
                }))
        .repartition(1);
  }

  /**
   * Helper method to create test dataset with string columns.
   *
   * @param rows number of rows to create
   */
  protected Dataset<Row> createTempDatasetWithStrings(int rows) {
    List<Row> rowList =
        Arrays.stream(IntStream.rangeClosed(1, rows).toArray())
            .mapToObj(i -> RowFactory.create((long) i, "value_" + i))
            .collect(Collectors.toList());

    return spark
        .createDataFrame(
            rowList,
            new StructType(
                new StructField[] {
                  new StructField("id", LongType$.MODULE$, false, Metadata.empty()),
                  new StructField("name", StringType$.MODULE$, false, Metadata.empty())
                }))
        .repartition(1);
  }

  /**
   * Helper method to find output dataset with column lineage from emitted events.
   *
   * @param jobNamePattern pattern to match in job name
   * @param outputNameSuffix suffix of output dataset name
   * @return Optional of OutputDataset with column lineage
   */
  protected Optional<OutputDataset> findOutputWithColumnLineage(
      String jobNamePattern, String outputNameSuffix) {
    List<RunEvent> events = getEventsEmitted(mockServer);

    return events.stream()
        .filter(e -> e.getEventType() == RunEvent.EventType.COMPLETE)
        .filter(e -> e.getJob().getName().contains(jobNamePattern))
        .filter(e -> !e.getOutputs().isEmpty())
        .flatMap(e -> e.getOutputs().stream())
        .filter(output -> output.getName().endsWith(outputNameSuffix))
        .filter(output -> output.getFacets() != null)
        .filter(output -> output.getFacets().getColumnLineage() != null)
        .findFirst();
  }

  /**
   * Helper method to log all emitted events and their column lineage status. Useful for debugging
   * test failures.
   */
  protected void logEmittedEvents() {
    List<RunEvent> events = getEventsEmitted(mockServer);
    log.info("===== EMITTED EVENTS ({}) =====", events.size());
    events.forEach(
        event -> {
          log.info(
              "Event type: {}, Job: {}, Inputs: {}, Outputs: {}",
              event.getEventType(),
              event.getJob().getName(),
              event.getInputs().size(),
              event.getOutputs().size());

          // Log input datasets
          event
              .getInputs()
              .forEach(
                  input -> {
                    log.info("  Input dataset: {}", input.getName());
                  });

          // Log output datasets
          event
              .getOutputs()
              .forEach(
                  output -> {
                    log.info("  Output dataset: {}", output.getName());
                    log.info("    Has facets: {}", output.getFacets() != null);
                    if (output.getFacets() != null) {
                      log.info(
                          "    Has columnLineage: {}",
                          output.getFacets().getColumnLineage() != null);
                      if (output.getFacets().getColumnLineage() != null) {
                        OpenLineage.ColumnLineageDatasetFacet columnLineage =
                            output.getFacets().getColumnLineage();
                        log.info(
                            "    Column lineage fields: {}",
                            columnLineage.getFields().getAdditionalProperties().keySet());

                        // Log full column lineage details
                        columnLineage
                            .getFields()
                            .getAdditionalProperties()
                            .forEach(
                                (fieldName, fieldLineage) -> {
                                  log.info("      Column '{}' dependencies:", fieldName);
                                  if (fieldLineage.getInputFields() != null) {
                                    fieldLineage
                                        .getInputFields()
                                        .forEach(
                                            inputField -> {
                                              log.info(
                                                  "        - {}/{} (field: {})",
                                                  inputField.getNamespace(),
                                                  inputField.getName(),
                                                  inputField.getField());
                                              if (inputField.getTransformations() != null
                                                  && !inputField.getTransformations().isEmpty()) {
                                                inputField
                                                    .getTransformations()
                                                    .forEach(
                                                        transformation -> {
                                                          log.info(
                                                              "          transformation: {} - {}",
                                                              transformation.getType(),
                                                              transformation.getSubtype());
                                                        });
                                              }
                                            });
                                  }
                                });
                      }
                    }
                  });
        });
    log.info("===== END EMITTED EVENTS =====");
  }

  /**
   * Helper method to verify that column lineage was extracted for a given output.
   *
   * @param jobNamePattern pattern to match in job name
   * @param outputNameSuffix suffix of output dataset name
   * @return ColumnLineageDatasetFacet for further assertions
   */
  protected ColumnLineageDatasetFacet verifyColumnLineageExists(
      String jobNamePattern, String outputNameSuffix) {
    Optional<OutputDataset> outputDataset =
        findOutputWithColumnLineage(jobNamePattern, outputNameSuffix);

    if (!outputDataset.isPresent()) {
      logEmittedEvents();
      throw new AssertionError(
          "No output dataset with column lineage found for pattern: "
              + jobNamePattern
              + ", suffix: "
              + outputNameSuffix);
    }

    return outputDataset.get().getFacets().getColumnLineage();
  }

  /**
   * Helper method to verify that a column has expected input field dependencies.
   *
   * @param columnLineage the column lineage facet
   * @param outputColumnName the output column to check
   * @param expectedInputFieldNames expected input field names (just the field name, not full path)
   */
  protected void verifyColumnDependencies(
      ColumnLineageDatasetFacet columnLineage,
      String outputColumnName,
      String... expectedInputFieldNames) {
    OpenLineage.ColumnLineageDatasetFacetFieldsAdditional fieldLineage =
        columnLineage.getFields().getAdditionalProperties().get(outputColumnName);

    if (fieldLineage == null) {
      throw new AssertionError(
          "Column '"
              + outputColumnName
              + "' not found in column lineage. "
              + "Available columns: "
              + columnLineage.getFields().getAdditionalProperties().keySet());
    }

    if (fieldLineage.getInputFields() == null || fieldLineage.getInputFields().isEmpty()) {
      throw new AssertionError("Column '" + outputColumnName + "' has no input field dependencies");
    }

    List<String> actualInputFields =
        fieldLineage.getInputFields().stream()
            .map(OpenLineage.InputField::getField)
            .collect(Collectors.toList());

    List<String> expectedFields = Arrays.asList(expectedInputFieldNames);

    if (!actualInputFields.containsAll(expectedFields)) {
      throw new AssertionError(
          "Column '"
              + outputColumnName
              + "' expected dependencies "
              + expectedFields
              + " but got "
              + actualInputFields);
    }
  }
}
