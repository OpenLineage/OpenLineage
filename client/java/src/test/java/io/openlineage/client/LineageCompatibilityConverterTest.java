/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.client.OpenLineage.JobEvent;
import io.openlineage.client.OpenLineage.RunEvent;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

class LineageCompatibilityConverterTest {
  private static final ObjectMapper MAPPER = OpenLineageClientUtils.newObjectMapper();

  @Test
  @SneakyThrows
  void legacyModeCreatesLegacyDatasetsAndRepresentableColumnLineage() {
    RunEvent original =
        runEvent(
            "{\"lineage\":{"
                + "\"_producer\":\"https://lineage-producer.example\","
                + "\"entries\":["
                + "{\"namespace\":\"out\",\"name\":\"orders\",\"type\":\"DATASET\","
                + "\"inputs\":["
                + datasetInput("in", "raw", null)
                + ","
                + datasetInput("in", "raw", "filter_id")
                + ","
                + "{\"namespace\":\"jobs\",\"name\":\"source\",\"type\":\"JOB\"}],"
                + "\"fields\":{\"id\":{\"inputs\":["
                + transformedDatasetInput("in", "raw", "source_id")
                + ","
                + "{\"namespace\":\"jobs\",\"name\":\"source\",\"type\":\"JOB\"}]}}},"
                + "{\"namespace\":\"out\",\"name\":\"orders\",\"type\":\"DATASET\","
                + "\"inputs\":["
                + datasetInput("in", "secondary", null)
                + "]},"
                + "{\"namespace\":\"jobs\",\"name\":\"target\",\"type\":\"JOB\","
                + "\"inputs\":["
                + datasetInput("in", "job-source", null)
                + "]}]}}",
            "[]",
            "[{\"namespace\":\"out\",\"name\":\"orders\"}]");
    JsonNode originalJson = MAPPER.valueToTree(original);

    RunEvent converted =
        LineageCompatibilityConverter.convert(original, LineageCompatibility.LEGACY);
    JsonNode convertedJson = MAPPER.valueToTree(converted);

    assertThat(convertedJson.get("inputs"))
        .isEqualTo(
            MAPPER.readTree(
                "[{\"namespace\":\"in\",\"name\":\"raw\"},"
                    + "{\"namespace\":\"in\",\"name\":\"secondary\"},"
                    + "{\"namespace\":\"in\",\"name\":\"job-source\"}]"));
    assertThat(convertedJson.get("outputs")).hasSize(1);

    JsonNode columnLineage = convertedJson.at("/outputs/0/facets/columnLineage");
    assertThat(columnLineage.get("_producer").asText())
        .isEqualTo("https://lineage-producer.example");
    assertThat(columnLineage.get("_schemaURL").asText()).contains("ColumnLineageDatasetFacet");
    assertThat(columnLineage.get("dataset"))
        .isEqualTo(
            MAPPER.readTree("[{\"namespace\":\"in\",\"name\":\"raw\",\"field\":\"filter_id\"}]"));
    assertThat(columnLineage.at("/fields/id/inputFields/0/field").asText()).isEqualTo("source_id");
    assertThat(columnLineage.at("/fields/id/inputFields/0/transformations/0/subtype").asText())
        .isEqualTo("IDENTITY");
    assertThat(originalJson).isEqualTo(MAPPER.valueToTree(original));
    JsonNode convertedAgain =
        MAPPER.valueToTree(
            LineageCompatibilityConverter.convert(converted, LineageCompatibility.LEGACY));
    assertThat(convertedAgain).isEqualTo(convertedJson);
  }

  @Test
  @SneakyThrows
  void legacyModeMergesWithoutOverwritingProducerProvidedColumnLineage() {
    String modernInput = datasetInput("in", "raw", "source_id");
    String existingInput = "{\"namespace\":\"in\",\"name\":\"existing\",\"field\":\"existing_id\"}";
    RunEvent event =
        runEvent(
            "{\"lineage\":{\"_producer\":\"https://lineage-producer.example\","
                + "\"entries\":[{\"namespace\":\"out\",\"name\":\"orders\","
                + "\"type\":\"DATASET\",\"inputs\":[],\"fields\":{\"id\":{\"inputs\":["
                + modernInput
                + "]}}}]}}",
            "[]",
            "[{\"namespace\":\"out\",\"name\":\"orders\",\"facets\":{"
                + "\"columnLineage\":{\"_producer\":\"https://existing-producer.example\","
                + "\"fields\":{\"id\":{\"inputFields\":["
                + existingInput
                + "]}}}}}]");

    JsonNode converted =
        MAPPER.valueToTree(
            LineageCompatibilityConverter.convert(event, LineageCompatibility.LEGACY));
    JsonNode columnLineage = converted.at("/outputs/0/facets/columnLineage");

    assertThat(columnLineage.get("_producer").asText())
        .isEqualTo("https://existing-producer.example");
    assertThat(columnLineage.at("/fields/id/inputFields"))
        .isEqualTo(MAPPER.readTree("[" + existingInput + "," + legacyInput(modernInput) + "]"));
  }

  @Test
  @SneakyThrows
  void modernModeCreatesAndMergesEntriesFromLegacyLineage() {
    RunEvent original =
        runEvent(
            "{}",
            "[{\"namespace\":\"in\",\"name\":\"raw\"},"
                + "{\"namespace\":\"in\",\"name\":\"lookup\"}]",
            "[{\"namespace\":\"out\",\"name\":\"orders\",\"facets\":{"
                + "\"columnLineage\":{\"fields\":{\"id\":{\"inputFields\":["
                + transformedLegacyInput("in", "raw", "source_id")
                + "]}},\"dataset\":["
                + transformedLegacyInput("in", "raw", "filter_id")
                + "]}}},"
                + "{\"namespace\":\"out\",\"name\":\"report\"},"
                + "{\"namespace\":\"out\",\"name\":\"orders\",\"facets\":{"
                + "\"columnLineage\":{\"fields\":{\"id\":{\"inputFields\":["
                + legacyInputField("in", "lookup", "lookup_id")
                + "]}}}}}]");
    JsonNode originalJson = MAPPER.valueToTree(original);

    RunEvent converted =
        LineageCompatibilityConverter.convert(original, LineageCompatibility.MODERN);
    JsonNode convertedJson = MAPPER.valueToTree(converted);
    JsonNode lineage = convertedJson.at("/job/facets/lineage");

    assertThat(lineage.get("_producer").asText()).isEqualTo("https://producer.example");
    assertThat(lineage.get("entries")).hasSize(2);
    assertThat(lineage.at("/entries/0/name").asText()).isEqualTo("orders");
    assertThat(lineage.at("/entries/1/name").asText()).isEqualTo("report");
    assertThat(lineage.at("/entries/0/inputs")).hasSize(3);
    assertThat(lineage.at("/entries/0/inputs/0/field").isMissingNode()).isTrue();
    assertThat(lineage.at("/entries/0/inputs/2/field").asText()).isEqualTo("filter_id");
    assertThat(lineage.at("/entries/0/fields/id/inputs")).hasSize(2);
    assertThat(lineage.at("/entries/0/fields/id/inputs/0/field").asText()).isEqualTo("source_id");
    assertThat(lineage.at("/entries/0/fields/id/inputs/1/field").asText()).isEqualTo("lookup_id");
    assertThat(lineage.at("/entries/0/fields/id/inputs/0/transformations/0/type").asText())
        .isEqualTo("DIRECT");
    assertThat(lineage.at("/entries/1/inputs")).hasSize(2);
    assertThat(originalJson).isEqualTo(MAPPER.valueToTree(original));

    RunEvent convertedAgain =
        LineageCompatibilityConverter.convert(converted, LineageCompatibility.BOTH);
    JsonNode convertedAgainJson = MAPPER.valueToTree(convertedAgain);
    assertThat(convertedAgainJson).isEqualTo(convertedJson);
  }

  @Test
  void modernModeSupportsJobEvents() {
    JobEvent event =
        jobEvent(
            "{}",
            "[{\"namespace\":\"in\",\"name\":\"raw\"}]",
            "[{\"namespace\":\"out\",\"name\":\"orders\"}]");

    JobEvent converted = LineageCompatibilityConverter.convert(event, LineageCompatibility.MODERN);

    assertThat(MAPPER.valueToTree(converted).at("/job/facets/lineage/entries")).hasSize(1);
  }

  @Test
  void translationModesDoNotReplaceProducerProvidedLineageOrCreateSinks() {
    RunEvent modernEvent =
        runEvent(
            "{\"lineage\":{\"entries\":[]}}",
            "[]",
            "[{\"namespace\":\"out\",\"name\":\"orders\"}]");
    RunEvent sinkEvent = runEvent("{}", "[]", "[]");

    assertThat(LineageCompatibilityConverter.convert(modernEvent, LineageCompatibility.MODERN))
        .isSameAs(modernEvent);
    assertThat(LineageCompatibilityConverter.convert(sinkEvent, LineageCompatibility.MODERN))
        .isSameAs(sinkEvent);
    assertThat(LineageCompatibilityConverter.convert(modernEvent, LineageCompatibility.NONE))
        .isSameAs(modernEvent);
  }

  @SneakyThrows
  private static RunEvent runEvent(String jobFacets, String inputs, String outputs) {
    return MAPPER.readValue(
        "{\"eventTime\":\"2026-08-18T10:00:00Z\","
            + "\"producer\":\"https://producer.example\",\"eventType\":\"COMPLETE\","
            + "\"run\":{\"runId\":\"11111111-1111-1111-1111-111111111111\"},"
            + "\"job\":{\"namespace\":\"jobs\",\"name\":\"example\",\"facets\":"
            + jobFacets
            + "},\"inputs\":"
            + inputs
            + ",\"outputs\":"
            + outputs
            + "}",
        RunEvent.class);
  }

  @SneakyThrows
  private static JobEvent jobEvent(String jobFacets, String inputs, String outputs) {
    return MAPPER.readValue(
        "{\"eventTime\":\"2026-08-18T10:00:00Z\","
            + "\"producer\":\"https://producer.example\","
            + "\"job\":{\"namespace\":\"jobs\",\"name\":\"example\",\"facets\":"
            + jobFacets
            + "},\"inputs\":"
            + inputs
            + ",\"outputs\":"
            + outputs
            + "}",
        JobEvent.class);
  }

  private static String datasetInput(String namespace, String name, String field) {
    return "{\"namespace\":\""
        + namespace
        + "\",\"name\":\""
        + name
        + "\",\"type\":\"DATASET\""
        + (field == null ? "" : ",\"field\":\"" + field + "\"")
        + "}";
  }

  private static String transformedDatasetInput(String namespace, String name, String field) {
    return datasetInput(namespace, name, field)
        .replace("}", ",\"transformations\":[{\"type\":\"DIRECT\",\"subtype\":\"IDENTITY\"}]}");
  }

  private static String legacyInput(String modernInput) {
    return modernInput.replace(",\"type\":\"DATASET\"", "");
  }

  private static String legacyInputField(String namespace, String name, String field) {
    return legacyInput(datasetInput(namespace, name, field));
  }

  private static String transformedLegacyInput(String namespace, String name, String field) {
    return legacyInput(transformedDatasetInput(namespace, name, field));
  }
}
