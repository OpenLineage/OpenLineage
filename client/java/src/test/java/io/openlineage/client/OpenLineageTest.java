/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.openlineage.client.OpenLineage.DataQualityMetricsInputDatasetFacet;
import io.openlineage.client.OpenLineage.DataQualityMetricsInputDatasetFacetColumnMetricsAdditional;
import io.openlineage.client.OpenLineage.DatasetEvent;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.Job;
import io.openlineage.client.OpenLineage.JobEvent;
import io.openlineage.client.OpenLineage.JobFacets;
import io.openlineage.client.OpenLineage.NominalTimeRunFacet;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.OutputStatisticsOutputDatasetFacet;
import io.openlineage.client.OpenLineage.Run;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunFacets;
import io.openlineage.client.OpenLineage.SchemaDatasetFacet;
import io.openlineage.server.OpenLineage.OutputDatasetFacet;
import java.net.URI;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OpenLineageTest {

  ObjectMapper mapper = new ObjectMapper();
  ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);

  @BeforeEach
  void setup() {
    mapper.registerModule(new JavaTimeModule());
    mapper.setSerializationInclusion(Include.NON_NULL);
    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
    mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
  }

  @Test
  void jsonRunEventSerialization() throws JsonProcessingException {
    URI producer = URI.create("producer");
    OpenLineage ol = new OpenLineage(producer);
    UUID runId = UUID.randomUUID();
    RunFacets runFacets =
        ol.newRunFacetsBuilder().nominalTime(ol.newNominalTimeRunFacet(now, now)).build();
    Run run = ol.newRun(runId, runFacets);
    String name = "jobName";
    String namespace = "namespace";
    JobFacets jobFacets = ol.newJobFacetsBuilder().build();
    Job job = ol.newJob(namespace, name, jobFacets);
    List<InputDataset> inputs = Arrays.asList(ol.newInputDataset("ins", "input", null, null));
    List<OutputDataset> outputs = Arrays.asList(ol.newOutputDataset("ons", "output", null, null));
    RunEvent runStateUpdate =
        ol.newRunEvent(now, OpenLineage.RunEvent.EventType.START, run, job, inputs, outputs);

    String json = mapper.writeValueAsString(runStateUpdate);
    RunEvent read = mapper.readValue(json, RunEvent.class);

    assertEquals(producer, read.getProducer());
    assertEquals(runId, read.getRun().getRunId());
    assertEquals(name, read.getJob().getName());
    assertEquals(namespace, read.getJob().getNamespace());
    assertEquals(runStateUpdate.getEventType(), read.getEventType());
    assertEquals(runStateUpdate.getEventTime(), read.getEventTime());
    assertEquals(1, runStateUpdate.getInputs().size());
    NominalTimeRunFacet nominalTime = runStateUpdate.getRun().getFacets().getNominalTime();
    assertEquals(now, nominalTime.getNominalStartTime());
    assertEquals(now, nominalTime.getNominalEndTime());
    InputDataset inputDataset = runStateUpdate.getInputs().get(0);
    assertEquals("ins", inputDataset.getNamespace());
    assertEquals("input", inputDataset.getName());
    assertEquals(1, runStateUpdate.getOutputs().size());
    OutputDataset outputDataset = runStateUpdate.getOutputs().get(0);
    assertEquals("ons", outputDataset.getNamespace());
    assertEquals("output", outputDataset.getName());

    assertEquals(roundTrip(json), roundTrip(mapper.writeValueAsString(read)));
  }

  @Test
  void jsonDatasetEventSerialization() throws JsonProcessingException {
    URI producer = URI.create("producer");
    OpenLineage ol = new OpenLineage(producer);

    DatasetEvent datasetEvent =
        ol.newDatasetEventBuilder()
            .eventTime(now)
            .dataset(
                ol.newStaticDataset(
                    "ns",
                    "ds",
                    ol.newDatasetFacetsBuilder()
                        .documentation(ol.newDocumentationDatasetFacet("foo"))
                        .build()))
            .build();

    String json = mapper.writeValueAsString(datasetEvent);
    DatasetEvent read = mapper.readValue(json, DatasetEvent.class);

    assertEquals("ns", read.getDataset().getNamespace());
    assertEquals("ds", read.getDataset().getName());
    assertEquals(now, read.getEventTime());
    assertEquals("foo", read.getDataset().getFacets().getDocumentation().getDescription());
  }

  @Test
  void jsonDatasetEventDeleteFacet() throws JsonProcessingException {
    URI producer = URI.create("producer");
    OpenLineage ol = new OpenLineage(producer);
    final String documentation = "documentation";

    DatasetEvent datasetEvent =
        ol.newDatasetEventBuilder()
            .eventTime(now)
            .dataset(
                ol.newStaticDataset(
                    "ns",
                    "ds",
                    ol.newDatasetFacetsBuilder()
                        .put(documentation, ol.newDocumentationDatasetFacet("foo"))
                        .build()))
            .build();

    assertNull(
        datasetEvent
            .getDataset()
            .getFacets()
            .getAdditionalProperties()
            .get(documentation)
            .get_deleted());

    String json1 = mapper.writeValueAsString(datasetEvent);
    DatasetEvent read1 = mapper.readValue(json1, DatasetEvent.class);
    assertNull(read1.getDataset().getFacets().getDocumentation().get_deleted());

    datasetEvent
        .getDataset()
        .getFacets()
        .getAdditionalProperties()
        .put(documentation, ol.newDeletedDatasetFacet());

    String json = mapper.writeValueAsString(datasetEvent);
    DatasetEvent read = mapper.readValue(json, DatasetEvent.class);

    assertEquals("ns", read.getDataset().getNamespace());
    assertEquals("ds", read.getDataset().getName());
    assertEquals(now, read.getEventTime());
    assertEquals(Boolean.TRUE, read.getDataset().getFacets().getDocumentation().get_deleted());
    assertEquals(Boolean.TRUE, read.getDataset().getFacets().getDocumentation().isDeleted());
  }

  @Test
  void jsonJobEventSerialization() throws JsonProcessingException {
    URI producer = URI.create("producer");
    OpenLineage ol = new OpenLineage(producer);
    JobEvent jobEvent =
        ol.newJobEventBuilder()
            .eventTime(now)
            .inputs(Collections.singletonList(ol.newInputDataset("ins", "input", null, null)))
            .outputs(Collections.singletonList(ol.newOutputDataset("ous", "output", null, null)))
            .job(ol.newJob("jns", "jname", null))
            .build();

    String json = mapper.writeValueAsString(jobEvent);
    JobEvent read = mapper.readValue(json, JobEvent.class);

    assertEquals("ins", read.getInputs().get(0).getNamespace());
    assertEquals("input", read.getInputs().get(0).getName());

    assertEquals("ous", read.getOutputs().get(0).getNamespace());
    assertEquals("output", read.getOutputs().get(0).getName());

    assertEquals("jns", read.getJob().getNamespace());
    assertEquals("jname", read.getJob().getName());
    assertEquals(now, read.getEventTime());
  }

  String roundTrip(String value) throws JsonMappingException, JsonProcessingException {
    return mapper.writeValueAsString(mapper.readValue(value, Object.class));
  }

  @Test
  void factory() throws JsonProcessingException {
    URI producer = URI.create("producer");
    OpenLineage ol = new OpenLineage(producer);
    UUID runId = UUID.randomUUID();
    UUID parentId = UUID.randomUUID();
    UUID rootParentId = UUID.randomUUID();

    RunFacets runFacets =
        ol.newRunFacetsBuilder()
            .nominalTime(
                ol.newNominalTimeRunFacetBuilder()
                    .nominalStartTime(now)
                    .nominalEndTime(now)
                    .build())
            .parent(
                ol.newParentRunFacetBuilder()
                    .job(ol.newParentRunFacetJob("parent-namespace", "parent-name"))
                    .run(ol.newParentRunFacetRun(parentId))
                    .root(
                        ol.newParentRunFacetRoot(
                            ol.newRootRun(rootParentId),
                            ol.newRootJob("root-namespace", "root-job-name")))
                    .build())
            .build();
    Run run = ol.newRunBuilder().runId(runId).facets(runFacets).build();
    String name = "jobName";
    String namespace = "namespace";
    JobFacets jobFacets = ol.newJobFacetsBuilder().build();
    Job job = ol.newJobBuilder().namespace(namespace).name(name).facets(jobFacets).build();

    List<InputDataset> inputs =
        Arrays.asList(
            ol.newInputDatasetBuilder()
                .namespace("ins")
                .name("input")
                .facets(
                    ol.newDatasetFacetsBuilder()
                        .version(ol.newDatasetVersionDatasetFacet("input-version"))
                        .build())
                .inputFacets(
                    ol.newInputDatasetInputFacetsBuilder()
                        .dataQualityMetrics(
                            ol.newDataQualityMetricsInputDatasetFacetBuilder()
                                .rowCount(10L)
                                .bytes(20L)
                                .fileCount(5L)
                                .columnMetrics(
                                    ol.newDataQualityMetricsInputDatasetFacetColumnMetricsBuilder()
                                        .put(
                                            "mycol",
                                            ol.newDataQualityMetricsInputDatasetFacetColumnMetricsAdditionalBuilder()
                                                .count(10D)
                                                .distinctCount(10L)
                                                .max(30D)
                                                .min(5D)
                                                .nullCount(1L)
                                                .sum(3000D)
                                                .quantiles(
                                                    ol.newDataQualityMetricsInputDatasetFacetColumnMetricsAdditionalQuantilesBuilder()
                                                        .put("25", 52D)
                                                        .build())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build());

    SchemaDatasetFacet schemaFacet =
        ol.newSchemaDatasetFacetBuilder()
            .fields(
                Arrays.asList(
                    ol.newSchemaDatasetFacetFieldsBuilder().name("user_id").type("int64").build(),
                    ol.newSchemaDatasetFacetFieldsBuilder()
                        .name("phones")
                        .type("array")
                        .description("List of phone numbers")
                        .fields(
                            Arrays.asList(
                                ol.newSchemaDatasetFacetFieldsBuilder()
                                    .name("_element")
                                    .type("string")
                                    .build()))
                        .build(),
                    ol.newSchemaDatasetFacetFieldsBuilder()
                        .name("addresses")
                        .type("struct")
                        .description("Has customer completed activation process")
                        .fields(
                            Arrays.asList(
                                ol.newSchemaDatasetFacetFieldsBuilder()
                                    .name("type")
                                    .type("string")
                                    .description("Address type, g.e. home, work, etc.")
                                    .build(),
                                ol.newSchemaDatasetFacetFieldsBuilder()
                                    .name("country")
                                    .type("string")
                                    .description("Country name")
                                    .build(),
                                ol.newSchemaDatasetFacetFieldsBuilder()
                                    .name("zip")
                                    .type("string")
                                    .description("Zip code")
                                    .build(),
                                ol.newSchemaDatasetFacetFieldsBuilder()
                                    .name("state")
                                    .type("string")
                                    .description("State name")
                                    .build(),
                                ol.newSchemaDatasetFacetFieldsBuilder()
                                    .name("street")
                                    .type("string")
                                    .description("Street name")
                                    .build()))
                        .build(),
                    ol.newSchemaDatasetFacetFieldsBuilder()
                        .name("custom_properties")
                        .type("map")
                        .fields(
                            Arrays.asList(
                                ol.newSchemaDatasetFacetFieldsBuilder()
                                    .name("key")
                                    .type("string")
                                    .build(),
                                ol.newSchemaDatasetFacetFieldsBuilder()
                                    .name("value")
                                    .type("union")
                                    .fields(
                                        Arrays.asList(
                                            ol.newSchemaDatasetFacetFieldsBuilder()
                                                .name("_0")
                                                .type("string")
                                                .build(),
                                            ol.newSchemaDatasetFacetFieldsBuilder()
                                                .name("_1")
                                                .type("int64")
                                                .build()))
                                    .build()))
                        .build()))
            .build();

    List<OutputDataset> outputs =
        Arrays.asList(
            ol.newOutputDatasetBuilder()
                .namespace("ons")
                .name("output")
                .facets(
                    ol.newDatasetFacetsBuilder()
                        .version(ol.newDatasetVersionDatasetFacet("output-version"))
                        .schema(schemaFacet)
                        .build())
                .outputFacets(
                    ol.newOutputDatasetOutputFacetsBuilder()
                        .outputStatistics(
                            ol.newOutputStatisticsOutputDatasetFacetBuilder()
                                .rowCount(10L)
                                .size(20L)
                                .fileCount(5L)
                                .build())
                        .build())
                .build());

    RunEvent runStateUpdate =
        ol.newRunEventBuilder()
            .eventType(OpenLineage.RunEvent.EventType.START)
            .eventTime(now)
            .run(run)
            .job(job)
            .inputs(inputs)
            .outputs(outputs)
            .build();
    ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(new JavaTimeModule());
    mapper.setSerializationInclusion(Include.NON_NULL);
    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
    String json = mapper.writeValueAsString(runStateUpdate);
    {
      RunEvent read = mapper.readValue(json, RunEvent.class);

      assertEquals(producer, read.getProducer());
      assertEquals(runId, read.getRun().getRunId());
      assertEquals(name, read.getJob().getName());
      assertEquals(namespace, read.getJob().getNamespace());
      assertEquals(runStateUpdate.getEventType(), read.getEventType());
      assertEquals(runStateUpdate.getEventTime(), read.getEventTime());
      assertEquals(1, runStateUpdate.getInputs().size());

      OpenLineage.ParentRunFacet parentRun = runStateUpdate.getRun().getFacets().getParent();
      assertEquals(parentId, parentRun.getRun().getRunId());
      assertEquals("parent-namespace", parentRun.getJob().getNamespace());
      assertEquals("parent-name", parentRun.getJob().getName());
      assertEquals(rootParentId, parentRun.getRoot().getRun().getRunId());
      assertEquals("root-namespace", parentRun.getRoot().getJob().getNamespace());
      assertEquals("root-job-name", parentRun.getRoot().getJob().getName());

      InputDataset inputDataset = runStateUpdate.getInputs().get(0);
      assertEquals("ins", inputDataset.getNamespace());
      assertEquals("input", inputDataset.getName());
      assertEquals("input-version", inputDataset.getFacets().getVersion().getDatasetVersion());

      DataQualityMetricsInputDatasetFacet dq =
          inputDataset.getInputFacets().getDataQualityMetrics();
      assertEquals(10L, dq.getRowCount());
      assertEquals(20L, dq.getBytes());
      assertEquals(5L, dq.getFileCount());
      DataQualityMetricsInputDatasetFacetColumnMetricsAdditional colMetrics =
          dq.getColumnMetrics().getAdditionalProperties().get("mycol");
      assertEquals(10D, colMetrics.getCount());
      assertEquals(10L, colMetrics.getDistinctCount());
      assertEquals(30D, colMetrics.getMax());
      assertEquals(5D, colMetrics.getMin());
      assertEquals(1L, colMetrics.getNullCount());
      assertEquals(3000D, colMetrics.getSum());
      assertEquals(52D, colMetrics.getQuantiles().getAdditionalProperties().get("25"));

      assertEquals(1, runStateUpdate.getOutputs().size());
      OutputDataset outputDataset = runStateUpdate.getOutputs().get(0);
      assertEquals("ons", outputDataset.getNamespace());
      assertEquals("output", outputDataset.getName());
      assertEquals("output-version", outputDataset.getFacets().getVersion().getDatasetVersion());

      SchemaDatasetFacet outputDatasetSchema = outputDataset.getFacets().getSchema();
      assertEquals(outputDatasetSchema, schemaFacet);

      assertEquals(10L, outputDataset.getOutputFacets().getOutputStatistics().getRowCount());
      assertEquals(20L, outputDataset.getOutputFacets().getOutputStatistics().getSize());
      assertEquals(5L, outputDataset.getOutputFacets().getOutputStatistics().getFileCount());

      assertEquals(roundTrip(json), roundTrip(mapper.writeValueAsString(read)));
      assertEquals(json, mapper.writeValueAsString(read));
    }

    {
      io.openlineage.server.OpenLineage.RunEvent readServer =
          mapper.readValue(json, io.openlineage.server.OpenLineage.RunEvent.class);

      assertEquals(producer, readServer.getProducer());
      assertEquals(runId, readServer.getRun().getRunId());
      assertEquals(name, readServer.getJob().getName());
      assertEquals(namespace, readServer.getJob().getNamespace());
      assertEquals(runStateUpdate.getEventType().name(), readServer.getEventType().name());
      assertEquals(runStateUpdate.getEventTime(), readServer.getEventTime());

      OutputDatasetFacet outputStatistics =
          readServer
              .getOutputs()
              .get(0)
              .getOutputFacets()
              .getAdditionalProperties()
              .get("outputStatistics");
      OutputStatisticsOutputDatasetFacet translated =
          mapper.convertValue(
              mapper.valueToTree(outputStatistics), OutputStatisticsOutputDatasetFacet.class);
      assertEquals((Long) 10L, translated.getRowCount());
      assertEquals((Long) 20L, translated.getSize());

      assertEquals(json, mapper.writeValueAsString(readServer));
    }
  }
}
