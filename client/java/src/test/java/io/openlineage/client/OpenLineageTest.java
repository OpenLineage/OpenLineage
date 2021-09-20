package io.openlineage.client;

import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.openlineage.client.OpenLineage.DataQualityMetricsInputDatasetFacet;
import io.openlineage.client.OpenLineage.DataQualityMetricsInputDatasetFacetColumnMetricsAdditional;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.Job;
import io.openlineage.client.OpenLineage.JobFacets;
import io.openlineage.client.OpenLineage.NominalTimeRunFacet;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.Run;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunFacets;

public class OpenLineageTest {

  ObjectMapper mapper = new ObjectMapper();
  {
    mapper.registerModule(new JavaTimeModule());
    mapper.setSerializationInclusion(Include.NON_NULL);
    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
    mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
  }

  @Test
  public void jsonSerialization() throws JsonProcessingException {
    ZonedDateTime now = ZonedDateTime.now(ZoneId.of("UTC"));

    URI producer = URI.create("producer");
    OpenLineage ol = new OpenLineage(producer);
    UUID runId = UUID.randomUUID();
    RunFacets runFacets = ol.newRunFacetsBuilder().nominalTime(ol.newNominalTimeRunFacet(now, now)).build();
    Run run = ol.newRun(runId, runFacets);
    String name = "jobName";
    String namespace = "namespace";
    JobFacets jobFacets = ol.newJobFacetsBuilder().build();
    Job job = ol.newJob(namespace, name, jobFacets);
    List<InputDataset> inputs = Arrays.asList(ol.newInputDataset("ins", "input", null, null));
    List<OutputDataset> outputs = Arrays.asList(ol.newOutputDataset("ons", "output", null, null));
    RunEvent runStateUpdate = ol.newRunEvent("START", now, run, job, inputs, outputs);


    String json = mapper.writeValueAsString(runStateUpdate);
    RunEvent read = mapper.readValue(json, RunEvent.class);

    assertEquals(producer,read.getProducer());
    assertEquals(runId,read.getRun().getRunId());
    assertEquals(name,read.getJob().getName());
    assertEquals(namespace,read.getJob().getNamespace());
    assertEquals(runStateUpdate.getEventType(),read.getEventType());
    assertEquals(runStateUpdate.getEventTime(),read.getEventTime());
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

  String roundTrip(String value) throws JsonMappingException, JsonProcessingException {
    return mapper.writeValueAsString(mapper.readValue(value, Object.class));
  }

  @Test
  public void factory() throws JsonProcessingException {
    ZonedDateTime now = ZonedDateTime.now(ZoneId.of("UTC"));

    URI producer = URI.create("producer");
    OpenLineage ol = new OpenLineage(producer);
    UUID runId = UUID.randomUUID();
    RunFacets runFacets =
        ol.newRunFacetsBuilder()
          .nominalTime(
            ol.newNominalTimeRunFacetBuilder()
            .nominalStartTime(now)
            .nominalEndTime(now)
            .build())
        .build();
    Run run = ol.newRunBuilder().runId(runId).facets(runFacets).build();
    String name = "jobName";
    String namespace = "namespace";
    JobFacets jobFacets = ol.newJobFacetsBuilder().build();
    Job job = ol.newJobBuilder().namespace(namespace).name(name).facets(jobFacets).build();

    List<InputDataset> inputs = Arrays.asList(ol.newInputDatasetBuilder().namespace("ins").name("input")
        .inputFacets(
            ol.newInputDatasetInputFacetsBuilder().dataQualityMetrics(
                ol.newDataQualityMetricsInputDatasetFacetBuilder()
                  .rowCount(10L)
                  .bytes(20L)
                  .columnMetrics(
                      ol.newDataQualityMetricsInputDatasetFacetColumnMetricsBuilder()
                        .put("mycol",
                            ol.newDataQualityMetricsInputDatasetFacetColumnMetricsAdditionalBuilder()
                            .count(10D).distinctCount(10L).max(30D).min(5D).nullCount(1L).sum(3000D).quantiles(
                                ol.newDataQualityMetricsInputDatasetFacetColumnMetricsAdditionalQuantilesBuilder().put("25", 52D).build())
                            .build())
                        .build())
                .build()
            ).build())
        .build());
    List<OutputDataset> outputs = Arrays.asList(ol.newOutputDatasetBuilder().namespace("ons").name("output")
        .outputFacets(
            ol.newOutputDatasetOutputFacetsBuilder()
                .outputStatistics(ol.newOutputStatisticsOutputDatasetFacet(10L, 20L)).build())
        .build());

    RunEvent runStateUpdate = ol.newRunEventBuilder()
        .eventType("START")
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
    RunEvent read = mapper.readValue(json, RunEvent.class);

    assertEquals(producer,read.getProducer());
    assertEquals(runId,read.getRun().getRunId());
    assertEquals(name,read.getJob().getName());
    assertEquals(namespace,read.getJob().getNamespace());
    assertEquals(runStateUpdate.getEventType(),read.getEventType());
    assertEquals(runStateUpdate.getEventTime(),read.getEventTime());
    assertEquals(1, runStateUpdate.getInputs().size());
    InputDataset inputDataset = runStateUpdate.getInputs().get(0);
    assertEquals("ins", inputDataset.getNamespace());
    assertEquals("input", inputDataset.getName());

    DataQualityMetricsInputDatasetFacet dq = inputDataset.getInputFacets().getDataQualityMetrics();
    assertEquals((Long)10L, dq.getRowCount());
    assertEquals((Long)20L, dq.getBytes());
    DataQualityMetricsInputDatasetFacetColumnMetricsAdditional colMetrics = dq.getColumnMetrics().getAdditionalProperties().get("mycol");
    assertEquals((Double)10D, colMetrics.getCount());
    assertEquals((Long)10L, colMetrics.getDistinctCount());
    assertEquals((Double)30D, colMetrics.getMax());
    assertEquals((Double)5D, colMetrics.getMin());
    assertEquals((Long)1L, colMetrics.getNullCount());
    assertEquals((Double)3000D, colMetrics.getSum());
    assertEquals((Double)52D, colMetrics.getQuantiles().getAdditionalProperties().get("25"));

    assertEquals(1, runStateUpdate.getOutputs().size());
    OutputDataset outputDataset = runStateUpdate.getOutputs().get(0);
    assertEquals("ons", outputDataset.getNamespace());
    assertEquals("output", outputDataset.getName());

    assertEquals(roundTrip(json), roundTrip(mapper.writeValueAsString(read)));
    assertEquals((Long)10L, outputDataset.getOutputFacets().getOutputStatistics().getRowCount());
    assertEquals((Long)20L, outputDataset.getOutputFacets().getOutputStatistics().getSize());

    assertEquals(json, mapper.writeValueAsString(read));


  }
}
