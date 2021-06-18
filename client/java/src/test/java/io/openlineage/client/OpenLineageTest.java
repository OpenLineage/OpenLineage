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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.Job;
import io.openlineage.client.OpenLineage.JobFacets;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.Run;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunFacets;

public class OpenLineageTest {

  @Test
  public void jsonSerialization() throws JsonProcessingException {
    ZonedDateTime now = ZonedDateTime.now(ZoneId.of("UTC"));

    URI producer = URI.create("producer");
    OpenLineage ol = new OpenLineage(producer);
    UUID runId = UUID.randomUUID();
    RunFacets runFacets = ol.newRunFacets(ol.newNominalTimeRunFacet(now, now), null);
    Run run = ol.newRun(runId, runFacets);
    String name = "jobName";
    String namespace = "namespace";
    JobFacets jobFacets = ol.newJobFacets(null, null, null);
    Job job = ol.newJob(namespace, name, jobFacets);
    List<InputDataset> inputs = Arrays.asList(ol.newInputDataset("ins", "input", null, null));
    List<OutputDataset> outputs = Arrays.asList(ol.newOutputDataset("ons", "output", null, null));
    RunEvent runStateUpdate = ol.newRunEvent("START", now, run, job, inputs, outputs);

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
    assertEquals(1, runStateUpdate.getOutputs().size());
    OutputDataset outputDataset = runStateUpdate.getOutputs().get(0);
    assertEquals("ons", outputDataset.getNamespace());
    assertEquals("output", outputDataset.getName());


    assertEquals(json, mapper.writeValueAsString(read));


  }

  @Test
  public void factory() throws JsonProcessingException {
    ZonedDateTime now = ZonedDateTime.now(ZoneId.of("UTC"));

    URI producer = URI.create("producer");
    OpenLineage ol = new OpenLineage(producer);
    UUID runId = UUID.randomUUID();
    RunFacets runFacets =
        ol.newRunFacetsBuilder()
        .setNominalTime(
            ol.newNominalTimeRunFacetBuilder()
            .setNominalEndTime(now)
            .setNominalEndTime(now)
            .build())
        .build();
    Run run = ol.newRunBuilder().setRunId(runId).setFacets(runFacets).build();
    String name = "jobName";
    String namespace = "namespace";
    JobFacets jobFacets = ol.newJobFacetsBuilder().build();
    Job job = ol.newJobBuilder().setNamespace(namespace).setName(name).setFacets(jobFacets).build();
    List<InputDataset> inputs = Arrays.asList(ol.newInputDatasetBuilder().setNamespace("ins").setName("input").build());
    List<OutputDataset> outputs = Arrays.asList(ol.newOutputDatasetBuilder().setNamespace("ons").setName("output").build());
    RunEvent runStateUpdate = ol.newRunEventBuilder()
        .setEventType("START")
        .setEventTime(now)
        .setRun(run)
        .setJob(job)
        .setInputs(inputs)
        .setOutputs(outputs)
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
    assertEquals(1, runStateUpdate.getOutputs().size());
    OutputDataset outputDataset = runStateUpdate.getOutputs().get(0);
    assertEquals("ons", outputDataset.getNamespace());
    assertEquals("output", outputDataset.getName());


    assertEquals(json, mapper.writeValueAsString(read));


  }
}
