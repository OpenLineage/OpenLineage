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

import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.Job;
import io.openlineage.client.OpenLineage.JobFacets;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.Run;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunFacet;
import io.openlineage.client.OpenLineage.RunFacets;
import io.openlineage.client.RunFacets.NominalTime;

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
    io.openlineage.client.RunFacets rf = new io.openlineage.client.RunFacets(producer);
    UUID runId = UUID.randomUUID();
    RunFacets runFacets = ol.newRunFacets();
    runFacets.getAdditionalProperties().put("nominalTime", rf.newNominalTime(now, now));
    Run run = ol.newRun(runId, runFacets);
    String name = "jobName";
    String namespace = "namespace";
    JobFacets jobFacets = ol.newJobFacets();
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
    RunFacet nominalTimeBasic = runStateUpdate.getRun().getFacets().getAdditionalProperties().get("nominalTime");
    NominalTime nominalTime = mapper.readValue(mapper.writeValueAsString(nominalTimeBasic), NominalTime.class);
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
    io.openlineage.client.RunFacets rf = new io.openlineage.client.RunFacets(producer);
    UUID runId = UUID.randomUUID();
    RunFacets runFacets =
        ol.newRunFacetsBuilder()
        .put("nominalTime",
            rf.newNominalTimeBuilder()
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

    assertEquals(roundTrip(json), roundTrip(mapper.writeValueAsString(read)));
  }
}
