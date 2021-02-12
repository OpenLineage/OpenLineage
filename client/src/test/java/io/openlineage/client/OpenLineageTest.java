package io.openlineage.client;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import io.openlineage.client.OpenLineage.Dataset;
import io.openlineage.client.OpenLineage.Job;
import io.openlineage.client.OpenLineage.JobFacets;
import io.openlineage.client.OpenLineage.Run;
import io.openlineage.client.OpenLineage.RunFacets;
import io.openlineage.client.OpenLineage.RunEvent;

public class OpenLineageTest {

  @Test
  public void jsonSerialization() throws JsonProcessingException {

    String producer = "producer";
    String runId = "runId";
    RunFacets runFacets = new RunFacets(null, null);
    Run run = new Run(runId, runFacets);
    String name = "jobName";
    String namespace = "namespace";
    JobFacets jobFacets = new JobFacets(null, null, null);
    Job job = new Job(namespace, name, jobFacets);
    List<Dataset> inputs = Arrays.asList();
    List<Dataset> outputs = Arrays.asList();
    RunEvent runStateUpdate = new RunEvent("START", "123", run, job, inputs, outputs, producer );

    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
    String json = mapper.writeValueAsString(runStateUpdate);
    RunEvent read = mapper.readValue(json, RunEvent.class);

    assertEquals(producer,read.getProducer());
    assertEquals(runId,read.getRun().getRunId());
    assertEquals(name,read.getJob().getName());
    assertEquals(namespace,read.getJob().getNamespace());
    assertEquals(runStateUpdate.getEventType(),read.getEventType());
    assertEquals(runStateUpdate.getEventTime(),read.getEventTime());

    assertEquals(json, mapper.writeValueAsString(read));


  }
}
