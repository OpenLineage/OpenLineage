package io.openlineage.client;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import io.openlineage.client.OpenLineage.Dataset;
import io.openlineage.client.OpenLineage.Job;
import io.openlineage.client.OpenLineage.JobFacets;
import io.openlineage.client.OpenLineage.Run;
import io.openlineage.client.OpenLineage.RunFacets;
import io.openlineage.client.OpenLineage.RunStateUpdate;

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
    OpenLineage.RunStateUpdate runStateUpdate = new OpenLineage.RunStateUpdate("START", "123", run, job, inputs, outputs, producer );

    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
    String json = mapper.writeValueAsString(runStateUpdate);
    System.out.println(json);
    RunStateUpdate read = mapper.readValue(json, RunStateUpdate.class);
    Assert.assertEquals(json, mapper.writeValueAsString(read));
  }
}
