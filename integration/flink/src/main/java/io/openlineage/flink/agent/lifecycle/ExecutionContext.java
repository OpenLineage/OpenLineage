package io.openlineage.flink.agent.lifecycle;

import io.openlineage.flink.agent.facets.CheckpointFacet;
import org.apache.flink.api.common.JobExecutionResult;

public interface ExecutionContext {

  void onJobSubmitted();

  void onJobCheckpoint(CheckpointFacet facet);

  void onJobCompleted(JobExecutionResult jobExecutionResult);

  void onJobFailed(Throwable failed);
}
