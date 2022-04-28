package io.openlineage.flink.agent.lifecycle;

import java.util.List;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.execution.JobClient;

public interface ExecutionContext {

  void onJobSubmitted(JobClient jobClient, List<Transformation<?>> transformations);

  void onJobExecuted(
      JobExecutionResult jobExecutionResult, List<Transformation<?>> transformations);
}
