package io.openlineage.flink;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.agent.EventEmitter;
import io.openlineage.flink.agent.lifecycle.ExecutionContext;
import io.openlineage.flink.agent.lifecycle.FlinkExecutionContext;
import java.util.ArrayList;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.execution.JobClient;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class OpenLineageFlinkJobListenerTest {

  @Test
  void shouldEmitOnlyOneRunEventOnJobSubmitted() {
    JobID jobId = mock(JobID.class);
    JobClient jobClient = mock(JobClient.class);
    EventEmitter emitter = mock(EventEmitter.class);
    ExecutionContext context = new FlinkExecutionContext(jobId, emitter);

    context.onJobSubmitted(jobClient, new ArrayList<>());

    ArgumentCaptor<OpenLineage.RunEvent> lineageEvent =
        ArgumentCaptor.forClass(OpenLineage.RunEvent.class);

    verify(emitter, times(1)).emit(lineageEvent.capture());
    System.out.println(lineageEvent);
  }
}
