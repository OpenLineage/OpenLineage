package io.openlineage.flink.agent.lifecycle;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.SinkLineage;
import io.openlineage.flink.TransformationUtils;
import io.openlineage.flink.agent.EventEmitter;
import io.openlineage.flink.agent.client.OpenLineageClient;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.visitor.Visitor;
import io.openlineage.flink.visitor.VisitorFactory;
import io.openlineage.flink.visitor.VisitorFactoryImpl;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.execution.JobClient;

@Slf4j
public class FlinkExecutionContext implements ExecutionContext {

  private final JobID jobId;
  private final EventEmitter eventEmitter;
  private final OpenLineageContext openLineageContext;

  public FlinkExecutionContext(JobID jobId, EventEmitter eventEmitter) {
    this.jobId = jobId;
    this.eventEmitter = eventEmitter;
    this.openLineageContext =
        OpenLineageContext.builder()
            .openLineage(new OpenLineage(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI))
            .build();
  }

  @Override
  public void onJobSubmitted(JobClient jobClient, List<Transformation<?>> transformations) {
    log.debug("JobClient - jobId: {}", jobClient.getJobID());

    TransformationUtils converter = new TransformationUtils();
    List<SinkLineage> sinkLineages = converter.convertToVisitable(transformations);

    VisitorFactory visitorFactory = new VisitorFactoryImpl();
    List<OpenLineage.InputDataset> inputDatasets = new ArrayList<>();
    List<OpenLineage.OutputDataset> outputDatasets = new ArrayList<>();

    for (var lineage : sinkLineages) {
      inputDatasets.addAll(getInputDatasets(visitorFactory, lineage.getSources()));
      outputDatasets.addAll(getOutputDatasets(visitorFactory, lineage.getSink()));
    }

    OpenLineage.RunEvent runEvent =
        openLineageContext
            .getOpenLineage()
            .newRunEventBuilder()
            .inputs(inputDatasets)
            .outputs(outputDatasets)
            .build();

    log.debug("Posting event for onJobSubmitted {}: {}", jobId, runEvent);
    eventEmitter.emit(runEvent);
  }

  @Override
  public void onJobExecuted(
      JobExecutionResult jobExecutionResult, List<Transformation<?>> transformations) {}

  private List<OpenLineage.InputDataset> getInputDatasets(
      VisitorFactory visitorFactory, List<Object> sources) {
    List<OpenLineage.InputDataset> inputDatasets = new ArrayList<>();
    List<Visitor<OpenLineage.InputDataset>> inputVisitors =
        visitorFactory.getInputVisitors(openLineageContext);

    for (var transformation : sources) {
      inputDatasets.addAll(
          inputVisitors.stream()
              .filter(inputVisitor -> inputVisitor.isDefinedAt(transformation))
              .map(inputVisitor -> inputVisitor.apply(transformation))
              .flatMap(List::stream)
              .collect(Collectors.toList()));
    }
    return inputDatasets;
  }

  private List<OpenLineage.OutputDataset> getOutputDatasets(
      VisitorFactory visitorFactory, Object sink) {
    List<Visitor<OpenLineage.OutputDataset>> outputVisitors =
        visitorFactory.getOutputVisitors(openLineageContext);

    return outputVisitors.stream()
        .filter(inputVisitor -> inputVisitor.isDefinedAt(sink))
        .map(inputVisitor -> inputVisitor.apply(sink))
        .flatMap(List::stream)
        .collect(Collectors.toList());
  }
}
