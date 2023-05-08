/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.lifecycle;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.client.OpenLineage.RunEventBuilder;
import io.openlineage.flink.SinkLineage;
import io.openlineage.flink.TransformationUtils;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.client.CheckpointFacet;
import io.openlineage.flink.client.EventEmitter;
import io.openlineage.flink.visitor.Visitor;
import io.openlineage.flink.visitor.VisitorFactory;
import io.openlineage.flink.visitor.VisitorFactoryImpl;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.dag.Transformation;

@Slf4j
@Builder
public class FlinkExecutionContext implements ExecutionContext {

  @Getter private final JobID jobId;
  protected final UUID runId;
  protected final EventEmitter eventEmitter;
  protected final OpenLineageContext openLineageContext;
  private final String jobName;
  private final String jobNamespace;

  @Getter private final List<Transformation<?>> transformations;

  @Override
  public void onJobSubmitted() {
    log.debug("JobClient - jobId: {}", jobId);
    RunEvent runEvent =
        buildEventForEventType(EventType.START)
            .run(new OpenLineage.RunBuilder().runId(runId).build())
            .build();
    log.debug("Posting event for onJobSubmitted {}: {}", jobId, runEvent);
    eventEmitter.emit(runEvent);
  }

  @Override
  public void onJobCheckpoint(CheckpointFacet facet) {
    log.debug("JobClient - jobId: {}", jobId);
    RunEvent runEvent =
        buildEventForEventType(EventType.RUNNING)
            .run(
                new OpenLineage.RunBuilder()
                    .runId(runId)
                    .facets(new OpenLineage.RunFacetsBuilder().put("checkpoints", facet).build())
                    .build())
            .build();
    log.debug("Posting event for onJobCheckpoint {}: {}", jobId, runEvent);
    eventEmitter.emit(runEvent);
  }

  public OpenLineage.RunEventBuilder buildEventForEventType(EventType eventType) {
    TransformationUtils converter = new TransformationUtils();
    List<SinkLineage> sinkLineages = converter.convertToVisitable(transformations);

    VisitorFactory visitorFactory = new VisitorFactoryImpl();
    List<OpenLineage.InputDataset> inputDatasets = new ArrayList<>();
    List<OpenLineage.OutputDataset> outputDatasets = new ArrayList<>();

    for (var lineage : sinkLineages) {
      inputDatasets.addAll(getInputDatasets(visitorFactory, lineage.getSources()));
      outputDatasets.addAll(getOutputDatasets(visitorFactory, lineage.getSink()));
    }

    return commonEventBuilder().inputs(inputDatasets).outputs(outputDatasets).eventType(eventType);
  }

  @Override
  public void onJobCompleted(JobExecutionResult jobExecutionResult) {
    OpenLineage openLineage = openLineageContext.getOpenLineage();
    eventEmitter.emit(
        commonEventBuilder()
            .run(openLineage.newRun(runId, null))
            .eventType(EventType.COMPLETE)
            .build());
  }

  @Override
  public void onJobFailed(Throwable failed) {
    OpenLineage openLineage = openLineageContext.getOpenLineage();
    eventEmitter.emit(
        commonEventBuilder()
            .run(
                openLineage.newRun(
                    runId,
                    openLineage
                        .newRunFacetsBuilder()
                        .errorMessage(
                            openLineage.newErrorMessageRunFacet(
                                failed.getMessage(), "JAVA", ExceptionUtils.getStackTrace(failed)))
                        .build()))
            .eventType(EventType.FAIL)
            .eventTime(ZonedDateTime.now())
            .build());
  }

  /**
   * Builds common elements of Openlineage events constructed.
   *
   * @return
   */
  private RunEventBuilder commonEventBuilder() {
    OpenLineage openLineage = openLineageContext.getOpenLineage();
    return openLineage
        .newRunEventBuilder()
        .job(openLineage.newJob(jobNamespace, jobName, null))
        .eventTime(ZonedDateTime.now());
  }

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
