/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.lifecycle;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
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
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.dag.Transformation;

@Slf4j
public class FlinkExecutionContext implements ExecutionContext {

  @Getter private final JobID jobId;

  private final UUID runId;

  private final EventEmitter eventEmitter;
  private final OpenLineageContext openLineageContext;

  @Getter private final List<Transformation<?>> transformations;

  public FlinkExecutionContext(JobID jobId, List<Transformation<?>> transformations) {
    this.jobId = jobId;
    this.runId = UUID.randomUUID();
    this.transformations = transformations;
    this.eventEmitter = new EventEmitter();
    this.openLineageContext =
        OpenLineageContext.builder()
            .openLineage(new OpenLineage(EventEmitter.OPEN_LINEAGE_CLIENT_URI))
            .build();
  }

  @Override
  public void onJobSubmitted() {
    log.debug("JobClient - jobId: {}", jobId);
    RunEvent runEvent = buildEventForEventType(EventType.START).build();
    log.debug("Posting event for onJobSubmitted {}: {}", jobId, runEvent);
    eventEmitter.emit(runEvent);
  }

  @Override
  public void onJobCheckpoint(CheckpointFacet facet) {
    log.debug("JobClient - jobId: {}", jobId);
    RunEvent runEvent =
        buildEventForEventType(EventType.OTHER)
            .run(
                new OpenLineage.RunBuilder()
                    .runId(runId)
                    .facets(new OpenLineage.RunFacetsBuilder().put("checkpoints", facet).build())
                    .build())
            .build();
    // TODO: introduce better event type than OTHER
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

    return openLineageContext
        .getOpenLineage()
        .newRunEventBuilder()
        .inputs(inputDatasets)
        .outputs(outputDatasets)
        .eventType(eventType);
  }

  @Override
  public void onJobCompleted(JobExecutionResult jobExecutionResult) {
    OpenLineage openLineage = openLineageContext.getOpenLineage();
    eventEmitter.emit(
        openLineage
            .newRunEventBuilder()
            .run(openLineage.newRun(runId, null))
            .eventTime(ZonedDateTime.now())
            .eventType(EventType.COMPLETE)
            .build());
  }

  @Override
  public void onJobFailed(Throwable failed) {
    OpenLineage openLineage = openLineageContext.getOpenLineage();
    eventEmitter.emit(
        openLineage
            .newRunEventBuilder()
            .run(
                openLineage.newRun(
                    runId,
                    openLineage
                        .newRunFacetsBuilder()
                        .errorMessage(
                            openLineage.newErrorMessageRunFacet(
                                failed.getMessage(), "JAVA", ExceptionUtils.getStackTrace(failed)))
                        .build()))
            .eventTime(ZonedDateTime.now())
            .eventType(EventType.FAIL)
            .build());
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
