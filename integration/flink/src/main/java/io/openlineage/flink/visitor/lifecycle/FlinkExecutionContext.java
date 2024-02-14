/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.lifecycle;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.JobFacets;
import io.openlineage.client.OpenLineage.JobFacetsBuilder;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.client.OpenLineage.RunEventBuilder;
import io.openlineage.client.circuitBreaker.CircuitBreaker;
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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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

  public static final String FLINK_INTEGRATION = "FLINK";
  public static final String FLINK_JOB_TYPE = "JOB";
  @Getter private final JobID jobId;
  protected final UUID runId;
  protected final EventEmitter eventEmitter;
  protected final OpenLineageContext openLineageContext;
  private final String jobName;
  private final String jobNamespace;
  private final String processingType;
  private final CircuitBreaker circuitBreaker;

  @Getter private final List<Transformation<?>> transformations;

  @Override
  public void onJobSubmitted() {
    log.debug("JobClient - jobId: {}", jobId);
    circuitBreaker.run(
        () -> {
          RunEvent runEvent =
              buildEventForEventType(EventType.START)
                  .run(new OpenLineage.RunBuilder().runId(runId).build())
                  .build();
          log.debug("Posting event for onJobSubmitted {}: {}", jobId, runEvent);
          eventEmitter.emit(runEvent);
          return null;
        });
  }

  @Override
  public void onJobCheckpoint(CheckpointFacet facet) {
    log.debug("JobClient - jobId: {}", jobId);
    circuitBreaker.run(
        () -> {
          RunEvent runEvent =
              buildEventForEventType(EventType.RUNNING)
                  .run(
                      new OpenLineage.RunBuilder()
                          .runId(runId)
                          .facets(
                              new OpenLineage.RunFacetsBuilder().put("checkpoints", facet).build())
                          .build())
                  .build();
          log.debug("Posting event for onJobCheckpoint {}: {}", jobId, runEvent);
          eventEmitter.emit(runEvent);
          return null;
        });
  }

  public OpenLineage.RunEventBuilder buildEventForEventType(EventType eventType) {
    TransformationUtils converter = new TransformationUtils();

    List<SinkLineage> sinkLineages = converter.convertToVisitable(transformations);

    VisitorFactory visitorFactory = new VisitorFactoryImpl();
    List<OpenLineage.InputDataset> inputDatasets = new ArrayList<>();
    List<OpenLineage.OutputDataset> outputDatasets = new ArrayList<>();

    Set<Object> sources = new HashSet<>();
    for (var lineage : sinkLineages) {
      sources.addAll(lineage.getSources());
      outputDatasets.addAll(getOutputDatasets(visitorFactory, lineage.getSink()));
    }
    inputDatasets.addAll(getInputDatasets(visitorFactory, Arrays.asList(sources.toArray())));

    return commonEventBuilder().inputs(inputDatasets).outputs(outputDatasets).eventType(eventType);
  }

  @Override
  public void onJobCompleted(JobExecutionResult jobExecutionResult) {
    circuitBreaker.run(
        () -> {
          OpenLineage openLineage = openLineageContext.getOpenLineage();
          eventEmitter.emit(
              commonEventBuilder()
                  .run(openLineage.newRun(runId, null))
                  .eventType(EventType.COMPLETE)
                  .build());
          return null;
        });
  }

  @Override
  public void onJobFailed(Throwable failed) {
    circuitBreaker.run(
        () -> {
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
                                      failed.getMessage(),
                                      "JAVA",
                                      ExceptionUtils.getStackTrace(failed)))
                              .build()))
                  .eventType(EventType.FAIL)
                  .eventTime(ZonedDateTime.now())
                  .build());
          return null;
        });
  }

  /**
   * Builds common elements of Openlineage events constructed.
   *
   * @return
   */
  private RunEventBuilder commonEventBuilder() {
    OpenLineage openLineage = openLineageContext.getOpenLineage();
    JobFacets jobFacets =
        new JobFacetsBuilder()
            .jobType(
                openLineage
                    .newJobTypeJobFacetBuilder()
                    .jobType(FLINK_JOB_TYPE)
                    .processingType(processingType)
                    .integration(FLINK_INTEGRATION)
                    .build())
            .build();

    return openLineage
        .newRunEventBuilder()
        .job(openLineage.newJob(jobNamespace, jobName, jobFacets))
        .eventTime(ZonedDateTime.now());
  }

  private List<OpenLineage.InputDataset> getInputDatasets(
      VisitorFactory visitorFactory, List<Object> sources) {
    List<OpenLineage.InputDataset> inputDatasets = new ArrayList<>();
    List<Visitor<OpenLineage.InputDataset>> inputVisitors =
        visitorFactory.getInputVisitors(openLineageContext);

    for (var transformation : sources) {
      log.debug("Getting input dataset from source {}", transformation.toString());
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

    log.debug("Getting output dataset from sink {}", sink.toString());
    return outputVisitors.stream()
        .filter(outputVisitor -> outputVisitor.isDefinedAt(sink))
        .map(outputVisitor -> outputVisitor.apply(sink))
        .flatMap(List::stream)
        .collect(Collectors.toList());
  }
}
