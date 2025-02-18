/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.lifecycle;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.JobFacets;
import io.openlineage.client.OpenLineage.JobFacetsBuilder;
import io.openlineage.client.OpenLineage.OwnershipJobFacetOwners;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.client.OpenLineage.RunEventBuilder;
import io.openlineage.client.OpenLineageConfig;
import io.openlineage.client.job.JobConfig;
import io.openlineage.flink.SinkLineage;
import io.openlineage.flink.TransformationUtils;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.client.CheckpointFacet;
import io.openlineage.flink.visitor.Visitor;
import io.openlineage.flink.visitor.VisitorFactory;
import io.openlineage.flink.visitor.VisitorFactoryImpl;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.dag.Transformation;

@Slf4j
@Builder
public class FlinkExecutionContext implements ExecutionContext {

  public static final String FLINK_INTEGRATION = "FLINK";
  public static final String FLINK_JOB_TYPE = "JOB";

  @Getter private final OpenLineageContext olContext;
  @Getter private final List<Transformation<?>> transformations;

  @Override
  public void onJobSubmitted() {
    log.debug("JobClient - jobId: {}", olContext.getJobId().getFlinkJobId());
    olContext
        .getCircuitBreaker()
        .run(
            () -> {
              RunEvent runEvent =
                  buildEventForEventType(EventType.START)
                      .run(new OpenLineage.RunBuilder().runId(olContext.getRunUuid()).build())
                      .build();
              log.debug("Posting event for onJobSubmitted {}: {}", olContext.getJobId(), runEvent);
              olContext
                  .getMeterRegistry()
                  .counter("openlineage.flink.event.submitted.start")
                  .increment();
              olContext.getEventEmitter().emit(runEvent);
              olContext
                  .getMeterRegistry()
                  .counter("openlineage.flink.event.submitted.end")
                  .increment();
              return null;
            });
  }

  @Override
  public void onJobCheckpoint(CheckpointFacet facet) {
    log.debug("JobClient - jobId: {}", olContext.getJobId());
    olContext
        .getCircuitBreaker()
        .run(
            () -> {
              RunEvent runEvent =
                  buildEventForEventType(EventType.RUNNING)
                      .run(
                          new OpenLineage.RunBuilder()
                              .runId(olContext.getRunUuid())
                              .facets(
                                  new OpenLineage.RunFacetsBuilder()
                                      .put("checkpoints", facet)
                                      .build())
                              .build())
                      .build();
              log.debug("Posting event for onJobCheckpoint {}: {}", olContext.getJobId(), runEvent);
              olContext
                  .getMeterRegistry()
                  .counter("openlineage.flink.event.checkpoint.start")
                  .increment();
              olContext.getEventEmitter().emit(runEvent);
              olContext
                  .getMeterRegistry()
                  .counter("openlineage.flink.event.checkpoint.end")
                  .increment();
              return null;
            });
  }

  public RunEventBuilder buildEventForEventType(EventType eventType) {
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
    olContext
        .getCircuitBreaker()
        .run(
            () -> {
              OpenLineage openLineage = olContext.getOpenLineage();
              olContext
                  .getMeterRegistry()
                  .counter("openlineage.flink.event.completed.start")
                  .increment();
              olContext
                  .getEventEmitter()
                  .emit(
                      commonEventBuilder()
                          .run(openLineage.newRun(olContext.getRunUuid(), null))
                          .eventType(EventType.COMPLETE)
                          .build());
              olContext
                  .getMeterRegistry()
                  .counter("openlineage.flink.event.completed.end")
                  .increment();
              return null;
            });
  }

  @Override
  public void onJobFailed(Throwable failed) {
    olContext
        .getCircuitBreaker()
        .run(
            () -> {
              OpenLineage openLineage = olContext.getOpenLineage();
              olContext
                  .getMeterRegistry()
                  .counter("openlineage.flink.event.failed.start")
                  .increment();
              olContext
                  .getEventEmitter()
                  .emit(
                      commonEventBuilder()
                          .run(
                              openLineage.newRun(
                                  olContext.getRunUuid(),
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
              olContext
                  .getMeterRegistry()
                  .counter("openlineage.flink.event.failed.end")
                  .increment();
              return null;
            });
  }

  /**
   * Builds common elements of Openlineage events constructed.
   *
   * @return
   */
  private RunEventBuilder commonEventBuilder() {
    OpenLineage openLineage = olContext.getOpenLineage();
    JobFacets jobFacets =
        buildOwnershipFacet(new JobFacetsBuilder())
            .jobType(
                openLineage
                    .newJobTypeJobFacetBuilder()
                    .jobType(FLINK_JOB_TYPE)
                    .processingType(olContext.getProcessingType())
                    .integration(FLINK_INTEGRATION)
                    .build())
            .build();

    return openLineage
        .newRunEventBuilder()
        .job(
            openLineage.newJob(
                olContext.getJobId().getJobNamespace(),
                olContext.getJobId().getJobName(),
                jobFacets))
        .eventTime(ZonedDateTime.now());
  }

  private JobFacetsBuilder buildOwnershipFacet(JobFacetsBuilder builder) {
    Optional.of(olContext.getConfig())
        .map(OpenLineageConfig::getJobConfig)
        .map(JobConfig::getOwners)
        .map(JobConfig.JobOwnersConfig::getAdditionalProperties)
        .filter(Objects::nonNull)
        .ifPresent(
            map -> {
              List<OwnershipJobFacetOwners> ownersList = new ArrayList<>();
              map.forEach(
                  (type, name) ->
                      ownersList.add(
                          olContext
                              .getOpenLineage()
                              .newOwnershipJobFacetOwnersBuilder()
                              .name(name)
                              .type(type)
                              .build()));
              builder.ownership(
                  olContext
                      .getOpenLineage()
                      .newOwnershipJobFacetBuilder()
                      .owners(ownersList)
                      .build());
            });

    return builder;
  }

  private List<OpenLineage.InputDataset> getInputDatasets(
      VisitorFactory visitorFactory, List<Object> sources) {
    List<OpenLineage.InputDataset> inputDatasets = new ArrayList<>();
    List<Visitor<OpenLineage.InputDataset>> inputVisitors =
        visitorFactory.getInputVisitors(olContext);

    for (var transformation : sources) {
      log.debug("Getting input dataset from source {}", transformation.toString());
      inputDatasets.addAll(
          inputVisitors.stream()
              .filter(inputVisitor -> inputVisitor.isDefinedAt(transformation))
              .map(
                  inputVisitor ->
                      olContext
                          .getMeterRegistry()
                          .timer(
                              "openlineage.flink.dataset.input.extraction.time",
                              "visitor",
                              inputVisitor.getClass().getName())
                          .record(() -> inputVisitor.apply(transformation)))
              .flatMap(List::stream)
              .collect(Collectors.toList()));
    }
    return inputDatasets;
  }

  private List<OpenLineage.OutputDataset> getOutputDatasets(
      VisitorFactory visitorFactory, Object sink) {
    List<Visitor<OpenLineage.OutputDataset>> outputVisitors =
        visitorFactory.getOutputVisitors(olContext);

    log.debug("Getting output dataset from sink {}", sink.toString());
    return outputVisitors.stream()
        .filter(outputVisitor -> outputVisitor.isDefinedAt(sink))
        .map(
            outputVisitor ->
                olContext
                    .getMeterRegistry()
                    .timer(
                        "openlineage.flink.dataset.output.extraction.time",
                        "visitor",
                        outputVisitor.getClass().getName())
                    .record(() -> outputVisitor.apply(sink)))
        .flatMap(List::stream)
        .collect(Collectors.toList());
  }
}
