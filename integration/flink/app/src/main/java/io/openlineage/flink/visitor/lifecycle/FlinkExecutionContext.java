/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.lifecycle;

import io.micrometer.core.instrument.MeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.JobFacets;
import io.openlineage.client.OpenLineage.JobFacetsBuilder;
import io.openlineage.client.OpenLineage.OwnershipJobFacetOwners;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.client.OpenLineage.RunEventBuilder;
import io.openlineage.client.circuitBreaker.CircuitBreaker;
import io.openlineage.flink.SinkLineage;
import io.openlineage.flink.TransformationUtils;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.client.CheckpointFacet;
import io.openlineage.flink.client.EventEmitter;
import io.openlineage.flink.client.FlinkOpenLineageConfig;
import io.openlineage.flink.client.FlinkOpenLineageConfig.JobConfig;
import io.openlineage.flink.client.FlinkOpenLineageConfig.JobOwnersConfig;
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
  private final FlinkOpenLineageConfig config;
  @Getter private final MeterRegistry meterRegistry;

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
          meterRegistry.counter("openlineage.flink.event.submitted.start").increment();
          eventEmitter.emit(runEvent);
          meterRegistry.counter("openlineage.flink.event.submitted.end").increment();
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
          meterRegistry.counter("openlineage.flink.event.checkpoint.start").increment();
          eventEmitter.emit(runEvent);
          meterRegistry.counter("openlineage.flink.event.checkpoint.end").increment();
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
    circuitBreaker.run(
        () -> {
          OpenLineage openLineage = openLineageContext.getOpenLineage();
          meterRegistry.counter("openlineage.flink.event.completed.start").increment();
          eventEmitter.emit(
              commonEventBuilder()
                  .run(openLineage.newRun(runId, null))
                  .eventType(EventType.COMPLETE)
                  .build());
          meterRegistry.counter("openlineage.flink.event.completed.end").increment();
          return null;
        });
  }

  @Override
  public void onJobFailed(Throwable failed) {
    circuitBreaker.run(
        () -> {
          OpenLineage openLineage = openLineageContext.getOpenLineage();
          meterRegistry.counter("openlineage.flink.event.failed.start").increment();
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
          meterRegistry.counter("openlineage.flink.event.failed.end").increment();
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
        buildOwnershipFacet(new JobFacetsBuilder())
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

  private JobFacetsBuilder buildOwnershipFacet(JobFacetsBuilder builder) {
    Optional.ofNullable(config)
        .map(FlinkOpenLineageConfig::getJob)
        .map(JobConfig::getOwners)
        .map(JobOwnersConfig::getAdditionalProperties)
        .filter(Objects::nonNull)
        .ifPresent(
            map -> {
              List<OwnershipJobFacetOwners> ownersList = new ArrayList<>();
              map.forEach(
                  (type, name) ->
                      ownersList.add(
                          openLineageContext
                              .getOpenLineage()
                              .newOwnershipJobFacetOwnersBuilder()
                              .name(name)
                              .type(type)
                              .build()));
              builder.ownership(
                  openLineageContext
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
        visitorFactory.getInputVisitors(openLineageContext);

    for (var transformation : sources) {
      log.debug("Getting input dataset from source {}", transformation.toString());
      inputDatasets.addAll(
          inputVisitors.stream()
              .filter(inputVisitor -> inputVisitor.isDefinedAt(transformation))
              .map(
                  inputVisitor ->
                      meterRegistry
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
        visitorFactory.getOutputVisitors(openLineageContext);

    log.debug("Getting output dataset from sink {}", sink.toString());
    return outputVisitors.stream()
        .filter(outputVisitor -> outputVisitor.isDefinedAt(sink))
        .map(
            outputVisitor ->
                meterRegistry
                    .timer(
                        "openlineage.flink.dataset.output.extraction.time",
                        "visitor",
                        outputVisitor.getClass().getName())
                    .record(() -> outputVisitor.apply(sink)))
        .flatMap(List::stream)
        .collect(Collectors.toList());
  }
}
