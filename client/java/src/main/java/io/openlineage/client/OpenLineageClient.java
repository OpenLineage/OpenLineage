/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.openlineage.client.circuitBreaker.CircuitBreaker;
import io.openlineage.client.job.JobConfig;
import io.openlineage.client.metrics.MicrometerProvider;
import io.openlineage.client.run.RunConfig;
import io.openlineage.client.transports.ConsoleTransport;
import io.openlineage.client.transports.Transport;
import io.openlineage.client.utils.TagField;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/** HTTP client used to emit {@link OpenLineage.RunEvent}s to HTTP backend. */
@Slf4j
public final class OpenLineageClient implements AutoCloseable {
  final Transport transport;
  final Optional<CircuitBreaker> circuitBreaker;
  final MeterRegistry meterRegistry;
  final String[] disabledFacets;
  final OpenLineageConfig openLineageConfig;

  Counter emitStart;
  Counter emitComplete;
  AtomicInteger engagedCircuitBreaker;
  Timer emitTime;

  /** Creates a new {@code OpenLineageClient} object. */
  public OpenLineageClient() {
    this(new ConsoleTransport());
  }

  public OpenLineageClient(@NonNull final Transport transport) {
    this(transport, new String[] {});
  }

  public OpenLineageClient(@NonNull final Transport transport, String... disabledFacets) {
    this(transport, null, null, null, disabledFacets);
  }

  public OpenLineageClient(
      @NonNull final Transport transport,
      CircuitBreaker circuitBreaker,
      MeterRegistry meterRegistry,
      String... disabledFacets) {
    this(transport, circuitBreaker, meterRegistry, null, disabledFacets);
  }

  public OpenLineageClient(
      @NonNull final Transport transport,
      CircuitBreaker circuitBreaker,
      MeterRegistry meterRegistry,
      OpenLineageConfig openLineageConfig,
      String... disabledFacets) {
    this.transport = transport;
    this.disabledFacets = Arrays.copyOf(disabledFacets, disabledFacets.length);
    this.circuitBreaker = Optional.ofNullable(circuitBreaker);
    this.openLineageConfig = openLineageConfig;
    if (meterRegistry == null) {
      this.meterRegistry = MicrometerProvider.getMeterRegistry();
    } else {
      this.meterRegistry = meterRegistry;
    }

    initializeMetrics();
    OpenLineageClientUtils.configureObjectMapper(disabledFacets);
  }

  /**
   * Emit the given run event to HTTP backend. The method will return successfully after the run
   * event has been emitted, regardless of any exceptions thrown by the HTTP backend.
   *
   * @param runEvent The run event to emit.
   */
  public void emit(@NonNull OpenLineage.RunEvent runEvent) {
    if (log.isDebugEnabled()) {
      log.debug(
          "OpenLineageClient will emit lineage event: {}", OpenLineageClientUtils.toJson(runEvent));
    }
    if (circuitBreaker.isPresent() && circuitBreaker.get().currentState().isClosed()) {
      engagedCircuitBreaker.set(1);
      log.warn("OpenLineageClient disabled with circuit breaker");
      return;
    } else {
      engagedCircuitBreaker.set(0);
    }
    OpenLineage.RunEvent enriched = enrichRunEvent(runEvent);
    emitStart.increment();
    emitTime.record(() -> transport.emit(enriched));
    emitComplete.increment();
  }

  /**
   * Emit the given dataset event to HTTP backend. The method will return successfully after the
   * dataset event has been emitted, regardless of any exceptions thrown by the HTTP backend.
   *
   * @param datasetEvent The dataset event to emit.
   */
  public void emit(@NonNull OpenLineage.DatasetEvent datasetEvent) {
    if (log.isDebugEnabled()) {
      log.debug(
          "OpenLineageClient will emit lineage event: {}",
          OpenLineageClientUtils.toJson(datasetEvent));
    }
    if (circuitBreaker.isPresent() && circuitBreaker.get().currentState().isClosed()) {
      engagedCircuitBreaker.set(1);
      log.warn("OpenLineageClient disabled with circuit breaker");
      return;
    } else {
      engagedCircuitBreaker.set(0);
    }
    emitStart.increment();
    emitTime.record(() -> transport.emit(datasetEvent));
    emitComplete.increment();
  }

  /**
   * Emit the given run event to HTTP backend. The method will return successfully after the run
   * event has been emitted, regardless of any exceptions thrown by the HTTP backend.
   *
   * @param jobEvent The job event to emit.
   */
  public void emit(@NonNull OpenLineage.JobEvent jobEvent) {
    if (log.isDebugEnabled()) {
      log.debug(
          "OpenLineageClient will emit lineage event: {}", OpenLineageClientUtils.toJson(jobEvent));
    }
    if (circuitBreaker.isPresent() && circuitBreaker.get().currentState().isClosed()) {
      engagedCircuitBreaker.set(1);
      log.warn("OpenLineageClient disabled with circuit breaker");
      return;
    } else {
      engagedCircuitBreaker.set(0);
    }
    OpenLineage.JobEvent enriched = enrichJobEvent(jobEvent);
    emitStart.increment();
    emitTime.record(() -> transport.emit(enriched));
    emitComplete.increment();
  }

  /**
   * Enriches a RunEvent with job tags, run tags (including default version tag), and job ownership
   * from config. Tags already present in the event are preserved; config tags override by key.
   * Returns a new enriched event (the original is not modified).
   */
  OpenLineage.RunEvent enrichRunEvent(@NonNull OpenLineage.RunEvent runEvent) {
    if (openLineageConfig == null) {
      return runEvent;
    }
    OpenLineage openLineage = new OpenLineage(runEvent.getProducer());

    OpenLineage.Job enrichedJob = enrichJob(openLineage, runEvent.getJob());
    OpenLineage.Run enrichedRun = enrichRun(openLineage, runEvent.getRun());

    return openLineage
        .newRunEventBuilder()
        .eventType(runEvent.getEventType())
        .eventTime(runEvent.getEventTime())
        .run(enrichedRun)
        .job(enrichedJob)
        .inputs(runEvent.getInputs())
        .outputs(runEvent.getOutputs())
        .build();
  }

  /**
   * Enriches a JobEvent with job tags and job ownership from config.
   * Returns a new enriched event (the original is not modified).
   */
  OpenLineage.JobEvent enrichJobEvent(@NonNull OpenLineage.JobEvent jobEvent) {
    if (openLineageConfig == null) {
      return jobEvent;
    }
    OpenLineage openLineage = new OpenLineage(jobEvent.getProducer());
    OpenLineage.Job enrichedJob = enrichJob(openLineage, jobEvent.getJob());

    return openLineage
        .newJobEventBuilder()
        .eventTime(jobEvent.getEventTime())
        .job(enrichedJob)
        .inputs(jobEvent.getInputs())
        .outputs(jobEvent.getOutputs())
        .build();
  }

  private OpenLineage.Job enrichJob(OpenLineage openLineage, OpenLineage.Job job) {
    if (job == null) {
      return null;
    }

    List<TagField> configJobTags = getConfigJobTags();
    Map<String, String> owners = getConfigOwners();

    if (configJobTags.isEmpty() && owners.isEmpty()) {
      return job;
    }

    OpenLineage.JobFacets existing = job.getFacets();
    OpenLineage.JobFacetsBuilder facetsBuilder = openLineage.newJobFacetsBuilder();

    // Copy all existing named facets
    if (existing != null) {
      if (existing.getDocumentation() != null) facetsBuilder.documentation(existing.getDocumentation());
      if (existing.getSql() != null) facetsBuilder.sql(existing.getSql());
      if (existing.getOwnership() != null) facetsBuilder.ownership(existing.getOwnership());
      if (existing.getJobType() != null) facetsBuilder.jobType(existing.getJobType());
      if (existing.getTags() != null) facetsBuilder.tags(existing.getTags());
      if (existing.getSourceCodeLocation() != null)
        facetsBuilder.sourceCodeLocation(existing.getSourceCodeLocation());
      // Copy additional (unknown) facets
      if (existing.getAdditionalProperties() != null) {
        existing.getAdditionalProperties().forEach(facetsBuilder::put);
      }
    }

    // Merge job tags from config (override existing tags by key)
    if (!configJobTags.isEmpty()) {
      OpenLineage.TagsJobFacet existingTags = (existing != null) ? existing.getTags() : null;
      facetsBuilder.tags(mergeJobTagFacet(openLineage, existingTags, configJobTags));
    }

    // Add ownership from config only if not already set by the integration
    if (!owners.isEmpty() && (existing == null || existing.getOwnership() == null)) {
      List<OpenLineage.OwnershipJobFacetOwners> ownersList = new ArrayList<>();
      owners.forEach(
          (type, name) ->
              ownersList.add(
                  openLineage.newOwnershipJobFacetOwnersBuilder().name(name).type(type).build()));
      facetsBuilder.ownership(openLineage.newOwnershipJobFacetBuilder().owners(ownersList).build());
    }

    return openLineage
        .newJobBuilder()
        .namespace(job.getNamespace())
        .name(job.getName())
        .facets(facetsBuilder.build())
        .build();
  }

  private OpenLineage.Run enrichRun(OpenLineage openLineage, OpenLineage.Run run) {
    if (run == null) {
      return null;
    }

    List<TagField> configRunTags = getConfigRunTags();
    if (configRunTags.isEmpty()) {
      return run;
    }

    OpenLineage.RunFacets existing = run.getFacets();
    OpenLineage.RunFacetsBuilder facetsBuilder = openLineage.newRunFacetsBuilder();

    // Copy all existing named facets
    if (existing != null) {
      if (existing.getNominalTime() != null) facetsBuilder.nominalTime(existing.getNominalTime());
      if (existing.getParent() != null) facetsBuilder.parent(existing.getParent());
      if (existing.getErrorMessage() != null) facetsBuilder.errorMessage(existing.getErrorMessage());
      if (existing.getProcessing_engine() != null)
        facetsBuilder.processing_engine(existing.getProcessing_engine());
      if (existing.getTags() != null) facetsBuilder.tags(existing.getTags());
      // Copy additional (unknown) facets
      if (existing.getAdditionalProperties() != null) {
        existing.getAdditionalProperties().forEach(facetsBuilder::put);
      }
    }

    // Merge run tags from config (override existing tags by key)
    OpenLineage.TagsRunFacet existingTags = (existing != null) ? existing.getTags() : null;
    facetsBuilder.tags(mergeRunTagFacet(openLineage, existingTags, configRunTags));

    return openLineage
        .newRunBuilder()
        .runId(run.getRunId())
        .facets(facetsBuilder.build())
        .build();
  }

  /** Returns job tags from config (source = CONFIG). */
  private List<TagField> getConfigJobTags() {
    return Optional.ofNullable(openLineageConfig)
        .map(OpenLineageConfig::getJobConfig)
        .map(JobConfig::getTags)
        .orElse(Collections.emptyList());
  }

  /** Returns run tags from config plus the default openlineage_client_version tag. */
  private List<TagField> getConfigRunTags() {
    List<TagField> tags = new ArrayList<>();
    // Default version tag (always present, source = OPENLINEAGE_CLIENT)
    tags.add(new TagField("openlineage_client_version", getClientVersion(), "OPENLINEAGE_CLIENT"));
    // User-configured run tags
    List<TagField> configTags =
        Optional.ofNullable(openLineageConfig)
            .map(OpenLineageConfig::getRunConfig)
            .map(RunConfig::getTags)
            .orElse(Collections.emptyList());
    tags.addAll(configTags);
    return tags;
  }

  /** Returns ownership map from config (supports both "owners" and "ownership" keys). */
  private Map<String, String> getConfigOwners() {
    return Optional.ofNullable(openLineageConfig)
        .map(OpenLineageConfig::getJobConfig)
        .map(JobConfig::getEffectiveOwners)
        .map(JobConfig.JobOwnersConfig::getAdditionalProperties)
        .filter(Objects::nonNull)
        .orElse(Collections.emptyMap());
  }

  private OpenLineage.TagsJobFacet mergeJobTagFacet(
      OpenLineage openLineage,
      OpenLineage.TagsJobFacet existing,
      List<TagField> configTags) {
    Map<String, OpenLineage.TagsJobFacetFields> tagMap = new HashMap<>();
    // Start with existing tags
    if (existing != null && existing.getTags() != null) {
      existing.getTags().forEach(t -> tagMap.put(t.getKey().toLowerCase(), t));
    }
    // Config tags override by key (case-insensitive)
    configTags.forEach(
        t ->
            tagMap.put(
                t.getKey().toLowerCase(),
                openLineage.newTagsJobFacetFields(t.getKey(), t.getValue(), t.getSource())));
    return openLineage
        .newTagsJobFacetBuilder()
        .tags(new ArrayList<>(tagMap.values()))
        .build();
  }

  private OpenLineage.TagsRunFacet mergeRunTagFacet(
      OpenLineage openLineage,
      OpenLineage.TagsRunFacet existing,
      List<TagField> configTags) {
    Map<String, OpenLineage.TagsRunFacetFields> tagMap = new HashMap<>();
    // Start with existing tags
    if (existing != null && existing.getTags() != null) {
      existing.getTags().forEach(t -> tagMap.put(t.getKey().toLowerCase(), t));
    }
    // Config tags override by key (case-insensitive)
    configTags.forEach(
        t ->
            tagMap.put(
                t.getKey().toLowerCase(),
                openLineage.newTagsRunFacetFields(t.getKey(), t.getValue(), t.getSource())));
    return openLineage
        .newTagsRunFacetBuilder()
        .tags(new ArrayList<>(tagMap.values()))
        .build();
  }

  @SuppressWarnings("PMD")
  static String getClientVersion() {
    try {
      Properties properties = new Properties();
      InputStream is =
          OpenLineageClient.class.getResourceAsStream(
              "/io/openlineage/client/client.version.properties");
      if (is == null) {
        return "unknown";
      }
      properties.load(is);
      return properties.getProperty("version", "unknown");
    } catch (IOException e) {
      return "unknown";
    }
  }

  public void initializeMetrics() {
    emitStart =
        this.meterRegistry.counter(
            "openlineage.emit.start", "openlineage.transport", transport.getClass().getName());
    emitComplete =
        this.meterRegistry.counter(
            "openlineage.emit.complete", "openlineage.transport", transport.getClass().getName());
    engagedCircuitBreaker =
        this.meterRegistry.gauge(
            "openlineage.circuitbreaker.engaged",
            Collections.singletonList(
                Tag.of("openlineage.circuitbreaker", circuitBreaker.getClass().getName())),
            new AtomicInteger(0));
    emitTime =
        this.meterRegistry.timer(
            "openlineage.emit.time", "openlineage.transport", transport.getClass().getName());
  }

  /** Shutdown the underlying transport, waiting for all events to complete. */
  @Override
  public void close() throws Exception {
    try {
      transport.close();
    } catch (Exception e) {
      throw new OpenLineageClientException("Failed to close transport " + transport, e);
    } finally {
      circuitBreaker.ifPresent(CircuitBreaker::close);
      meterRegistry.close();
      OpenLineageClientUtils.getExecutor().ifPresent(ExecutorService::shutdown);
    }
  }

  /**
   * @return an new {@link OpenLineageClient.Builder} object for building {@link
   *     OpenLineageClient}s.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for {@link OpenLineageClient} instances.
   *
   * <p>Usage:
   *
   * <pre>{@code
   * OpenLineageClient client = OpenLineageClient().builder()
   *     .url("http://localhost:5000")
   *     .build()
   * }</pre>
   */
  public static final class Builder {
    private static final Transport DEFAULT_TRANSPORT = new ConsoleTransport();
    private Transport transport;
    private String[] disabledFacets;
    private CircuitBreaker circuitBreaker;
    private MeterRegistry meterRegistry;
    private OpenLineageConfig openLineageConfig;

    private Builder() {
      this.transport = DEFAULT_TRANSPORT;
      disabledFacets = new String[] {};
    }

    public Builder transport(@NonNull Transport transport) {
      this.transport = transport;
      return this;
    }

    public Builder circuitBreaker(@NonNull CircuitBreaker circuitBreaker) {
      this.circuitBreaker = circuitBreaker;
      return this;
    }

    public Builder meterRegistry(@NonNull MeterRegistry meterRegistry) {
      this.meterRegistry = meterRegistry;
      return this;
    }

    public Builder disableFacets(@NonNull String... disabledFacets) {
      this.disabledFacets = Arrays.copyOf(disabledFacets, disabledFacets.length);
      return this;
    }

    public Builder config(@NonNull OpenLineageConfig openLineageConfig) {
      this.openLineageConfig = openLineageConfig;
      return this;
    }

    /**
     * @return an {@link OpenLineageClient} object with the properties of this {@link
     *     OpenLineageClient.Builder}.
     */
    public OpenLineageClient build() {
      return new OpenLineageClient(
          transport, circuitBreaker, meterRegistry, openLineageConfig, disabledFacets);
    }
  }
}
