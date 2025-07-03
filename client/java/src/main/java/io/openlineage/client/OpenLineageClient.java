/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.openlineage.client.circuitBreaker.CircuitBreaker;
import io.openlineage.client.metrics.MicrometerProvider;
import io.openlineage.client.transports.ConsoleTransport;
import io.openlineage.client.transports.Transport;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
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
    this(transport, null, null, disabledFacets);
  }

  public OpenLineageClient(
      @NonNull final Transport transport,
      CircuitBreaker circuitBreaker,
      MeterRegistry meterRegistry,
      String... disabledFacets) {
    this.transport = transport;
    this.disabledFacets = Arrays.copyOf(disabledFacets, disabledFacets.length);
    this.circuitBreaker = Optional.ofNullable(circuitBreaker);
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
    emitStart.increment();
    emitTime.record(() -> transport.emit(runEvent));
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
    emitStart.increment();
    emitTime.record(() -> transport.emit(jobEvent));
    emitComplete.increment();
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

    /**
     * @return an {@link OpenLineageClient} object with the properties of this {@link
     *     OpenLineageClient.Builder}.
     */
    public OpenLineageClient build() {
      return new OpenLineageClient(transport, circuitBreaker, meterRegistry, disabledFacets);
    }
  }
}
