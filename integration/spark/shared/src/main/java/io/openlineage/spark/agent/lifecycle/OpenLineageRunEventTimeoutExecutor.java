/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.agent.lifecycle;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.JobFacets;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.RunFacets;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.circuitBreaker.CircuitBreakerConfig;
import io.openlineage.spark.agent.facets.DebugRunFacet;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import io.openlineage.spark.api.TimeoutConfig;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OpenLineageRunEventTimeoutExecutor {

  private static final String INCOMPLETE_LINEAGE_MESSSAGE =
      "Incomplete lineage: timeout caught. Inputs missing: %s, Outputs missing: %s, RunFacets missing: %s, JobFacets missing: %s";

  private final TimeoutConfig timeoutConfig;
  private final ExecutorService executor;
  private final Duration globalTimeout;

  private long datasetsTimeSpent;
  private long runFacetsTimeSpent;
  private long jobFacetsTimeSpent;

  private boolean inputsTimedOut;
  private boolean outputsTimedOut;
  private boolean runFacetsTimedOut;
  private boolean jobFacetsTimedOut;

  public OpenLineageRunEventTimeoutExecutor(OpenLineageContext context) {
    globalTimeout =
        Optional.ofNullable(context)
            .map(OpenLineageContext::getOpenLineageConfig)
            .map(SparkOpenLineageConfig::getCircuitBreaker)
            .map(CircuitBreakerConfig::getTimeout)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .orElse(null);

    timeoutConfig =
        Optional.ofNullable(context)
            .filter(c -> globalTimeout != null)
            .map(OpenLineageContext::getOpenLineageConfig)
            .map(SparkOpenLineageConfig::getTimeoutConfig)
            .orElse(null);

    executor = OpenLineageClientUtils.getOrCreateExecutor();
  }

  public List<InputDataset> timeoutInputDatasets(Callable<List<InputDataset>> callable) {
    if (outputsTimedOut) {
      inputsTimedOut = true;
      return Collections.emptyList();
    }
    TimeoutExecutor<List<InputDataset>> timeoutExecutor =
        new TimeoutExecutor<>(
            callable, effectiveDatasetsTimeout().orElse(0L), Collections.emptyList());
    List<InputDataset> inputDatasets = timeoutExecutor.run();
    datasetsTimeSpent += timeoutExecutor.timeSpent;
    inputsTimedOut = timeoutExecutor.cancelled;
    return inputDatasets;
  }

  public List<OutputDataset> timeoutOutputDatasets(Callable<List<OutputDataset>> callable) {
    if (inputsTimedOut) {
      outputsTimedOut = true;
      return Collections.emptyList();
    }
    TimeoutExecutor<List<OutputDataset>> timeoutExecutor =
        new TimeoutExecutor<>(
            callable, effectiveDatasetsTimeout().orElse(0L), Collections.emptyList());
    List<OutputDataset> outputDatasets = timeoutExecutor.run();
    datasetsTimeSpent += timeoutExecutor.timeSpent;
    outputsTimedOut = timeoutExecutor.cancelled;
    return outputDatasets;
  }

  public JobFacets timeoutJobFacets(Callable<JobFacets> callable) {
    if (facetsTimeoutConfigured().isPresent() && !facetsTimeout().isPresent()) {
      jobFacetsTimedOut = true;
      return null;
    }
    TimeoutExecutor<JobFacets> timeoutExecutor =
        new TimeoutExecutor<>(callable, facetsTimeout().orElse(0L), null);
    JobFacets jobFacets = timeoutExecutor.run();
    jobFacetsTimeSpent += timeoutExecutor.timeSpent;
    jobFacetsTimedOut = timeoutExecutor.cancelled;
    return jobFacets;
  }

  public RunFacets timeoutRunFacets(Callable<RunFacets> callable, OpenLineage openLineage) {
    RunFacets runFacets = null;
    if (facetsTimeoutConfigured().isPresent() && !facetsTimeout().isPresent()) {
      runFacetsTimedOut = true;
    } else {
      TimeoutExecutor<RunFacets> timeoutExecutor =
          new TimeoutExecutor<>(callable, facetsTimeout().orElse(0L), null);
      runFacets = timeoutExecutor.run();
      runFacetsTimeSpent += timeoutExecutor.timeSpent;
      runFacetsTimedOut = timeoutExecutor.cancelled;
    }

    if (hasAnyTimeoutOccurred()) {
      if (runFacets == null) {
        runFacets = openLineage.newRunFacetsBuilder().build();
      }
      addTimeoutLog(runFacets);
    }

    return runFacets;
  }

  private boolean hasAnyTimeoutOccurred() {
    return inputsTimedOut || outputsTimedOut || runFacetsTimedOut || jobFacetsTimedOut;
  }

  private void addTimeoutLog(RunFacets runFacets) {
    String message =
        String.format(
            INCOMPLETE_LINEAGE_MESSSAGE,
            inputsTimedOut,
            outputsTimedOut,
            runFacetsTimedOut,
            jobFacetsTimedOut);
    log.warn(message);

    if (runFacets.getAdditionalProperties().containsKey("debug")) {
      DebugRunFacet debugFacet = (DebugRunFacet) runFacets.getAdditionalProperties().get("debug");
      debugFacet.getLogs().add(message);
    } else {
      DebugRunFacet debugFacet =
          new DebugRunFacet(null, null, null, null, null, null, Arrays.asList(message), 100);
      runFacets.getAdditionalProperties().put("debug", debugFacet);
    }
  }

  private Optional<Long> facetsTimeout() {
    return facetsTimeoutConfigured()
        .map(l -> l - datasetsTimeSpent - runFacetsTimeSpent - jobFacetsTimeSpent)
        .filter(l -> l > 0);
  }

  private Optional<Long> effectiveDatasetsTimeout() {
    if (globalTimeout == null || timeoutConfig == null) {
      return Optional.empty();
    }

    Optional<Long> datasetsTimeoutConfigured = datasetsTimeoutConfigured();
    Optional<Long> facetsTimeoutConfigured = facetsTimeoutConfigured();

    // no timeout configured, return empty
    long timeout;
    if (datasetsTimeoutConfigured.isPresent()) {
      // take min of both timeout settings
      // only datasets timeout configured
      timeout =
          facetsTimeoutConfigured
              .map(aLong -> Math.min(datasetsTimeoutConfigured.get(), aLong))
              .orElseGet(datasetsTimeoutConfigured::get);
    } else {
      // only facets timeout configured
      timeout = facetsTimeoutConfigured.orElse(0L);
    }

    return Optional.of(timeout - datasetsTimeSpent)
        .filter(t -> t > 0); // if facets timeout is configured, return it
  }

  private Optional<Long> datasetsTimeoutConfigured() {
    if (timeoutConfig != null
        && globalTimeout != null
        && timeoutConfig.getBuildDatasetsTimePercentage() != null
        && timeoutConfig.getBuildDatasetsTimePercentage() > 0
        && timeoutConfig.getBuildDatasetsTimePercentage() <= 100) {
      return Optional.of(
          (globalTimeout.toMillis() * timeoutConfig.getBuildDatasetsTimePercentage()) / 100);
    }
    return Optional.empty();
  }

  private Optional<Long> facetsTimeoutConfigured() {
    if (timeoutConfig != null
        && globalTimeout != null
        && timeoutConfig.getFacetsBuildingTimePercentage() != null
        && timeoutConfig.getFacetsBuildingTimePercentage() > 0
        && timeoutConfig.getFacetsBuildingTimePercentage() <= 100) {
      return Optional.of(
          (globalTimeout.toMillis() * timeoutConfig.getFacetsBuildingTimePercentage()) / 100);
    }
    return Optional.empty();
  }

  private class TimeoutExecutor<T> {
    private final Callable<T> callable;
    private final long timeout;
    private final T emptyResult;

    boolean cancelled;
    int timeSpent;

    TimeoutExecutor(Callable<T> callable, long timeout, T emptyResult) {
      this.callable = callable;
      this.timeout = timeout;
      this.emptyResult = emptyResult;
    }

    T run() {
      if (timeout <= 0) {
        return noTimeoutCall(callable);
      }
      long startTime = System.currentTimeMillis();
      Future<T> callableExecutor = executor.submit(callable);
      try {
        T result = callableExecutor.get(timeout, MILLISECONDS);
        timeSpent = (int) (System.currentTimeMillis() - startTime);
        return result;
      } catch (InterruptedException | ExecutionException ex) {
        throw new RuntimeException(ex);
      } catch (TimeoutException timeoutException) {
        callableExecutor.cancel(true);
        timeSpent = (int) (System.currentTimeMillis() - startTime);
        cancelled = true;
        return emptyResult;
      }
    }

    private T noTimeoutCall(Callable<T> callable) {
      try {
        return callable.call();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
