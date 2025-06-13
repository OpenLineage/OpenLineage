/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.agent.lifecycle;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.JobFacets;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.RunFacets;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.circuitBreaker.CircuitBreakerConfig;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import io.openlineage.spark.api.TimeoutConfig;
import java.time.Duration;
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

  private final TimeoutConfig timeoutConfig;
  private final ExecutorService executor;
  private final Optional<Duration> globalTimeout;

  private long datasetsTimeSpent;

  public OpenLineageRunEventTimeoutExecutor(OpenLineageContext context) {
    globalTimeout =
        Optional.ofNullable(context)
            .map(OpenLineageContext::getOpenLineageConfig)
            .map(SparkOpenLineageConfig::getCircuitBreaker)
            .map(CircuitBreakerConfig::getTimeout)
            .filter(Optional::isPresent)
            .map(Optional::get);

    timeoutConfig =
        Optional.ofNullable(context)
            .filter(c -> globalTimeout.isPresent())
            .map(OpenLineageContext::getOpenLineageConfig)
            .map(SparkOpenLineageConfig::getTimeoutConfig)
            .orElse(null);

    executor = OpenLineageClientUtils.getExecutor();
  }

  public List<InputDataset> timeoutInputDatasets(Callable<List<InputDataset>> callable) {
    if (!isDatasetsTimeoutEnabled()) {
      try {
        return callable.call();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    long startTime = System.currentTimeMillis();
    Future<List<InputDataset>> callableExecutor = executor.submit(callable);
    try {
      List<InputDataset> datasets = callableExecutor.get(datasetsTimeout(), MILLISECONDS);
      datasetsTimeSpent += System.currentTimeMillis() - startTime;
      return datasets;
    } catch (InterruptedException | ExecutionException ex) {
      throw new RuntimeException(ex);
    } catch (TimeoutException timeoutException) {
      callableExecutor.cancel(true);
      return Collections.emptyList();
    }
  }

  public List<OutputDataset> timeoutOutputDatasets(Callable<List<OutputDataset>> callable) {
    if (!isDatasetsTimeoutEnabled()) {
      try {
        return callable.call();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    long startTime = System.currentTimeMillis();
    Future<List<OutputDataset>> callableExecutor = executor.submit(callable);
    try {
      List<OutputDataset> datasets = callableExecutor.get(datasetsTimeout(), MILLISECONDS);
      datasetsTimeSpent += System.currentTimeMillis() - startTime;
      return datasets;
    } catch (InterruptedException | ExecutionException ex) {
      throw new RuntimeException(ex);
    } catch (TimeoutException timeoutException) {
      callableExecutor.cancel(true);
      return Collections.emptyList();
    }
  }

  public JobFacets timeoutJobFacets(Callable<JobFacets> callable) {
    if (timeoutConfig == null) {
      try {
        return callable.call();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    // TODO: implement timeout logic
    return null;
  }

  public RunFacets timeoutRunFacets(Callable<RunFacets> callable) {
    RunFacets runFacets = null;
    if (timeoutConfig == null) {
      try {
        runFacets = callable.call();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    // TODO: implement timeout logic
    return runFacets;
  }

  private long datasetsTimeout() {
    return (globalTimeout.get().toMillis() * timeoutConfig.getBuildDatasetsTimePercentage()) / 100;
  }

  private boolean isDatasetsTimeoutEnabled() {
    return timeoutConfig != null
        && timeoutConfig.getBuildDatasetsTimePercentage() != null
        && timeoutConfig.getBuildDatasetsTimePercentage() > 0
        && timeoutConfig.getBuildDatasetsTimePercentage() < 100;
  }

  // TODO: write integration test for timeout
}
