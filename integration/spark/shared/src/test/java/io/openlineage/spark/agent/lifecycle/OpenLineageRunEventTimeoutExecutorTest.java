/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.agent.lifecycle;

import static java.lang.Thread.sleep;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.JobFacets;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.RunFacets;
import io.openlineage.client.circuitBreaker.TimeoutCircuitBreakerConfig;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.facets.DebugRunFacet;
import io.openlineage.spark.agent.facets.builder.DebugRunFacetBuilderDelegate;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import io.openlineage.spark.api.TimeoutConfig;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OpenLineageRunEventTimeoutExecutorTest {

  public static final String DEBUG = "debug";
  TimeoutCircuitBreakerConfig circuitBreakerConfig = mock(TimeoutCircuitBreakerConfig.class);
  TimeoutConfig timeoutConfig = mock(TimeoutConfig.class);
  OpenLineageContext openLineageContext = mock(OpenLineageContext.class);
  SparkOpenLineageConfig config = mock(SparkOpenLineageConfig.class);
  OpenLineageRunEventTimeoutExecutor executor;

  List<InputDataset> inputDatasets = Collections.singletonList(mock(InputDataset.class));
  Callable<List<InputDataset>> inputsCallableWithTimeout =
      () -> {
        sleep(100);
        return inputDatasets;
      };

  List<OutputDataset> outputDatasets = Collections.singletonList(mock(OutputDataset.class));
  Callable<List<OutputDataset>> outputsCallableWithTimeout =
      () -> {
        sleep(100);
        return outputDatasets;
      };

  JobFacets jobFacets = mock(JobFacets.class);
  Callable<JobFacets> jobFacetsCallableWithTimeout =
      () -> {
        sleep(100);
        return jobFacets;
      };

  RunFacets runFacets = mock(RunFacets.class);
  Callable<RunFacets> runFacetsCallableWithTimeout =
      () -> {
        sleep(100);
        return runFacets;
      };

  @BeforeEach
  void setup() {
    when(openLineageContext.getOpenLineageConfig()).thenReturn(config);
    when(openLineageContext.getOpenLineage())
        .thenReturn(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI));
    when(openLineageContext.getMeterRegistry()).thenReturn(new SimpleMeterRegistry());
    when(config.getTimeoutConfig()).thenReturn(timeoutConfig);
    when(config.getCircuitBreaker()).thenReturn(circuitBreakerConfig);
    when(circuitBreakerConfig.getTimeout()).thenReturn(Optional.of(Duration.ofMillis(100)));

    executor = new OpenLineageRunEventTimeoutExecutor(openLineageContext);
  }

  @Test
  void testInputDatasetsNoTimeout() {
    assertThat(executor.timeoutInputDatasets(() -> inputDatasets)).isEqualTo(inputDatasets);
  }

  @Test
  @SneakyThrows
  void testInputDatasetsTimeout() {
    // should time out within 80ms
    when(timeoutConfig.getBuildDatasetsTimePercentage()).thenReturn(80);

    // timeout should be applied
    assertThat(executor.timeoutInputDatasets(inputsCallableWithTimeout))
        .isEqualTo(Collections.emptyList());

    assertThat(
            executor.timeoutOutputDatasets(
                () -> {
                  // this should not be called as outputs already timed out
                  throw new AssertionError();
                }))
        .isEqualTo(Collections.emptyList());
  }

  @Test
  @SneakyThrows
  void testInputDatasetsTimeoutDoesWorkForIncorrectConfiguration() {
    when(timeoutConfig.getBuildDatasetsTimePercentage()).thenReturn(0);
    assertThat(executor.timeoutInputDatasets(() -> inputDatasets)).isEqualTo(inputDatasets);

    when(timeoutConfig.getBuildDatasetsTimePercentage()).thenReturn(101);
    assertThat(executor.timeoutInputDatasets(() -> inputDatasets)).isEqualTo(inputDatasets);

    when(timeoutConfig.getBuildDatasetsTimePercentage()).thenReturn(null);
    assertThat(executor.timeoutInputDatasets(() -> inputDatasets)).isEqualTo(inputDatasets);

    when(config.getTimeoutConfig()).thenReturn(null);
    assertThat(executor.timeoutInputDatasets(() -> inputDatasets)).isEqualTo(inputDatasets);
  }

  @Test
  void testOutputDatasetsNoTimeout() {
    assertThat(executor.timeoutOutputDatasets(() -> outputDatasets)).isEqualTo(outputDatasets);
    assertThat(
            executor
                .timeoutRunFacets(
                    () -> openLineageContext.getOpenLineage().newRunFacetsBuilder().build(),
                    openLineageContext.getOpenLineage())
                .getAdditionalProperties())
        .doesNotContainKey(DEBUG);
  }

  @Test
  @SneakyThrows
  void testOutputDatasetsTimeout() {
    // should time out within 80ms
    when(timeoutConfig.getBuildDatasetsTimePercentage()).thenReturn(80);

    // timeout should be applied
    assertThat(executor.timeoutOutputDatasets(outputsCallableWithTimeout))
        .isEqualTo(Collections.emptyList());

    assertThat(
            executor.timeoutInputDatasets(
                () -> {
                  // this should not be called
                  throw new AssertionError();
                }))
        .isEqualTo(Collections.emptyList());
  }

  @Test
  @SneakyThrows
  void testOutputDatasetsTimeoutWithFacetsTimeout() {
    // should time out within 80ms
    when(circuitBreakerConfig.getTimeout()).thenReturn(Optional.of(Duration.ofMillis(200)));
    when(timeoutConfig.getBuildDatasetsTimePercentage())
        .thenReturn(100); // this should not cause timeout
    when(timeoutConfig.getFacetsBuildingTimePercentage())
        .thenReturn(40); // this should cause timeout

    // timeout should be applied
    assertThat(executor.timeoutOutputDatasets(outputsCallableWithTimeout))
        .isEqualTo(Collections.emptyList());
  }

  @Test
  void testOutputDatasetsCumulativeTimeout() {
    // should time out within 80ms
    when(timeoutConfig.getBuildDatasetsTimePercentage()).thenReturn(80);

    List<InputDataset> inputDatasets = Collections.singletonList(mock(InputDataset.class));
    Callable<List<InputDataset>> inputsCallable =
        () -> {
          sleep(50);
          return inputDatasets;
        };

    List<OutputDataset> outputDatasets = Collections.singletonList(mock(OutputDataset.class));
    Callable<List<OutputDataset>> outputsCallable =
        () -> {
          sleep(50);
          return outputDatasets;
        };

    // inputs should run with despite sleep
    assertThat(executor.timeoutInputDatasets(inputsCallable)).isEqualTo(inputDatasets);

    // the other call should timeout out as cumulative `sleep` is more than 80ms
    assertThat(executor.timeoutOutputDatasets(outputsCallable)).isEmpty();
  }

  @Test
  @SneakyThrows
  void testDatasetsTimeoutAddsLogToDebugFacet() {
    // should time out within 80ms
    when(timeoutConfig.getBuildDatasetsTimePercentage()).thenReturn(80);
    executor.timeoutOutputDatasets(outputsCallableWithTimeout);

    // Run facets already contains debug facet
    RunFacets runFacets =
        executor.timeoutRunFacets(
            () ->
                openLineageContext
                    .getOpenLineage()
                    .newRunFacetsBuilder()
                    .put(DEBUG, new DebugRunFacetBuilderDelegate(openLineageContext).buildFacet())
                    .build(),
            openLineageContext.getOpenLineage());
    DebugRunFacet facet = (DebugRunFacet) runFacets.getAdditionalProperties().get(DEBUG);
    assertThat(facet.getLogs().get(0)).startsWith("Incomplete lineage:");

    // Run facets does not contain debug facet
    runFacets =
        executor.timeoutRunFacets(
            () -> openLineageContext.getOpenLineage().newRunFacetsBuilder().build(),
            openLineageContext.getOpenLineage());
    facet = (DebugRunFacet) runFacets.getAdditionalProperties().get(DEBUG);
    assertThat(facet.getLogs().get(0)).startsWith("Incomplete lineage:");
  }

  @Test
  void testJobFacetsNoTimeout() {
    assertThat(executor.timeoutJobFacets(() -> jobFacets)).isEqualTo(jobFacets);
  }

  @Test
  @SneakyThrows
  void testJobFacetsTimeout() {
    // should time out within 80ms
    when(timeoutConfig.getFacetsBuildingTimePercentage()).thenReturn(80);

    // timeout should be applied
    assertThat(executor.timeoutJobFacets(jobFacetsCallableWithTimeout)).isNull();
  }

  @Test
  void testRunFacetsNoTimeout() {
    assertThat(executor.timeoutRunFacets(() -> runFacets, openLineageContext.getOpenLineage()))
        .isEqualTo(runFacets);
  }

  @Test
  @SneakyThrows
  void testRunFacetsTimeout() {
    // should time out within 80ms
    when(timeoutConfig.getFacetsBuildingTimePercentage()).thenReturn(80);

    // timeout should be applied
    RunFacets runFacets =
        executor.timeoutRunFacets(
            runFacetsCallableWithTimeout, openLineageContext.getOpenLineage());
    assertThat(runFacets).isNotNull();
    assertThat(runFacets.getAdditionalProperties().get(DEBUG)).isNotNull();

    assertThat(
            executor.timeoutJobFacets(
                () -> {
                  // this should not be called
                  throw new AssertionError();
                }))
        .isEqualTo(null);
  }

  @Test
  void testFacetsTimeoutStopsExecution() {
    // mock outputs facets exceed timeout configured
    when(timeoutConfig.getFacetsBuildingTimePercentage()).thenReturn(80);
    when(timeoutConfig.getBuildDatasetsTimePercentage()).thenReturn(null);

    // verifies facets timeout works when dataset facets timeout not configured
    assertThat(executor.timeoutOutputDatasets(outputsCallableWithTimeout))
        .isEqualTo(Collections.emptyList());

    assertThat(
            executor.timeoutInputDatasets(
                () -> {
                  // this should not be called
                  throw new AssertionError();
                }))
        .isEqualTo(Collections.emptyList());

    assertThat(
            executor.timeoutJobFacets(
                () -> {
                  // this should not be called
                  throw new AssertionError();
                }))
        .isNull();

    assertThat(
            executor.timeoutRunFacets(
                () -> {
                  // this should not be called
                  throw new AssertionError();
                },
                openLineageContext.getOpenLineage()))
        .isNotNull()
        .extracting(RunFacets::getAdditionalProperties)
        .extracting(DEBUG)
        .isNotNull();
  }

  @Test
  void testFacetsCumulativeTimeout() {
    when(timeoutConfig.getFacetsBuildingTimePercentage()).thenReturn(80);

    List<InputDataset> inputDatasets = Collections.singletonList(mock(InputDataset.class));
    Callable<List<InputDataset>> inputsCallable =
        () -> {
          sleep(30);
          return inputDatasets;
        };

    List<OutputDataset> outputDatasets = Collections.singletonList(mock(OutputDataset.class));
    Callable<List<OutputDataset>> outputsCallable =
        () -> {
          sleep(30);
          return outputDatasets;
        };

    // inputs should run with despite sleep
    assertThat(executor.timeoutInputDatasets(inputsCallable)).isEqualTo(inputDatasets);
    assertThat(executor.timeoutOutputDatasets(outputsCallable)).isEqualTo(outputDatasets);

    Callable<JobFacets> jobFacetsCallable =
        () -> {
          sleep(30);
          return jobFacets;
        };
    assertThat(executor.timeoutJobFacets(jobFacetsCallable)).isNull();

    assertThat(
            executor.timeoutRunFacets(
                () -> {
                  // this should not be called
                  throw new AssertionError();
                },
                openLineageContext.getOpenLineage()))
        .isNotNull()
        .extracting(RunFacets::getAdditionalProperties)
        .extracting(DEBUG)
        .isNotNull();
  }
}
