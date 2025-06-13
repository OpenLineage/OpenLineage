/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.agent.lifecycle;

import static java.lang.Thread.sleep;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.circuitBreaker.TimeoutCircuitBreakerConfig;
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

  @BeforeEach
  void setup() {
    when(openLineageContext.getOpenLineageConfig()).thenReturn(config);
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
  }

  @Test
  @SneakyThrows
  void testOutputDatasetsTimeout() {
    // should time out within 80ms
    when(timeoutConfig.getBuildDatasetsTimePercentage()).thenReturn(80);

    // timeout should be applied
    assertThat(executor.timeoutOutputDatasets(outputsCallableWithTimeout))
        .isEqualTo(Collections.emptyList());
  }

  //  @Test
  //  void testOutputDatasetsCumulativeTimeout() {
  //    // TODO:
  //  }

  // TODO: test facetsBuildingTimePercentage
}
