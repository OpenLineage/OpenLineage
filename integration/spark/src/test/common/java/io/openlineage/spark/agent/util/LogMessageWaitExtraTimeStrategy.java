/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent.util;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import lombok.SneakyThrows;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

/**
 * Custom wait strategy for running Spark Docker containers. It waits for defined logs to appear
 * (like SparkShutdown hook) and waits extra time (specified as a parameter in millis), to give an
 * asynchronous OpenLineage event more time to get delivered. It is used to prevent SparkContainers
 * stop too early.
 */
public class LogMessageWaitExtraTimeStrategy extends LogMessageWaitStrategy {

  private Duration extraWaitingTime = Duration.of(0, ChronoUnit.SECONDS);

  public LogMessageWaitStrategy withExtraWaitingTime(Duration extraWaitingTime) {
    this.extraWaitingTime = extraWaitingTime;
    return this;
  }

  @Override
  @SneakyThrows(InterruptedException.class)
  protected void waitUntilReady() {
    super.waitUntilReady();
    Thread.sleep(extraWaitingTime.toMillis());
  }
}
