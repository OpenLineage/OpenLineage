/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.metrics;

import io.micrometer.statsd.StatsdMeterRegistry;
import java.util.Map;

/**
 * StatsDMetricsBuilder is a class that implements the {@link MeterRegistryFactory} interface
 * specifically for {@link StatsdMeterRegistry}. It provides methods to check if the required {@link
 * StatsdMeterRegistry} class is available and build a {@link StatsdMeterRegistry} instance based on
 * a configuration map.
 *
 * @see <a
 *     href="https://docs.micrometer.io/micrometer/reference/implementations/statsD.html">StatsdConfig
 *     and StatsD micrometer interface docs</a>
 */
public class StatsDMeterRegistryFactory implements MeterRegistryFactory<StatsdMeterRegistry> {

  @Override
  public StatsdMeterRegistry registry(Map<String, Object> config) {
    return StatsdMeterRegistry.builder(s -> (String) config.get(s)).build();
  }

  @Override
  public String type() {
    return "statsd";
  }
}
