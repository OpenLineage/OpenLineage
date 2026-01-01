/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.metrics;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Map;

/**
 * This class implements the MetricsBuilder interface with SimpleMeterRegistry as its type.
 * SimpleMeterRegistry is a type of MeterRegistry, designed for testing functionality that does not
 * require a backend monitoring system.
 */
public class SimpleMeterRegistryFactory implements MeterRegistryFactory<SimpleMeterRegistry> {

  /**
   * Constructs a SimpleMeterRegistry. This method doesn't use the given map parameter, as
   * SimpleMeterRegistry does not require configuration options.
   *
   * @param config The map intended to contain the configurations. This parameter is not used.
   * @return A new SimpleMeterRegistry instance.
   */
  @Override
  public SimpleMeterRegistry registry(Map<String, Object> config) {
    return new SimpleMeterRegistry();
  }

  @Override
  public String type() {
    return "simple";
  }
}
