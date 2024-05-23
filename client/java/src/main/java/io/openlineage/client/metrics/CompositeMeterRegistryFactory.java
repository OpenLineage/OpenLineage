/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.metrics;

import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import java.util.List;
import java.util.Map;

/**
 * A builder class that provides implementations to build composite meter registries. This class
 * implements the MetricsBuilder interface with CompositeMeterRegistry as its type.
 *
 * <p>CompositeMeterRegistry is a type of MeterRegistry, that encapsulates two or more meter
 * registries into one, and manages unified functionalities across all registries.
 */
public class CompositeMeterRegistryFactory implements MeterRegistryFactory<CompositeMeterRegistry> {

  /**
   * Constructs a CompositeMeterRegistry from a given map of configuration options. The "registries"
   * key in the map is expected to provide a list of meter registry configurations. Each
   * configuration is parsed and, if parsed successfully, added to the CompositeMeterRegistry.
   *
   * @param config The map containing the configurations for composite meter registry.
   * @return A CompositeMeterRegistry built from the provided configuration. An empty
   *     CompositeMeterRegistry is returned if the the map doesn't contain a list of configurations
   *     extended by registries.
   */
  @Override
  public CompositeMeterRegistry registry(Map<String, Object> config) {
    CompositeMeterRegistry meterRegistry = new CompositeMeterRegistry();
    Object registries = config.get("registries");
    if (!(registries instanceof List) || ((List<?>) registries).isEmpty()) {
      return meterRegistry;
    }
    for (Object registryConfig : (List<Object>) registries) {
      if (registryConfig instanceof Map || !((Map<?, ?>) registryConfig).isEmpty()) {
        MicrometerProvider.parseMeterRegistryConfig((Map<String, Object>) registryConfig)
            .ifPresent(meterRegistry::add);
      }
    }
    return meterRegistry;
  }

  @Override
  public String type() {
    return "composite";
  }
}
