/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.openlineage.client.utils.ReflectionUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.commons.lang3.StringUtils;

/**
 * MicrometerProvider is a class that manages global OpenLineage meter registry implementation that
 * allows integrations to both add metrics backend from common OpenLineage config, or grab an
 * instance of an initialized MeterRegistry.
 */
public class MicrometerProvider {

  private static final List<MeterRegistryFactory> factories;
  private static CompositeMeterRegistry registry;

  static {
    ServiceLoader<MeterRegistryFactory> loader = ServiceLoader.load(MeterRegistryFactory.class);
    factories =
        Stream.concat(
                getMetricsBuilders().stream(), StreamSupport.stream(loader.spliterator(), false))
            .collect(Collectors.toList());
    registry = new CompositeMeterRegistry();
  }

  /**
   * Parses the configuration map to create an optional MeterRegistry.
   *
   * @param config The configuration map for the MeterRegistry.
   * @return Optional MeterRegistry created based on the configuration. Empty optional if the type
   *     is invalid or empty.
   */
  public static Optional<MeterRegistry> parseMeterRegistryConfig(Map<String, Object> config) {
    if (config == null) {
      return Optional.empty();
    }
    Object type = config.get("type");
    if (!(type instanceof String) || type == null || StringUtils.isEmpty((String) type)) {
      return Optional.empty();
    }
    return getConfigBuilder((String) type).map(x -> x.registry(config));
  }

  /**
   * Adds a MeterRegistry to the common OpenLineage meter registry based on the provided
   * configuration.
   *
   * @param config The configuration for the MeterRegistry.
   * @return Common registry configured with the added MeterRegistry.
   */
  public static MeterRegistry addMeterRegistryFromConfig(Map<String, Object> config) {
    Optional<MeterRegistry> meterRegistry = parseMeterRegistryConfig(config);
    meterRegistry.ifPresent(x -> registry.add(x));
    return registry;
  }

  /**
   * Adds a MeterRegistry to the common OpenLineage meter registry.
   *
   * @param meterRegistry The MeterRegistry to
   */
  public static MeterRegistry addMeterRegistry(MeterRegistry meterRegistry) {
    registry.add(meterRegistry);
    return registry;
  }

  /**
   * Returns the global OpenLineage meter registry.
   *
   * @return The global OpenLineage meter registry.
   */
  public static MeterRegistry getMeterRegistry() {
    return registry;
  }

  /**
   * Clears the global OpenLineage meter registry and creates a new instance of a
   * CompositeMeterRegistry.
   *
   * @return The newly created CompositeMeterRegistry instance.
   */
  public static MeterRegistry clear() {
    registry.close();
    registry = new CompositeMeterRegistry();
    return registry;
  }

  private static List<MeterRegistryFactory> getMetricsBuilders() {
    List<MeterRegistryFactory> builders = new ArrayList<>();
    if (ReflectionUtils.hasClass("io.micrometer.statsd.StatsdMeterRegistry")) {
      builders.add(new StatsDMeterRegistryFactory());
    }
    builders.add(new SimpleMeterRegistryFactory());
    builders.add(new CompositeMeterRegistryFactory());
    return builders;
  }

  private static Optional<MeterRegistryFactory> getConfigBuilder(String type) {
    return factories.stream().filter(builder -> builder.type().equals(type)).findFirst();
  }
}
