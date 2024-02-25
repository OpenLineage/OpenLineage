/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.circuitBreaker;

import java.util.Arrays;
import java.util.List;
import java.util.ServiceLoader;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class CircuitBreakerResolver {

  private static List<CircuitBreakerBuilder> builders =
      Arrays.asList(
          new StaticCircuitBreakerBuilder(),
          new SimpleMemoryCircuitBreakerBuilder(),
          new JavaRuntimeCircuitBreakerBuilder());

  public static Class<? extends CircuitBreakerConfig> resolveCircuitBreakerConfigByType(
      String type) {
    CircuitBreakerBuilder builder =
        getCircuitBreakerBuilder(b -> b.getType().equalsIgnoreCase(type));
    return builder.getConfig().getClass();
  }

  public static String resolveCircuitBreakerTypeByConfigClass(Class circuitBreakerConfigClass) {
    CircuitBreakerBuilder builder =
        getCircuitBreakerBuilder(b -> b.getConfig().getClass().equals(circuitBreakerConfigClass));
    return builder.getType();
  }

  public static CircuitBreaker resolveCircuitBreakerByConfig(
      CircuitBreakerConfig circuitBreakerConfig) {
    CircuitBreakerBuilder builder =
        getCircuitBreakerBuilder(
            b -> b.getConfig().getClass().equals(circuitBreakerConfig.getClass()));
    return builder.build(circuitBreakerConfig);
  }

  private static CircuitBreakerBuilder getCircuitBreakerBuilder(
      Predicate<CircuitBreakerBuilder> predicate) {

    return Stream.concat(
            builders.stream(),
            StreamSupport.stream(CircuitBreakerServiceLoader.load().spliterator(), false))
        .filter(predicate)
        .findFirst()
        .orElse(new NoOpCircuitBreakerBuilder());
  }

  static class CircuitBreakerServiceLoader {
    static ServiceLoader<CircuitBreakerBuilder> load() {
      return ServiceLoader.load(CircuitBreakerBuilder.class);
    }
  }
}
