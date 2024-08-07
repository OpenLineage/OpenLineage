/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client;

import com.fasterxml.jackson.core.type.TypeReference;
import io.micrometer.core.instrument.MeterRegistry;
import io.openlineage.client.circuitBreaker.CircuitBreakerFactory;
import io.openlineage.client.metrics.MicrometerProvider;
import io.openlineage.client.transports.NoopTransport;
import io.openlineage.client.transports.Transport;
import io.openlineage.client.transports.TransportFactory;
import java.util.Optional;

/** Factory class for creating new {@link OpenLineageClient} objects. */
public final class Clients {
  private Clients() {}

  /**
   * @return a new {@code OpenLineageClient} object.
   */
  public static OpenLineageClient newClient() {
    return newClient(new DefaultConfigPathProvider());
  }

  public static OpenLineageClient newClient(ConfigPathProvider configPathProvider) {
    if (isDisabled()) {
      return OpenLineageClient.builder().transport(new NoopTransport()).build();
    }
    final OpenLineageConfig openLineageConfig =
        OpenLineageClientUtils.loadOpenLineageConfigYaml(
            configPathProvider, new TypeReference<OpenLineageConfig>() {});
    return newClient(openLineageConfig);
  }

  public static OpenLineageClient newClient(OpenLineageConfig openLineageConfig) {
    if (isDisabled()) {
      return OpenLineageClient.builder().transport(new NoopTransport()).build();
    }
    final TransportFactory factory = new TransportFactory(openLineageConfig.getTransportConfig());
    final Transport transport = factory.build();
    // ...
    OpenLineageClient.Builder builder = OpenLineageClient.builder();

    if (openLineageConfig.getFacetsConfig() != null) {
      builder.disableFacets(openLineageConfig.getFacetsConfig().getEffectiveDisabledFacets());
    }

    Optional.ofNullable(openLineageConfig.getCircuitBreaker())
        .map(CircuitBreakerFactory::new)
        .ifPresent(f -> builder.circuitBreaker(f.build()));

    Optional.ofNullable(openLineageConfig.getMetricsConfig())
        .map(MicrometerProvider::addMeterRegistryFromConfig)
        .ifPresent(f -> builder.meterRegistry((MeterRegistry) f)); // Java 8 requires cast here :(
    return builder.transport(transport).build();
  }

  private static boolean isDisabled() {
    String disabled = Environment.getEnvironmentVariable("OPENLINEAGE_DISABLED");
    return (Boolean.parseBoolean(disabled));
  }
}
