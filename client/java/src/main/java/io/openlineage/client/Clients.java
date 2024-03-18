/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client;

import io.openlineage.client.circuitBreaker.CircuitBreakerFactory;
import io.openlineage.client.metrics.MicrometerProvider;
import io.openlineage.client.transports.NoopTransport;
import io.openlineage.client.transports.Transport;
import io.openlineage.client.transports.TransportFactory;
import java.util.Optional;

/** Factory class for creating new {@link OpenLineageClient} objects. */
public final class Clients {
  private Clients() {}

  /** Returns a new {@code OpenLineageClient} object. */
  public static OpenLineageClient newClient() {
    return newClient(new DefaultConfigPathProvider());
  }

  public static OpenLineageClient newClient(ConfigPathProvider configPathProvider) {
    if (isDisabled()) {
      return OpenLineageClient.builder().transport(new NoopTransport()).build();
    }
    final OpenLineageYaml openLineageYaml =
        OpenLineageClientUtils.loadOpenLineageYaml(configPathProvider);
    return newClient(openLineageYaml);
  }

  public static OpenLineageClient newClient(OpenLineageYaml openLineageYaml) {
    if (isDisabled()) {
      return OpenLineageClient.builder().transport(new NoopTransport()).build();
    }
    final TransportFactory factory = new TransportFactory(openLineageYaml.getTransportConfig());
    final Transport transport = factory.build();
    // ...
    OpenLineageClient.Builder builder = OpenLineageClient.builder();

    if (openLineageYaml.getFacetsConfig() != null) {
      builder.disableFacets(openLineageYaml.getFacetsConfig().getDisabledFacets());
    }

    Optional.ofNullable(openLineageYaml.getCircuitBreaker())
        .map(CircuitBreakerFactory::new)
        .ifPresent(f -> builder.circuitBreaker(f.build()));

    Optional.ofNullable(openLineageYaml.getMetricsConfig())
        .map(MicrometerProvider::addMeterRegistryFromConfig)
        .ifPresent(builder::meterRegistry);
    return builder.transport(transport).build();
  }

  private static boolean isDisabled() {
    String disabled = Environment.getEnvironmentVariable("OPENLINEAGE_DISABLED");
    return (Boolean.parseBoolean(disabled));
  }
}
