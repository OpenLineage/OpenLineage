/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.flink.api;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.circuitBreaker.CircuitBreakerFactory;
import io.openlineage.client.dataset.namespace.resolver.DatasetNamespaceCombinedResolver;
import io.openlineage.client.metrics.MicrometerProvider;
import io.openlineage.flink.client.EventEmitter;
import io.openlineage.flink.client.Versions;
import io.openlineage.flink.config.FlinkOpenLineageConfig;
import org.apache.flink.runtime.util.EnvironmentInformation;

public class OpenLineageContextFactory {

  public static OpenLineageContext.OpenLineageContextBuilder fromConfig(
      FlinkOpenLineageConfig config) {
    return OpenLineageContext.builder()
        .openLineage(new OpenLineage(EventEmitter.OPEN_LINEAGE_CLIENT_URI))
        .config(config)
        .circuitBreaker(new CircuitBreakerFactory(config.getCircuitBreaker()).build())
        .meterRegistry(initializeMetrics(config))
        .namespaceResolver(new DatasetNamespaceCombinedResolver(config))
        .eventEmitter(new EventEmitter(config));
  }

  private static MeterRegistry initializeMetrics(FlinkOpenLineageConfig config) {
    MeterRegistry meterRegistry =
        MicrometerProvider.addMeterRegistryFromConfig(config.getMetricsConfig());
    String disabledFacets;
    if (config.getFacetsConfig() != null) {
      disabledFacets = String.join(";", config.getFacetsConfig().getEffectiveDisabledFacets());
    } else {
      disabledFacets = "";
    }
    meterRegistry
        .config()
        .commonTags(
            Tags.of(
                Tag.of("openlineage.flink.integration.version", Versions.getVersion()),
                Tag.of("openlineage.flink.version", EnvironmentInformation.getVersion()),
                Tag.of("openlineage.flink.disabled.facets", disabledFacets)));
    ((CompositeMeterRegistry) meterRegistry)
        .getRegistries()
        .forEach(
            r ->
                r.config()
                    .commonTags(
                        Tags.of(
                            Tag.of("openlineage.flink.integration.version", Versions.getVersion()),
                            Tag.of(
                                "openlineage.flink.version", EnvironmentInformation.getVersion()),
                            Tag.of("openlineage.flink.disabled.facets", disabledFacets))));
    return meterRegistry;
  }
}
