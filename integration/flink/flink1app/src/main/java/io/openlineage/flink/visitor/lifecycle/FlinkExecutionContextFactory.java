/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.lifecycle;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.circuitBreaker.CircuitBreakerFactory;
import io.openlineage.client.dataset.namespace.resolver.DatasetNamespaceCombinedResolver;
import io.openlineage.client.metrics.MicrometerProvider;
import io.openlineage.client.utils.UUIDUtils;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.client.EventEmitter;
import io.openlineage.flink.client.FlinkConfigParser;
import io.openlineage.flink.client.FlinkOpenLineageConfig;
import io.openlineage.flink.client.Versions;
import java.util.List;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.util.EnvironmentInformation;

public class FlinkExecutionContextFactory {

  public static FlinkExecutionContext getContext(
      Configuration configuration,
      String jobNamespace,
      String jobName,
      JobID jobId,
      String jobType,
      List<Transformation<?>> transformations) {
    FlinkOpenLineageConfig config = FlinkConfigParser.parse(configuration);
    return getContext(
        config, jobNamespace, jobName, jobId, jobType, new EventEmitter(config), transformations);
  }

  public static FlinkExecutionContext getContext(
      FlinkOpenLineageConfig config,
      String jobNamespace,
      String jobName,
      JobID jobId,
      String jobType,
      EventEmitter eventEmitter,
      List<Transformation<?>> transformations) {
    return new FlinkExecutionContext.FlinkExecutionContextBuilder()
        .jobId(jobId)
        .processingType(jobType)
        .jobName(jobName)
        .jobNamespace(jobNamespace)
        .transformations(transformations)
        .runId(UUIDUtils.generateNewUUID())
        .circuitBreaker(new CircuitBreakerFactory(config.getCircuitBreaker()).build())
        .openLineageContext(
            OpenLineageContext.builder()
                .openLineage(new OpenLineage(EventEmitter.OPEN_LINEAGE_CLIENT_URI))
                .config(config)
                .build())
        .eventEmitter(eventEmitter)
        .config(config)
        .meterRegistry(initializeMetrics(config))
        .namespaceResolver(new DatasetNamespaceCombinedResolver(config))
        .build();
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
