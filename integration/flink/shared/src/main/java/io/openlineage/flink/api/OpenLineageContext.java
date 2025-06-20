/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.api;

import io.micrometer.core.instrument.MeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.circuitBreaker.CircuitBreaker;
import io.openlineage.client.dataset.namespace.resolver.DatasetNamespaceCombinedResolver;
import io.openlineage.client.utils.UUIDUtils;
import io.openlineage.flink.client.EventEmitter;
import io.openlineage.flink.config.FlinkOpenLineageConfig;
import java.util.UUID;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import org.apache.flink.api.common.JobID;

/**
 * Context holder with references to several required objects during construction of an OpenLineage
 * {@link io.openlineage.client.OpenLineage.RunEvent}. An {@link OpenLineageContext} should be
 * created once for every detected Flink job execution.
 *
 * <p>This API is evolving and may change in future releases
 *
 * @apiNote This interface is evolving and may change in future releases
 */
@Builder
@Getter
@ToString
public class OpenLineageContext {

  @Builder.Default UUID runUuid = UUIDUtils.generateNewUUID();

  /**
   * A non-null, preconfigured {@link OpenLineage} client instance for constructing OpenLineage
   * model objects
   */
  @NonNull OpenLineage openLineage;

  /** Flink OpenLineage config */
  @NonNull FlinkOpenLineageConfig config;

  MeterRegistry meterRegistry;

  DatasetNamespaceCombinedResolver namespaceResolver;

  CircuitBreaker circuitBreaker;

  @Setter JobIdentifier jobId;

  String processingType;

  EventEmitter eventEmitter;

  @Getter
  @Builder
  @EqualsAndHashCode
  public static class JobIdentifier {
    private String jobName;
    private String jobNamespace;

    /** Assigned during the event emitted. */
    private JobID flinkJobId;
  }
}
