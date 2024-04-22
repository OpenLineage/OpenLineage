/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.lifecycle;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.circuitBreaker.CircuitBreakerFactory;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.client.EventEmitter;
import io.openlineage.flink.client.FlinkConfigParser;
import io.openlineage.flink.client.FlinkOpenLineageConfig;
import java.util.List;
import java.util.UUID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;

public class FlinkExecutionContextFactory {

  public static FlinkExecutionContext getContext(
      Configuration configuration,
      String jobNamespace,
      String jobName,
      JobID jobId,
      String jobType,
      List<Transformation<?>> transformations) {
    FlinkOpenLineageConfig openLineageConfig = FlinkConfigParser.parse(configuration);
    return new FlinkExecutionContext.FlinkExecutionContextBuilder()
        .jobId(jobId)
        .processingType(jobType)
        .jobName(jobName)
        .jobNamespace(jobNamespace)
        .transformations(transformations)
        .runId(UUID.randomUUID())
        .circuitBreaker(new CircuitBreakerFactory(openLineageConfig.getCircuitBreaker()).build())
        .openLineageContext(
            OpenLineageContext.builder()
                .openLineage(new OpenLineage(EventEmitter.OPEN_LINEAGE_CLIENT_URI))
                .build())
        .eventEmitter(new EventEmitter(openLineageConfig))
        .build();
  }
}
