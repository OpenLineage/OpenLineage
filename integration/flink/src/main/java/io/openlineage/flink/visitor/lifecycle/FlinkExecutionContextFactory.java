/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.lifecycle;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.client.EventEmitter;
import java.util.List;
import java.util.UUID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.dag.Transformation;

public class FlinkExecutionContextFactory {

  public static FlinkExecutionContext getContext(
      String jobNamespace, String jobName, JobID jobId, List<Transformation<?>> transformations) {
    return new FlinkExecutionContext.FlinkExecutionContextBuilder()
        .jobId(jobId)
        .jobName(jobName)
        .jobNamespace(jobNamespace)
        .transformations(transformations)
        .runId(UUID.randomUUID())
        .openLineageContext(
            OpenLineageContext.builder()
                .openLineage(new OpenLineage(EventEmitter.OPEN_LINEAGE_CLIENT_URI))
                .build())
        .eventEmitter(new EventEmitter())
        .build();
  }
}
