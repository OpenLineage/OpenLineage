/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.lifecycle;

import io.openlineage.flink.api.OpenLineageContext.JobIdentifier;
import io.openlineage.flink.api.OpenLineageContextFactory;
import io.openlineage.flink.client.EventEmitter;
import io.openlineage.flink.config.FlinkConfigParser;
import io.openlineage.flink.config.FlinkOpenLineageConfig;
import java.util.List;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;

public class FlinkExecutionContextFactory {

  public static FlinkExecutionContext getContext(
      Configuration configuration,
      JobIdentifier jobIdentifier,
      String jobType,
      List<Transformation<?>> transformations) {
    FlinkOpenLineageConfig config = FlinkConfigParser.parse(configuration);
    return getContext(config, jobIdentifier, jobType, new EventEmitter(config), transformations);
  }

  public static FlinkExecutionContext getContext(
      FlinkOpenLineageConfig config,
      JobIdentifier jobIdentifier,
      String jobType,
      EventEmitter eventEmitter,
      List<Transformation<?>> transformations) {
    return new FlinkExecutionContext.FlinkExecutionContextBuilder()
        .transformations(transformations)
        .olContext(
            OpenLineageContextFactory.fromConfig(config)
                .jobId(jobIdentifier)
                .processingType(jobType)
                .eventEmitter(eventEmitter)
                .build())
        .build();
  }
}
