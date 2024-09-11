/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.client;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.flink.config.FlinkOpenLineageConfig;
import java.util.UUID;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Builder
@Getter
public class OpenLineageContext {

  private final OpenLineageClient client;

  private final OpenLineage openLineage;

  private final FlinkOpenLineageConfig config;

  private final UUID runId;

  /** Assigned during the event emitted. */
  @Setter private JobIdentifier jobId;

  @Getter
  @Builder
  @EqualsAndHashCode
  public static class JobIdentifier {
    private String jobNme;
    private String jobNamespace;
  }
}
