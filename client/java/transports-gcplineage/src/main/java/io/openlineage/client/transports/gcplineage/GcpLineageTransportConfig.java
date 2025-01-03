/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.transports.gcplineage;

import io.openlineage.client.MergeConfig;
import io.openlineage.client.transports.TransportConfig;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@NoArgsConstructor
@AllArgsConstructor
@ToString
public class GcpLineageTransportConfig
    implements TransportConfig, MergeConfig<GcpLineageTransportConfig> {

  enum Mode {
    sync,
    async
  }

  @Getter @Setter private @Nullable String endpoint;

  @Getter @Setter private @Nullable String projectId;

  @Getter @Setter private @Nullable String credentialsFile;

  @Getter @Setter private @Nullable String location;

  @Getter @Setter private @Nullable Mode mode;

  @Getter @Setter private @Nullable Map<String, String> properties;

  @Override
  public GcpLineageTransportConfig mergeWithNonNull(GcpLineageTransportConfig other) {
    return new GcpLineageTransportConfig(
        mergePropertyWith(endpoint, other.endpoint),
        mergePropertyWith(projectId, other.projectId),
        mergePropertyWith(credentialsFile, other.credentialsFile),
        mergePropertyWith(location, other.location),
        mergePropertyWith(mode, other.mode),
        mergePropertyWith(properties, other.properties));
  }
}
