/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.transports.dataplex;

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
public class DataplexConfig implements TransportConfig, MergeConfig<DataplexConfig> {

  @Getter @Setter private @Nullable String endpoint;

  @Getter @Setter private @Nullable String projectId;

  @Getter @Setter private @Nullable String credentialsFile;

  @Getter @Setter private @Nullable String location;


  @Override
  public DataplexConfig mergeWithNonNull(DataplexConfig other) {
    return new DataplexConfig(
        mergePropertyWith(endpoint, other.endpoint),
        mergePropertyWith(projectId, other.projectId),
        mergePropertyWith(credentialsFile, other.credentialsFile),
        mergePropertyWith(location, other.location));
  }
}
