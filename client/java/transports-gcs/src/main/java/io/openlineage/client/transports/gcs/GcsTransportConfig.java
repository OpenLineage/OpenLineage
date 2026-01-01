/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports.gcs;

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
public class GcsTransportConfig implements TransportConfig, MergeConfig<GcsTransportConfig> {

  @Getter @Setter private String projectId;
  @Getter @Setter private String bucketName;
  @Getter @Setter private @Nullable String credentialsFile;
  @Getter @Setter private @Nullable String fileNamePrefix;

  @Override
  public GcsTransportConfig mergeWithNonNull(GcsTransportConfig other) {
    return new GcsTransportConfig(
        mergePropertyWith(projectId, other.projectId),
        mergePropertyWith(bucketName, other.bucketName),
        mergePropertyWith(credentialsFile, other.credentialsFile),
        mergePropertyWith(fileNamePrefix, other.fileNamePrefix));
  }
}
