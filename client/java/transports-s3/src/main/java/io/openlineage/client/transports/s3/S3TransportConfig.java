/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports.s3;

import io.openlineage.client.MergeConfig;
import io.openlineage.client.transports.TransportConfig;
import javax.annotation.Nullable;
import lombok.*;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class S3TransportConfig implements TransportConfig, MergeConfig<S3TransportConfig> {

  /** If not null, overrides the default endpoint. */
  @Nullable private String endpoint;

  private String bucketName;
  private @Nullable String fileNamePrefix;

  @Override
  public S3TransportConfig mergeWithNonNull(S3TransportConfig s3TransportConfig) {
    return new S3TransportConfig(
        mergePropertyWith(endpoint, s3TransportConfig.getEndpoint()),
        mergePropertyWith(bucketName, s3TransportConfig.getBucketName()),
        mergePropertyWith(fileNamePrefix, s3TransportConfig.getFileNamePrefix()));
  }
}
