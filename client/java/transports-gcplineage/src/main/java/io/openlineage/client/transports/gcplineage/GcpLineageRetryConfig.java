/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports.gcplineage;

import datalineage.shaded.org.threeten.bp.Duration;
import io.openlineage.client.MergeConfig;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class GcpLineageRetryConfig implements MergeConfig<GcpLineageRetryConfig> {
  private @Nullable Integer maxAttempts;
  private @Nullable Duration totalTimeout;
  private @Nullable Duration initialRetryDelay;
  private @Nullable Double retryDelayMultiplier;
  private @Nullable Duration maxRetryDelay;
  private @Nullable Duration initialRpcTimeout;
  private @Nullable Double rpcTimeoutMultiplier;
  private @Nullable Duration maxRpcTimeout;

  public void setTotalTimeout(String totalTimeout) {
    this.totalTimeout = Duration.parse(totalTimeout);
  }

  public void setInitialRetryDelay(String initialRetryDelay) {
    this.initialRetryDelay = Duration.parse(initialRetryDelay);
  }

  public void setMaxRetryDelay(String maxRetryDelay) {
    this.maxRetryDelay = Duration.parse(maxRetryDelay);
  }

  public void setInitialRpcTimeout(String initialRpcTimeout) {
    this.initialRpcTimeout = Duration.parse(initialRpcTimeout);
  }

  public void setMaxRpcTimeout(String maxRpcTimeout) {
    this.maxRpcTimeout = Duration.parse(maxRpcTimeout);
  }

  @Override
  public GcpLineageRetryConfig mergeWithNonNull(GcpLineageRetryConfig gcpLineageRetryConfig) {
    return new GcpLineageRetryConfig(
        mergePropertyWith(maxAttempts, gcpLineageRetryConfig.maxAttempts),
        mergePropertyWith(totalTimeout, gcpLineageRetryConfig.totalTimeout),
        mergePropertyWith(initialRetryDelay, gcpLineageRetryConfig.initialRetryDelay),
        mergePropertyWith(retryDelayMultiplier, gcpLineageRetryConfig.retryDelayMultiplier),
        mergePropertyWith(maxRetryDelay, gcpLineageRetryConfig.maxRetryDelay),
        mergePropertyWith(initialRpcTimeout, gcpLineageRetryConfig.initialRpcTimeout),
        mergePropertyWith(rpcTimeoutMultiplier, gcpLineageRetryConfig.rpcTimeoutMultiplier),
        mergePropertyWith(maxRpcTimeout, gcpLineageRetryConfig.maxRpcTimeout));
  }
}
