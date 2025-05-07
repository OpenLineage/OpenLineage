/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports.datazone;

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
public final class AmazonDataZoneConfig
    implements TransportConfig, MergeConfig<AmazonDataZoneConfig> {
  @Getter @Setter private String domainId;
  @Getter @Setter @Nullable private String endpointOverride;

  @Override
  public AmazonDataZoneConfig mergeWithNonNull(AmazonDataZoneConfig other) {
    return new AmazonDataZoneConfig(
        mergePropertyWith(domainId, other.domainId),
        mergePropertyWith(endpointOverride, other.getEndpointOverride()));
  }
}
