/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import io.openlineage.client.MergeConfig;
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

  @Override
  public AmazonDataZoneConfig mergeWithNonNull(AmazonDataZoneConfig other) {
    return new AmazonDataZoneConfig(mergePropertyWith(domainId, other.domainId));
  }
}
