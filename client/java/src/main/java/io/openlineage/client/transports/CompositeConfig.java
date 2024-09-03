/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import io.openlineage.client.MergeConfig;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * CompositeConfig is a configuration class for CompositeTransport, implementing TransportConfig. It
 * allows merging configurations from different sources.
 */
@ToString
@AllArgsConstructor
@NoArgsConstructor
public final class CompositeConfig implements TransportConfig, MergeConfig<CompositeConfig> {

  @Getter @Setter private List<Map<String, Object>> transports;

  @Getter @Setter private boolean continueOnFailure;

  @Override
  public CompositeConfig mergeWithNonNull(CompositeConfig other) {
    // Merge the transports and continueOnFailure fields from both configs
    List<Map<String, Object>> mergedTransports =
        mergePropertyWith(transports, other.getTransports());
    boolean mergedContinueOnFailure =
        mergePropertyWith(continueOnFailure, other.isContinueOnFailure());

    return new CompositeConfig(mergedTransports, mergedContinueOnFailure);
  }
}
