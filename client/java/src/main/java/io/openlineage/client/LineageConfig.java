/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client;

import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/** Configuration for automatic lineage compatibility translation. */
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class LineageConfig implements MergeConfig<LineageConfig> {
  private LineageCompatibility compatibility;

  /**
   * Returns the configured translation mode, defaulting to {@link LineageCompatibility#NONE}.
   *
   * @return the effective translation mode
   */
  public LineageCompatibility getCompatibility() {
    return Optional.ofNullable(compatibility).orElse(LineageCompatibility.NONE);
  }

  @Override
  public LineageConfig mergeWithNonNull(LineageConfig other) {
    return new LineageConfig(mergePropertyWith(compatibility, other.compatibility));
  }
}
