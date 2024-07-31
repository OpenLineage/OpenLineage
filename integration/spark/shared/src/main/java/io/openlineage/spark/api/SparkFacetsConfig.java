/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import io.openlineage.client.transports.FacetsConfig;
import io.openlineage.spark.agent.facets.DebugFacetConfig;
import io.openlineage.spark.agent.facets.LogicalPlanFacetConfig;
import io.openlineage.spark.agent.facets.UnknownEntryFacetConfig;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@NoArgsConstructor
@Setter
@AllArgsConstructor
public class SparkFacetsConfig extends FacetsConfig {
  private LogicalPlanFacetConfig logicalPlan;
  private UnknownEntryFacetConfig unknownEntry;
  private DebugFacetConfig debug;

  public SparkFacetsConfig(
      String[] disabledFacets,
      String[] customEnvironmentVariables,
      LogicalPlanFacetConfig logicalPlan,
      UnknownEntryFacetConfig unknownEntry,
      DebugFacetConfig debug) {
    super(disabledFacets, customEnvironmentVariables);
    this.logicalPlan = logicalPlan;
    this.unknownEntry = unknownEntry;
    this.debug = debug;
  }

  public LogicalPlanFacetConfig getLogicalPlan() {
    if (logicalPlan == null) {
      logicalPlan = new LogicalPlanFacetConfig();
    }
    return logicalPlan;
  }

  public UnknownEntryFacetConfig getUnknownEntry() {
    if (unknownEntry == null) {
      unknownEntry = new UnknownEntryFacetConfig();
    }
    return unknownEntry;
  }

  public DebugFacetConfig getDebug() {
    if (debug == null) {
      debug = new DebugFacetConfig();
    }
    return debug;
  }

  @Override
  public FacetsConfig mergeWithNonNull(FacetsConfig facetsConfig) {
    SparkFacetsConfig other = (SparkFacetsConfig) facetsConfig;
    return new SparkFacetsConfig(
        mergePropertyWith(getDisabledFacets(), other.getDisabledFacets()),
        mergePropertyWith(getCustomEnvironmentVariables(), other.getCustomEnvironmentVariables()),
        mergePropertyWith(logicalPlan, other.logicalPlan),
        mergePropertyWith(unknownEntry, other.unknownEntry),
        mergePropertyWith(debug, other.debug));
  }
}
