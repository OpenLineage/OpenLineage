/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.OpenLineage.RunFacetsBuilder;
import io.openlineage.spark.agent.facets.builder.DebugRunFacetBuilderDelegate;
import io.openlineage.spark.api.DebugConfig;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FacetUtils {
  public static boolean isFacetDisabled(OpenLineageContext context, String facetName) {
    return Optional.ofNullable(context)
        .map(OpenLineageContext::getOpenLineageConfig)
        .map(SparkOpenLineageConfig::getFacetsConfig)
        .map(
            facetsConfig ->
                Arrays.asList(facetsConfig.getEffectiveDisabledFacets()).contains(facetName))
        .orElse(SparkOpenLineageConfig.DISABLED_BY_DEFAULT.contains(facetName));
  }

  public static void attachSmartDebugFacet(
      OpenLineageContext context, RunFacetsBuilder runFacetsBuilder) {
    DebugConfig debugConfig = context.getOpenLineageConfig().getDebugConfig();
    if (debugConfig == null) {
      return;
    }

    boolean hasDetectedInputs = context.getLineageRunStatus().isDetectedInputs();
    boolean hasDetectedOutputs = context.getLineageRunStatus().isDetectedOutputs();

    if (!debugConfig.isSmartDebugActive(hasDetectedInputs, hasDetectedOutputs)) {
      return;
    }

    List<String> logs = new LinkedList<>();
    if (!hasDetectedInputs) {
      logs.add("No input datasets detected");
    }
    if (!hasDetectedOutputs) {
      logs.add("No output datasets detected");
    }

    runFacetsBuilder.put("debug", new DebugRunFacetBuilderDelegate(context).buildFacet(logs));
  }
}
