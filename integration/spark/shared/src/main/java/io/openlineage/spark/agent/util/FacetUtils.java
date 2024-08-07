/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.Arrays;
import java.util.Optional;

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
}
