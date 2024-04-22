/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.spark.api.OpenLineageContext;
import java.util.Arrays;
import java.util.Optional;

public class FacetUtils {

  public static boolean isFacetDisabled(OpenLineageContext context, String facetName) {
    return Optional.ofNullable(context)
        .map(c -> c.getOpenLineageConfig())
        .map(c -> c.getFacetsConfig())
        .map(c -> c.getDisabledFacets())
        .map(c -> Arrays.asList(c))
        .map(l -> l.contains(facetName))
        .orElse(false);
  }
}
