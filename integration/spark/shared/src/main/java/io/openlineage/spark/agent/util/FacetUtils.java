/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.spark.api.OpenLineageContext;
import java.util.Arrays;
import java.util.Optional;
import org.apache.spark.SparkContext;

public final class FacetUtils {
  private FacetUtils() {}

  private static final String SPARK_CONF_FACETS_DISABLED = "spark.openlineage.facets.disabled";
  private static final String DISABLED_FACETS_SEPARATOR = ";";

  public static boolean isFacetDisabled(OpenLineageContext context, String facetName) {
    return Optional.ofNullable(context)
        .map(OpenLineageContext::getSparkContext)
        .map(SparkContext::getConf)
        .flatMap(conf -> SparkConfUtils.findSparkConfigKey(conf, SPARK_CONF_FACETS_DISABLED))
        .map(s -> s.replace("[", "").replace("]", ""))
        .map(s -> s.split(DISABLED_FACETS_SEPARATOR))
        .map(facets -> Arrays.asList(facets).contains(facetName))
        .orElse(false);
  }
}
