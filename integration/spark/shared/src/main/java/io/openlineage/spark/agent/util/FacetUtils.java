/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.spark.api.OpenLineageContext;
import java.util.Arrays;
import java.util.Optional;

public class FacetUtils {

  private static final String SPARK_CONF_FACETS_DISABLED = "openlineage.facets.disabled";
  private static final String disabledFacetsSeparator = ";";

  public static boolean isFacetDisabled(OpenLineageContext context, String facetName) {
    return Optional.ofNullable(context)
        .map(c -> c.getSparkContext())
        .map(sparkContext -> sparkContext.getConf())
        .flatMap(conf -> SparkConfUtils.findSparkConfigKey(conf, SPARK_CONF_FACETS_DISABLED))
        .map(s -> s.split(disabledFacetsSeparator))
        .map(facets -> Arrays.asList(facets).contains(facetName))
        .orElse(false);
  }
}
