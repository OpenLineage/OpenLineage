/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import io.openlineage.spark.agent.facets.EnvironmentFacet;
import io.openlineage.spark.api.CustomFacetBuilder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.scheduler.SparkListenerJobStart;
import io.openlineage.spark.api.OpenLineageContext;

/**
 * {@link CustomFacetBuilder} that generates a {@link EnvironmentFacet} when using OpenLineage on
 * Spark.
 */
@Slf4j
public class CustomEnvironmentFacetBuilder
    extends CustomFacetBuilder<SparkListenerJobStart, EnvironmentFacet> {
  private Map<String, Object> envProperties;
  private Optional<List<String>> customEnvironmentVariables;

  public CustomEnvironmentFacetBuilder() {

  }
  public CustomEnvironmentFacetBuilder(OpenLineageContext openLineageContext) {
    customEnvironmentVariables = openLineageContext.getCustomEnvironmentVariables();
}

  @Override
  protected void build(
      SparkListenerJobStart event, BiConsumer<String, ? super EnvironmentFacet> consumer) {
    consumer.accept(
        "environment-properties", new EnvironmentFacet(getCustomEnvironmentalAttributes(event)));
  }

  private Map<String, Object> getCustomEnvironmentalAttributes(SparkListenerJobStart jobStart) {
    envProperties = new HashMap<>();
    // extract some custom environment variables if needed
    customEnvironmentVariables.ifPresent(envVars -> envVars.forEach(envVar -> envProperties.put(envVar, System.getenv().get(envVar))));
    return envProperties;
  }
}
