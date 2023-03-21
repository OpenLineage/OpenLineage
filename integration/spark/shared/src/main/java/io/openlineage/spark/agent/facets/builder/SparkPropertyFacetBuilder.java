/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import io.openlineage.spark.agent.facets.SparkPropertyFacet;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import org.apache.spark.SparkConf;
import org.apache.spark.scheduler.SparkListenerJobStart;

public class SparkPropertyFacetBuilder
    extends CustomFacetBuilder<SparkListenerJobStart, SparkPropertyFacet> {
  private static final Set<String> DEFAULT_ALLOWED_PROPERTIES =
      new HashSet<>(Arrays.asList("spark.master", "spark.app.name"));
  private static final String ALLOWED_PROPERTIES_KEY = "spark.openlineage.capturedProperties";
  private final SparkConf conf;
  private final Set<String> allowerProperties;

  public SparkPropertyFacetBuilder(OpenLineageContext context) {
    conf = context.getSparkContext().getConf();
    allowerProperties =
        conf.contains(ALLOWED_PROPERTIES_KEY)
            ? Arrays.stream(conf.get(ALLOWED_PROPERTIES_KEY).split(",")).collect(Collectors.toSet())
            : DEFAULT_ALLOWED_PROPERTIES;
  }

  @Override
  protected void build(
      SparkListenerJobStart event, BiConsumer<String, ? super SparkPropertyFacet> consumer) {
    Map<String, Object> m = new HashMap<>();
    Arrays.stream(conf.getAll())
        .filter(t -> allowerProperties.contains(t._1))
        .forEach(t -> m.putIfAbsent(t._1, t._2));
    event.properties().entrySet().stream()
        .filter(e -> allowerProperties.contains(e.getKey()))
        .forEach(e -> m.putIfAbsent(e.getKey().toString(), e.getValue()));
    consumer.accept("spark_properties", new SparkPropertyFacet(m));
  }
}
