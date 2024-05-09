/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import io.openlineage.spark.agent.facets.SparkPropertyFacet;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class SparkPropertyFacetBuilder
    extends CustomFacetBuilder<SparkListenerEvent, SparkPropertyFacet> {
  private static final Set<String> DEFAULT_ALLOWED_PROPERTIES =
      new HashSet<>(Arrays.asList("spark.master", "spark.app.name", "spark.app.id"));
  private static final String ALLOWED_PROPERTIES_KEY = "spark.openlineage.capturedProperties";
  private SparkConf conf;
  private Set<String> allowerProperties;

  public SparkPropertyFacetBuilder(OpenLineageContext context) {
    fillConfAndAllowerProperties(context.getSparkContext());
  }

  public SparkPropertyFacetBuilder() {
    try {
      SparkSession session = SparkSession.active();
      fillConfAndAllowerProperties(session.sparkContext());
    } catch (IllegalStateException ie) {
      log.info("No active or default Spark session found");
      conf = new SparkConf();
      allowerProperties = new HashSet<>();
    }
  }

  private void fillConfAndAllowerProperties(SparkContext context) {
    conf = context.getConf();
    allowerProperties =
        conf.contains(ALLOWED_PROPERTIES_KEY)
            ? Arrays.stream(conf.get(ALLOWED_PROPERTIES_KEY).split(",")).collect(Collectors.toSet())
            : DEFAULT_ALLOWED_PROPERTIES;
  }

  @Override
  protected void build(
      SparkListenerEvent event, BiConsumer<String, ? super SparkPropertyFacet> consumer) {
    consumer.accept("spark_properties", buildFacet(event));
  }

  public SparkPropertyFacet buildFacet(SparkListenerEvent event) {
    Map<String, Object> m = new HashMap<>();
    Arrays.stream(conf.getAll())
        .filter(t -> allowerProperties.contains(t._1))
        .forEach(t -> m.putIfAbsent(t._1, t._2));
    if (event instanceof SparkListenerJobStart) {
      SparkListenerJobStart startEvent = (SparkListenerJobStart) event;
      startEvent.properties().entrySet().stream()
          .filter(e -> allowerProperties.contains(e.getKey()))
          .forEach(e -> m.putIfAbsent(e.getKey().toString(), e.getValue()));
    }

    try {
      SparkSession session = SparkSession.active();
      allowerProperties.forEach(item -> m.putIfAbsent(item, session.conf().get(item)));
    } catch (RuntimeException e) {
      log.info(
          "Cannot add SparkPropertyFacet: Spark session is in a wrong status or a key in capturedProperties does not exist in run-time config");
    }

    return new SparkPropertyFacet(m);
  }
}
