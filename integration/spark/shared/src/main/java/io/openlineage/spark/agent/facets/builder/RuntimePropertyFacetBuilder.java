/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import io.openlineage.spark.agent.facets.RuntimePropertyFacet;
import io.openlineage.spark.api.CustomFacetBuilder;
import java.util.function.BiConsumer;
import org.apache.spark.scheduler.SparkListenerEvent;

public class RuntimePropertyFacetBuilder
    extends CustomFacetBuilder<SparkListenerEvent, RuntimePropertyFacet> {

  public RuntimePropertyFacetBuilder() {}

  @Override
  protected void build(
      SparkListenerEvent event, BiConsumer<String, ? super RuntimePropertyFacet> consumer) {
    consumer.accept("spark_properties", new RuntimePropertyFacet());
  }
}
