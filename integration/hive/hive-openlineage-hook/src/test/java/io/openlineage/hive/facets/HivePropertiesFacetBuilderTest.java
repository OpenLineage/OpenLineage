/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.facets;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

class HivePropertiesFacetBuilderTest {
  @Test
  void testDefaultAllowedProperties() {
    Configuration conf = new Configuration();
    conf.set("hive.execution.engine", "tez");
    conf.set("hive.query.id", "123");
    conf.set("not.allowed.property", "value");

    HivePropertiesFacet facet = new HivePropertiesFacetBuilder(conf).build();
    assertThat(facet.getProperties()).hasSize(2);
    assertThat(facet.getProperties().get("hive.query.id")).isEqualTo("123");
    assertThat(facet.getProperties().get("hive.execution.engine")).isEqualTo("tez");
  }

  @Test
  void testCustomAllowedProperties() {
    Configuration conf = new Configuration();
    conf.set("hive.openlineage.capturedProperties", "custom.property,another.custom");
    conf.set("hive.execution.engine", "mr");
    conf.set("custom.property", "value1");
    conf.set("another.custom", "value2");

    HivePropertiesFacet facet = new HivePropertiesFacetBuilder(conf).build();
    assertThat(facet.getProperties()).hasSize(2);
    assertThat(facet.getProperties().get("custom.property")).isEqualTo("value1");
    assertThat(facet.getProperties().get("another.custom")).isEqualTo("value2");
  }
}
