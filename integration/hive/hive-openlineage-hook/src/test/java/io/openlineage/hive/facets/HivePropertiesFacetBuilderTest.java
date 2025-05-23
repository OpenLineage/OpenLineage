/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.facets;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.hive.api.OpenLineageContext;
import io.openlineage.hive.client.HiveOpenLineageConfig;
import io.openlineage.hive.client.Versions;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

class HivePropertiesFacetBuilderTest {

  OpenLineageContext getOpenLineageContext(Configuration conf) {
    return OpenLineageContext.builder()
        .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
        .queryString("xxx")
        .eventTime(Instant.ofEpochMilli(System.currentTimeMillis()).atZone(ZoneId.of("UTC")))
        .eventType(EventType.OTHER)
        .readEntities(new HashSet<>())
        .writeEntities(new HashSet<>())
        .operationName("CREATETABLE_AS_SELECT")
        .hadoopConf(conf)
        .openlineageHiveIntegrationVersion(Versions.getVersion())
        .openLineageConfig(new HiveOpenLineageConfig())
        .build();
  }

  @Test
  void testDefaultAllowedProperties() {
    Configuration conf = new Configuration();
    conf.set("hive.execution.engine", "tez");
    conf.set("hive.query.id", "123");
    conf.set("not.allowed.property", "value");
    HivePropertiesFacet facet = new HivePropertiesFacetBuilder(getOpenLineageContext(conf)).build();
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
    HivePropertiesFacet facet = new HivePropertiesFacetBuilder(getOpenLineageContext(conf)).build();
    assertThat(facet.getProperties()).hasSize(2);
    assertThat(facet.getProperties().get("custom.property")).isEqualTo("value1");
    assertThat(facet.getProperties().get("another.custom")).isEqualTo("value2");
  }
}
