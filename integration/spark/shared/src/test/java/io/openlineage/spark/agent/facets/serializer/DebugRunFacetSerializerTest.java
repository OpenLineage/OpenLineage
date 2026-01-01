/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.serializer;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.spark.agent.facets.DebugRunFacet;
import io.openlineage.spark.agent.facets.DebugRunFacet.ClasspathDebugFacet;
import io.openlineage.spark.agent.facets.DebugRunFacet.LogicalPlanDebugFacet;
import io.openlineage.spark.agent.facets.DebugRunFacet.MemoryDebugFacet;
import io.openlineage.spark.agent.facets.DebugRunFacet.MetricsDebugFacet;
import io.openlineage.spark.agent.facets.DebugRunFacet.SparkConfigDebugFacet;
import io.openlineage.spark.agent.facets.DebugRunFacet.SystemDebugFacet;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

class DebugRunFacetSerializerTest {

  DebugRunFacet facet =
      new DebugRunFacet(
          SparkConfigDebugFacet.builder().catalogClass("someString").build(),
          ClasspathDebugFacet.builder().build(),
          SystemDebugFacet.builder().build(),
          new LogicalPlanDebugFacet(Collections.emptyList()),
          new MetricsDebugFacet(Collections.emptyList()),
          MemoryDebugFacet.builder().build(),
          Collections.emptyList(),
          10);

  @Test
  void testSerializeWritesAllFields() {
    String json = OpenLineageClientUtils.toJson(facet);
    assertThat(json)
        .containsIgnoringCase("\"classpath\"")
        .containsIgnoringCase("\"system\"")
        .containsIgnoringCase("\"config\"")
        .containsIgnoringCase("\"logicalPlan\"")
        .containsIgnoringCase("\"metrics\"")
        .containsIgnoringCase("\"logs\"");
  }

  @Test
  void testSerializeDoesNotExceedSizeLimit() {
    char[] charArray = new char[20 * 1024];
    Arrays.fill(charArray, 'a');
    String largeString = new String(charArray); // 20KB string

    facet =
        new DebugRunFacet(
            SparkConfigDebugFacet.builder().catalogClass(largeString).build(),
            ClasspathDebugFacet.builder().build(),
            SystemDebugFacet.builder().build(),
            new LogicalPlanDebugFacet(Collections.emptyList()),
            new MetricsDebugFacet(Collections.emptyList()),
            MemoryDebugFacet.builder().build(),
            Collections.emptyList(),
            10);

    String json = OpenLineageClientUtils.toJson(facet);
    assertThat(json)
        .isEqualTo("{\"logs\":[\"Skipping debug facet due to payload size: 20 kilobytes\"]}");
  }

  @Test
  void testSerializeDoesNotWriteAllowedPayloadSize() {
    String json = OpenLineageClientUtils.toJson(facet);
    assertThat(json).doesNotContainIgnoringCase("\"payloadSizeLimitInKilobytes\"");
  }
}
