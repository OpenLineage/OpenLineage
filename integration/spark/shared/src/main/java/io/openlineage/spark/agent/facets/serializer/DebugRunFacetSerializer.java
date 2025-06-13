/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.serializer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.spark.agent.facets.DebugRunFacet;
import io.openlineage.spark.agent.facets.DebugRunFacet.ClasspathDebugFacet;
import io.openlineage.spark.agent.facets.DebugRunFacet.LogicalPlanDebugFacet;
import io.openlineage.spark.agent.facets.DebugRunFacet.MetricsDebugFacet;
import io.openlineage.spark.agent.facets.DebugRunFacet.SparkConfigDebugFacet;
import io.openlineage.spark.agent.facets.DebugRunFacet.SystemDebugFacet;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Custom serializer to handle the serialization of the {@link DebugRunFacet} class. Implements a
 * serialization limit which does not serialize a facet in case its size exceeds the limit.
 */
@Slf4j
public class DebugRunFacetSerializer extends StdSerializer<DebugRunFacet> {

  protected DebugRunFacetSerializer() {
    super(DebugRunFacet.class);
  }

  @Override
  public void serialize(
      DebugRunFacet facet, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
      throws IOException {

    int payloadSize =
        OpenLineageClientUtils.toJson(new DebugRunFacetWithStandardSerializer(facet))
                .getBytes(StandardCharsets.UTF_8)
                .length
            / 1024;

    if (payloadSize > facet.getPayloadSizeLimitInKilobytes()) {
      log.warn(
          "DebugRunFacetSerializer skipping serialization of DebugRunFacet due to payload size: {} kilobytes",
          payloadSize);

      // replace debug facet with a simple JSON object indicating the reason for skipping
      jsonGenerator.writeStartObject();
      jsonGenerator.writeObjectField(
          "logs",
          Collections.singletonList(
              String.format(
                  "Skipping debug facet due to payload size: %d kilobytes", payloadSize)));
      jsonGenerator.writeEndObject();

      return;
    }

    jsonGenerator.writeStartObject();
    jsonGenerator.writeObjectField("classpath", facet.getClasspath());
    jsonGenerator.writeObjectField("system", facet.getSystem());
    jsonGenerator.writeObjectField("config", facet.getConfig());
    jsonGenerator.writeObjectField("logicalPlan", facet.getLogicalPlan());
    jsonGenerator.writeObjectField("metrics", facet.getMetrics());
    jsonGenerator.writeObjectField("logs", facet.getLogs());

    jsonGenerator.writeEndObject();
  }

  /**
   * A copy of the DebugRunFacet to be used for serialization with different serializer through the
   * same ObjectMapper.
   */
  @Getter
  private static class DebugRunFacetWithStandardSerializer {
    private final ClasspathDebugFacet classpath;
    private final SystemDebugFacet system;
    private final SparkConfigDebugFacet config;
    private final LogicalPlanDebugFacet logicalPlan;
    private final MetricsDebugFacet metrics;
    private final List<String> logs;

    private DebugRunFacetWithStandardSerializer(DebugRunFacet facet) {
      this.classpath = facet.getClasspath();
      this.system = facet.getSystem();
      this.config = facet.getConfig();
      this.logicalPlan = facet.getLogicalPlan();
      this.metrics = facet.getMetrics();
      this.logs = facet.getLogs();
    }
  }
}
