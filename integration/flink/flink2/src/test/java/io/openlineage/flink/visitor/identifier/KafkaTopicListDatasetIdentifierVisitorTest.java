/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.identifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.DatasetIdentifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import org.apache.flink.connector.kafka.lineage.DefaultKafkaDatasetFacet;
import org.apache.flink.connector.kafka.lineage.DefaultKafkaDatasetIdentifier;
import org.apache.flink.connector.kafka.lineage.KafkaDatasetFacet;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.table.planner.lineage.TableLineageDatasetImpl;
import org.junit.jupiter.api.Test;

class KafkaTopicListDatasetIdentifierVisitorTest {
  KafkaTopicListDatasetIdentifierVisitor visitor = new KafkaTopicListDatasetIdentifierVisitor();
  LineageDataset dataset = mock(LineageDataset.class);

  @Test
  void testIsDefined() {
    KafkaDatasetFacet facetWithTopicPattern =
        new DefaultKafkaDatasetFacet(
            DefaultKafkaDatasetIdentifier.ofPattern(Pattern.compile("topic*")), new Properties());
    when(dataset.facets())
        .thenReturn(Collections.singletonMap("otherFacet", facetWithTopicPattern));
    assertThat(visitor.isDefinedAt(dataset)).isFalse();

    when(dataset.facets()).thenReturn(Collections.singletonMap("kafka", facetWithTopicPattern));
    assertThat(visitor.isDefinedAt(dataset)).isFalse();

    KafkaDatasetFacet facetWithTopicList =
        new DefaultKafkaDatasetFacet(
            DefaultKafkaDatasetIdentifier.ofTopics(Arrays.asList("topic")), new Properties());

    when(dataset.facets()).thenReturn(Collections.singletonMap("kafka", facetWithTopicList));
    assertThat(visitor.isDefinedAt(dataset)).isTrue();
  }

  @Test
  void testIsDefinedForTableLineageDatasetImpl() {
    KafkaDatasetFacet facetWithTopicList =
        new DefaultKafkaDatasetFacet(
            DefaultKafkaDatasetIdentifier.ofTopics(Arrays.asList("topic")), new Properties());
    TableLineageDatasetImpl dataset = mock(TableLineageDatasetImpl.class);
    when(dataset.facets()).thenReturn(Collections.singletonMap("kafka", facetWithTopicList));
    assertThat(visitor.isDefinedAt(dataset)).isFalse();
  }

  @Test
  void testApply() {
    KafkaDatasetFacet facet =
        new DefaultKafkaDatasetFacet(
            DefaultKafkaDatasetIdentifier.ofTopics(Arrays.asList("topic1", "topic2")),
            new Properties());

    when(dataset.facets()).thenReturn(Map.of("kafka", facet));
    when(dataset.namespace()).thenReturn("kafka://localhost");

    assertThat(visitor.apply(dataset))
        .containsExactlyInAnyOrder(
            new DatasetIdentifier("topic1", "kafka://localhost"),
            new DatasetIdentifier("topic2", "kafka://localhost"));
  }
}
