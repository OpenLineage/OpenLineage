/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.identifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.flink.client.OpenLineageContext;
import io.openlineage.flink.visitor.identifier.KafkaTopicPatternDatasetIdentifierVisitor.ConsumerUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.kafka.lineage.DefaultKafkaDatasetFacet;
import org.apache.flink.connector.kafka.lineage.DefaultKafkaDatasetIdentifier;
import org.apache.flink.connector.kafka.lineage.KafkaDatasetFacet;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/** Test class for {@link KafkaTopicPatternDatasetIdentifierVisitor} */
@Slf4j
class KafkaTopicPatternDatasetIdentifierVisitorTest {

  OpenLineageContext context = mock(OpenLineageContext.class, RETURNS_DEEP_STUBS);
  KafkaTopicPatternDatasetIdentifierVisitor visitor =
      new KafkaTopicPatternDatasetIdentifierVisitor(context);

  LineageDataset dataset = mock(LineageDataset.class);

  @BeforeEach
  void setup() {
    when((context.getConfig().getDatasetConfig().getKafkaConfig()).isResolveTopicPattern())
        .thenReturn(true);
  }

  @Test
  void testIsDefined() {
    KafkaDatasetFacet facetWithTopicList =
        new DefaultKafkaDatasetFacet(
            DefaultKafkaDatasetIdentifier.ofTopics(Arrays.asList("topic")));
    when(dataset.facets()).thenReturn(Collections.singletonMap("otherFacet", facetWithTopicList));
    assertThat(visitor.isDefinedAt(dataset)).isFalse();

    when(dataset.facets()).thenReturn(Collections.singletonMap("kafka", facetWithTopicList));
    assertThat(visitor.isDefinedAt(dataset)).isFalse();

    KafkaDatasetFacet facetWithTopicPattern =
        new DefaultKafkaDatasetFacet(
            DefaultKafkaDatasetIdentifier.ofPattern(Pattern.compile("topic*")), new Properties());
    when(dataset.facets()).thenReturn(Collections.singletonMap("kafka", facetWithTopicPattern));
    assertThat(visitor.isDefinedAt(dataset)).isTrue();
  }

  @Test
  void testIsDefinedWhenPatternResolvingDisabled() {
    KafkaDatasetFacet facet =
        new DefaultKafkaDatasetFacet(
            DefaultKafkaDatasetIdentifier.ofPattern(Pattern.compile("topic*")), new Properties());

    when(dataset.facets()).thenReturn(Map.of("kafka", facet));
    assertThat(visitor.isDefinedAt(dataset)).isTrue();

    when((context.getConfig().getDatasetConfig().getKafkaConfig()).isResolveTopicPattern())
        .thenReturn(false);
    assertThat(visitor.isDefinedAt(dataset)).isFalse();

    when(context.getConfig().getDatasetConfig()).thenReturn(null);
    assertThat(visitor.isDefinedAt(dataset)).isFalse();
  }

  @Test
  void testApply() {
    KafkaDatasetFacet facet =
        new DefaultKafkaDatasetFacet(
            DefaultKafkaDatasetIdentifier.ofPattern(Pattern.compile("topic.*")), new Properties());

    when(dataset.facets()).thenReturn(Map.of("kafka", facet));
    when(dataset.namespace()).thenReturn("kafka://localhost");

    KafkaConsumer kafkaConsumer = mock(KafkaConsumer.class);
    when(kafkaConsumer.listTopics())
        .thenReturn(
            Map.of(
                "a", Collections.singletonList(new PartitionInfo("topic1", 7, null, null, null)),
                "b", Collections.singletonList(new PartitionInfo("topic2", 7, null, null, null))));

    try (MockedStatic<ConsumerUtils> mockedStatic = mockStatic(ConsumerUtils.class)) {
      when(ConsumerUtils.getConsumer(any())).thenReturn(kafkaConsumer);

      assertThat(visitor.apply(dataset))
          .containsExactlyInAnyOrder(
              new DatasetIdentifier("topic1", "kafka://localhost"),
              new DatasetIdentifier("topic2", "kafka://localhost"));
    }
  }

  @Test
  void testApplyExceptionIsThrown() {
    KafkaConsumer kafkaConsumer = mock(KafkaConsumer.class);
    when(kafkaConsumer.listTopics()).thenThrow(new RuntimeException("whatever exception"));
    try (MockedStatic<ConsumerUtils> mockedStatic = mockStatic(ConsumerUtils.class)) {
      when(ConsumerUtils.getConsumer(any())).thenReturn(kafkaConsumer);

      assertThat(visitor.apply(dataset)).isEmpty();
    }
  }
}
