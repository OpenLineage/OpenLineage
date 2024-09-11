/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.identifier;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.flink.client.OpenLineageContext;
import io.openlineage.flink.util.KafkaDatasetFacetUtil;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicsDescriptor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

/** Visitor */
@Slf4j
public class KafkaTopicPatternDatasetIdentifierVisitor implements DatasetIdentifierVisitor {
  private final OpenLineageContext context;

  public KafkaTopicPatternDatasetIdentifierVisitor(OpenLineageContext context) {
    this.context = context;
  }

  @Override
  public boolean isDefinedAt(LineageDataset dataset) {
    Optional<Boolean> resolveTopics =
        Optional.ofNullable(context)
            .map(c -> c.getConfig())
            .map(c -> c.getDatasetConfig())
            .map(c -> c.getKafkaConfig())
            .map(c -> c.isResolveTopicPattern());

    if (resolveTopics.isEmpty() || !resolveTopics.get()) {
      return false;
    }

    return KafkaDatasetFacetUtil.isOnClasspath()
        && getTopicPattern(dataset).isPresent()
        && getKafkaProperties(dataset).isPresent();
  }

  @Override
  public Collection<DatasetIdentifier> apply(LineageDataset dataset) {
    Collection<List<PartitionInfo>> partitions;
    try {
      partitions =
          ConsumerUtils.getConsumer(this.getKafkaProperties(dataset).get()).listTopics().values();
    } catch (Exception e) {
      log.warn("Couldn't resolve kafka topics", e);
      return Collections.emptyList();
    }
    KafkaTopicsDescriptor descriptor =
        new KafkaTopicsDescriptor(null, getTopicPattern(dataset).get());

    return partitions.stream()
        .flatMap(l -> l.stream())
        .map(p -> p.topic())
        .distinct()
        .filter(t -> descriptor.isMatchingTopic(t))
        .map(t -> new DatasetIdentifier(t, dataset.namespace()))
        .collect(Collectors.toList());
  }

  private Optional<Pattern> getTopicPattern(LineageDataset dataset) {
    return KafkaDatasetFacetUtil.getFacet(dataset)
        .map(f -> f.getTopicIdentifier())
        .map(f -> f.getTopicPattern());
  }

  private Optional<Properties> getKafkaProperties(LineageDataset dataset) {
    return KafkaDatasetFacetUtil.getFacet(dataset).map(f -> f.getProperties());
  }

  static class ConsumerUtils {
    static KafkaConsumer getConsumer(Properties properties) {
      return new KafkaConsumer(properties);
    }
  }
}
