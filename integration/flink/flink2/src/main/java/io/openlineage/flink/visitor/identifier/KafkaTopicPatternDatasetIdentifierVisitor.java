/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.identifier;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.config.FlinkDatasetConfig;
import io.openlineage.flink.config.FlinkDatasetKafkaConfig;
import io.openlineage.flink.config.FlinkOpenLineageConfig;
import io.openlineage.flink.util.KafkaDatasetFacetUtil;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.kafka.lineage.KafkaDatasetFacet;
import org.apache.flink.connector.kafka.lineage.KafkaDatasetIdentifier;
import org.apache.flink.streaming.api.lineage.LineageDataset;
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
            .map(OpenLineageContext::getConfig)
            .map(FlinkOpenLineageConfig::getDatasetConfig)
            .map(FlinkDatasetConfig::getKafkaConfig)
            .map(FlinkDatasetKafkaConfig::isResolveTopicPattern);

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
    Pattern pattern = getTopicPattern(dataset).get();
    partitions.forEach(p -> log.info("Partitions: {}", p));

    return partitions.stream()
        .flatMap(Collection::stream)
        .map(PartitionInfo::topic)
        .distinct()
        .filter(t -> pattern.matcher(t).matches())
        .map(t -> new DatasetIdentifier(t, dataset.namespace()))
        .collect(Collectors.toList());
  }

  private Optional<Pattern> getTopicPattern(LineageDataset dataset) {
    return KafkaDatasetFacetUtil.getFacet(dataset)
        .map(KafkaDatasetFacet::getTopicIdentifier)
        .map(KafkaDatasetIdentifier::getTopicPattern);
  }

  private Optional<Properties> getKafkaProperties(LineageDataset dataset) {
    return KafkaDatasetFacetUtil.getFacet(dataset).map(KafkaDatasetFacet::getProperties);
  }

  static class ConsumerUtils {
    static KafkaConsumer getConsumer(Properties properties) {
      return new KafkaConsumer(properties);
    }
  }
}
