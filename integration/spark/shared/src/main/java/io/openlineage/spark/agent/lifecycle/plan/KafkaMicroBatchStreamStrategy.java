/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.dataset.namespace.resolver.HostListNamespaceResolverConfig;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.DatasetFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.execution.datasources.v2.StreamingDataSourceV2Relation;
import scala.Option;

@Slf4j
final class KafkaMicroBatchStreamStrategy extends StreamStrategy {
  private static final String KAFKA_SOURCE_OFFSET_CLASS_NAME =
      "org.apache.spark.sql.kafka010.KafkaSourceOffset";

  public KafkaMicroBatchStreamStrategy(
      DatasetFactory<InputDataset> inputDatasetDatasetFactory,
      StreamingDataSourceV2Relation relation) {
    super(inputDatasetDatasetFactory, relation);

    new HostListNamespaceResolverConfig();
  }

  @Override
  public List<InputDataset> getInputDatasets() {
    Optional<String> bootstrapServersOpt = getBootstrapServers();
    Set<String> topics = getTopics();

    if (!bootstrapServersOpt.isPresent() || topics.isEmpty()) {
      log.warn(
          "Could not generate an input dataset because bootstrapServers need to be present and at least one topic must exist");
      return Collections.emptyList();
    }

    return generateInputDatasets(bootstrapServersOpt, topics);
  }

  private Optional<String> getBootstrapServers() {
    Optional<Map<String, Object>> executorKafkaParams =
        tryReadField(relation.stream(), "executorKafkaParams");
    return executorKafkaParams.map(m -> (String) m.get("bootstrap.servers"));
  }

  private Set<String> getTopics() {
    Option<Offset> offsetOption = relation.startOffset();
    if (offsetOption.isDefined()) {
      Offset offset = offsetOption.get();
      Class<?> offetClass = offset.getClass();
      String offsetClassName = offetClass.getCanonicalName();
      if (KAFKA_SOURCE_OFFSET_CLASS_NAME.equals(offsetClassName)) {
        Optional<scala.collection.immutable.Map<TopicPartition, Long>> topicPartitionsMap =
            tryReadField(offset, "partitionToOffsets");
        return topicPartitionsMap
            .map(ScalaConversionUtils::toJavaMap)
            .map(Map::keySet)
            .map(this::convertTopicPartitions)
            .orElseGet(Collections::emptySet);
      } else {
        log.warn("Encountered an unsupported offset class: {}", offsetClassName);
        return Collections.emptySet();
      }
    } else {
      return Collections.emptySet();
    }
  }

  private List<InputDataset> generateInputDatasets(
      Optional<String> bootstrapServers, Collection<String> topics) {
    String namespace = KafkaBootstrapServerResolver.resolve(bootstrapServers);

    List<InputDataset> datasets = new ArrayList<>();
    for (String topic : topics) {
      InputDataset dataset = datasetFactory.getDataset(topic, namespace, relation.schema());
      datasets.add(dataset);
    }
    return datasets;
  }

  private <T> Optional<T> tryReadField(Object target, String fieldName) {
    try {
      T value = (T) FieldUtils.readDeclaredField(target, fieldName, true);
      return Optional.ofNullable(value);
    } catch (IllegalArgumentException e) {
      log.error("Could not read the field '{}' because it does not exist", fieldName, e);
      return Optional.empty();
    } catch (IllegalAccessException e) {
      log.error("Could not read the field '{}'", fieldName, e);
      return Optional.empty();
    }
  }

  private Set<String> convertTopicPartitions(Collection collection) {
    HashSet<String> set = new HashSet<>();
    for (Object item : collection) {
      TopicPartitionProxy proxy = new TopicPartitionProxy(item);
      Optional<String> topicOptional = proxy.topic();
      topicOptional.ifPresent(set::add);
    }
    return set;
  }
}
