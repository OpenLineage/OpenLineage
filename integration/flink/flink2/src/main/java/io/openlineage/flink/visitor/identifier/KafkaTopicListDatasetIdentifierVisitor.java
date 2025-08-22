/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.identifier;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.flink.util.KafkaDatasetFacetUtil;
import io.openlineage.flink.wrapper.TableLineageDatasetWrapper;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.kafka.lineage.KafkaDatasetFacet;
import org.apache.flink.connector.kafka.lineage.KafkaDatasetIdentifier;
import org.apache.flink.streaming.api.lineage.LineageDataset;

/** Visitor */
@Slf4j
public class KafkaTopicListDatasetIdentifierVisitor implements DatasetIdentifierVisitor {
  @Override
  public boolean isDefinedAt(LineageDataset dataset) {
    // TableLineageDatasetImpl is not available on the classpath in real workloads (like container
    // tests
    if (new TableLineageDatasetWrapper(dataset).isTableLineageDataset()) {
      return false;
    }

    return KafkaDatasetFacetUtil.isOnClasspath() && !getTopicList(dataset).isEmpty();
  }

  private Collection<String> getTopicList(LineageDataset dataset) {
    return KafkaDatasetFacetUtil.getFacet(dataset)
        .map(KafkaDatasetFacet::getTopicIdentifier)
        .map(KafkaDatasetIdentifier::getTopics)
        .orElse(Collections.emptyList());
  }

  @Override
  public Collection<DatasetIdentifier> apply(LineageDataset dataset) {
    return getTopicList(dataset).stream()
        .map(topic -> new DatasetIdentifier(topic, dataset.namespace()))
        .collect(Collectors.toList());
  }
}
