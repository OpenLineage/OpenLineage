/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.utils.KafkaUtils;
import io.openlineage.flink.visitor.wrapper.KafkaSourceWrapper;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.kafka.source.KafkaSource;

@Slf4j
public class KafkaSourceVisitor extends Visitor<OpenLineage.InputDataset> {

  public KafkaSourceVisitor(@NonNull OpenLineageContext context) {
    super(context);
  }

  @Override
  public boolean isDefinedAt(Object source) {
    return source instanceof KafkaSource;
  }

  @Override
  public List<OpenLineage.InputDataset> apply(Object kafkaSource) {
    try {
      KafkaSourceWrapper wrapper = KafkaSourceWrapper.of((KafkaSource<?>) kafkaSource, context);
      List<String> topics = wrapper.getTopics();
      Properties properties = wrapper.getProps();

      topics.forEach(topic -> log.debug("Kafka input topic: {}", topic));
      return topics.stream()
          .map(
              topic -> {
                DatasetIdentifier di = KafkaUtils.datasetIdentifierOf(properties, topic);

                OpenLineage.DatasetFacetsBuilder datasetFacetsBuilder =
                    inputDataset().getDatasetFacetsBuilder();
                // The issue here is that we assign dataset a schema that we're trying to read with.
                // The read schema may be in mismatch with real dataset schema.
                wrapper.getSchemaFacet().map(facet -> datasetFacetsBuilder.schema(facet));
                return inputDataset().getDataset(di, datasetFacetsBuilder);
              })
          .collect(Collectors.toList());
    } catch (IllegalAccessException e) {
      log.error("Can't access the field. ", e);
    }
    return Collections.emptyList();
  }
}
