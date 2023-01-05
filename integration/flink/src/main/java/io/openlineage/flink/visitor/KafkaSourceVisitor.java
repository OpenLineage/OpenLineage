/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.utils.AvroSchemaUtils;
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
      KafkaSourceWrapper wrapper = KafkaSourceWrapper.of((KafkaSource<?>) kafkaSource);
      List<String> topics = wrapper.getTopics();
      Properties properties = wrapper.getProps();
      String bootstrapServers = properties.getProperty("bootstrap.servers");

      topics.forEach(topic -> log.debug("Kafka input topic: {}", topic));
      return topics.stream()
          .map(
              topic -> {
                OpenLineage.DatasetFacetsBuilder datasetFacetsBuilder =
                    inputDataset().getDatasetFacetsBuilder();
                // The issue here is that we assign dataset a schema that we're trying to read with.
                // The read schema may be in mismatch with real dataset schema.
                wrapper
                    .getAvroSchema()
                    .map(
                        schema ->
                            datasetFacetsBuilder.schema(
                                AvroSchemaUtils.convert(context.getOpenLineage(), schema)));
                return inputDataset().getDataset(topic, bootstrapServers, datasetFacetsBuilder);
              })
          .collect(Collectors.toList());
    } catch (IllegalAccessException e) {
      log.error("Can't access the field. ", e);
    }
    return Collections.emptyList();
  }
}
