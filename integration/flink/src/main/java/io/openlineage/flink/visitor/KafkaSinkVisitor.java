/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.utils.AvroSchemaUtils;
import io.openlineage.flink.visitor.wrapper.KafkaSinkWrapper;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.kafka.sink.KafkaSink;

@Slf4j
public class KafkaSinkVisitor extends Visitor<OpenLineage.OutputDataset> {

  public KafkaSinkVisitor(@NonNull OpenLineageContext context) {
    super(context);
  }

  @Override
  public boolean isDefinedAt(Object sink) {
    return sink instanceof KafkaSink;
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(Object kafkaSink) {
    KafkaSinkWrapper wrapper = KafkaSinkWrapper.of((KafkaSink) kafkaSink);
    try {
      Properties properties = wrapper.getKafkaProducerConfig();
      String bootstrapServers = properties.getProperty("bootstrap.servers");
      String topic = wrapper.getKafkaTopic();

      OpenLineage.DatasetFacetsBuilder datasetFacetsBuilder =
          outputDataset().getDatasetFacetsBuilder();

      wrapper
          .getAvroSchema()
          .map(
              schema ->
                  datasetFacetsBuilder.schema(
                      AvroSchemaUtils.convert(context.getOpenLineage(), schema)));

      log.debug("Kafka output topic: {}", topic);

      return Collections.singletonList(
          outputDataset().getDataset(topic, bootstrapServers, datasetFacetsBuilder));
    } catch (IllegalAccessException e) {
      log.error("Can't access the field. ", e);
    }
    return Collections.emptyList();
  }
}
