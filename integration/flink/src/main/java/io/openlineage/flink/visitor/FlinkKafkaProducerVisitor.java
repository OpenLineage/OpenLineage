/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.utils.AvroSchemaUtils;
import io.openlineage.flink.visitor.wrapper.FlinkKafkaProducerWrapper;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

@Slf4j
public class FlinkKafkaProducerVisitor extends Visitor<OpenLineage.OutputDataset> {
  public FlinkKafkaProducerVisitor(@NonNull OpenLineageContext context) {
    super(context);
  }

  @Override
  public boolean isDefinedAt(Object sink) {
    return sink instanceof FlinkKafkaProducer;
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(Object flinkKafkaProducer) {
    FlinkKafkaProducerWrapper wrapper =
        FlinkKafkaProducerWrapper.of((FlinkKafkaProducer) flinkKafkaProducer);
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
  }
}
