/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.utils.AvroSchemaUtils;
import io.openlineage.flink.utils.KafkaUtils;
import io.openlineage.flink.visitor.wrapper.FlinkKafkaProducerWrapper;
import java.util.Collections;
import java.util.List;
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

    DatasetIdentifier di =
        KafkaUtils.datasetIdentifierOf(wrapper.getKafkaProducerConfig(), wrapper.getKafkaTopic());

    OpenLineage.DatasetFacetsBuilder datasetFacetsBuilder =
        outputDataset().getDatasetFacetsBuilder();

    wrapper
        .getAvroSchema()
        .map(
            schema ->
                datasetFacetsBuilder.schema(
                    AvroSchemaUtils.convert(context.getOpenLineage(), schema)));

    log.debug("Kafka output topic: {}", di.getName());

    return Collections.singletonList(outputDataset().getDataset(di, datasetFacetsBuilder));
  }
}
