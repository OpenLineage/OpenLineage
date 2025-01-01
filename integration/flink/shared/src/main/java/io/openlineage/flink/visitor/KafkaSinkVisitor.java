/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.DatasetFacetsBuilder;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.utils.AvroSchemaUtils;
import io.openlineage.flink.utils.KafkaUtils;
import io.openlineage.flink.visitor.wrapper.AvroUtils;
import io.openlineage.flink.visitor.wrapper.KafkaSinkWrapper;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
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
    KafkaSinkWrapper wrapper = KafkaSinkWrapper.of((KafkaSink) kafkaSink, context);
    List<String> topics = wrapper.getTopicsOfMultiTopicSink();
    log.debug("Extracting output dataset for KafkaSinkVisitor with topics {}", topics);
    if (topics != null && !topics.isEmpty()) {
      DatasetFacetsBuilder facetsBuilder = outputDataset().getDatasetFacetsBuilder();
      wrapper
          .getSchemaOfMultiTopicSink()
          .map(s -> AvroUtils.getAvroSchema(Optional.of(s)))
          .filter(Optional::isPresent)
          .map(Optional::get)
          .map(s -> AvroSchemaUtils.convert(context.getOpenLineage(), s))
          .ifPresent(schema -> facetsBuilder.schema(schema));

      return topics.stream()
          .map(
              topic ->
                  outputDataset()
                      .getDataset(
                          KafkaUtils.datasetIdentifierOf(wrapper.getKafkaProducerConfig(), topic),
                          facetsBuilder))
          .collect(Collectors.toList());
    }

    try {
      DatasetIdentifier di =
          KafkaUtils.datasetIdentifierOf(wrapper.getKafkaProducerConfig(), wrapper.getKafkaTopic());

      OpenLineage.DatasetFacetsBuilder datasetFacetsBuilder =
          outputDataset().getDatasetFacetsBuilder();

      wrapper.getSchemaFacet().map(facet -> datasetFacetsBuilder.schema(facet));

      log.debug("KafkaSinkVisitor extracted output topic: {}", di.getName());
      return Collections.singletonList(outputDataset().getDataset(di, datasetFacetsBuilder));
    } catch (IllegalAccessException | java.util.NoSuchElementException e) {
      log.error("Can't access the field. ", e);
    }
    return Collections.emptyList();
  }
}
