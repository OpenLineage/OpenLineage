/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.utils.AvroSchemaUtils;
import io.openlineage.flink.utils.KafkaUtils;
import io.openlineage.flink.visitor.wrapper.FlinkKafkaConsumerWrapper;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

@Slf4j
public class FlinkKafkaConsumerVisitor extends Visitor<OpenLineage.InputDataset> {

  public FlinkKafkaConsumerVisitor(@NonNull OpenLineageContext context) {
    super(context);
  }

  @Override
  public boolean isDefinedAt(Object object) {
    return object instanceof FlinkKafkaConsumer;
  }

  @Override
  public List<OpenLineage.InputDataset> apply(Object object) {
    try {
      FlinkKafkaConsumerWrapper wrapper = FlinkKafkaConsumerWrapper.of((FlinkKafkaConsumer) object);

      OpenLineage openLineage = context.getOpenLineage();
      return wrapper.getTopics().stream()
          .map(
              topic -> {
                DatasetIdentifier di =
                    KafkaUtils.datasetIdentifierOf(wrapper.getKafkaProperties(), topic);

                OpenLineage.DatasetFacetsBuilder datasetFacetsBuilder =
                    inputDataset().getDatasetFacetsBuilder();

                wrapper
                    .getAvroSchema()
                    .ifPresent(
                        schema ->
                            datasetFacetsBuilder.schema(
                                AvroSchemaUtils.convert(openLineage, schema)));

                return inputDataset().getDataset(di, datasetFacetsBuilder);
              })
          .collect(Collectors.toList());
    } catch (IllegalAccessException e) {
      log.error("Can't access the field. ", e);
    }
    return Collections.emptyList();
  }
}
