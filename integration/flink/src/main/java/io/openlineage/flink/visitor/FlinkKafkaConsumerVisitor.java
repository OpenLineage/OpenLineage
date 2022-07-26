/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.utils.AvroSchemaUtils;
import io.openlineage.flink.visitor.wrapper.FlinkKafkaConsumerWrapper;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
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
      Properties properties = wrapper.getKafkaProperties();
      String bootstrapServers = properties.getProperty("bootstrap.servers");

      OpenLineage openLineage = context.getOpenLineage();
      return wrapper.getTopics().stream()
          .map(
              topic -> {
                OpenLineage.InputDatasetBuilder builder =
                    openLineage.newInputDatasetBuilder().namespace(bootstrapServers).name(topic);

                OpenLineage.DatasetFacetsBuilder facetsBuilder =
                    openLineage.newDatasetFacetsBuilder();

                wrapper
                    .getAvroSchema()
                    .ifPresent(
                        schema ->
                            facetsBuilder.schema(AvroSchemaUtils.convert(openLineage, schema)));

                return builder.facets(facetsBuilder.build()).build();
              })
          .collect(Collectors.toList());
    } catch (IllegalAccessException e) {
      log.error("Can't access the field. ", e);
    }
    return Collections.emptyList();
  }
}
