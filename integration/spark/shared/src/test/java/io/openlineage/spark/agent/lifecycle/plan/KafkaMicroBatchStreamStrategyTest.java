/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.kafka010.KafkaMicroBatchStream;
import org.apache.spark.sql.kafka010.KafkaSourceOffset;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class KafkaMicroBatchStreamStrategyTest {
  private static final String BOOTSTRAP_SERVERS = "broker.example:9092";

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void createsOneDatasetPerTopic(boolean shaded) throws IllegalAccessException {
    Map<Object, Long> offsets = new HashMap<>();
    offsets.put(topicPartition("events", 0, shaded), 10L);
    offsets.put(topicPartition("events", 1, shaded), 20L);
    offsets.put(topicPartition("audit", 0, shaded), 30L);

    List<InputDataset> datasets =
        strategy(BOOTSTRAP_SERVERS, Optional.of(offset(offsets))).getInputDatasets();

    assertThat(datasets)
        .extracting(InputDataset::getName)
        .containsExactlyInAnyOrder("events", "audit");
    assertThat(datasets)
        .allSatisfy(
            dataset -> {
              assertThat(dataset.getNamespace()).isEqualTo("kafka://broker.example:9092");
              assertThat(dataset.getFacets().getSchema().getFields())
                  .singleElement()
                  .satisfies(
                      field -> {
                        assertThat(field.getName()).isEqualTo("value");
                        assertThat(field.getType()).isEqualTo("string");
                      });
            });
  }

  private static Object topicPartition(String topic, int partition, boolean shaded) {
    return shaded
        ? new kafkashaded.org.apache.kafka.common.TopicPartition(topic, partition)
        : new TopicPartition(topic, partition);
  }

  private static KafkaSourceOffset offset(Map<?, Long> offsets) throws IllegalAccessException {
    KafkaSourceOffset offset = new KafkaSourceOffset(ScalaConversionUtils.asScalaMapEmpty());
    // The unshaded Spark constructor serializes its keys as Kafka TopicPartitions. Inject the
    // runtime map after construction to exercise the same field read with shaded keys.
    FieldUtils.writeDeclaredField(
        offset, "partitionToOffsets", ScalaConversionUtils.fromJavaMap(offsets), true);
    return offset;
  }

  private static KafkaMicroBatchStreamStrategy strategy(
      String bootstrapServers, Optional<Offset> offset) throws IllegalAccessException {
    OpenLineageContext context =
        OpenLineageContext.builder()
            .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
            .meterRegistry(new SimpleMeterRegistry())
            .openLineageConfig(new SparkOpenLineageConfig())
            .build();
    StructType schema = new StructType().add("value", DataTypes.StringType);
    KafkaMicroBatchStream stream = mock(KafkaMicroBatchStream.class);
    FieldUtils.writeDeclaredField(
        stream,
        "executorKafkaParams",
        Collections.singletonMap("bootstrap.servers", bootstrapServers),
        true);
    return new KafkaMicroBatchStreamStrategy(DatasetFactory.input(context), schema, stream, offset);
  }
}
