/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark40.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.sql.execution.datasources.v2.StreamingDataSourceV2Relation;
import org.apache.spark.sql.execution.datasources.v2.StreamingDataSourceV2ScanRelation;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.apache.spark.sql.kafka010.KafkaMicroBatchStream;
import org.apache.spark.sql.kafka010.KafkaSourceOffset;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import scala.Option;

class StreamingDataSourceV2ScanRelationDatasetBuilderTest {

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testKafkaInputsWithNativeAndShadedTopicPartitions(boolean shaded)
      throws IllegalAccessException {
    OpenLineageContext context =
        OpenLineageContext.builder()
            .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
            .meterRegistry(new SimpleMeterRegistry())
            .openLineageConfig(new SparkOpenLineageConfig())
            .build();
    KafkaMicroBatchStream stream = mock(KafkaMicroBatchStream.class);
    FieldUtils.writeDeclaredField(
        stream,
        "executorKafkaParams",
        Collections.singletonMap("bootstrap.servers", "broker:9092"),
        true);

    Map<Object, Long> partitionOffsets = new LinkedHashMap<>();
    partitionOffsets.put(topicPartition("orders", 0, shaded), 10L);
    partitionOffsets.put(topicPartition("orders", 1, shaded), 20L);
    partitionOffsets.put(topicPartition("payments", 0, shaded), 30L);

    KafkaSourceOffset offset = new KafkaSourceOffset(ScalaConversionUtils.asScalaMapEmpty());
    // Model the relocated runtime field without asking the unshaded Kafka JSON codec to read it.
    FieldUtils.writeDeclaredField(
        offset, "partitionToOffsets", ScalaConversionUtils.fromJavaMap(partitionOffsets), true);

    StructType schema =
        new StructType().add("key", DataTypes.BinaryType).add("value", DataTypes.BinaryType);
    StreamingDataSourceV2ScanRelation scanRelation = mock(StreamingDataSourceV2ScanRelation.class);
    StreamingDataSourceV2Relation relation = mock(StreamingDataSourceV2Relation.class);
    when(scanRelation.stream()).thenReturn(stream);
    when(scanRelation.startOffset()).thenReturn(Option.apply(offset));
    when(scanRelation.relation()).thenReturn(relation);
    when(relation.schema()).thenReturn(schema);

    List<OpenLineage.InputDataset> inputs =
        new StreamingDataSourceV2ScanRelationDatasetBuilder(context)
            .apply(mock(SparkListenerSQLExecutionStart.class), scanRelation);

    assertThat(inputs)
        .extracting(OpenLineage.InputDataset::getName, OpenLineage.InputDataset::getNamespace)
        .containsExactlyInAnyOrder(
            tuple("orders", "kafka://broker:9092"), tuple("payments", "kafka://broker:9092"));
    assertThat(inputs)
        .allSatisfy(
            input ->
                assertThat(input.getFacets().getSchema().getFields())
                    .extracting(
                        OpenLineage.SchemaDatasetFacetFields::getName,
                        OpenLineage.SchemaDatasetFacetFields::getType)
                    .containsExactly(tuple("key", "binary"), tuple("value", "binary")));
  }

  private Object topicPartition(String topic, int partition, boolean shaded) {
    return shaded
        ? new kafkashaded.org.apache.kafka.common.TopicPartition(topic, partition)
        : new TopicPartition(topic, partition);
  }
}
