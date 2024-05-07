/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.SchemaDatasetFacet;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.client.EventEmitter;
import io.openlineage.flink.visitor.wrapper.IcebergSourceWrapper;
import io.openlineage.flink.visitor.wrapper.KafkaSourceWrapper;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import lombok.SneakyThrows;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.source.IcebergSource;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class HybridSourceVisitorTest {
  OpenLineageContext context = mock(OpenLineageContext.class);
  OpenLineage openLineage = new OpenLineage(EventEmitter.OPEN_LINEAGE_CLIENT_URI);
  HybridSourceVisitor hybridSourceVisitor = new HybridSourceVisitor(context);
  Properties props = new Properties();
  SchemaDatasetFacet schema =
      openLineage.newSchemaDatasetFacet(
          Collections.singletonList(
              openLineage.newSchemaDatasetFacetFields("a", "long", null, null)));

  KafkaSourceWrapper kafkaSourceWrapper = mock(KafkaSourceWrapper.class);
  IcebergSourceWrapper icebergSourceWrapper = mock(IcebergSourceWrapper.class);
  IcebergSource icebergSource = mock(IcebergSource.class);
  KafkaSource kafkaSource = mock(KafkaSource.class);

  @BeforeEach
  @SneakyThrows
  public void setup() {
    when(context.getOpenLineage()).thenReturn(openLineage);
  }

  @Test
  void testIsDefined() {
    assertFalse(hybridSourceVisitor.isDefinedAt(mock(Object.class)));
    assertTrue(hybridSourceVisitor.isDefinedAt(mock(HybridSource.class)));
  }

  @Test
  @SneakyThrows
  void testApply() {
    // Mock Iceberg Source
    Table table = mock(Table.class, RETURNS_DEEP_STUBS);
    MockedStatic<IcebergSourceWrapper> mockedIcebergStatic = mockStatic(IcebergSourceWrapper.class);
    when(IcebergSourceWrapper.of(icebergSource, IcebergSource.class))
        .thenReturn(icebergSourceWrapper);
    when(table.location()).thenReturn("s3://bucket/table/");
    when(table.schema().columns())
        .thenReturn(
            Collections.singletonList(Types.NestedField.of(1, false, "a", Types.LongType.get())));
    when(icebergSourceWrapper.getTable()).thenReturn(table);

    // Mock Kafka Source
    props.put("bootstrap.servers", "server1;server2");
    MockedStatic<KafkaSourceWrapper> mockedKafkaStatic = mockStatic(KafkaSourceWrapper.class);
    when(KafkaSourceWrapper.of(kafkaSource, context)).thenReturn(kafkaSourceWrapper);

    when(kafkaSourceWrapper.getTopics()).thenReturn(Arrays.asList("topic1", "topic2"));
    when(kafkaSourceWrapper.getProps()).thenReturn(props);
    when(kafkaSourceWrapper.getSchemaFacet()).thenReturn(Optional.of(schema));

    when(icebergSource.getBoundedness()).thenReturn(Boundedness.BOUNDED);
    when(kafkaSource.getBoundedness()).thenReturn(Boundedness.CONTINUOUS_UNBOUNDED);

    HybridSource hybridSource =
        new HybridSource.HybridSourceBuilder()
            .addSource(icebergSource)
            .addSource(kafkaSource)
            .build();

    List<OpenLineage.InputDataset> inputDatasets = hybridSourceVisitor.apply(hybridSource);
    assertEquals(3, inputDatasets.size());
    // Validate Iceberg
    List<OpenLineage.SchemaDatasetFacetFields> icebergFields =
        inputDatasets.get(0).getFacets().getSchema().getFields();

    assertEquals("table", inputDatasets.get(0).getName());
    assertEquals("s3://bucket", inputDatasets.get(0).getNamespace());

    assertEquals(1, icebergFields.size());
    assertEquals("a", icebergFields.get(0).getName());
    assertEquals("LONG", icebergFields.get(0).getType());

    // Validate Kafka
    assertEquals("topic1", inputDatasets.get(1).getName());
    assertEquals("kafka://server1;server2", inputDatasets.get(1).getNamespace());

    List<OpenLineage.SchemaDatasetFacetFields> kafkaFields =
        inputDatasets.get(1).getFacets().getSchema().getFields();
    assertEquals(1, kafkaFields.size());
    assertEquals("a", kafkaFields.get(0).getName());
    assertEquals("long", kafkaFields.get(0).getType());
  }
}
