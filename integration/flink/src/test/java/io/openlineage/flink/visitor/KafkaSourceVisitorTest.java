package io.openlineage.flink.visitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.agent.client.OpenLineageClient;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.visitor.wrapper.KafkaSourceWrapper;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class KafkaSourceVisitorTest {

  OpenLineageContext context = mock(OpenLineageContext.class);
  KafkaSource kafkaSource = mock(KafkaSource.class);
  KafkaSourceVisitor kafkaSourceVisitor = new KafkaSourceVisitor(context);
  KafkaSourceWrapper wrapper = mock(KafkaSourceWrapper.class);
  Properties props = new Properties();
  OpenLineage openLineage = new OpenLineage(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI);
  Schema schema =
      SchemaBuilder.record("InputEvent")
          .namespace("io.openlineage.flink.avro.event")
          .fields()
          .name("a")
          .type()
          .nullable()
          .longType()
          .noDefault()
          .endRecord();

  @BeforeEach
  @SneakyThrows
  public void setup() {
    when(context.getOpenLineage()).thenReturn(openLineage);
  }

  @Test
  public void testIsDefined() {
    assertEquals(false, kafkaSourceVisitor.isDefinedAt(mock(Object.class)));
    assertEquals(true, kafkaSourceVisitor.isDefinedAt(mock(KafkaSource.class)));
  }

  @Test
  @SneakyThrows
  public void testApply() {
    props.put("bootstrap.servers", "server1;server2");

    try (MockedStatic<KafkaSourceWrapper> mockedStatic = mockStatic(KafkaSourceWrapper.class)) {
      when(KafkaSourceWrapper.of(kafkaSource)).thenReturn(wrapper);

      when(wrapper.getTopics()).thenReturn(Arrays.asList("topic1", "topic2"));
      when(wrapper.getProps()).thenReturn(props);
      when(wrapper.getAvroSchema()).thenReturn(Optional.of(schema));

      List<OpenLineage.InputDataset> inputDatasets = kafkaSourceVisitor.apply(kafkaSource);
      List<OpenLineage.SchemaDatasetFacetFields> fields =
          inputDatasets.get(0).getFacets().getSchema().getFields();

      assertEquals(2, inputDatasets.size());
      assertEquals("topic1", inputDatasets.get(0).getName());
      assertEquals("server1;server2", inputDatasets.get(0).getNamespace());

      assertEquals(1, fields.size());
      assertEquals("a", fields.get(0).getName());
      assertEquals("long", fields.get(0).getType());
    }
  }

  @Test
  @SneakyThrows
  public void testApplyWhenIllegalAccessExceptionThrown() {
    try (MockedStatic<KafkaSourceWrapper> mockedStatic = mockStatic(KafkaSourceWrapper.class)) {
      when(KafkaSourceWrapper.of(kafkaSource)).thenReturn(wrapper);

      when(wrapper.getTopics()).thenThrow(new IllegalAccessException(""));
      List<OpenLineage.InputDataset> inputDatasets = kafkaSourceVisitor.apply(kafkaSource);

      assertEquals(0, inputDatasets.size());
    }
  }
}
