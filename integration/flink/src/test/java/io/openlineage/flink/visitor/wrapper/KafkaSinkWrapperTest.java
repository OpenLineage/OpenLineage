package io.openlineage.flink.visitor.wrapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.formats.avro.AvroSerializationSchema;
import org.apache.flink.formats.avro.RegistryAvroSerializationSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class KafkaSinkWrapperTest {

  private Properties props = mock(Properties.class);
  private static Schema schema =
      SchemaBuilder.record("OutputEvent")
          .namespace("io.openlineage.flink.avro.event")
          .fields()
          .name("a")
          .type()
          .nullable()
          .longType()
          .noDefault()
          .endRecord();

  private KafkaSink kafkaSink;
  private KafkaSinkWrapper wrapper;
  private KafkaRecordSerializationSchema serializationSchema =
      mock(KafkaRecordSerializationSchema.class);

  @BeforeEach
  @SneakyThrows
  public void setup() {
    props.put("bootstrap.servers", "server1;server2");
    kafkaSink =
        KafkaSink.builder()
            .setBootstrapServers("server1;server2")
            .setKafkaProducerConfig(props)
            .setRecordSerializer(serializationSchema)
            .build();

    wrapper = KafkaSinkWrapper.of(kafkaSink);
  }

  @Test
  public void testGetProducerConfig() {
    assertEquals(props, wrapper.getKafkaProducerConfig());
  }

  @Test
  @SneakyThrows
  public void testGetKafkaTopic() {
    Function<?, ?> topicSelector = mock(Function.class);
    Function noArgFunction = mock(Function.class);
    try (MockedStatic<WrapperUtils> mockedStatic = mockStatic(WrapperUtils.class)) {
      when(WrapperUtils.getFieldValue(
              serializationSchema.getClass(), serializationSchema, "topicSelector"))
          .thenReturn(Optional.ofNullable(topicSelector));
      when(WrapperUtils.getFieldValue(topicSelector.getClass(), topicSelector, "topicSelector"))
          .thenReturn(Optional.ofNullable(noArgFunction));
      when(noArgFunction.apply(null)).thenReturn("topic");

      assertEquals("topic", wrapper.getKafkaTopic());
    }
  }

  @Test
  public void testGetAvroSchema() {
    try (MockedStatic<WrapperUtils> mockedStatic = mockStatic(WrapperUtils.class)) {
      RegistryAvroSerializationSchema avroSerializationSchema =
          mock(RegistryAvroSerializationSchema.class);
      GenericDatumWriter genericDatumWriter = new GenericDatumWriter(schema);
      when(WrapperUtils.getFieldValue(
              serializationSchema.getClass(), serializationSchema, "valueSerializationSchema"))
          .thenReturn(Optional.of(avroSerializationSchema));

      when(WrapperUtils.invoke(
              AvroSerializationSchema.class, avroSerializationSchema, "getDatumWriter"))
          .thenReturn(Optional.of(genericDatumWriter));

      when(WrapperUtils.getFieldValue(GenericDatumWriter.class, genericDatumWriter, "root"))
          .thenReturn(Optional.of(schema));

      assertEquals(schema, wrapper.getAvroSchema().get());
    }
  }

  @Test
  public void testGetAvroSchemaWhenNoValueSerializationSchemaPresent() {
    try (MockedStatic<WrapperUtils> mockedStatic = mockStatic(WrapperUtils.class)) {
      when(WrapperUtils.getFieldValue(
              serializationSchema.getClass(), serializationSchema, "valueSerializationSchema"))
          .thenReturn(Optional.empty());

      assertFalse(wrapper.getAvroSchema().isPresent());
    }
  }

  @Test
  public void testGetAvroSchemaWhenNoDatumWriterPresent() {
    try (MockedStatic<WrapperUtils> mockedStatic = mockStatic(WrapperUtils.class)) {
      RegistryAvroSerializationSchema avroSerializationSchema =
          mock(RegistryAvroSerializationSchema.class);
      when(WrapperUtils.getFieldValue(
              serializationSchema.getClass(), serializationSchema, "valueSerializationSchema"))
          .thenReturn(Optional.of(avroSerializationSchema));

      when(WrapperUtils.invoke(
              AvroSerializationSchema.class, avroSerializationSchema, "getDatumWriter"))
          .thenReturn(Optional.empty());

      assertFalse(wrapper.getAvroSchema().isPresent());
    }
  }
}
