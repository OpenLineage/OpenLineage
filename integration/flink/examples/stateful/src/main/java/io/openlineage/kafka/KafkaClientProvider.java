package io.openlineage.kafka;

import io.openlineage.flink.avro.event.InputEvent;
import io.openlineage.flink.avro.event.OutputEvent;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;

import static io.openlineage.common.config.ConfigWrapper.fromResource;

public class KafkaClientProvider {

    private KafkaClientProvider() {
    }

    public static KafkaSource<InputEvent> aKafkaSource(String topic) {
        return KafkaSource.<InputEvent>builder()
                .setTopics(topic)
                .setProperties(fromResource("kafka-consumer.conf").toProperties())
                .setValueOnlyDeserializer(ConfluentRegistryAvroDeserializationSchema
                        .forSpecific(InputEvent.class, "http://schema-registry:8081"))
                .build();
    }

    public static KafkaSink<OutputEvent> aKafkaSink(String topic) {
        return KafkaSink.<OutputEvent>builder()
                .setKafkaProducerConfig(fromResource("kafka-producer.conf").toProperties())
                .setBootstrapServers("kafka:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setValueSerializationSchema(ConfluentRegistryAvroSerializationSchema
                                .forSpecific(OutputEvent.class, topic, "http://schema-registry:8081"))
                        .setTopic(topic)
                        .build())
                .build();
    }

}
