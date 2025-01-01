/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE;
import static io.openlineage.common.config.ConfigWrapper.fromResource;
import static io.openlineage.flink.StreamEnvironment.setupEnv;
import static org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks;

import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.openlineage.flink.proto.event.InputEvent;
import io.openlineage.flink.proto.event.OutputEvent;
import io.openlineage.util.OpenLineageFlinkJobListenerBuilder;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkProtobufApplication {

  private static final String SCHEMA_REGISTRY_URL = "http://schema-registry:8081";
  public static void main(String[] args) throws Exception {
    ParameterTool parameters = ParameterTool.fromArgs(args);
    StreamExecutionEnvironment env = setupEnv(args);

    String inputTopic = parameters.getRequired("input-topics");
    String outputTopic = parameters.getRequired("output-topics");

    KafkaSource<InputEvent> source =
        KafkaSource.<InputEvent>builder()
            .setProperties(fromResource("kafka-consumer.conf").toProperties())
            .setBootstrapServers("kafka-host:9092")
            .setValueOnlyDeserializer(
                new DeserializationSchema<InputEvent>() {
                  @Override
                  public InputEvent deserialize(byte[] bytes) throws IOException {
                    CachedSchemaRegistryClient cachedSchemaRegistryClient = new CachedSchemaRegistryClient(
                        SCHEMA_REGISTRY_URL,
                        10,
                        List.of(new AvroSchemaProvider(), new ProtobufSchemaProvider()),
                        Collections.emptyMap()
                    );
                    KafkaProtobufDeserializer<InputEvent> deserializer = new KafkaProtobufDeserializer(
                        cachedSchemaRegistryClient
                    );

                    Map<String, String> serdeConfig = new HashMap<>();
                    serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
                    serdeConfig.put(SPECIFIC_PROTOBUF_VALUE_TYPE,InputEvent.class.getName());

                    deserializer.configure(serdeConfig, false);
                    return deserializer.deserialize(inputTopic, bytes);
                  }

                  @Override
                  public boolean isEndOfStream(InputEvent simpleMessage) {
                    return false;
                  }

                  @Override
                  public TypeInformation<InputEvent> getProducedType() {
                    return null;
                  }
                }
            )
            .setTopics(inputTopic)
            .build();

    KafkaSink<OutputEvent> sink = KafkaSink.<OutputEvent>builder()
        .setKafkaProducerConfig(fromResource("kafka-producer.conf").toProperties())
        .setBootstrapServers("kafka-host:9092")
        .setRecordSerializer(
            KafkaRecordSerializationSchema.builder()
                .setValueSerializationSchema(
                    new SerializationSchema<OutputEvent>() {
                      @Override
                      public byte[] serialize(OutputEvent outputEvent) {
                        CachedSchemaRegistryClient cachedSchemaRegistryClient = new CachedSchemaRegistryClient(
                            SCHEMA_REGISTRY_URL,
                            10,
                            List.of(new AvroSchemaProvider(), new ProtobufSchemaProvider()),
                            Collections.emptyMap()
                        );
                        KafkaProtobufSerializer<OutputEvent> serializer = new KafkaProtobufSerializer(
                            cachedSchemaRegistryClient
                        );


                        Map<String, String> serdeConfig = new HashMap<>();
                        serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
                        serdeConfig.put(SPECIFIC_PROTOBUF_VALUE_TYPE,OutputEvent.class.getName());

                        serializer.configure(serdeConfig, false);

                        return serializer.serialize(outputTopic, outputEvent);
                      }
                    }
                )
                .setTopic(outputTopic)
                .build())
        .build();

    env.fromSource(
            source,
            noWatermarks(),
            "kafka-source")
        .uid("kafka-source")
        .returns(InputEvent.class)
        .keyBy(InputEvent::getId)
        .process(new StatefulProtoCounter())
        .returns(OutputEvent.class)
        .name("process")
        .uid("process")
        .sinkTo(sink)
        .name("kafka-sink")
        .uid("kafka-sink");

    String jobName = parameters.get("job-name");
    //env.getConfig().addDefaultKryoSerializer(InputEvent.class, ProtobufKryoSerializer.class);
    env.registerJobListener(
        OpenLineageFlinkJobListenerBuilder.create()
            .executionEnvironment(env)
            .jobName(jobName)
            .build());
    env.execute(jobName);
  }

}
