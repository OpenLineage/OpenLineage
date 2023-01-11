/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.kafka


import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

class SimpleKafkaProducer implements AutoCloseable {
    private final KafkaProducer<String, SpecificRecord> producer

    SimpleKafkaProducer(Kafka kafka) {
        final Properties properties = new Properties()
        properties.put('bootstrap.servers', kafka.bootstrapServers)
        properties.put('acks', 'all')
        properties.put('retries', 2)
        properties.put('key.serializer', StringSerializer.class)
        properties.put('value.serializer', KafkaAvroSerializer.class)
        properties.put('schema.registry.url', kafka.schemaRegistryUrl)
        properties.put('specific.avro.reader', true)
        properties.put('auto.register.schemas', true)
        properties.put('value.subject.name.strategy', 'io.confluent.kafka.serializers.subject.TopicNameStrategy')
        producer = new KafkaProducer<>(properties)
    }

    @Override
    void close() throws Exception {
        if (producer != null) {
            producer.close()
        }
    }

    void produce(String topic, SpecificRecord event, String key) {
        println "Sending to topic=$topic, key=$key, value=$event"
        producer.send(new ProducerRecord<String, SpecificRecord>(topic, key, event)).get()
    }
}
