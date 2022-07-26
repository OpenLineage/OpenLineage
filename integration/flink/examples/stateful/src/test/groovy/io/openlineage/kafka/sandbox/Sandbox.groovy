/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.kafka.sandbox

import io.openlineage.flink.avro.event.InputEvent
import io.openlineage.flink.avro.event.OutputEvent
import io.openlineage.kafka.Kafka
import io.openlineage.kafka.KafkaUtils
import io.openlineage.kafka.SchemaUtils
import io.openlineage.kafka.SimpleKafkaConsumer
import io.openlineage.kafka.SimpleKafkaProducer
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test

@Disabled
class Sandbox {
    final Kafka KAFKA = new Kafka(bootstrapServers: '127.0.0.1:19092', schemaRegistryUrl: 'http://0.0.0.0:28081')

    @Test
    void setupKafka() {
        def topics = [
                "io.openlineage.flink.kafka.input",
                "io.openlineage.flink.kafka.output"
        ]
        topics.each {topic -> KafkaUtils.recreateTopic(KAFKA, topic, 1, 1, 1) }
    }

    @Test
    void listTopicNames() {
        KafkaUtils.getTopicNames(KAFKA).findAll().each {
            println it
        }
    }

    @Test
    void listTopicsWithDescription() {
        KafkaUtils.describeTopics(KAFKA, KafkaUtils.getTopicNames(KAFKA, true)).findAll().each {
            println it
        }
    }

    @Test
    void listSchemaSubjects() {
        SchemaUtils.getSubjects(KAFKA).each {
            println it
        }
    }

    @Test
    void listSchemas() {
        SchemaUtils.getSubjects(KAFKA).each { subject ->
            SchemaUtils.getSchemaMetadata(KAFKA, subject).each { metadata ->
                println "subject: $subject, version: $metadata.version, id: $metadata.id, schema, $metadata.schema"
            }
        }
    }

    @Test
    void deleteAllSchemas() {
        SchemaUtils.getSubjects(KAFKA).each { subject ->
            SchemaUtils.deleteSubject(KAFKA, subject)
        }
    }

    @Test
    void registerSchema() {
        SchemaUtils.registerSubjectSchema(KAFKA, 'io.openlineage.flink.avro.event.InputEvent', InputEvent.SCHEMA$)
    }

    @Test
    void sendEventToKafka() {
        new SimpleKafkaProducer(KAFKA).withCloseable { producer ->
            10.times { number ->
                String id = 'eventId'
                producer.produce('io.openlineage.flink.kafka.input',
                        InputEvent.newBuilder().setId(id).setVersion(number).build(), id)
            }
        }
    }

    @Test
    void readEventFromKafka() {
        new SimpleKafkaConsumer<OutputEvent>(KAFKA)
                .withCloseable { consumer ->
                    consumer.consume('io.openlineage.flink.kafka.output', 3, 1)
        }
    }
}
