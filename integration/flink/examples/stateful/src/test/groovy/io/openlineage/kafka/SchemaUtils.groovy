/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.kafka

import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.SchemaMetadata
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.avro.Schema

class SchemaUtils {

    static SchemaRegistryClient client(Kafka kafka) {
        new CachedSchemaRegistryClient(kafka.schemaRegistryUrl, 1000)
    }

    static Set<String> getSubjects(Kafka kafka) {
        client(kafka).getAllSubjects().sort()
    }

    static List<SchemaMetadata> getSchemaMetadata(Kafka kafka, String subject) {
        def client = client(kafka)
        client.getAllVersions(subject).collect { version ->
            client.getSchemaMetadata(subject, version)
        }
    }

    static void deleteSubject(Kafka kafka, String subject) {
        client(kafka).deleteSubject(subject)
        println "subject: $subject deleted."
    }

    static void registerSubjectSchema(Kafka kafka, String subject, Schema schema) {
        client(kafka).register(subject, new AvroSchema(schema))
        println "subject: $subject schema created."
    }
}
