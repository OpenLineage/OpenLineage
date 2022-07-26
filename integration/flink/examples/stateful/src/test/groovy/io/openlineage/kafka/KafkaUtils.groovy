/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.kafka

import com.google.common.primitives.Shorts
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.ListTopicsOptions
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.admin.TopicDescription
import org.apache.kafka.common.config.ConfigResource
import org.awaitility.Awaitility

import static java.util.concurrent.TimeUnit.SECONDS
import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC

class KafkaUtils {

    static AdminClient client(Kafka kafka) {
        AdminClient.create(['bootstrap.servers': kafka.bootstrapServers])
    }

    static void deleteTopic(Kafka kafka, String topic) {
        client(kafka).withCloseable { client ->
            if (topicExists(client, topic)) {
                client.deleteTopics([topic]).topicNameValues().get(topic).get()
                Awaitility.await()
                        .atMost(10, SECONDS)
                        .pollDelay(0, SECONDS)
                        .until { !topicExists(client, topic)}
            }
        }
    }

    static void createTopic(Kafka kafka, String topic, int numPartitions, int replicationFactor, int minInSyncReplicas) {
        client(kafka).withCloseable { client ->
            NewTopic newTopic = new NewTopic(topic, numPartitions, Shorts.checkedCast(replicationFactor))
                    .configs(['min.insync.replicas': minInSyncReplicas.toString()])
            client.createTopics([newTopic]).values().get(topic).get()
        }
    }

    static void recreateTopic(Kafka kafka, String topic, int numPartitions, int replicationFactor, int minInSyncReplicas) {
        deleteTopic(kafka, topic)
        createTopic(kafka, topic, numPartitions, replicationFactor, minInSyncReplicas)
    }

    static Set<String> getTopicNames(Kafka kafka, boolean includeInternal = false) {
        client(kafka).withCloseable { client ->
            def listTopicOptions = new ListTopicsOptions()
            listTopicOptions.listInternal(true)
            def topics = client.listTopics(listTopicOptions).names().get()
            if (!includeInternal) {
                topics = topics.findAll({!it.startsWith('_')})
            }
            return topics.sort()
        }
    }

    static Map<String, TopicDescription> describeTopics(Kafka kafka, Set<String> topics) {
        client(kafka).withCloseable { client ->
            return client.describeTopics(topics).all().get()
        }
    }

    private static boolean topicExists(AdminClient client, String topic) {
        client.listTopics().names().get().contains(topic)
    }
}
