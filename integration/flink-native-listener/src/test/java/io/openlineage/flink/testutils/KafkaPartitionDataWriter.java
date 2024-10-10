/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openlineage.flink.testutils;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import org.apache.flink.connector.testframe.external.ExternalSystemSplitDataWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * Source split data writer for writing test data into Kafka topic partitions.
 *
 * <p>This code has been copied from flink-kafka-connector
 * https://github.com/apache/flink-connector-kafka/tree/main/flink-connector-kafka/src/test/java/org/apache/flink/connector/kafka/testutils
 */
public class KafkaPartitionDataWriter implements ExternalSystemSplitDataWriter<String> {

  private final KafkaProducer<byte[], byte[]> kafkaProducer;
  private final TopicPartition topicPartition;

  public KafkaPartitionDataWriter(Properties producerProperties, TopicPartition topicPartition) {
    this.kafkaProducer = new KafkaProducer<>(producerProperties);
    this.topicPartition = topicPartition;
  }

  @Override
  public void writeRecords(List<String> records) {
    for (String record : records) {
      ProducerRecord<byte[], byte[]> producerRecord =
          new ProducerRecord<>(
              topicPartition.topic(),
              topicPartition.partition(),
              null,
              record.getBytes(StandardCharsets.UTF_8));
      kafkaProducer.send(producerRecord);
    }
    kafkaProducer.flush();
  }

  @Override
  public void close() {
    kafkaProducer.close();
  }

  public TopicPartition getTopicPartition() {
    return topicPartition;
  }
}
