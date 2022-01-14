/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openlineage.proxy.api.models;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * KafkaLineageStream is responsible for sending open lineage events to kafka. The collect() method
 * is called each time an open lineage event is emitted by the data platform.
 */
@Slf4j
public class KafkaLineageStream extends LineageStream {
  private final String topicName;
  private final String localServerId;
  private final KafkaProducer<String, String> producer;

  public KafkaLineageStream(@NonNull final KafkaConfig kafkaConfig) {
    super(Type.KAFKA);
    this.topicName = kafkaConfig.getTopicName();
    this.localServerId = kafkaConfig.getLocalServerId();
    this.producer = new KafkaProducer<>(kafkaConfig.getProperties());
  }

  @Override
  public void collect(@NonNull String eventAsString) {
    log.debug("Received lineage event: {}", eventAsString);

    final ProducerRecord<String, String> record =
        new ProducerRecord<>(topicName, localServerId, eventAsString);
    try {
      producer.send(record);
    } catch (Exception e) {
      log.error("Failed to collect lineage event: {}", eventAsString, e);
    }
  }
}
