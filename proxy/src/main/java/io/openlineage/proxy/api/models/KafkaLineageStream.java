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

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KafkaLineageStream is responsible for sending open lineage events to kafka. The collect() method
 * is called each time an open lineage event is emitted by the data platform.
 */
public class KafkaLineageStream extends LineageStream {
  private static final Logger log = LoggerFactory.getLogger(KafkaLineageStream.class);

  private final String localServerId;
  private final Properties producerProperties;
  private KafkaProducer<String, String> producer;
  private String topicName;

  public KafkaLineageStream(
      String localServerId, String topicName, String bootstrapServerURL, Properties properties) {
    super(Type.KAFKA);

    this.localServerId = localServerId;

    log.info("Kafka Properties: " + properties.toString());

    this.producerProperties = properties;
    this.producerProperties.put("bootstrap.servers", bootstrapServerURL);
    this.producerProperties.put("server.id", localServerId);

    this.producer = new KafkaProducer<>(producerProperties);
    this.topicName = topicName;
  }

  @Override
  public void collect(String event) {
    log.debug("Lineage Event: " + event);
    try {
      String eventString = event;

      log.debug("String Event: " + eventString);

      ProducerRecord<String, String> record =
          new ProducerRecord<>(topicName, localServerId, eventString);

      producer.send(record);
    } catch (Exception error) {
      log.error("Unable to send lineage event to kafka", error);
    }
  }
}
