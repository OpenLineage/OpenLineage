/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class TransportFactoryTest {

  @Test
  void createsConsoleTransport() {
    TransportConfig config = new ConsoleConfig();
    TransportFactory transportFactory = new TransportFactory(config);

    assertThat(transportFactory.build()).isInstanceOf(ConsoleTransport.class);
  }

  @Test
  void createsHttpTransport() {
    HttpConfig config = new HttpConfig();
    config.setUrl(URI.create("http://localhost:1500"));
    TransportFactory transportFactory = new TransportFactory(config);

    assertThat(transportFactory.build()).isInstanceOf(HttpTransport.class);
  }

  @Test
  void createsKafkaTransport() {
    KafkaConfig config = new KafkaConfig();
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty(
        "key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.setProperty(
        "value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    config.setProperties(properties);
    config.setTopicName("test-topic");
    TransportFactory transportFactory = new TransportFactory(config);

    assertThat(transportFactory.build()).isInstanceOf(KafkaTransport.class);
  }
}
