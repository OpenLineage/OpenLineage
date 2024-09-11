/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openlineage.flink.testutils;

import java.util.Map;
import java.util.Properties;

/**
 * Abstract class providing a Kafka test environment.
 *
 * <p>This code has been copied from flink-kafka-connector
 * https://github.com/apache/flink-connector-kafka/tree/main/flink-connector-kafka/src/test/java/org/apache/flink/connector/kafka/testutils
 */
@SuppressWarnings("PMD")
public abstract class KafkaTestEnvironment {
  /** Configuration class for {@link KafkaTestEnvironment}. */
  public static class Config {

    private int numKafkaClusters = 1;
    private int kafkaServersNumber = 1;
    private Properties kafkaServerProperties;
    private boolean secureMode;

    /** Please use {@link KafkaTestEnvironment#createConfig()} method. */
    private Config() {}

    public int getKafkaServersNumber() {
      return kafkaServersNumber;
    }

    public Config setKafkaServersNumber(int kafkaServersNumber) {
      this.kafkaServersNumber = kafkaServersNumber;
      return this;
    }

    public Properties getKafkaServerProperties() {
      return kafkaServerProperties;
    }

    public Config setKafkaServerProperties(Properties kafkaServerProperties) {
      this.kafkaServerProperties = kafkaServerProperties;
      return this;
    }

    public boolean isSecureMode() {
      return secureMode;
    }

    public Config setSecureMode(boolean secureMode) {
      this.secureMode = secureMode;
      return this;
    }

    public Config setHideKafkaBehindProxy(boolean hideKafkaBehindProxy) {
      return this;
    }
  }

  protected static final String KAFKA_HOST = "localhost";

  public static Config createConfig() {
    return new Config();
  }

  public abstract void prepare(Config config) throws Exception;

  public void shutdown() throws Exception {}

  public abstract void deleteTestTopic(String topic);

  public abstract void createTestTopic(
      String topic, int numberOfPartitions, int replicationFactor, Properties properties);

  public void createTestTopic(String topic, int numberOfPartitions, int replicationFactor) {
    this.createTestTopic(topic, numberOfPartitions, replicationFactor, new Properties());
  }

  public abstract Properties getStandardProperties();

  public abstract Properties getSecureProperties();

  public abstract String getBrokerConnectionString();

  public abstract String getVersion();

  public Properties getIdempotentProducerConfig() {
    Properties props = new Properties();
    props.put("enable.idempotence", "true");
    props.put("acks", "all");
    props.put("retries", "3");
    return props;
  }

  @SuppressWarnings("PMD")
  protected void maybePrintDanglingThreadStacktrace(String threadNameKeyword) {
    for (Map.Entry<Thread, StackTraceElement[]> threadEntry :
        Thread.getAllStackTraces().entrySet()) {
      if (threadEntry.getKey().getName().contains(threadNameKeyword)) {
        System.out.println("Dangling thread found:");
        for (StackTraceElement ste : threadEntry.getValue()) {
          System.out.println(ste);
        }
      }
    }
  }
}
