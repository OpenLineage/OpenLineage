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

import java.time.Duration;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

/**
 * Collection of methods to interact with a Kafka cluster.
 *
 * <p>This code has been copied from flink-kafka-connector
 * https://github.com/apache/flink-connector-kafka/tree/main/flink-connector-kafka/src/test/java/org/apache/flink/connector/kafka/testutils
 */
public class KafkaUtil {
  private KafkaUtil() {}

  /**
   * This method helps to set commonly used Kafka configurations and aligns the internal Kafka log
   * levels with the ones used by the capturing logger.
   *
   * @param dockerImageVersion describing the Kafka image
   * @param logger to derive the log level from
   * @return configured Kafka container
   */
  public static KafkaContainer createKafkaContainer(String dockerImageVersion, Logger logger) {
    return createKafkaContainer(dockerImageVersion, logger, null);
  }

  /**
   * This method helps to set commonly used Kafka configurations and aligns the internal Kafka log
   * levels with the ones used by the capturing logger, and set the prefix of logger.
   */
  public static KafkaContainer createKafkaContainer(
      String dockerImageVersion, Logger logger, String loggerPrefix) {
    String logLevel;
    if (logger.isTraceEnabled()) {
      logLevel = "TRACE";
    } else if (logger.isDebugEnabled()) {
      logLevel = "DEBUG";
    } else if (logger.isInfoEnabled()) {
      logLevel = "INFO";
    } else if (logger.isWarnEnabled()) {
      logLevel = "WARN";
    } else if (logger.isErrorEnabled()) {
      logLevel = "ERROR";
    } else {
      logLevel = "OFF";
    }

    Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(logger);
    if (!StringUtils.isNullOrWhitespaceOnly(loggerPrefix)) {
      logConsumer.withPrefix(loggerPrefix);
    }
    return new KafkaContainer(DockerImageName.parse(dockerImageVersion))
        .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .withEnv("KAFKA_CONFLUENT_SUPPORT_METRICS_ENABLE", "false")
        .withEnv("KAFKA_LOG4J_ROOT_LOGLEVEL", logLevel)
        .withEnv("KAFKA_LOG4J_LOGGERS", "state.change.logger=" + logLevel)
        .withEnv("KAFKA_CONFLUENT_SUPPORT_METRICS_ENABLE", "false")
        .withEnv("KAFKA_TRANSACTION_MAX_TIMEOUT_MS", String.valueOf(Duration.ofHours(2).toMillis()))
        .withEnv("KAFKA_LOG4J_TOOLS_ROOT_LOGLEVEL", logLevel)
        .withLogConsumer(logConsumer);
  }
}
