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

import java.net.URL;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.connector.testframe.external.ExternalContextFactory;
import org.testcontainers.containers.KafkaContainer;

/**
 * Factory of {@link KafkaSourceExternalContext}.
 *
 * <p>This code has been copied from flink-kafka-connector
 * https://github.com/apache/flink-connector-kafka/tree/main/flink-connector-kafka/src/test/java/org/apache/flink/connector/kafka/testutils
 */
public class KafkaSourceExternalContextFactory
    implements ExternalContextFactory<KafkaSourceExternalContext> {

  private final KafkaContainer kafkaContainer;
  private final List<URL> connectorJars;
  private final KafkaSourceExternalContext.SplitMappingMode splitMappingMode;

  public KafkaSourceExternalContextFactory(
      KafkaContainer kafkaContainer,
      List<URL> connectorJars,
      KafkaSourceExternalContext.SplitMappingMode splitMappingMode) {
    this.kafkaContainer = kafkaContainer;
    this.connectorJars = connectorJars;
    this.splitMappingMode = splitMappingMode;
  }

  protected String getBootstrapServer() {
    final String internalEndpoints =
        kafkaContainer.getNetworkAliases().stream()
            .map(host -> String.join(":", host, Integer.toString(9092)))
            .collect(Collectors.joining(","));
    return String.join(",", kafkaContainer.getBootstrapServers(), internalEndpoints);
  }

  @Override
  public KafkaSourceExternalContext createExternalContext(String testName) {
    return new KafkaSourceExternalContext(
        getBootstrapServer(), this.splitMappingMode, this.connectorJars);
  }
}
