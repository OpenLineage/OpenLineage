/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.flink.utils;

import io.openlineage.client.utils.DatasetIdentifier;
import java.util.Properties;

public class KafkaUtils {

  private static final String KAFKA_DATASET_PREFIX = "kafka://";
  private static final String COMMA = ",";
  private static final String SEMICOLON = ";";

  /**
   * Generate dataset identifier based on bootstrap servers in properties and topic name
   *
   * @param properties
   * @param topic
   * @return
   */
  public static DatasetIdentifier datasetIdentifierOf(Properties properties, String topic) {
    String bootstrapServers = properties.getProperty("bootstrap.servers");

    if (bootstrapServers.contains(COMMA)) {
      bootstrapServers = bootstrapServers.split(COMMA)[0];
    } else if (bootstrapServers.contains(SEMICOLON)) {
      bootstrapServers = bootstrapServers.split(SEMICOLON)[0];
    }

    return new DatasetIdentifier(topic, String.format(KAFKA_DATASET_PREFIX + bootstrapServers));
  }
}
