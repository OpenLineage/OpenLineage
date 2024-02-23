/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.flink.utils;

import io.openlineage.client.utils.DatasetIdentifier;
import java.util.Properties;

public class KafkaUtils {

  private static final String KAFKA_DATASET_PREFIX = "kafka://";

  /**
   * Generate dataset identifier based on bootstrap servers in properties and topic name
   *
   * @param properties
   * @param topic
   * @return
   */
  public static DatasetIdentifier datasetIdentifierOf(Properties properties, String topic) {
    String bootstrapServers = properties.getProperty("bootstrap.servers");

    return new DatasetIdentifier(topic, String.format(KAFKA_DATASET_PREFIX + bootstrapServers));
  }
}
