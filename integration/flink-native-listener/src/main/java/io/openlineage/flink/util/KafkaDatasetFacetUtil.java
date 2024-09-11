/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.util;

import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.kafka.lineage.KafkaDatasetFacet;
import org.apache.flink.streaming.api.lineage.LineageDataset;

@Slf4j
public class KafkaDatasetFacetUtil {

  private static final String KAFKA_FACET_NAME = "kafka";

  /**
   * Determines if {@link KafkaDatasetFacet} class is available on the classpath.
   *
   * @return
   */
  public static boolean isOnClasspath() {
    try {
      Class.forName("org.apache.flink.connector.kafka.lineage.KafkaDatasetFacet");
      return true;
    } catch (ClassNotFoundException e) {
      log.debug("KafkaDatasetFacet class not present on classpath");
      return false;
    }
  }

  /**
   * Extracts kafka facet from {@link LineageDataset}
   *
   * @param dataset
   * @return
   */
  public static Optional<KafkaDatasetFacet> getFacet(LineageDataset dataset) {
    return Optional.ofNullable(dataset.facets().get(KAFKA_FACET_NAME))
        .map(f -> (KafkaDatasetFacet) f);
  }
}
