/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.util;

import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.kafka.lineage.TypeDatasetFacet;
import org.apache.flink.streaming.api.lineage.LineageDataset;

@Slf4j
public class TypeDatasetFacetUtil {

  private static final String TYPE_FACET_NAME = "type";

  /**
   * Determines if {@link TypeDatasetFacet} class is available on the classpath.
   *
   * @return
   */
  public static boolean isOnClasspath() {
    try {
      Class.forName("org.apache.flink.connector.kafka.lineage.TypeDatasetFacet");
      return true;
    } catch (ClassNotFoundException e) {
      log.debug("TypeDatasetFacet class not present on classpath");
      return false;
    }
  }

  /**
   * Extracts kafka facet from {@link LineageDataset}
   *
   * @param dataset
   * @return
   */
  public static Optional<TypeDatasetFacet> getFacet(LineageDataset dataset) {
    return Optional.ofNullable(dataset.facets().get(TYPE_FACET_NAME))
        .map(f -> (TypeDatasetFacet) f);
  }
}
