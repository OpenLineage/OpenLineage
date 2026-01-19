/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.gravitino.GravitinoInfo;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GravitinoUtils {
  private static final Logger log = LoggerFactory.getLogger(GravitinoUtils.class);

  /**
   * Gets the Gravitino namespace if available.
   *
   * @param gravitinoInfo the Gravitino information containing URI and metalake
   * @return Optional Gravitino namespace, or empty if unavailable
   */
  public static Optional<String> getGravitinoNamespace(GravitinoInfo gravitinoInfo) {
    try {
      Optional<String> gravitinoUri = gravitinoInfo.getUri();
      Optional<String> metalake = gravitinoInfo.getMetalake();

      if (gravitinoUri.isPresent() && metalake.isPresent()) {
        return Optional.of(gravitinoUri.get() + "/api/metalakes/" + metalake.get());
      }
    } catch (Exception e) {
      // Fallback gracefully if Gravitino info is not available
      log.debug("Failed to get Gravitino namespace", e);
    }

    return Optional.empty();
  }

  // For datasource v2
  public static DatasetIdentifier getGravitinoDatasetIdentifier(
      GravitinoInfo gravitinoInfo,
      String catalogName,
      String[] defaultNameSpace,
      Identifier identifier) {
    String[] gravitinoNameSpace = identifier.namespace();
    if (gravitinoNameSpace == null || gravitinoNameSpace.length == 0) {
      gravitinoNameSpace = defaultNameSpace;
    }
    return getGravitinoDatasetIdentifier(
        gravitinoInfo, catalogName, gravitinoNameSpace, identifier.name());
  }

  private static DatasetIdentifier getGravitinoDatasetIdentifier(
      GravitinoInfo gravitinoInfo, String catalogName, String[] nameSpace, String name) {
    String datasetName =
        Stream.concat(
                Stream.concat(Stream.of(catalogName), Arrays.stream(nameSpace)), Stream.of(name))
            .collect(Collectors.joining("."));

    String namespace =
        getGravitinoNamespace(gravitinoInfo)
            .orElseThrow(() -> new IllegalArgumentException("Gravitino namespace is required"));

    log.debug(
        "Generated Gravitino dataset identifier: namespace={}, name={}", namespace, datasetName);
    return new DatasetIdentifier(datasetName, namespace);
  }
}
