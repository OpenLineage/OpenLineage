/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.gravitino.GravitinoInfo;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GravitinoUtils {
  private static final Logger log = LoggerFactory.getLogger(GravitinoUtils.class);

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

    String metalake =
        gravitinoInfo
            .getMetalake()
            .orElseThrow(
                () -> new IllegalArgumentException("Metalake is required in GravitinoInfo"));
    String uri =
        gravitinoInfo
            .getUri()
            .orElseThrow(() -> new IllegalArgumentException("URI is required in GravitinoInfo"));

    String namespace = uri + "/api/metalakes/" + metalake;

    log.debug(
        "Generated Gravitino dataset identifier: namespace={}, name={}", namespace, datasetName);
    return new DatasetIdentifier(datasetName, namespace);
  }
}
