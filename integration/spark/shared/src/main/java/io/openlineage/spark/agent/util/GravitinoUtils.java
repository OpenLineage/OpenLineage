/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.gravitino.GravitinoInfoProviderImpl;
import java.net.URI;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.connector.catalog.Identifier;

@Slf4j
public class GravitinoUtils {
  private static final String DEFAULT_CATALOG_NAME = "spark_catalog";
  private static final String DEFAULT_DATABASE_NAME = "default";

  // For datasource v1, like parquet
  public static DatasetIdentifier getGravitinoDatasetIdentifier(URI uri) {
    GravitinoInfoProviderImpl provider = GravitinoInfoProviderImpl.getInstance();
    String metalake = provider.getMetalakeName();
    log.debug(
        "Creating Gravitino dataset identifier from URI: {} with metalake: {}", uri, metalake);
    return new DatasetIdentifier(uri.toString(), metalake);
  }

  // For createTableLike and hive catalogTable
  public static DatasetIdentifier getGravitinoDatasetIdentifier(TableIdentifier identifier) {
    GravitinoInfoProviderImpl provider = GravitinoInfoProviderImpl.getInstance();
    String metalake = provider.getMetalakeName();
    String catalogName = provider.getGravitinoCatalog(DEFAULT_CATALOG_NAME);
    String[] database = {DEFAULT_DATABASE_NAME};
    if (!identifier.database().isEmpty()) {
      database = new String[] {identifier.database().get()};
    }
    log.debug(
        "Creating Gravitino dataset identifier from TableIdentifier: {}.{} with catalog: {}, metalake: {}",
        database[0],
        identifier.table(),
        catalogName,
        metalake);
    return getGravitinoDatasetIdentifier(metalake, catalogName, database, identifier.table());
  }

  // For datasource v2
  public static DatasetIdentifier getGravitinoDatasetIdentifier(
      String metalake, String catalogName, String[] defaultNameSpace, Identifier identifier) {
    String[] gravitinoNameSpace = identifier.namespace();
    if (gravitinoNameSpace == null || gravitinoNameSpace.length == 0) {
      gravitinoNameSpace = defaultNameSpace;
    }
    return getGravitinoDatasetIdentifier(
        metalake, catalogName, gravitinoNameSpace, identifier.name());
  }

  private static DatasetIdentifier getGravitinoDatasetIdentifier(
      String metalake, String catalogName, String[] nameSpace, String name) {
    String datasetName =
        Stream.concat(
                Stream.concat(Stream.of(catalogName), Arrays.stream(nameSpace)), Stream.of(name))
            .collect(Collectors.joining("."));
    log.debug(
        "Generated Gravitino dataset identifier: namespace={}, name={}", metalake, datasetName);
    return new DatasetIdentifier(datasetName, metalake);
  }
}
