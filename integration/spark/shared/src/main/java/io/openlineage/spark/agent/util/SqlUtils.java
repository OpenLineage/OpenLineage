/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.sql.OpenLineageSql;
import io.openlineage.sql.SqlMeta;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SqlUtils {
  public static <D extends OpenLineage.Dataset> List<D> getDatasets(
      DatasetFactory<D> datasetFactory, String sql, String dialect, String namespace) {
    return getDatasets(datasetFactory, sql, dialect, namespace, null, null);
  }

  public static <D extends OpenLineage.Dataset> List<D> getDatasets(
      DatasetFactory<D> datasetFactory,
      String sql,
      String dialect,
      String namespace,
      String defaultDatabase,
      String defaultSchema) {
    Optional<SqlMeta> sqlMeta = OpenLineageSql.parse(Collections.singletonList(sql), dialect);
    return sqlMeta
        .map(
            meta ->
                meta.inTables().stream()
                    .map(
                        dbtm -> {
                          return datasetFactory.getDataset(
                              new DatasetIdentifier(
                                  getName(defaultDatabase, defaultSchema, dbtm.qualifiedName()),
                                  namespace),
                              new OpenLineage.DatasetFacetsBuilder());
                        })
                    .collect(Collectors.toList()))
        .orElse(Collections.emptyList());
  }

  private static String getName(String defaultDatabase, String defaultSchema, String parsedName) {
    // database and schema from parser have priority over default ones
    String[] parts = parsedName.split("\\.");
    if (parts.length == 2) {
      return String.format("%s.%s.%s", defaultDatabase, parts[0], parts[1]);
    } else if (parts.length == 1) {
      return String.format("%s.%s.%s", defaultDatabase, defaultSchema, parts[0]);
    }
    return parsedName;
  }
}
