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
    Optional<SqlMeta> sqlMeta = OpenLineageSql.parse(Collections.singletonList(sql), dialect);
    if (sqlMeta.isPresent()) {
      return getDatasets(datasetFactory, sqlMeta.get(), namespace);
    }
    return Collections.emptyList();
  }

  public static <D extends OpenLineage.Dataset> List<D> getDatasets(
      DatasetFactory<D> datasetFactory, SqlMeta meta, String namespace) {
    return meta.inTables().stream()
        .map(
            dbtm -> {
              return datasetFactory.getDataset(
                  new DatasetIdentifier(dbtm.qualifiedName(), namespace),
                  new OpenLineage.DatasetFacetsBuilder());
            })
        .collect(Collectors.toList());
  }
}
