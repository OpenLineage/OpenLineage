/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.sql.ColumnMeta;
import io.openlineage.sql.DbTableMeta;
import io.openlineage.sql.OpenLineageSql;
import io.openlineage.sql.SqlMeta;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

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
                              datasetFactory.createCompositeFacetBuilder());
                        })
                    .collect(Collectors.toList()))
        .orElse(Collections.emptyList());
  }

  public static <D extends OpenLineage.Dataset> List<D> getDatasets(
      DatasetFactory<D> datasetFactory,
      String sql,
      String dialect,
      String namespace,
      String database,
      String databaseSchema,
      StructType tableSchema) {
    Optional<SqlMeta> sqlMeta = OpenLineageSql.parse(Collections.singletonList(sql), dialect);
    if (!sqlMeta.isPresent()) {
      return Collections.emptyList();
    }

    return createDatasets(
        datasetFactory,
        sqlMeta.get(),
        tableSchema,
        dbtm ->
            new DatasetIdentifier(
                getName(database, databaseSchema, dbtm.qualifiedName()), namespace));
  }

  @SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
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

  public static <D extends OpenLineage.Dataset> List<D> createDatasets(
      DatasetFactory<D> datasetFactory,
      SqlMeta meta,
      StructType schema,
      Function<DbTableMeta, DatasetIdentifier> datasetIdentifierFunction) {
    if (meta.columnLineage().isEmpty()) {
      int numberOfTables = meta.inTables().size();

      return meta.inTables().stream()
          .map(
              dbtm -> {
                DatasetIdentifier di = datasetIdentifierFunction.apply(dbtm);

                if (numberOfTables > 1) {
                  return datasetFactory.getDataset(di.getName(), di.getNamespace());
                }

                return datasetFactory.getDataset(di.getName(), di.getNamespace(), schema);
              })
          .collect(Collectors.toList());
    }
    return meta.inTables().stream()
        .map(
            dbtm -> {
              DatasetIdentifier di = datasetIdentifierFunction.apply(dbtm);
              return datasetFactory.getDataset(
                  di.getName(), di.getNamespace(), generateSchemaFromSqlMeta(dbtm, schema, meta));
            })
        .collect(Collectors.toList());
  }

  public static StructType generateSchemaFromSqlMeta(
      DbTableMeta origin, StructType schema, SqlMeta sqlMeta) {
    StructType originSchema = new StructType();
    for (StructField f : schema.fields()) {
      List<ColumnMeta> fields =
          sqlMeta.columnLineage().stream()
              .filter(cl -> cl.descendant().name().equals(f.name()))
              .flatMap(
                  cl ->
                      cl.lineage().stream()
                          .filter(
                              cm -> cm.origin().isPresent() && cm.origin().get().equals(origin)))
              .collect(Collectors.toList());
      for (ColumnMeta cm : fields) {
        originSchema = originSchema.add(cm.name(), f.dataType());
      }
    }
    return originSchema;
  }
}
