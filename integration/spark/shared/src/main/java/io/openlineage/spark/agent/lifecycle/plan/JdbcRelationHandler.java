/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.JdbcUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.sql.ColumnMeta;
import io.openlineage.sql.DbTableMeta;
import io.openlineage.sql.ExtractionError;
import io.openlineage.sql.SqlMeta;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

@Slf4j
public class JdbcRelationHandler<D extends OpenLineage.Dataset> {

  public List<D> handleJdbcRelation(LogicalRelation x, DatasetFactory<D> datasetFactory) {
    // strip the jdbc: prefix from the url. this leaves us with a url like
    // postgresql://<hostname>:<port>/<database_name>?params
    // we don't parse the URI here because different drivers use different
    // connection
    // formats that aren't always amenable to how Java parses URIs. E.g., the oracle
    // driver format looks like oracle:<drivertype>:<user>/<password>@<database>
    // whereas postgres, mysql, and sqlserver use the scheme://hostname:port/db
    // format.
    JDBCRelation relation = (JDBCRelation) x.relation();
    String url = JdbcUtils.sanitizeJdbcUrl(relation.jdbcOptions().url());
    SqlMeta sqlMeta = JdbcUtils.extractQueryFromSpark(relation).get();
    if (!sqlMeta.errors().isEmpty()) { // error return nothing
      log.error(
          String.format(
              "error while parsing query: %s",
              sqlMeta.errors().stream()
                  .map(ExtractionError::toString)
                  .collect(Collectors.joining(","))));
    } else if (sqlMeta.inTables().isEmpty()) {
      log.error("no tables defined in query, this should not happen");
    } else if (sqlMeta.columnLineage().isEmpty()) {
      return Collections.singletonList(
          datasetFactory.getDataset(
              sqlMeta.inTables().get(0).qualifiedName(), url, relation.schema()));
    } else {
      return sqlMeta.inTables().stream()
          .map(
              dbtm ->
                  datasetFactory.getDataset(
                      dbtm.qualifiedName(),
                      url,
                      generateJDBCSchema(dbtm, relation.schema(), sqlMeta)))
          .collect(Collectors.toList());
    }
    return Collections.emptyList();
  }

  private static StructType generateJDBCSchema(
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
