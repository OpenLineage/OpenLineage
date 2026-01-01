/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan.handlers;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.JdbcSparkUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.sql.SqlMeta;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;

@Slf4j
public class JdbcRelationHandler<D extends OpenLineage.Dataset> {

  private final DatasetFactory<D> datasetFactory;

  public JdbcRelationHandler(DatasetFactory<D> datasetFactory) {
    this.datasetFactory = datasetFactory;
  }

  public List<D> handleRelation(LogicalRelation x) {
    // strip the jdbc: prefix from the url. this leaves us with a url like
    // postgresql://<hostname>:<port>/<database_name>?params
    // we don't parse the URI here because different drivers use different
    // connection
    // formats that aren't always amenable to how Java parses URIs. E.g., the oracle
    // driver format looks like oracle:<drivertype>:<user>/<password>@<database>
    // whereas postgres, mysql, and sqlserver use the scheme://hostname:port/db
    // format.
    return getDatasets((JDBCRelation) x.relation());
  }

  public List<D> getDatasets(JDBCRelation relation) {
    Optional<SqlMeta> sqlMeta = JdbcSparkUtils.extractQueryFromSpark(relation);
    if (!sqlMeta.isPresent()) {
      return Collections.emptyList();
    }
    return JdbcSparkUtils.getDatasets(datasetFactory, sqlMeta.get(), relation);
  }
}
