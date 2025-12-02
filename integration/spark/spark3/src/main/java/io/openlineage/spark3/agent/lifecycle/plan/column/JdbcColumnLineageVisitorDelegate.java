/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import static io.openlineage.spark3.agent.lifecycle.plan.column.InputFieldsCollector.extractDatasetIdentifier;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.jdbc.JdbcDatasetUtils;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import io.openlineage.spark.agent.util.JdbcSparkUtils;
import io.openlineage.spark.agent.util.SqlCollector;
import io.openlineage.sql.ColumnMeta;
import io.openlineage.sql.DbTableMeta;
import io.openlineage.sql.SqlMeta;
import java.util.List;
import java.util.Locale;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;

@Slf4j
public class JdbcColumnLineageVisitorDelegate {

  private final SqlMeta sqlMeta;
  private final String jdbcUrl;
  private final JDBCOptions jdbcOptions;
  private final ColumnLevelLineageContext context;
  private final List<DatasetIdentifier> datasetIdentifiers;
  private final List<Attribute> attributes;
  private final SqlCollector sqlCollector;

  public JdbcColumnLineageVisitorDelegate(
      ColumnLevelLineageContext context, JDBCRelation relation, List<Attribute> attributes) {
    this.context = context;
    this.attributes = attributes;
    jdbcUrl = relation.jdbcOptions().url();
    sqlMeta = JdbcSparkUtils.extractQueryFromSpark(relation).orElse(null);
    jdbcOptions = relation.jdbcOptions();
    datasetIdentifiers = extractDatasetIdentifier(context, relation);
    this.sqlCollector =
        new SqlCollector(this.context, this.sqlMeta, datasetIdentifiers, attributes);
  }

  public boolean isDefinedAt() {
    return sqlMeta != null;
  }

  public void collectInputs() {
    extractInputsFromSimpleWildcardSelect();
    sqlCollector.collectInputs(
        dbTableMeta ->
            JdbcDatasetUtils.getDatasetIdentifier(
                jdbcUrl, dbTableMeta.name(), jdbcOptions.asConnectionProperties()));
  }

  public void collectExpressionDependencies() {
    sqlCollector.collectExpressionDependencies();
  }

  private void extractInputsFromSimpleWildcardSelect() {
    if (sqlMeta.inTables().size() != 1 || !sqlMeta.columnLineage().isEmpty()) {
      return;
    }

    if (!JdbcSparkUtils.queryStringFromJdbcOptions(jdbcOptions)
        .toLowerCase(Locale.ROOT)
        .startsWith("select * from")) {
      return;
    }
    DbTableMeta table = sqlMeta.inTables().get(0);

    datasetIdentifiers.forEach(
        di ->
            attributes.stream()
                .filter(
                    sf ->
                        context
                            .getNamespaceResolver()
                            .resolve(
                                JdbcDatasetUtils.getDatasetIdentifier(
                                    jdbcUrl, table.name(), jdbcOptions.asConnectionProperties()))
                            .getName()
                            .equals(di.getName()))
                .forEach(
                    sf -> {
                      ColumnMeta cm = new ColumnMeta(table, sf.name());
                      context
                          .getBuilder()
                          .addInput(context.getBuilder().getMapping(cm), di, cm.name());
                    }));
  }
}
