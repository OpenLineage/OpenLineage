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
import shaded.io.openlineage.sql.ColumnLineage;
import shaded.io.openlineage.sql.ColumnMeta;
import shaded.io.openlineage.sql.DbTableMeta;
import shaded.io.openlineage.sql.SqlMeta;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
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

  public JdbcColumnLineageVisitorDelegate(
      ColumnLevelLineageContext context, JDBCRelation relation, List<Attribute> attributes) {
    this.context = context;
    this.attributes = attributes;
    jdbcUrl = relation.jdbcOptions().url();
    sqlMeta = JdbcSparkUtils.extractQueryFromSpark(relation).orElse(null);
    jdbcOptions = relation.jdbcOptions();
    datasetIdentifiers = extractDatasetIdentifier(context, relation);
  }

  public boolean isDefinedAt() {
    return sqlMeta != null;
  }

  public void collectInputs() {
    extractInputsFromSimpleWildcardSelect();
    List<ColumnLineage> columnLineages = sqlMeta.columnLineage();
    Set<ColumnMeta> inputs =
        columnLineages.stream().flatMap(cl -> cl.lineage().stream()).collect(Collectors.toSet());
    columnLineages.forEach(cl -> inputs.remove(cl.descendant()));
    datasetIdentifiers.forEach(
        di ->
            inputs.stream()
                .filter(
                    cm ->
                        cm.origin().isPresent()
                            && context
                                .getNamespaceResolver()
                                .resolve(
                                    JdbcDatasetUtils.getDatasetIdentifier(
                                        jdbcUrl,
                                        cm.origin().get().name(),
                                        jdbcOptions.asConnectionProperties()))
                                .getName()
                                .equals(di.getName()))
                .forEach(
                    cm ->
                        context
                            .getBuilder()
                            .addInput(context.getBuilder().getMapping(cm), di, cm.name())));
  }

  public void collectExpressionDependencies() {
    sqlMeta
        .columnLineage()
        .forEach(
            p -> {
              ExprId descendantId = getDescendantId(attributes, p.descendant());
              context.getBuilder().addExternalMapping(p.descendant(), descendantId);

              p.lineage()
                  .forEach(
                      e -> context.getBuilder().addExternalMapping(e, NamedExpression.newExprId()));
              if (!p.lineage().isEmpty()) {
                p.lineage().stream()
                    .map(context.getBuilder()::getMapping)
                    .forEach(eid -> context.getBuilder().addDependency(descendantId, eid));
              }
            });
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

  private ExprId getDescendantId(List<Attribute> output, ColumnMeta column) {
    return output.stream()
        .filter(e -> e.name().equals(column.name()))
        .map(NamedExpression::exprId)
        .findFirst()
        .orElseGet(NamedExpression::newExprId);
  }
}
