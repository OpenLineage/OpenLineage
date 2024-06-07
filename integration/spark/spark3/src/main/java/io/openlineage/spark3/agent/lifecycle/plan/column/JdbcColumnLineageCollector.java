/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.JdbcUtils;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import io.openlineage.spark.agent.util.JdbcSparkUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.sql.ColumnLineage;
import io.openlineage.sql.ColumnMeta;
import io.openlineage.sql.SqlMeta;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;

@Slf4j
public class JdbcColumnLineageCollector {

  public static void extractExternalInputs(
      ColumnLevelLineageContext context,
      LogicalPlan node,
      List<DatasetIdentifier> datasetIdentifiers) {
    extractExternalInputs(
        context, (JDBCRelation) ((LogicalRelation) node).relation(), datasetIdentifiers);
  }

  public static void extractExternalInputs(
      ColumnLevelLineageContext context,
      JDBCRelation relation,
      List<DatasetIdentifier> datasetIdentifiers) {
    Optional<SqlMeta> sqlMeta = JdbcSparkUtils.extractQueryFromSpark(relation);
    String jdbcUrl = relation.jdbcOptions().url();
    sqlMeta.ifPresent(
        meta -> {
          List<ColumnLineage> columnLineages = meta.columnLineage();
          Set<ColumnMeta> inputs =
              columnLineages.stream()
                  .flatMap(cl -> cl.lineage().stream())
                  .collect(Collectors.toSet());
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
                                          JdbcUtils.getDatasetIdentifierFromJdbcUrl(
                                              jdbcUrl, cm.origin().get().name()))
                                      .getName()
                                      .equals(di.getName()))
                      .forEach(
                          cm ->
                              context
                                  .getBuilder()
                                  .addInput(context.getBuilder().getMapping(cm), di, cm.name())));
        });
  }

  public static void extractExpressionsFromJDBC(
      ColumnLevelLineageContext context, LogicalPlan node) {
    extractExpressionsFromJDBC(
        context,
        (JDBCRelation) ((LogicalRelation) node).relation(),
        ScalaConversionUtils.fromSeq(node.output()));
  }

  public static void extractExpressionsFromJDBC(
      ColumnLevelLineageContext context, JDBCRelation relation, List<Attribute> output) {
    Optional<SqlMeta> sqlMeta = JdbcSparkUtils.extractQueryFromSpark(relation);
    sqlMeta.ifPresent(
        meta ->
            meta.columnLineage()
                .forEach(
                    p -> {
                      ExprId descendantId = getDescendantId(output, p.descendant());
                      context.getBuilder().addExternalMapping(p.descendant(), descendantId);

                      p.lineage()
                          .forEach(
                              e ->
                                  context
                                      .getBuilder()
                                      .addExternalMapping(e, NamedExpression.newExprId()));
                      if (!p.lineage().isEmpty()) {
                        p.lineage().stream()
                            .map(context.getBuilder()::getMapping)
                            .forEach(eid -> context.getBuilder().addDependency(descendantId, eid));
                      }
                    }));
  }

  private static ExprId getDescendantId(List<Attribute> output, ColumnMeta column) {
    return output.stream()
        .filter(e -> e.name().equals(column.name()))
        .map(NamedExpression::exprId)
        .findFirst()
        .orElseGet(NamedExpression::newExprId);
  }
}
