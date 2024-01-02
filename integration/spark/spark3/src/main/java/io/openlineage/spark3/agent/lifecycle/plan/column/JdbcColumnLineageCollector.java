/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.util.JdbcUtils;
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
      LogicalPlan node,
      ColumnLevelLineageBuilder builder,
      List<DatasetIdentifier> datasetIdentifiers) {
    extractExternalInputs(
        (JDBCRelation) ((LogicalRelation) node).relation(), builder, datasetIdentifiers);
  }

  public static void extractExternalInputs(
      JDBCRelation relation,
      ColumnLevelLineageBuilder builder,
      List<DatasetIdentifier> datasetIdentifiers) {
    Optional<SqlMeta> sqlMeta = JdbcUtils.extractQueryFromSpark(relation);
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
                                  && JdbcUtils.getDatasetIdentifierFromJdbcUrl(
                                          jdbcUrl, cm.origin().get().name())
                                      .getName()
                                      .equals(di.getName()))
                      .forEach(cm -> builder.addInput(builder.getMapping(cm), di, cm.name())));
        });
  }

  public static void extractExpressionsFromJDBC(
      LogicalPlan node, ColumnLevelLineageBuilder builder) {
    extractExpressionsFromJDBC(
        (JDBCRelation) ((LogicalRelation) node).relation(),
        builder,
        ScalaConversionUtils.fromSeq(node.output()));
  }

  public static void extractExpressionsFromJDBC(
      JDBCRelation relation, ColumnLevelLineageBuilder builder, List<Attribute> output) {
    Optional<SqlMeta> sqlMeta = JdbcUtils.extractQueryFromSpark(relation);
    sqlMeta.ifPresent(
        meta ->
            meta.columnLineage()
                .forEach(
                    p -> {
                      ExprId descendantId = getDescendantId(output, p.descendant());
                      builder.addExternalMapping(p.descendant(), descendantId);

                      p.lineage()
                          .forEach(e -> builder.addExternalMapping(e, NamedExpression.newExprId()));
                      if (!p.lineage().isEmpty()) {
                        p.lineage().stream()
                            .map(builder::getMapping)
                            .forEach(eid -> builder.addDependency(descendantId, eid));
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
