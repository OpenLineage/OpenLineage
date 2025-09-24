/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark3.agent.lifecycle.plan.column;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.sql.ColumnLineage;
import io.openlineage.sql.ColumnMeta;
import io.openlineage.sql.OpenLineageSql;
import io.openlineage.sql.SqlMeta;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;

public class QueryRelationColumnLineageCollector {

  public static void extractExternalInputs(ColumnLevelLineageContext context, LogicalPlan plan) {
    if (plan instanceof DataSourceV2ScanRelation) {
      extractExternalInputs(context, ((DataSourceV2ScanRelation) plan).relation());
    }
    if (plan instanceof DataSourceV2Relation) {
      extractExternalInputs(context, (DataSourceV2Relation) plan);
    }
  }

  public static void extractExternalInputs(
      ColumnLevelLineageContext context, DataSourceV2Relation relation) {
    String namespace = relation.table().properties().get("openlineage.dataset.namespace");
    String query = relation.table().properties().get("openlineage.dataset.query");
    Optional<SqlMeta> optionalSqlMeta =
        OpenLineageSql.parse(Collections.singletonList(query), namespace);
    optionalSqlMeta.ifPresent(
        sqm -> {
          List<ColumnLineage> columnLineages = sqm.columnLineage();
          Set<ColumnMeta> inputs =
              columnLineages.stream()
                  .flatMap(cl -> cl.lineage().stream())
                  .collect(Collectors.toSet());
          columnLineages.forEach(cl -> inputs.remove(cl.descendant()));
          List<DatasetIdentifier> dis =
              sqm.inTables().stream()
                  .map(dbtm -> new DatasetIdentifier(dbtm.qualifiedName(), namespace))
                  .collect(Collectors.toList());
          dis.forEach(
              di ->
                  inputs.stream()
                      .filter(
                          cm ->
                              cm.origin()
                                  .map(origin -> origin.qualifiedName().equals(di.getName()))
                                  .orElse(false))
                      .forEach(
                          cm ->
                              context
                                  .getBuilder()
                                  .addInput(context.getBuilder().getMapping(cm), di, cm.name())));
        });
  }

  public static void extractExpressionsFromQuery(
      ColumnLevelLineageBuilder builder, LogicalPlan node) {
    extractExpressionsFromQuery(
        builder, (DataSourceV2Relation) node, ScalaConversionUtils.fromSeq(node.output()));
  }

  public static void extractExpressionsFromQuery(
      ColumnLevelLineageBuilder builder, DataSourceV2Relation relation, List<Attribute> output) {
    String namespace = relation.table().properties().get("openlineage.dataset.namespace");
    String query = relation.table().properties().get("openlineage.dataset.query");
    Optional<SqlMeta> optionalSqlMeta =
        OpenLineageSql.parse(Collections.singletonList(query), namespace);
    optionalSqlMeta.ifPresent(
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
