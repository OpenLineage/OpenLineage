/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import io.openlineage.sql.ColumnLineage;
import io.openlineage.sql.ColumnMeta;
import io.openlineage.sql.DbTableMeta;
import io.openlineage.sql.SqlMeta;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;

@RequiredArgsConstructor
public class SqlCollector {
  private final ColumnLevelLineageContext context;
  private final SqlMeta sql;
  private final List<DatasetIdentifier> inputIdentifiers;
  private final List<Attribute> attributes;

  public void collectInputs(Function<DbTableMeta, DatasetIdentifier> createDatasetIdentifier) {
    List<ColumnLineage> columnLineages = sql.columnLineage();
    Set<ColumnMeta> inputs =
        columnLineages.stream().flatMap(cl -> cl.lineage().stream()).collect(Collectors.toSet());
    columnLineages.forEach(cl -> inputs.remove(cl.descendant()));

    inputIdentifiers.forEach(
        di ->
            inputs.stream()
                .filter(
                    cm ->
                        cm.origin().isPresent()
                            && context
                                .getNamespaceResolver()
                                .resolve(createDatasetIdentifier.apply(cm.origin().get()))
                                .getName()
                                .equals(di.getName()))
                .forEach(
                    cm ->
                        context
                            .getBuilder()
                            .addInput(context.getBuilder().getMapping(cm), di, cm.name())));
  }

  public void collectExpressionDependencies() {
    sql.columnLineage()
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

  private ExprId getDescendantId(List<Attribute> output, ColumnMeta column) {
    return output.stream()
        .filter(e -> e.name().equals(column.name()))
        .map(NamedExpression::exprId)
        .findFirst()
        .orElseGet(NamedExpression::newExprId);
  }
}
