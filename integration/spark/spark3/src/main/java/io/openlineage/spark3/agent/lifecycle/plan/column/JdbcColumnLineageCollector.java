/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.JdbcUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.sql.ColumnLineage;
import io.openlineage.sql.ColumnMeta;
import io.openlineage.sql.ExtractionError;
import io.openlineage.sql.SqlMeta;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import scala.collection.Seq;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
@Slf4j
public class JdbcColumnLineageCollector {
    
    public static void extractExternalInputs(
            LogicalPlan node,
            ColumnLevelLineageBuilder builder,
            List<DatasetIdentifier> datasetIdentifiers){
        extractExternalInputs((JDBCRelation) ((LogicalRelation) node).relation(), builder, datasetIdentifiers);
    }
    public static void extractExternalInputs(
            JDBCRelation relation,
            ColumnLevelLineageBuilder builder,
            List<DatasetIdentifier> datasetIdentifiers) {
        SqlMeta sqlMeta =
                JdbcUtils.extractQueryFromSpark(relation).get();
        if (!sqlMeta.errors().isEmpty()) { // error return nothing
            log.error(
                    String.format(
                            "error while parsing query: %s",
                            sqlMeta.errors().stream()
                                    .map(ExtractionError::toString)
                                    .collect(Collectors.joining(","))));
        } else if (sqlMeta.inTables().isEmpty()) {
            log.error("no tables defined in query, this should not happen");
        } else {
            List<ColumnLineage> columnLineages = sqlMeta.columnLineage();
            Set<ColumnMeta> inputs =
                    columnLineages.stream().flatMap(cl -> cl.lineage().stream()).collect(Collectors.toSet());
            columnLineages.forEach(cl -> inputs.remove(cl.descendant()));
            datasetIdentifiers.forEach(
                    di ->
                            inputs.stream()
                                    .filter(
                                            cm ->
                                                    cm.origin().isPresent() && cm.origin().get().name().equals(di.getName()))
                                    .forEach(cm -> builder.addInput(builder.getMapping(cm), di, cm.name())));
        }
    }
    
    
    public static void extractExpressionsFromJDBC(
            LogicalPlan node, ColumnLevelLineageBuilder builder){
        extractExpressionsFromJDBC((JDBCRelation) ((LogicalRelation) node).relation(), builder, ScalaConversionUtils.fromSeq(node.output()));
    }
    
    public static void extractExpressionsFromJDBC(
            JDBCRelation relation, ColumnLevelLineageBuilder builder, List<Attribute> output) {
        SqlMeta sqlMeta =
                JdbcUtils.extractQueryFromSpark(relation).get();
        if (!sqlMeta.errors().isEmpty()) { // error return nothing
            log.error(
                    String.format(
                            "error while parsing query: %s",
                            sqlMeta.errors().stream()
                                    .map(ExtractionError::toString)
                                    .collect(Collectors.joining(","))));
        } else if (sqlMeta.inTables().isEmpty()) {
            log.error("no tables defined in query, this should not happen");
        } else {
            sqlMeta
                    .columnLineage()
                    .forEach(
                            p -> {
                                ExprId decendantId = getDecendantId(output, p.descendant());
                                builder.addExternalMapping(p.descendant(), decendantId);

                                p.lineage()
                                        .forEach(e -> builder.addExternalMapping(e, NamedExpression.newExprId()));
                                if (p.lineage().size() > 1) {
                                    p.lineage().stream()
                                            .map(builder::getMapping)
                                            .forEach(eid -> builder.addDependency(decendantId, eid));
                                }
                            });
        }
    }
    private static ExprId getDecendantId(List<Attribute> output, ColumnMeta column) {
        return output.stream()
                .filter(e -> e.name().equals(column.name()))
                .map(NamedExpression::exprId)
                .findFirst()
                .orElseGet(NamedExpression::newExprId);
    }
}
