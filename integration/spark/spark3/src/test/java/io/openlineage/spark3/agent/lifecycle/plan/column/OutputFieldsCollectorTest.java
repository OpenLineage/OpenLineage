/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.extension.column.v1.ColumnLevelLineageNode;
import io.openlineage.spark.extension.column.v1.OlExprId;
import io.openlineage.spark.extension.column.v1.OutputDatasetField;
import java.util.Arrays;
import java.util.Collections;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import scala.collection.immutable.Seq;

class OutputFieldsCollectorTest {

  private static final String SOME_NAME = "some-name";
  LogicalPlan plan = mock(LogicalPlan.class);
  OpenLineageContext olContext = mock(OpenLineageContext.class);
  ColumnLevelLineageBuilder builder = mock(ColumnLevelLineageBuilder.class);
  ColumnLevelLineageContext clContext = mock(ColumnLevelLineageContext.class);
  Attribute attr1 = mock(Attribute.class);
  Attribute attr2 = mock(Attribute.class);
  ExprId exprId1 = mock(ExprId.class);
  ExprId exprId2 = mock(ExprId.class);

  Seq<Attribute> attrs = ScalaConversionUtils.fromList(Arrays.asList(attr1, attr2)).toSeq();

  @BeforeEach
  void setup() {
    when(attr1.name()).thenReturn("name1");
    when(attr1.exprId()).thenReturn(exprId1);

    when(attr2.name()).thenReturn("name2");
    when(attr2.exprId()).thenReturn(exprId2);

    when(clContext.getBuilder()).thenReturn(builder);
    when(clContext.getOlContext()).thenReturn(olContext);
    when(plan.output()).thenReturn(ScalaConversionUtils.asScalaSeqEmpty());
    when(builder.hasOutputs()).thenReturn(true);
    when(clContext.getEvent()).thenReturn(mock(SparkListenerEvent.class));
  }

  @Test
  void verifyOutputAttributeIsCollected() {
    when(plan.output()).thenReturn(attrs);

    OutputFieldsCollector.collect(clContext, plan);

    Mockito.verify(builder, times(1)).addOutput(exprId1, "name1");
    Mockito.verify(builder, times(1)).addOutput(exprId2, "name2");
  }

  @Test
  void verifyAggregateExpressionsAreCollected() {
    NamedExpression namedExpression = mock(NamedExpression.class);
    ExprId exprId = mock(ExprId.class);

    when(namedExpression.name()).thenReturn(SOME_NAME);
    when(namedExpression.exprId()).thenReturn(exprId);

    Aggregate aggregate = mock(Aggregate.class);
    when(aggregate.output()).thenReturn(ScalaConversionUtils.asScalaSeqEmpty());
    when(aggregate.aggregateExpressions())
        .thenReturn(
            scala.collection.JavaConverters.collectionAsScalaIterableConverter(
                    Arrays.asList(namedExpression))
                .asScala()
                .toSeq());

    OutputFieldsCollector.collect(clContext, aggregate);

    Mockito.verify(builder, times(1)).addOutput(exprId, SOME_NAME);
  }

  @Test
  void verifyProjectListIsCollected() {
    NamedExpression namedExpression = mock(NamedExpression.class);
    ExprId exprId = mock(ExprId.class);

    when(namedExpression.name()).thenReturn(SOME_NAME);
    when(namedExpression.exprId()).thenReturn(exprId);

    Project project = mock(Project.class);
    when(project.output()).thenReturn(ScalaConversionUtils.asScalaSeqEmpty());
    when(project.projectList())
        .thenReturn(
            scala.collection.JavaConverters.collectionAsScalaIterableConverter(
                    Arrays.asList(namedExpression))
                .asScala()
                .toSeq());

    OutputFieldsCollector.collect(clContext, project);

    Mockito.verify(builder, times(1)).addOutput(exprId, SOME_NAME);
  }

  @Test
  void verifyChildrenOutputIsCollectedWhenNoDirectOutput() {
    LogicalPlan childPlan = mock(LogicalPlan.class);
    when(childPlan.output()).thenReturn(attrs);

    when(plan.output()).thenReturn(ScalaConversionUtils.asScalaSeqEmpty());
    when(builder.hasOutputs()).thenReturn(false).thenReturn(true);
    when(plan.children())
        .thenReturn(
            scala.collection.JavaConverters.collectionAsScalaIterableConverter(
                    Arrays.asList(childPlan))
                .asScala()
                .toSeq());

    OutputFieldsCollector.collect(clContext, plan);

    Mockito.verify(builder, times(1)).addOutput(exprId1, "name1");
    Mockito.verify(builder, times(1)).addOutput(exprId2, "name2");
  }

  @Test
  void testExtensionOutputDatasetField() {
    LogicalPlan columnLineagePlanNode =
        mock(LogicalPlan.class, withSettings().extraInterfaces(ColumnLevelLineageNode.class));

    when(((ColumnLevelLineageNode) columnLineagePlanNode).getColumnLevelLineageOutputs(any()))
        .thenReturn(
            Collections.singletonList(new OutputDatasetField(new OlExprId(1L), "nameExtension1")));

    OutputFieldsCollector.collect(clContext, columnLineagePlanNode);

    Mockito.verify(builder, times(1))
        .addOutput(refEq(ExprId.apply(1L), "jvmId"), eq("nameExtension1"));
  }
}
