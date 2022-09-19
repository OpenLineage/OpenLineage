/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import scala.collection.Seq;
import scala.collection.Seq$;

class OutputFieldsCollectorTest {

  private static final String SOME_NAME = "some-name";
  LogicalPlan plan = mock(LogicalPlan.class);
  ColumnLevelLineageBuilder builder = mock(ColumnLevelLineageBuilder.class);

  Attribute attr1 = mock(Attribute.class);
  Attribute attr2 = mock(Attribute.class);
  ExprId exprId1 = mock(ExprId.class);
  ExprId exprId2 = mock(ExprId.class);

  Seq<Attribute> attrs =
      scala.collection.JavaConverters.collectionAsScalaIterableConverter(
              Arrays.asList(attr1, attr2))
          .asScala()
          .toSeq();

  @BeforeEach
  void setup() {
    when(attr1.name()).thenReturn("name1");
    when(attr1.exprId()).thenReturn(exprId1);

    when(attr2.name()).thenReturn("name2");
    when(attr2.exprId()).thenReturn(exprId2);

    when(plan.output()).thenReturn((Seq<Attribute>) Seq$.MODULE$.empty());
    when(builder.hasOutputs()).thenReturn(true);
  }

  @Test
  void verifyOutputAttributeIsCollected() {
    when(plan.output()).thenReturn(attrs);

    OutputFieldsCollector.collect(plan, builder);

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
    when(aggregate.output()).thenReturn((Seq<Attribute>) Seq$.MODULE$.empty());
    when(aggregate.aggregateExpressions())
        .thenReturn(
            scala.collection.JavaConverters.collectionAsScalaIterableConverter(
                    Arrays.asList(namedExpression))
                .asScala()
                .toSeq());

    OutputFieldsCollector.collect(aggregate, builder);

    Mockito.verify(builder, times(1)).addOutput(exprId, SOME_NAME);
  }

  @Test
  void verifyProjectListIsCollected() {
    NamedExpression namedExpression = mock(NamedExpression.class);
    ExprId exprId = mock(ExprId.class);

    when(namedExpression.name()).thenReturn(SOME_NAME);
    when(namedExpression.exprId()).thenReturn(exprId);

    Project project = mock(Project.class);
    when(project.output()).thenReturn((Seq<Attribute>) Seq$.MODULE$.empty());
    when(project.projectList())
        .thenReturn(
            scala.collection.JavaConverters.collectionAsScalaIterableConverter(
                    Arrays.asList(namedExpression))
                .asScala()
                .toSeq());

    OutputFieldsCollector.collect(project, builder);

    Mockito.verify(builder, times(1)).addOutput(exprId, SOME_NAME);
  }

  @Test
  void verifyChildrenOutputIsCollectedWhenNoDirectOutput() {
    LogicalPlan childPlan = mock(LogicalPlan.class);
    when(childPlan.output()).thenReturn(attrs);

    when(plan.output()).thenReturn((Seq<Attribute>) Seq$.MODULE$.empty());
    when(builder.hasOutputs()).thenReturn(false).thenReturn(true);
    when(plan.children())
        .thenReturn(
            scala.collection.JavaConverters.collectionAsScalaIterableConverter(
                    Arrays.asList(childPlan))
                .asScala()
                .toSeq());

    OutputFieldsCollector.collect(plan, builder);

    Mockito.verify(builder, times(1)).addOutput(exprId1, "name1");
    Mockito.verify(builder, times(1)).addOutput(exprId2, "name2");
  }
}
