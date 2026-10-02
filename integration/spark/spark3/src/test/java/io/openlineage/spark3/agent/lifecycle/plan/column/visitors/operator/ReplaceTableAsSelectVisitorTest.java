/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.AliasBuilder.alias;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_1;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_2;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_3;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.NAME_1;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.NAME_2;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.NAME_3;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.asSeq;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.field;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceTableAsSelect;
import org.junit.jupiter.api.Test;
import scala.collection.immutable.Seq;

class ReplaceTableAsSelectVisitorTest {
  ReplaceTableAsSelectVisitor visitor = new ReplaceTableAsSelectVisitor();
  ColumnLevelLineageBuilder builder = mock(ColumnLevelLineageBuilder.class);

  @Test
  void testIsDefinedAt() {
    assertTrue(visitor.isDefinedAt(getReplaceTableAsSelectNode(null)));
    assertTrue(visitor.isDefinedAt(getReplaceTableAsSelectNode(asSeq())));
    assertFalse(visitor.isDefinedAt(getReplaceTableAsSelectNode(asSeq(getProject()))));
    assertFalse(visitor.isDefinedAt(mock(LogicalPlan.class)));
  }

  @Test
  void testApply() {
    Project project = getProject();
    ReplaceTableAsSelect replaceTableAsSelect =
        new ReplaceTableAsSelect(null, null, null, project, null, null, false);

    visitor.apply(replaceTableAsSelect, builder);

    verify(builder)
        .addDependency(
            EXPR_ID_2, EXPR_ID_1, "name2", TransformationInfo.identity("name1 AS name2"));
  }

  @Test
  void testApplyVisitsOperatorsBelowQueryRoot() {
    Project innerProject = getProject();
    Project outerProject =
        new Project(asSeq(alias(field(NAME_2, EXPR_ID_2)).as(NAME_3, EXPR_ID_3)), innerProject);
    ReplaceTableAsSelect replaceTableAsSelect =
        new ReplaceTableAsSelect(null, null, null, outerProject, null, null, false);

    visitor.apply(replaceTableAsSelect, builder);

    verify(builder)
        .addDependency(
            EXPR_ID_3, EXPR_ID_2, "name3", TransformationInfo.identity("name2 AS name3"));
    verify(builder)
        .addDependency(
            EXPR_ID_2, EXPR_ID_1, "name2", TransformationInfo.identity("name1 AS name2"));
  }

  private static LogicalPlan getReplaceTableAsSelectNode(Seq<LogicalPlan> children) {
    ReplaceTableAsSelect node = mock(ReplaceTableAsSelect.class);
    when(node.children()).thenReturn(children);
    return node;
  }

  private static Project getProject() {
    return new Project(
        asSeq(alias(field(NAME_1, EXPR_ID_1)).as(NAME_2, EXPR_ID_2)), mock(LogicalPlan.class));
  }
}
