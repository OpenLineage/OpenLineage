/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.utils;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class CopyIntoCommandUtilsTest {

  @Test
  void testMatchesCommandClassName() {
    assertThat(
            CopyIntoCommandUtils.matchesCommandClassName(
                "com.databricks.sql.transaction.tahoe.commands.CopyIntoCommand"))
        .isTrue();
    assertThat(
            CopyIntoCommandUtils.matchesCommandClassName(
                "com.databricks.sql.transaction.tahoe.commands.CopyIntoCommandEdge"))
        .isTrue();
    assertThat(CopyIntoCommandUtils.matchesCommandClassName("AppendData")).isFalse();
    assertThat(CopyIntoCommandUtils.matchesCommandClassName(null)).isFalse();
  }

  @Test
  void testTargetWhenCommandHasNoSuchMember() {
    LogicalPlan plan = Mockito.mock(LogicalPlan.class);
    assertThat(CopyIntoCommandUtils.target(plan)).isEmpty();
    assertThat(CopyIntoCommandUtils.sourcePath(plan)).isEmpty();
    assertThat(CopyIntoCommandUtils.sourceQuery(plan)).isEmpty();
  }
}
