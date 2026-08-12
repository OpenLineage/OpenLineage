/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.utils;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.spark.sql.catalyst.plans.logical.OneRowRelation;
import org.junit.jupiter.api.Test;

class UpdateCommandUtilsTest {

  @Test
  void testMatchesCommandClassName() {
    assertThat(
            UpdateCommandUtils.matchesCommandClassName(
                "com.databricks.sql.transaction.tahoe.commands.UpdateCommandEdge"))
        .isTrue();
    assertThat(
            UpdateCommandUtils.matchesCommandClassName(
                "com.databricks.sql.transaction.tahoe.commands.UpdateCommand"))
        .isTrue();
    assertThat(
            UpdateCommandUtils.matchesCommandClassName(
                "com.databricks.sql.transaction.tahoe.commands.DeleteCommandEdge"))
        .isFalse();
    assertThat(
            UpdateCommandUtils.matchesCommandClassName(
                "com.databricks.sql.transaction.tahoe.commands.DeleteCommand"))
        .isFalse();
    assertThat(UpdateCommandUtils.matchesCommandClassName("UpdateTable")).isFalse();
    assertThat(UpdateCommandUtils.matchesCommandClassName(null)).isFalse();
  }

  @Test
  void testTargetWhenCommandHasNoSuchMember() {
    assertThat(UpdateCommandUtils.target(new OneRowRelation())).isEmpty();
  }
}
