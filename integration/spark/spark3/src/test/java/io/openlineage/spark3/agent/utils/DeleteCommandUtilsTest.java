/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.utils;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.spark.sql.catalyst.plans.logical.OneRowRelation;
import org.junit.jupiter.api.Test;

class DeleteCommandUtilsTest {

  @Test
  void testMatchesCommandClassName() {
    assertThat(
            DeleteCommandUtils.matchesCommandClassName(
                "com.databricks.sql.transaction.tahoe.commands.DeleteCommandEdge"))
        .isTrue();
    assertThat(
            DeleteCommandUtils.matchesCommandClassName(
                "com.databricks.sql.transaction.tahoe.commands.DeleteCommand"))
        .isTrue();
    assertThat(
            DeleteCommandUtils.matchesCommandClassName(
                "com.databricks.sql.transaction.tahoe.commands.UpdateCommandEdge"))
        .isFalse();
    assertThat(
            DeleteCommandUtils.matchesCommandClassName(
                "com.databricks.sql.transaction.tahoe.commands.UpdateCommand"))
        .isFalse();
    assertThat(DeleteCommandUtils.matchesCommandClassName("DeleteFromTable")).isFalse();
    assertThat(DeleteCommandUtils.matchesCommandClassName(null)).isFalse();
  }

  @Test
  void testTargetWhenCommandHasNoSuchMember() {
    assertThat(DeleteCommandUtils.target(new OneRowRelation())).isEmpty();
  }
}
