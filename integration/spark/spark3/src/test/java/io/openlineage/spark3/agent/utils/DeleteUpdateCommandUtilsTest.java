/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.utils;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.spark.sql.catalyst.plans.logical.OneRowRelation;
import org.junit.jupiter.api.Test;

class DeleteUpdateCommandUtilsTest {

  @Test
  void testMatchesCommandClassName() {
    assertThat(
            DeleteUpdateCommandUtils.matchesCommandClassName(
                "com.databricks.sql.transaction.tahoe.commands.DeleteCommandEdge"))
        .isTrue();
    assertThat(
            DeleteUpdateCommandUtils.matchesCommandClassName(
                "com.databricks.sql.transaction.tahoe.commands.UpdateCommandEdge"))
        .isTrue();
    assertThat(
            DeleteUpdateCommandUtils.matchesCommandClassName(
                "com.databricks.sql.transaction.tahoe.commands.DeleteCommand"))
        .isTrue();
    assertThat(
            DeleteUpdateCommandUtils.matchesCommandClassName(
                "com.databricks.sql.transaction.tahoe.commands.UpdateCommand"))
        .isTrue();
    assertThat(DeleteUpdateCommandUtils.matchesCommandClassName("DeleteFromTable")).isFalse();
    assertThat(DeleteUpdateCommandUtils.matchesCommandClassName(null)).isFalse();
  }

  @Test
  void testTargetWhenCommandHasNoSuchMember() {
    assertThat(DeleteUpdateCommandUtils.target(new OneRowRelation())).isEmpty();
  }
}
