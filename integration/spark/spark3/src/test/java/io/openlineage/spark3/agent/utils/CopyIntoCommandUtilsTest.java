/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.utils;

import static org.assertj.core.api.Assertions.assertThat;

import com.databricks.sql.transaction.tahoe.commands.CopyIntoCommandEdge;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.OneRowRelation;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import scala.Option;

class CopyIntoCommandUtilsTest {

  private static final String FILE_FORMAT = "CSV";
  private static final String VOLUME_PATH =
      "/Volumes/sangeeta_catalog/default/sangeeta_vol/input/csv_input";

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
    assertThat(
            CopyIntoCommandUtils.matchesCommandClassName(
                "com.databricks.sql.transaction.tahoe.commands.edge.CopyIntoCommandEdge"))
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

  /**
   * On Databricks the emitted event carried a name-only {@code unity-catalog} dataset instead of
   * the storage location that {@code DELETE} produces for the same table. That happens when the
   * target relation is never found and the builder degrades to parsing the SQL text, so extraction
   * must not depend on the command spelling its members {@code target} / {@code sourcePath}.
   */
  @Test
  void testTargetFromMemberWithUnknownName() {
    DataSourceV2Relation targetRelation = catalogRelation();
    CopyIntoCommandEdge command =
        new CopyIntoCommandEdge(FILE_FORMAT, targetRelation, new OneRowRelation(), VOLUME_PATH);

    assertThat(CopyIntoCommandUtils.targetFromCommand(command)).contains(targetRelation);
  }

  @Test
  void testSourcePathFromMemberWithUnknownName() {
    CopyIntoCommandEdge command =
        new CopyIntoCommandEdge(FILE_FORMAT, catalogRelation(), new OneRowRelation(), VOLUME_PATH);

    assertThat(CopyIntoCommandUtils.sourcePathFromCommand(command)).contains(VOLUME_PATH);
  }

  @Test
  void testSourceQueryIsNotTheTargetRelation() {
    DataSourceV2Relation targetRelation = catalogRelation();
    OneRowRelation sourceRelation = new OneRowRelation();
    CopyIntoCommandEdge command =
        new CopyIntoCommandEdge(FILE_FORMAT, targetRelation, sourceRelation, VOLUME_PATH);

    assertThat(CopyIntoCommandUtils.sourceQueryFromCommand(command)).contains(sourceRelation);
  }

  /** A source scan must never be reported as the table the statement wrote to. */
  @Test
  void testTargetIsEmptyWhenNoMemberIsCatalogBacked() {
    CopyIntoCommandEdge command =
        new CopyIntoCommandEdge(
            FILE_FORMAT, new OneRowRelation(), new OneRowRelation(), VOLUME_PATH);

    assertThat(CopyIntoCommandUtils.targetFromCommand(command)).isEmpty();
  }

  /** A catalog-backed target, which is what makes the delegate emit Unity Catalog identity. */
  private static DataSourceV2Relation catalogRelation() {
    DataSourceV2Relation relation = Mockito.mock(DataSourceV2Relation.class);
    Mockito.when(relation.identifier())
        .thenReturn(
            Option.apply(Identifier.of(new String[] {"openlineage_dml"}, "copy_into_table")));
    return relation;
  }
}
