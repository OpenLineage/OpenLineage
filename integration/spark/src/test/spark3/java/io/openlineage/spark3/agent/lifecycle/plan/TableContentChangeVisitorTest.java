/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark3.agent.lifecycle.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import java.util.Collections;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.spark.sql.catalyst.plans.logical.DeleteFromTable;
import org.apache.spark.sql.catalyst.plans.logical.InsertIntoStatement;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.MergeIntoTable;
import org.apache.spark.sql.catalyst.plans.logical.OverwriteByExpression;
import org.apache.spark.sql.catalyst.plans.logical.OverwritePartitionsDynamic;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceData;
import org.apache.spark.sql.catalyst.plans.logical.UpdateTable;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class TableContentChangeVisitorTest {

  OpenLineageContext openLineageContext = mock(OpenLineageContext.class);
  DataSourceV2Relation dataSourceV2Relation = mock(DataSourceV2Relation.class);
  OpenLineage openLineage;
  TableContentChangeVisitor visitor;

  @BeforeEach
  @SneakyThrows
  public void setUp() {
    openLineage = mock(OpenLineage.class);
    when(openLineageContext.getOpenLineage()).thenReturn(openLineage);
    visitor = new TableContentChangeVisitor(openLineageContext);
  }

  @Test
  public void testApplyForOverwriteByExpression() {
    OverwriteByExpression logicalPlan = mock(OverwriteByExpression.class);
    when(logicalPlan.table()).thenReturn(dataSourceV2Relation);
    verify(
        logicalPlan, OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE);
  }

  @Test
  public void testApplyForOverwritePartitionsDynamic() {
    OverwritePartitionsDynamic logicalPlan = mock(OverwritePartitionsDynamic.class);
    when(logicalPlan.table()).thenReturn(dataSourceV2Relation);
    verify(
        logicalPlan, OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE);
  }

  @Test
  public void testApplyForInsertIntoStatement() {
    InsertIntoStatement logicalPlan = mock(InsertIntoStatement.class);
    when(logicalPlan.table()).thenReturn(dataSourceV2Relation);
    when(logicalPlan.overwrite()).thenReturn(true);
    verify(
        logicalPlan, OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE);
  }

  @Test
  public void testApplyForDeleteFromTable() {
    DeleteFromTable logicalPlan = mock(DeleteFromTable.class);
    when(logicalPlan.table()).thenReturn(dataSourceV2Relation);
    verify(logicalPlan, null);
  }

  @Test
  public void testApplyForUpdateTable() {
    UpdateTable logicalPlan = mock(UpdateTable.class);
    when(logicalPlan.table()).thenReturn(dataSourceV2Relation);
    verify(logicalPlan, null);
  }

  @Test
  public void testApplyForReplaceData() {
    ReplaceData logicalPlan = mock(ReplaceData.class);
    when(logicalPlan.table()).thenReturn(dataSourceV2Relation);
    verify(logicalPlan, null);
  }

  @Test
  public void testApplyForMergeIntoTable() {
    MergeIntoTable logicalPlan = mock(MergeIntoTable.class);
    when(logicalPlan.targetTable()).thenReturn(dataSourceV2Relation);
    verify(logicalPlan, null);
  }

  @Test
  public void testApplyForInsertIntoStatementWithOverwriteDisabled() {
    InsertIntoStatement logicalPlan = mock(InsertIntoStatement.class);
    when(logicalPlan.table()).thenReturn(dataSourceV2Relation);
    when(logicalPlan.overwrite()).thenReturn(false);
    verify(logicalPlan, null);
  }

  @Test
  public void testIsDefined() {
    assertTrue(visitor.isDefinedAt(mock(OverwriteByExpression.class)));
    assertTrue(visitor.isDefinedAt(mock(OverwritePartitionsDynamic.class)));
    assertTrue(visitor.isDefinedAt(mock(InsertIntoStatement.class)));
    assertTrue(visitor.isDefinedAt(mock(DeleteFromTable.class)));
    assertTrue(visitor.isDefinedAt(mock(UpdateTable.class)));
    assertTrue(visitor.isDefinedAt(mock(ReplaceData.class)));
    assertFalse(visitor.isDefinedAt(mock(LogicalPlan.class)));
  }

  private void verify(
      LogicalPlan logicalPlan,
      OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange lifecycleStateChange) {
    try (MockedStatic mocked = mockStatic(PlanUtils3.class)) {
      OpenLineage.Dataset dataset = mock(OpenLineage.Dataset.class);
      OpenLineage.DatasetFacetsBuilder datasetFacetsBuilder =
          mock(OpenLineage.DatasetFacetsBuilder.class);
      OpenLineage.LifecycleStateChangeDatasetFacet lifecycleStateChangeDatasetFacet =
          mock(OpenLineage.LifecycleStateChangeDatasetFacet.class);

      when(openLineage.newDatasetFacetsBuilder()).thenReturn(datasetFacetsBuilder);
      when(openLineage.newLifecycleStateChangeDatasetFacet(lifecycleStateChange, null))
          .thenReturn(lifecycleStateChangeDatasetFacet);

      when(PlanUtils3.fromDataSourceV2Relation(
              any(), eq(openLineageContext), eq(dataSourceV2Relation), eq(datasetFacetsBuilder)))
          .thenReturn(Collections.singletonList(dataset));

      List<OpenLineage.OutputDataset> datasetList = visitor.apply(logicalPlan);

      assertEquals(1, datasetList.size());
      assertEquals(dataset, datasetList.get(0));
      if (lifecycleStateChange != null) {
        Mockito.verify(datasetFacetsBuilder)
            .lifecycleStateChange(eq(lifecycleStateChangeDatasetFacet));
      }
    }
  }
}
