/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

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
import io.openlineage.spark3.agent.lifecycle.plan.catalog.CatalogUtils3;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.spark.sql.catalyst.plans.logical.DeleteFromTable;
import org.apache.spark.sql.catalyst.plans.logical.InsertIntoStatement;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.MergeIntoTable;
import org.apache.spark.sql.catalyst.plans.logical.OverwriteByExpression;
import org.apache.spark.sql.catalyst.plans.logical.OverwritePartitionsDynamic;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceData;
import org.apache.spark.sql.catalyst.plans.logical.UpdateTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import scala.Option;

public class TableContentChangeDatasetBuilderTest {

  OpenLineageContext openLineageContext = mock(OpenLineageContext.class);
  DataSourceV2Relation dataSourceV2Relation = mock(DataSourceV2Relation.class);
  Identifier identifier = mock(Identifier.class);
  TableCatalog tableCatalog = mock(TableCatalog.class);
  Table table = mock(Table.class);
  Map<String, String> tableProperties = new HashMap<>();
  OpenLineage openLineage;
  TableContentChangeDatasetBuilder builder;

  @BeforeEach
  @SneakyThrows
  public void setUp() {
    openLineage = mock(OpenLineage.class);
    when(openLineageContext.getOpenLineage()).thenReturn(openLineage);
    builder = new TableContentChangeDatasetBuilder(openLineageContext);
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
  public void testIsDefinedAtLogicalPlan() {
    assertTrue(builder.isDefinedAtLogicalPlan(mock(OverwriteByExpression.class)));
    assertTrue(builder.isDefinedAtLogicalPlan(mock(OverwritePartitionsDynamic.class)));
    assertTrue(builder.isDefinedAtLogicalPlan(mock(InsertIntoStatement.class)));
    assertTrue(builder.isDefinedAtLogicalPlan(mock(DeleteFromTable.class)));
    assertTrue(builder.isDefinedAtLogicalPlan(mock(UpdateTable.class)));
    assertTrue(builder.isDefinedAtLogicalPlan(mock(ReplaceData.class)));
    assertFalse(builder.isDefinedAtLogicalPlan(mock(LogicalPlan.class)));
  }

  private void verify(
      LogicalPlan logicalPlan,
      OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange lifecycleStateChange) {
    try (MockedStatic mockedPlanUtils3 = mockStatic(PlanUtils3.class)) {
      try (MockedStatic mockedVersions = mockStatic(CatalogUtils3.class)) {
        OpenLineage.Dataset dataset = mock(OpenLineage.OutputDataset.class);
        OpenLineage.DatasetFacetsBuilder datasetFacetsBuilder =
            mock(OpenLineage.DatasetFacetsBuilder.class);
        mock(OpenLineage.DatasetFacetsBuilder.class);
        OpenLineage.LifecycleStateChangeDatasetFacet lifecycleStateChangeDatasetFacet =
            mock(OpenLineage.LifecycleStateChangeDatasetFacet.class);
        OpenLineage.DatasetVersionDatasetFacet datasetVersionDatasetFacet =
            mock(OpenLineage.DatasetVersionDatasetFacet.class);

        when(dataSourceV2Relation.identifier()).thenReturn(Option.apply(identifier));
        when(dataSourceV2Relation.catalog()).thenReturn(Option.apply(tableCatalog));
        when(dataSourceV2Relation.table()).thenReturn(table);
        when(table.properties()).thenReturn(tableProperties);

        when(openLineage.newDatasetFacetsBuilder()).thenReturn(datasetFacetsBuilder);
        when(openLineage.newLifecycleStateChangeDatasetFacet(lifecycleStateChange, null))
            .thenReturn(lifecycleStateChangeDatasetFacet);
        when(openLineage.newDatasetVersionDatasetFacet("v2"))
            .thenReturn(datasetVersionDatasetFacet);

        when(PlanUtils3.fromDataSourceV2Relation(
                any(), eq(openLineageContext), eq(dataSourceV2Relation), eq(datasetFacetsBuilder)))
            .thenReturn(Collections.singletonList(dataset));
        when(CatalogUtils3.getDatasetVersion(any(), any(), any())).thenReturn(Optional.of("v2"));

        List<OpenLineage.OutputDataset> datasetList = builder.apply(logicalPlan);

        assertEquals(1, datasetList.size());
        assertEquals(dataset, datasetList.get(0));
        Mockito.verify(datasetFacetsBuilder).version(eq(datasetVersionDatasetFacet));

        if (lifecycleStateChange != null) {
          Mockito.verify(datasetFacetsBuilder)
              .lifecycleStateChange(eq(lifecycleStateChangeDatasetFacet));
        }
      }
    }
  }
}
