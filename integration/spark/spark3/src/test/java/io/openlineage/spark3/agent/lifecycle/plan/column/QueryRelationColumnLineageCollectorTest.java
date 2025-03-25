/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark3.agent.lifecycle.plan.column;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.sql.ColumnMeta;
import io.openlineage.sql.DbTableMeta;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class QueryRelationColumnLineageCollectorTest {

  ColumnLevelLineageBuilder builder = mock(ColumnLevelLineageBuilder.class);
  ColumnLevelLineageContext context = mock(ColumnLevelLineageContext.class);
  OpenLineageContext openLineageContext = mock(OpenLineageContext.class);
  DataSourceV2Relation relation = mock(DataSourceV2Relation.class);
  Table table = mock(Table.class);

  @BeforeEach
  void setup() {
    when(relation.table()).thenReturn(table);
    when(context.getBuilder()).thenReturn(builder);
    when(context.getOlContext()).thenReturn(openLineageContext);
  }

  @Test
  void testInputCollection() {
    // given
    ExprId exprId1 = ExprId.apply(1);
    ExprId exprId2 = ExprId.apply(2);
    Map<ColumnMeta, ExprId> mockMap = new HashMap();
    mockMap.put(
        new ColumnMeta(new DbTableMeta("some-project", "dataset", "t1"), "some_column1"), exprId1);
    mockMap.put(
        new ColumnMeta(new DbTableMeta("some-project", "dataset", "t2"), "some_column2"), exprId2);

    when(builder.getMapping(any(ColumnMeta.class)))
        .thenAnswer(invocation -> mockMap.get(invocation.getArgument(0)));
    String query =
        "SELECT t1.some_column1, t2.some_column2 FROM `some-project.dataset.t1` t1 JOIN"
            + " `some-project.dataset.t2` t2 ON t2.foreign_id = t1.id";
    Map<String, String> properties = new HashMap<>();
    properties.put("openlineage.dataset.query", query);
    String namespace = "bigquery";
    properties.put("openlineage.dataset.namespace", namespace);
    when(table.properties()).thenReturn(properties);

    // when
    QueryRelationColumnLineageCollector.extractExternalInputs(context, relation);

    DatasetIdentifier datasetIdentifier1 =
        new DatasetIdentifier("some-project.dataset.t1", namespace);
    DatasetIdentifier datasetIdentifier2 =
        new DatasetIdentifier("some-project.dataset.t2", namespace);

    // then
    verify(builder, times(1)).addInput(exprId1, datasetIdentifier1, "some_column1");
    verify(builder, times(1)).addInput(exprId2, datasetIdentifier2, "some_column2");
  }

  @Test
  void testInvalidQuery() {
    // given
    String query =
        "SELECT t3.some_not_existing_column FROM `some-project.dataset.t1` t1 JOIN"
            + " `some-project.dataset.t2` t2 ON t2.foreign_id = t1.id";
    Map<String, String> properties = new HashMap<>();
    properties.put("openlineage.dataset.query", query);
    properties.put("openlineage.dataset.namespace", "bigquery");
    when(table.properties()).thenReturn(properties);

    // when
    QueryRelationColumnLineageCollector.extractExternalInputs(context, relation);

    // then
    verify(builder, never())
        .addInput(any(ExprId.class), any(DatasetIdentifier.class), any(String.class));
  }
}
