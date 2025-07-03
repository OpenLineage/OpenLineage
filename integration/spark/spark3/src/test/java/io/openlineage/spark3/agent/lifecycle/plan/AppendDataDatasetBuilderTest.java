/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.lifecycle.SparkOpenLineageExtensionVisitorWrapper;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NamedRelation;
import org.apache.spark.sql.catalyst.plans.logical.AppendData;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.junit.jupiter.api.Test;

class AppendDataDatasetBuilderTest {

  public static final String TABLE_NAME = "table";
  OpenLineageContext context =
      OpenLineageContext.builder()
          .sparkSession(mock(SparkSession.class))
          .sparkContext(mock(SparkContext.class))
          .applicationName("app-name")
          .applicationUuid(UUID.fromString("8d99e33e-bbbb-cccc-dddd-18f2343aaaaa"))
          .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
          .meterRegistry(new SimpleMeterRegistry())
          .openLineageConfig(new SparkOpenLineageConfig())
          .sparkExtensionVisitorWrapper(mock(SparkOpenLineageExtensionVisitorWrapper.class))
          .build();
  DatasetFactory<OpenLineage.OutputDataset> factory = mock(DatasetFactory.class);
  AppendDataDatasetBuilder builder = new AppendDataDatasetBuilder(context, factory);

  @Test
  void isDefinedAtLogicalPlan() {
    assertTrue(builder.isDefinedAtLogicalPlan(mock(AppendData.class)));
    assertFalse(builder.isDefinedAtLogicalPlan(mock(LogicalPlan.class)));
  }

  @Test
  void testApplyWithoutDataSourceV2Relation() {
    AppendData appendData = mock(AppendData.class);
    when(appendData.table())
        .thenReturn(
            (NamedRelation)
                mock(LogicalPlan.class, withSettings().extraInterfaces(NamedRelation.class)));

    assertEquals(0, builder.apply(new SparkListenerSQLExecutionEnd(1L, 1L), appendData).size());
  }

  @Test
  void testJobNameSuffix() {
    AppendData appendData = mock(AppendData.class);
    NamedRelation table = mock(NamedRelation.class);

    when(appendData.table()).thenReturn(table);
    when(table.name()).thenReturn(TABLE_NAME);

    assertThat(builder.jobNameSuffix(appendData)).isPresent().get().isEqualTo(TABLE_NAME);
    assertThat(builder.jobNameSuffix(mock(AppendData.class))).isEmpty();
  }

  @Test
  void testJobNameSuffixForDataSourceV2Relation() {
    AppendData appendData = mock(AppendData.class);
    DataSourceV2Relation table = mock(DataSourceV2Relation.class, RETURNS_DEEP_STUBS);

    when(appendData.table()).thenReturn(table);
    when(table.table().name()).thenReturn(TABLE_NAME);

    assertThat(builder.jobNameSuffix(appendData)).isPresent().get().isEqualTo(TABLE_NAME);
    assertThat(builder.jobNameSuffix(mock(AppendData.class))).isEmpty();
  }
}
