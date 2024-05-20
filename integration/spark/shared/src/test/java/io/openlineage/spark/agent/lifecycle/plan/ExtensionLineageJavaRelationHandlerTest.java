/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.lifecycle.plan.handlers.ExtensionLineageJavaRelationHandler;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.extension.v1.LineageRelation;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.sources.BaseRelation;
import org.junit.jupiter.api.Test;

class ExtensionLineageJavaRelationHandlerTest {
  OpenLineageContext openLineageContext = mock(OpenLineageContext.class);
  DatasetFactory datasetFactory = DatasetFactory.input(openLineageContext);
  ExtensionLineageJavaRelationHandler<InputDataset> handler =
      new ExtensionLineageJavaRelationHandler<>(openLineageContext, datasetFactory);

  LogicalRelation relation = mock(LogicalRelation.class);

  @Test
  void testApplyWhenRelationSchemaIsNull() {
    when(openLineageContext.getOpenLineage())
        .thenReturn(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI));
    DatasetIdentifier datasetIdentifier = new DatasetIdentifier("name", "namespace");
    LineageRelation lineageRelation =
        (LineageRelation)
            mock(BaseRelation.class, withSettings().extraInterfaces(LineageRelation.class));
    when(lineageRelation.getLineageDatasetIdentifier(any())).thenReturn(datasetIdentifier);
    when(relation.schema()).thenReturn(null);
    when(relation.relation()).thenReturn((BaseRelation) lineageRelation);

    assertThat(handler.handleRelation(mock(SparkListenerEvent.class), relation)).isNotEmpty();

    InputDataset dataset = handler.handleRelation(mock(SparkListenerEvent.class), relation).get(0);
    assertThat(dataset)
        .hasFieldOrPropertyWithValue("name", "name")
        .hasFieldOrPropertyWithValue("namespace", "namespace");
    assertThat(dataset.getFacets().getSchema()).isNull();
  }

  @Test
  void testApplyWhenCalledForNonLineageRelation() {
    when(relation.relation()).thenReturn(mock(BaseRelation.class));
    assertThat(handler.handleRelation(mock(SparkListenerEvent.class), relation)).isEmpty();
  }
}
