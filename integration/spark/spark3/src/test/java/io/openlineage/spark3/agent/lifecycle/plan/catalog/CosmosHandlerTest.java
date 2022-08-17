/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.spark.agent.util.DatasetIdentifier;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.junit.jupiter.api.Test;

public class CosmosHandlerTest {

  @Test
  void testGetDatasetIdentifier() {
    DataSourceV2Relation relation = mock(DataSourceV2Relation.class, RETURNS_DEEP_STUBS);
    when(relation.table().name())
        .thenReturn("com.azure.cosmos.spark.items.openlineage-test-cosmos.exampledata.volcanoes");

    CosmosHandler cosmosHandler = new CosmosHandler();

    DatasetIdentifier di = cosmosHandler.getDatasetIdentifier(relation);
    assertEquals(
        di.getNamespace(),
        "azurecosmos://openlineage-test-cosmos.documents.azure.com/dbs/exampledata");
    assertEquals(di.getName(), "/colls/volcanoes");
  }
}
