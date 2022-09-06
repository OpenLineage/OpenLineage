/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.spark.api.OpenLineageContext;
import java.util.Collections;
import java.util.Optional;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.delta.catalog.DeltaCatalog;
import org.apache.spark.sql.delta.catalog.DeltaTableV2;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class DeltaHandlerTest {

  DeltaCatalog deltaCatalog = mock(DeltaCatalog.class);
  DeltaTableV2 deltaTable = Mockito.mock(DeltaTableV2.class, RETURNS_DEEP_STUBS);
  Identifier identifier = Identifier.of(new String[] {"database", "schema"}, "table");

  DeltaHandler deltaHandler = new DeltaHandler(mock(OpenLineageContext.class));

  @Test
  void testGetVersionString() {
    when(deltaCatalog.loadTable(identifier)).thenReturn(deltaTable);
    when(deltaTable.snapshot().version()).thenReturn(2L);

    Optional<String> version =
        deltaHandler.getDatasetVersion(deltaCatalog, identifier, Collections.emptyMap());

    assertTrue(version.isPresent());
    assertEquals(version.get(), "2");
  }

  @Test
  void testGetIdentifierLoadsTablePath() {
    when(deltaCatalog.loadTable(identifier)).thenReturn(deltaTable);
    when(deltaTable.properties().get("location")).thenReturn("/some-location");

    assertEquals(
        "/some-location",
        deltaHandler
            .getDatasetIdentifier(
                mock(SparkSession.class), deltaCatalog, identifier, Collections.emptyMap())
            .getName());
  }
}
