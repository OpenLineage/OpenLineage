/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.DatasetIdentifier.SymlinkType;
import java.util.Collections;
import java.util.HashMap;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class V2SessionCatalogHandlerTest {
  private V2SessionCatalogHandler catalogHandler = new V2SessionCatalogHandler();
  private V2SessionCatalog catalog = mock(V2SessionCatalog.class);

  @Test
  void testGetDatasetIdentifierWhenOnlyTableLocationPresent() {
    Identifier id = Identifier.of(new String[] {"catalog", "schema"}, "table");

    HashMap<String, String> properties = new HashMap();
    properties.put("location", "file:/tmp/warehouse/schema.db/table");

    when(catalog.loadNamespaceMetadata(id.namespace())).thenReturn(Collections.emptyMap());

    DatasetIdentifier datasetIdentifier =
        catalogHandler.getDatasetIdentifier(mock(SparkSession.class), catalog, id, properties);

    assertThat(datasetIdentifier)
        .hasFieldOrPropertyWithValue("name", "/tmp/warehouse/schema.db/table")
        .hasFieldOrPropertyWithValue("namespace", "file");

    // no warehouse -> no symlinks
    assertThat(datasetIdentifier.getSymlinks()).hasSize(0);
  }

  @Test
  void testGetDatasetIdentifierWhenWarehouseLocationPresent() {
    Identifier id = Identifier.of(new String[] {"catalog", "schema"}, "table");

    HashMap<String, String> properties = new HashMap();
    properties.put("location", "file:/tmp/warehouse/schema.db/table");

    when(catalog.loadNamespaceMetadata(id.namespace()))
        .thenReturn(Collections.singletonMap("location", "file:/tmp/warehouse"));

    DatasetIdentifier datasetIdentifier =
        catalogHandler.getDatasetIdentifier(mock(SparkSession.class), catalog, id, properties);

    assertThat(datasetIdentifier)
        .hasFieldOrPropertyWithValue("name", "/tmp/warehouse/schema.db/table")
        .hasFieldOrPropertyWithValue("namespace", "file");

    assertThat(datasetIdentifier.getSymlinks()).hasSize(1);

    assertThat(datasetIdentifier.getSymlinks().get(0))
        .hasFieldOrPropertyWithValue("name", "catalog.schema.table")
        .hasFieldOrPropertyWithValue("namespace", "file:/tmp/warehouse")
        .hasFieldOrPropertyWithValue("type", SymlinkType.TABLE);
  }

  @Test
  void testGetDatasetIdentifierWhenNoTableLocationPresent() {
    Identifier id = Identifier.of(new String[] {"catalog", "schema"}, "table");

    HashMap<String, String> properties = new HashMap();

    when(catalog.loadNamespaceMetadata(id.namespace())).thenReturn(Collections.emptyMap());

    OpenLineageClientException thrown =
        assertThrows(
            OpenLineageClientException.class,
            () ->
                catalogHandler.getDatasetIdentifier(
                    mock(SparkSession.class), catalog, id, properties));

    assertThat(thrown.getMessage())
        .contains("Unable to extract DatasetIdentifier from V2SessionCatalog");
  }
}
