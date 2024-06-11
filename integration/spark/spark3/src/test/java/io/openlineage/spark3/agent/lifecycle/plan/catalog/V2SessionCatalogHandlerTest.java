/*
/* Copyright 2018-2024 contributors to the OpenLineage project
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
  void testGetDatasetIdentifierWhenLocationPresent() {
    Identifier id = Identifier.of(new String[] {"db", "schema"}, "table");
    when(catalog.loadNamespaceMetadata(id.namespace()))
        .thenReturn(Collections.singletonMap("location", "file:/tmp/warehouse/location"));

    DatasetIdentifier datasetIdentifier =
        catalogHandler.getDatasetIdentifier(mock(SparkSession.class), catalog, id, new HashMap<>());

    assertThat(datasetIdentifier.getName()).isEqualTo("/tmp/warehouse/location/table");
    assertThat("file").isEqualTo(datasetIdentifier.getNamespace());
    assertThat("db.schema.table").isEqualTo(datasetIdentifier.getSymlinks().get(0).getName());
    assertThat("file:/tmp/warehouse/location")
        .isEqualTo(datasetIdentifier.getSymlinks().get(0).getNamespace());
    assertThat(datasetIdentifier.getSymlinks().get(0).getType()).isEqualTo(SymlinkType.TABLE);
  }

  @Test
  void testGetDatasetIdentifierWhenNoLocationPresent() {
    Identifier id = Identifier.of(new String[] {"db", "schema"}, "table");
    when(catalog.loadNamespaceMetadata(id.namespace())).thenReturn(Collections.emptyMap());

    OpenLineageClientException thrown =
        assertThrows(
            OpenLineageClientException.class,
            () ->
                catalogHandler.getDatasetIdentifier(
                    mock(SparkSession.class), catalog, id, new HashMap<>()));

    assertThat(thrown.getMessage())
        .contains("Unable to extract DatasetIdentifier from V2SessionCatalog");
  }
}
