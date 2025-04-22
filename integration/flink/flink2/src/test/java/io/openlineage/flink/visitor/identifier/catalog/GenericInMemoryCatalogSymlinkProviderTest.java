/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.identifier.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.DatasetIdentifier.SymlinkType;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.listener.CatalogContext;
import org.junit.jupiter.api.Test;

public class GenericInMemoryCatalogSymlinkProviderTest {

  GenericInMemoryCatalogSymlinkProvider provider = new GenericInMemoryCatalogSymlinkProvider();
  CatalogContext catalogContext = mock(CatalogContext.class);
  LineageDataset lineageDataset = mock(LineageDataset.class);

  @Test
  void testIsDefined() {
    assertThat(provider.isDefinedAt(GenericInMemoryCatalog.class)).isTrue();
    assertThat(provider.isDefinedAt(AbstractCatalog.class)).isFalse();
  }

  @Test
  void testGetSymlink() {
    when(lineageDataset.name()).thenReturn("catalogName.tableName");
    when(catalogContext.getCatalogName()).thenReturn("catalogName");
    assertThat(provider.getSymlink(catalogContext, lineageDataset))
        .isPresent()
        .get()
        .hasFieldOrPropertyWithValue("namespace", "flink://")
        .hasFieldOrPropertyWithValue("name", "tableName")
        .hasFieldOrPropertyWithValue("type", SymlinkType.TABLE);
  }

  @Test
  void testGetSymlinkRemovesOnlyCatalogFromDatasetName() {
    when(lineageDataset.name()).thenReturn("catalogName.catalogName.tableName");
    when(catalogContext.getCatalogName()).thenReturn("catalogName");
    assertThat(provider.getSymlink(catalogContext, lineageDataset))
        .isPresent()
        .get()
        .hasFieldOrPropertyWithValue("namespace", "flink://")
        .hasFieldOrPropertyWithValue("name", "catalogName.tableName")
        .hasFieldOrPropertyWithValue("type", SymlinkType.TABLE);
  }
}
