/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.HierarchyDatasetFacet;
import io.openlineage.client.OpenLineage.HierarchyDatasetFacetLevel;
import io.openlineage.spark.agent.Versions;
import java.util.List;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Option;

class HierarchyDatasetFacetUtilsTest {

  OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);

  TableCatalog tableCatalog;

  @BeforeEach
  void setup() {
    tableCatalog = mock(TableCatalog.class);
    when(tableCatalog.name()).thenReturn("my_catalog");
  }

  @Test
  void testV2IdentifierWithOneNamespacePart() {
    Identifier identifier = Identifier.of(new String[] {"my_database"}, "my_table");

    HierarchyDatasetFacet facet =
        HierarchyDatasetFacetUtils.buildHierarchyFacet(openLineage, tableCatalog, identifier);

    List<HierarchyDatasetFacetLevel> levels = facet.getHierarchy();
    assertThat(levels).hasSize(2);
    assertLevel(levels.get(0), "DATABASE", "my_database");
    assertLevel(levels.get(1), "TABLE", "my_table");
  }

  @Test
  void testV2IdentifierWithTwoNamespaceParts() {
    Identifier identifier = Identifier.of(new String[] {"my_database", "my_schema"}, "my_table");

    HierarchyDatasetFacet facet =
        HierarchyDatasetFacetUtils.buildHierarchyFacet(openLineage, tableCatalog, identifier);

    List<HierarchyDatasetFacetLevel> levels = facet.getHierarchy();
    assertThat(levels).hasSize(3);
    assertLevel(levels.get(0), "DATABASE", "my_database");
    assertLevel(levels.get(1), "SCHEMA", "my_schema");
    assertLevel(levels.get(2), "TABLE", "my_table");
  }

  @Test
  void testV2IdentifierWithNoNamespaceParts() {
    Identifier identifier = Identifier.of(new String[] {}, "my_table");

    HierarchyDatasetFacet facet =
        HierarchyDatasetFacetUtils.buildHierarchyFacet(openLineage, tableCatalog, identifier);

    List<HierarchyDatasetFacetLevel> levels = facet.getHierarchy();
    assertThat(levels).hasSize(1);
    assertLevel(levels.get(0), "TABLE", "my_table");
  }

  @Test
  void testV1TableIdentifierWithDatabase() {
    TableIdentifier identifier = mock(TableIdentifier.class);
    when(identifier.database()).thenReturn(Option.apply("my_database"));
    when(identifier.table()).thenReturn("my_table");

    HierarchyDatasetFacet facet =
        HierarchyDatasetFacetUtils.buildHierarchyFacet(openLineage, identifier);

    List<HierarchyDatasetFacetLevel> levels = facet.getHierarchy();
    assertThat(levels).hasSize(2);
    assertLevel(levels.get(0), "DATABASE", "my_database");
    assertLevel(levels.get(1), "TABLE", "my_table");
  }

  @Test
  void testV1TableIdentifierWithoutDatabase() {
    TableIdentifier identifier = mock(TableIdentifier.class);
    when(identifier.database()).thenReturn(Option.empty());
    when(identifier.table()).thenReturn("my_table");

    HierarchyDatasetFacet facet =
        HierarchyDatasetFacetUtils.buildHierarchyFacet(openLineage, identifier);

    List<HierarchyDatasetFacetLevel> levels = facet.getHierarchy();
    assertThat(levels).hasSize(1);
    assertLevel(levels.get(0), "TABLE", "my_table");
  }

  @Test
  void testV1TableIdentifierWithCatalogName() {
    TableIdentifier identifier = mock(TableIdentifier.class);
    when(identifier.database()).thenReturn(Option.apply("my_database"));
    when(identifier.table()).thenReturn("my_table");

    HierarchyDatasetFacet facet =
        HierarchyDatasetFacetUtils.buildHierarchyFacet(openLineage, "my_catalog", identifier);

    List<HierarchyDatasetFacetLevel> levels = facet.getHierarchy();
    assertThat(levels).hasSize(2);
    assertLevel(levels.get(0), "DATABASE", "my_database");
    assertLevel(levels.get(1), "TABLE", "my_table");
  }

  private void assertLevel(
      HierarchyDatasetFacetLevel level, String expectedType, String expectedName) {
    assertThat(level.getType()).isEqualTo(expectedType);
    assertThat(level.getName()).isEqualTo(expectedName);
  }
}
