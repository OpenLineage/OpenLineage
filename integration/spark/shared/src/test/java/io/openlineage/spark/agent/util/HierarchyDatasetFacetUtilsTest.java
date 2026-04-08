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

  private static final String DATABASE = "DATABASE";
  private static final String SCHEMA = "SCHEMA";
  private static final String TABLE = "TABLE";
  private static final String MY_DATABASE = "my_database";
  private static final String MY_TABLE = "my_table";
  private static final String MY_SCHEMA = "my_schema";

  OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);

  TableCatalog tableCatalog;

  @BeforeEach
  void setup() {
    tableCatalog = mock(TableCatalog.class);
    when(tableCatalog.name()).thenReturn("my_catalog");
  }

  @Test
  void testV2IdentifierWithOneNamespacePart() {
    Identifier identifier = Identifier.of(new String[] {MY_DATABASE}, MY_TABLE);

    HierarchyDatasetFacet facet =
        HierarchyDatasetFacetUtils.buildHierarchyFacet(openLineage, tableCatalog, identifier);

    List<HierarchyDatasetFacetLevel> levels = facet.getHierarchy();
    assertThat(levels).hasSize(2);
    assertLevel(levels.get(0), DATABASE, MY_DATABASE);
    assertLevel(levels.get(1), TABLE, MY_TABLE);
  }

  @Test
  void testV2IdentifierWithTwoNamespaceParts() {
    Identifier identifier = Identifier.of(new String[] {MY_DATABASE, MY_SCHEMA}, MY_TABLE);

    HierarchyDatasetFacet facet =
        HierarchyDatasetFacetUtils.buildHierarchyFacet(openLineage, tableCatalog, identifier);

    List<HierarchyDatasetFacetLevel> levels = facet.getHierarchy();
    assertThat(levels).hasSize(3);
    assertLevel(levels.get(0), DATABASE, MY_DATABASE);
    assertLevel(levels.get(1), SCHEMA, MY_SCHEMA);
    assertLevel(levels.get(2), TABLE, MY_TABLE);
  }

  @Test
  void testV2IdentifierWithNoNamespaceParts() {
    Identifier identifier = Identifier.of(new String[] {}, MY_TABLE);

    HierarchyDatasetFacet facet =
        HierarchyDatasetFacetUtils.buildHierarchyFacet(openLineage, tableCatalog, identifier);

    List<HierarchyDatasetFacetLevel> levels = facet.getHierarchy();
    assertThat(levels).hasSize(1);
    assertLevel(levels.get(0), TABLE, MY_TABLE);
  }

  @Test
  void testV1TableIdentifierWithDatabase() {
    TableIdentifier identifier = mock(TableIdentifier.class);
    when(identifier.database()).thenReturn(Option.apply(MY_DATABASE));
    when(identifier.table()).thenReturn(MY_TABLE);

    HierarchyDatasetFacet facet =
        HierarchyDatasetFacetUtils.buildHierarchyFacet(openLineage, identifier);

    List<HierarchyDatasetFacetLevel> levels = facet.getHierarchy();
    assertThat(levels).hasSize(2);
    assertLevel(levels.get(0), DATABASE, MY_DATABASE);
    assertLevel(levels.get(1), TABLE, MY_TABLE);
  }

  @Test
  void testV1TableIdentifierWithoutDatabase() {
    TableIdentifier identifier = mock(TableIdentifier.class);
    when(identifier.database()).thenReturn(Option.empty());
    when(identifier.table()).thenReturn(MY_TABLE);

    HierarchyDatasetFacet facet =
        HierarchyDatasetFacetUtils.buildHierarchyFacet(openLineage, identifier);

    List<HierarchyDatasetFacetLevel> levels = facet.getHierarchy();
    assertThat(levels).hasSize(1);
    assertLevel(levels.get(0), TABLE, MY_TABLE);
  }

  @Test
  void testV1TableIdentifierWithCatalogName() {
    TableIdentifier identifier = mock(TableIdentifier.class);
    when(identifier.database()).thenReturn(Option.apply(MY_DATABASE));
    when(identifier.table()).thenReturn(MY_TABLE);

    HierarchyDatasetFacet facet =
        HierarchyDatasetFacetUtils.buildHierarchyFacet(openLineage, "my_catalog", identifier);

    List<HierarchyDatasetFacetLevel> levels = facet.getHierarchy();
    assertThat(levels).hasSize(2);
    assertLevel(levels.get(0), DATABASE, MY_DATABASE);
    assertLevel(levels.get(1), TABLE, MY_TABLE);
  }

  private void assertLevel(
      HierarchyDatasetFacetLevel level, String expectedType, String expectedName) {
    assertThat(level.getType()).isEqualTo(expectedType);
    assertThat(level.getName()).isEqualTo(expectedName);
  }
}
