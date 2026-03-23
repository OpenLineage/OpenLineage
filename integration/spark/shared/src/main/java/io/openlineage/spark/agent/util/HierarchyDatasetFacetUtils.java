/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.HierarchyDatasetFacet;
import io.openlineage.client.OpenLineage.HierarchyDatasetFacetLevel;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import scala.Option;

/** Utility class for building {@link HierarchyDatasetFacet} from various Spark catalog types. */
public class HierarchyDatasetFacetUtils {

  private static final String DATABASE = "DATABASE";
  private static final String SCHEMA = "SCHEMA";
  private static final String TABLE = "TABLE";

  /**
   * Build a {@link HierarchyDatasetFacet} from a V2 catalog identifier.
   *
   * <p>Mapping logic for namespace levels:
   *
   * <ul>
   *   <li>0 namespace parts: TABLE
   *   <li>1 namespace part: DATABASE + TABLE
   *   <li>2+ namespace parts: DATABASE + SCHEMA + ... + TABLE
   * </ul>
   *
   * <p>The catalog name is not included in the hierarchy as it is already captured by the {@code
   * catalog} facet.
   *
   * @param openLineage the OpenLineage instance used for facet construction
   * @param tableCatalog the V2 table catalog (unused, catalog info goes in the catalog facet)
   * @param identifier the V2 table identifier (provides namespace + table name)
   * @return a {@link HierarchyDatasetFacet} with the appropriate hierarchy levels
   */
  @SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
  public static HierarchyDatasetFacet buildHierarchyFacet(
      OpenLineage openLineage, TableCatalog tableCatalog, Identifier identifier) {
    List<HierarchyDatasetFacetLevel> levels = new ArrayList<>();

    String[] namespace = identifier.namespace();
    if (namespace.length == 1) {
      levels.add(level(openLineage, DATABASE, namespace[0]));
    } else if (namespace.length >= 2) {
      levels.add(level(openLineage, DATABASE, namespace[0]));
      for (int i = 1; i < namespace.length; i++) {
        levels.add(level(openLineage, SCHEMA, namespace[i]));
      }
    }

    levels.add(level(openLineage, TABLE, identifier.name()));
    return openLineage.newHierarchyDatasetFacet(levels);
  }

  /**
   * Build a {@link HierarchyDatasetFacet} from a V1 {@link TableIdentifier}.
   *
   * @param openLineage the OpenLineage instance used for facet construction
   * @param identifier the V1 table identifier (optional database + table name)
   * @return a {@link HierarchyDatasetFacet} with the appropriate hierarchy levels
   */
  public static HierarchyDatasetFacet buildHierarchyFacet(
      OpenLineage openLineage, TableIdentifier identifier) {
    List<HierarchyDatasetFacetLevel> levels = new ArrayList<>();
    addDatabaseLevel(openLineage, identifier.database(), levels);
    levels.add(level(openLineage, TABLE, identifier.table()));
    return openLineage.newHierarchyDatasetFacet(levels);
  }

  /**
   * Build a {@link HierarchyDatasetFacet} from a V1 {@link TableIdentifier} with an explicit
   * catalog name.
   *
   * <p>The catalog name is not included in the hierarchy as it is already captured by the {@code
   * catalog} facet.
   *
   * @param openLineage the OpenLineage instance used for facet construction
   * @param catalogName the catalog name (unused, catalog info goes in the catalog facet)
   * @param identifier the V1 table identifier (optional database + table name)
   * @return a {@link HierarchyDatasetFacet} with the appropriate hierarchy levels
   */
  public static HierarchyDatasetFacet buildHierarchyFacet(
      OpenLineage openLineage, String catalogName, TableIdentifier identifier) {
    List<HierarchyDatasetFacetLevel> levels = new ArrayList<>();
    addDatabaseLevel(openLineage, identifier.database(), levels);
    levels.add(level(openLineage, TABLE, identifier.table()));
    return openLineage.newHierarchyDatasetFacet(levels);
  }

  private static void addDatabaseLevel(
      OpenLineage openLineage, Option<String> database, List<HierarchyDatasetFacetLevel> levels) {
    if (database != null && database.isDefined()) {
      levels.add(level(openLineage, DATABASE, database.get()));
    }
  }

  private static HierarchyDatasetFacetLevel level(
      OpenLineage openLineage, String type, String name) {
    return openLineage.newHierarchyDatasetFacetLevel(type, name);
  }
}
