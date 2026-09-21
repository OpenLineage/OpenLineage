/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan.catalog;

import io.openlineage.client.utils.DatasetIdentifier;
import java.util.Optional;
import lombok.Getter;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;

public interface RelationHandler {

  /**
   * The catalog that owns the table a relation points at, together with the table's identifier
   * within it. Both may differ from {@code relation.catalog()} / {@code relation.identifier()} when
   * the relation was loaded through an intermediate catalog.
   */
  @Getter
  class OwningCatalog {
    private final TableCatalog catalog;
    private final Identifier identifier;

    private OwningCatalog(TableCatalog catalog, Identifier identifier) {
      this.catalog = catalog;
      this.identifier = identifier;
    }

    public static OwningCatalog of(TableCatalog catalog, Identifier identifier) {
      return new OwningCatalog(catalog, identifier);
    }
  }

  boolean hasClasses();

  boolean isClass(DataSourceV2Relation relation);

  DatasetIdentifier getDatasetIdentifier(DataSourceV2Relation relation);

  /**
   * The catalog that owns the relation's table, when the handler can recover one. Used to resolve
   * facets - storage, catalog, dataset version - which otherwise would be looked up against a
   * catalog no {@link CatalogHandler} supports and silently come back empty.
   */
  default Optional<OwningCatalog> getOwningCatalog(DataSourceV2Relation relation) {
    return Optional.empty();
  }

  String getName();
}
