/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.gravitino;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.lifecycle.plan.catalog.CatalogHandler;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.MissingDatasetIdentifierCatalogException;
import java.util.Map;
import java.util.StringJoiner;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;

/** Resolves Lance REST tables through their Gravitino parent catalog. */
public final class LanceCatalogHandler implements CatalogHandler {
  private static final String LANCE_CATALOG_CLASS = "org.lance.spark.LanceNamespaceSparkCatalog";

  @Override
  public boolean hasClasses() {
    return true;
  }

  @Override
  public boolean isClass(TableCatalog catalog) {
    return LANCE_CATALOG_CLASS.equals(catalog.getClass().getName());
  }

  @Override
  public DatasetIdentifier getDatasetIdentifier(
      SparkSession session,
      TableCatalog catalog,
      Identifier identifier,
      Map<String, String> properties) {
    String prefix = "spark.sql.catalog." + catalog.name();
    String uri = session.conf().get(prefix + ".uri", "").trim();
    String parentCatalog = session.conf().get(prefix + ".parent", "").trim();
    if (uri.isEmpty() || parentCatalog.isEmpty()) {
      throw new MissingDatasetIdentifierCatalogException(
          "Lance REST URI or parent catalog is missing");
    }

    StringJoiner name = new StringJoiner(".");
    name.add(parentCatalog);
    for (String part : identifier.namespace()) {
      name.add(part);
    }
    name.add(identifier.name());
    // The REST endpoint identifies the source. Gravitino discovery resolves its managed identity;
    // the OpenLineage Job namespace must never determine a Dataset's metalake.
    return new DatasetIdentifier(name.toString(), uri);
  }

  @Override
  public String getName() {
    return "gravitino-lance";
  }
}
