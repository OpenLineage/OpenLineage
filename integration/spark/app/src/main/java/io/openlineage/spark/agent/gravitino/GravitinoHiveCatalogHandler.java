/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.gravitino;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.DatasetIdentifier.SymlinkType;
import io.openlineage.spark.agent.lifecycle.plan.catalog.CatalogHandler;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.MissingDatasetIdentifierCatalogException;
import java.util.Map;
import java.util.StringJoiner;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;

/** Resolves Gravitino's Hive V2 catalog to its logical metalake/catalog/schema/table identity. */
public final class GravitinoHiveCatalogHandler implements CatalogHandler {
  private static final String HIVE_CATALOG_CLASS_PREFIX =
      "org.apache.gravitino.spark.connector.hive.GravitinoHiveCatalog";
  private static final String METALAKE_CONF = "spark.sql.gravitino.metalake";

  @Override
  public boolean hasClasses() {
    // The connector is loaded by Spark at runtime; avoid a compile-time dependency on it.
    return true;
  }

  @Override
  public boolean isClass(TableCatalog catalog) {
    return isGravitinoHiveCatalog(catalog.getClass().getName());
  }

  static boolean isGravitinoHiveCatalog(String className) {
    return className.startsWith(HIVE_CATALOG_CLASS_PREFIX);
  }

  @Override
  public DatasetIdentifier getDatasetIdentifier(
      SparkSession session,
      TableCatalog catalog,
      Identifier identifier,
      Map<String, String> properties) {
    String metalake = session.conf().get(METALAKE_CONF, "").trim();
    if (metalake.isEmpty()) {
      throw new MissingDatasetIdentifierCatalogException("Missing " + METALAKE_CONF);
    }

    StringJoiner name = new StringJoiner(".");
    name.add(catalog.name());
    for (String part : identifier.namespace()) {
      name.add(part);
    }
    name.add(identifier.name());
    StringJoiner table = new StringJoiner(".");
    for (String part : identifier.namespace()) table.add(part);
    table.add(identifier.name());
    return new DatasetIdentifier(name.toString(), metalake)
        .withSymlink(
            table.toString(), "gravitino://" + metalake + "/" + catalog.name(), SymlinkType.TABLE);
  }

  @Override
  public String getName() {
    return "gravitino-hive";
  }
}
