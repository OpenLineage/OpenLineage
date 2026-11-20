/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg;

import java.util.Collections;
import lombok.SneakyThrows;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.iceberg.CachingCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.spark.PathIdentifier;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Loads Iceberg tables for lineage without adding them to the session's table cache.
 *
 * <p>Iceberg is optional at runtime, so this subclass must stay out of the catalog handlers:
 * verifying a handler that instantiates it would load {@link SparkCatalog}.
 */
final class IsolatedSparkCatalog extends SparkCatalog {

  private final Catalog isolated;

  private IsolatedSparkCatalog(Catalog isolated) {
    this.isolated = isolated;
  }

  @Override
  protected Catalog buildIcebergCatalog(String name, CaseInsensitiveStringMap options) {
    return isolated;
  }

  @SneakyThrows
  static Table loadTable(SparkCatalog catalog, Identifier identifier) {
    if (identifier instanceof PathIdentifier
        || !(catalog.icebergCatalog() instanceof CachingCatalog)) {
      return catalog.loadTable(identifier);
    }

    IsolatedSparkCatalog lookup = new IsolatedSparkCatalog(isolate(catalog.icebergCatalog()));
    SparkSession.active()
        .withActive(
            () -> {
              lookup.initialize(
                  catalog.name(),
                  new CaseInsensitiveStringMap(
                      Collections.singletonMap(CatalogProperties.CACHE_ENABLED, "false")));
              return null;
            });
    return lookup.loadTable(identifier);
  }

  static Catalog isolate(Catalog catalog) throws IllegalAccessException {
    if (!(catalog instanceof CachingCatalog)) {
      return catalog;
    }

    boolean caseSensitive = true;
    Catalog delegate = catalog;
    while (delegate instanceof CachingCatalog) {
      // Iceberg exposes neither field:
      // https://github.com/apache/iceberg/blob/5f7c992ca673bf41df1d37543b24d646c24568a9/core/src/main/java/org/apache/iceberg/CachingCatalog.java#L63-L64
      caseSensitive &= (boolean) FieldUtils.readField(delegate, "caseSensitive", true);
      delegate = (Catalog) FieldUtils.readField(delegate, "catalog", true);
    }
    // Retain Iceberg's case and metadata-table rules without updating the session cache.
    return CachingCatalog.wrap(
        delegate, caseSensitive, CatalogProperties.CACHE_EXPIRATION_INTERVAL_MS_OFF);
  }
}
