/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg;

import static io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg.IcebergHandler.TYPE;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.SparkConfUtils;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.MissingDatasetIdentifierCatalogException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;

@Slf4j
class HiveCatalogTypeHandler extends BaseCatalogTypeHandler {
  // NOTE: This is default handler, so it contains the logic for edge cases that can happen for
  // undefined catalogs

  private static final String HIVE_CATALOG_TYPE = "hive";
  private static final String ICEBERG_PATH_IDENTIFIER_CLASS_NAME =
      "org.apache.iceberg.spark.PathIdentifier";

  @Override
  String getType() {
    return HIVE_CATALOG_TYPE;
  }

  @Override
  boolean matchesCatalogType(Map<String, String> catalogConf) {
    return HIVE_CATALOG_TYPE.equalsIgnoreCase(catalogConf.get(TYPE));
  }

  @Override
  @SneakyThrows
  DatasetIdentifier getPrimaryIdentifier(
      SparkSession session,
      Map<String, String> catalogConf,
      Identifier identifier,
      TableCatalog tableCatalog) {

    // Several things to be aware of:
    // 1. You can read iceberg data without using an Iceberg catalog
    // 2. You can't write iceberg data without using an Iceberg catalog (Spark crashes)
    // 3. Iceberg will configure a default catalog called "default_iceberg". This catalog (usually)
    // lacks the warehouse property.
    // 4. When you read the metadata.json path of an Iceberg dataset, the concrete type of the
    // Identifier interface is "org.apache.iceberg.spark.PathIdentifier"

    // A heuristic to check for:
    // Is the catalog name "default_iceberg"?
    // Is the warehouse property set?
    // Is the identifier of type "org.apache.iceberg.spark.PathIdentifier"?
    // If the answer to all 3 is "YES" then we cannot assume that we are reading from a catalog that
    // belongs to this Spark application
    String warehouseLocation = catalogConf.get(CatalogProperties.WAREHOUSE_LOCATION);
    boolean isDefaultIcebergCatalog = "default_iceberg".equals(tableCatalog.name());
    boolean lacksWarehouseProperty =
        warehouseLocation == null || warehouseLocation.trim().isEmpty();
    boolean isPathIdentifier =
        ICEBERG_PATH_IDENTIFIER_CLASS_NAME.equals(identifier.getClass().getName());
    if (isDefaultIcebergCatalog && lacksWarehouseProperty && isPathIdentifier) {
      if (log.isDebugEnabled()) {
        log.debug(
            "Encountered an Iceberg-formatted dataset ({}) that does not belong to the configured Iceberg catalog (catalog={})",
            identifierToString(identifier),
            tableCatalog.name());
      }

      return getIcebergTable(tableCatalog, identifier)
          .map(tbl -> PathUtils.fromPath(new Path(tbl.location())))
          .orElseThrow(
              () ->
                  new MissingDatasetIdentifierCatalogException(
                      String.format(
                          "Unable to determine the location of the Iceberg dataset %s in catalog %s",
                          identifierToString(identifier), tableCatalog.name())));
    }

    DatasetIdentifier base = super.getPrimaryIdentifier(session, catalogConf, identifier, tableCatalog);
    try {
      return base.withSymlink(
          new DatasetIdentifier.Symlink(
              identifier.toString(),
              PathUtils.prepareHiveUri(getUri(session, catalogConf)).toString(),
              DatasetIdentifier.SymlinkType.TABLE));
    } catch (Exception e) {
      // The Hive symlink is supplementary; a metastore URI we can't resolve shouldn't take down
      // the whole dataset identifier that was already successfully computed above.
      log.debug("Couldn't resolve Hive metastore URI for symlink on {}, skipping", identifier, e);
      return base;
    }
  }

  private static URI getUri(SparkSession session, Map<String, String> catalogConf)
      throws URISyntaxException {
    URI metastoreUri;
    String confUri = catalogConf.get(CatalogProperties.URI);
    if (confUri == null) {
      metastoreUri =
          SparkConfUtils.getMetastoreUri(session.sparkContext())
              .orElseThrow(() -> new MissingDatasetIdentifierCatalogException(HIVE_CATALOG_TYPE));
    } else {
      metastoreUri = new URI(confUri);
    }
    return metastoreUri;
  }

  @Override
  @SneakyThrows
  Optional<DatasetIdentifier.Symlink> getSymlinkIdentifiers(
      SparkSession session, Map<String, String> catalogConf, String table) {
    return Optional.empty();
  }

  private String identifierToString(Identifier identifier) {
    Class<? extends Identifier> cls = identifier.getClass();
    String[] namespace = identifier.namespace();
    String ns = namespace.length > 1 ? Arrays.toString(namespace) : namespace[0];
    return String.format("%s(namespace=%s; name=%s)", cls.getSimpleName(), ns, identifier.name());
  }
}
