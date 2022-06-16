/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import java.net.URI;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.internal.StaticSQLConf;

@Slf4j
public class PathUtils {

  private static final String DEFAULT_SCHEME = "file";

  private static Optional<SparkConf> sparkConf = Optional.empty();

  public static DatasetIdentifier fromPath(Path path) {
    return PathUtils.fromPath(path, DEFAULT_SCHEME);
  }

  public static DatasetIdentifier fromPath(Path path, String defaultScheme) {
    if (path.isAbsoluteAndSchemeAuthorityNull()) {
      return new DatasetIdentifier(path.toString(), defaultScheme);
    }
    URI uri = path.toUri();
    String namespace =
        Optional.ofNullable(uri.getAuthority())
            .map(a -> String.format("%s://%s", uri.getScheme(), a))
            .orElseGet(() -> (uri.getScheme() != null) ? uri.getScheme() : defaultScheme);
    String name = removeFirstSlashIfSingleSlashInString(uri.getPath());
    return new DatasetIdentifier(name, namespace);
  }

  public static DatasetIdentifier fromURI(URI location, String defaultScheme) {
    return fromPath(new Path(location), defaultScheme);
  }

  public static DatasetIdentifier fromCatalogTable(CatalogTable catalogTable) {
    return fromCatalogTable(catalogTable, loadSparkConf());
  }

  /**
   * Create DatasetIdentifier from CatalogTable, using storage's locationURI if it exists. In other
   * way, use defaultTablePath.
   */
  @SneakyThrows
  public static DatasetIdentifier fromCatalogTable(
      CatalogTable catalogTable, Optional<SparkConf> sparkConf) {
    Optional<URI> metastoreUri = extractMetastoreUri(sparkConf);
    if (metastoreUri.isPresent() && metastoreUri.get() != null) {
      // dealing with Hive tables
      return prepareHiveDatasetIdentifier(catalogTable, metastoreUri.get());
    } else {
      if (catalogTable.storage() != null && catalogTable.storage().locationUri().isDefined()) {
        // location is present -> use it for dataset identifier with `file:/` scheme
        return PathUtils.fromURI(catalogTable.storage().locationUri().get(), "file");
      }

      try {
        // read it from default table path
        return prepareDatasetIdentifierFromDefaultTablePath(catalogTable);
      } catch (IllegalStateException e) {
        // session inactive - no way to find DatasetProvider
        throw new IllegalArgumentException(
            "Unable to extract DatasetIdentifier from a CatalogTable", e);
      }
    }
  }

  @SneakyThrows
  private static DatasetIdentifier prepareDatasetIdentifierFromDefaultTablePath(
      CatalogTable catalogTable) {
    String path =
        SparkSession.active()
            .sessionState()
            .catalog()
            .defaultTablePath(catalogTable.identifier())
            .getPath();

    return PathUtils.fromURI(new URI("file", null, path, null, null), "file");
  }

  @SneakyThrows
  private static DatasetIdentifier prepareHiveDatasetIdentifier(
      CatalogTable catalogTable, URI metastoreUri) {
    String qualifiedName = catalogTable.qualifiedName();
    if (!qualifiedName.startsWith("/")) {
      qualifiedName = String.format("/%s", qualifiedName);
    }
    return PathUtils.fromPath(
        new Path(enrichHiveMetastoreURIWithTableName(metastoreUri, qualifiedName)));
  }

  @SneakyThrows
  public static URI enrichHiveMetastoreURIWithTableName(URI metastoreUri, String qualifiedName) {
    return new URI(
        "hive", null, metastoreUri.getHost(), metastoreUri.getPort(), qualifiedName, null, null);
  }

  /**
   * SparkConf does not change through job lifetime but it can get lost once session is closed. It's
   * good to have it set in case of SPARK-29046
   */
  private static Optional<SparkConf> loadSparkConf() {
    if (!sparkConf.isPresent() && SparkSession.getActiveSession().isDefined()) {
      sparkConf = Optional.of(SparkSession.getActiveSession().get().sparkContext().getConf());
    }
    return sparkConf;
  }

  private static Optional<URI> extractMetastoreUri(Optional<SparkConf> sparkConf) {
    // make sure SparkConf is present
    if (!sparkConf.isPresent()) {
      return Optional.empty();
    }

    // make sure enableHiveSupport is called
    Optional<String> setting =
        SparkConfUtils.findSparkConfigKey(
            sparkConf.get(), StaticSQLConf.CATALOG_IMPLEMENTATION().key());
    if (!setting.isPresent() || !setting.get().equals("hive")) {
      return Optional.empty();
    }

    return SparkConfUtils.getMetastoreUri(sparkConf.get());
  }

  private static String removeFirstSlashIfSingleSlashInString(String name) {
    if (name.chars().filter(x -> x == '/').count() == 1 && name.startsWith("/")) {
      return name.substring(1);
    }
    return name;
  }
}
