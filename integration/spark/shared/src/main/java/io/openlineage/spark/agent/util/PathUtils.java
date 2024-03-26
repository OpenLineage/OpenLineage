/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.DatasetIdentifierUtils;
import java.io.File;
import java.net.URI;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.internal.StaticSQLConf;

@Slf4j
public class PathUtils {

  private static final String DEFAULT_SCHEME = "file";
  private static final String DEFAULT_SEPARATOR = "/";
  private static Optional<SparkConf> sparkConf = Optional.empty();

  public static DatasetIdentifier fromPath(Path path) {
    return fromPath(path, DEFAULT_SCHEME);
  }

  public static DatasetIdentifier fromPath(Path path, String defaultScheme) {
    return fromURI(path.toUri(), defaultScheme);
  }

  public static DatasetIdentifier fromURI(URI location) {
    return fromURI(location, DEFAULT_SCHEME);
  }

  public static DatasetIdentifier fromURI(URI location, String defaultScheme) {
    return DatasetIdentifierUtils.fromURI(location, defaultScheme);
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

    DatasetIdentifier di;
    URI uri;

    if (catalogTable.storage() != null && catalogTable.storage().locationUri().isDefined()) {
      uri = prepareUriFromLocation(catalogTable);
      di = PathUtils.fromURI(uri, DEFAULT_SCHEME);
    } else {
      // try to obtain location
      try {
        uri = prepareUriFromDefaultTablePath(catalogTable);
        di = PathUtils.fromURI(uri, DEFAULT_SCHEME);
      } catch (IllegalStateException e) {
        // session inactive - no way to find DatasetProvider
        throw new IllegalArgumentException(
            "Unable to extract DatasetIdentifier from a CatalogTable", e);
      }
    }

    Optional<URI> metastoreUri = extractMetastoreUri(sparkConf);
    // TODO: Is the call to "metastoreUri.get()" really needed?
    //   Java's Optional should prevent the null in the first place.
    if (metastoreUri.isPresent() && metastoreUri.get() != null) {
      // dealing with Hive tables
      DatasetIdentifier symlink = prepareHiveDatasetIdentifier(catalogTable, metastoreUri.get());
      return di.withSymlink(
          symlink.getName(), symlink.getNamespace(), DatasetIdentifier.SymlinkType.TABLE);
    } else {

      return di.withSymlink(
          nameFromTableIdentifier(catalogTable.identifier()),
          StringUtils.substringBeforeLast(uri.toString(), File.separator),
          DatasetIdentifier.SymlinkType.TABLE);
    }
  }

  private static URI prepareUriFromDefaultTablePath(CatalogTable catalogTable) {
    URI uri =
        SparkSession.active().sessionState().catalog().defaultTablePath(catalogTable.identifier());

    return uri;
  }

  @SneakyThrows
  private static URI prepareUriFromLocation(CatalogTable catalogTable) {
    URI uri = catalogTable.storage().locationUri().get();

    if (uri.getPath() != null
        && uri.getPath().startsWith(DEFAULT_SEPARATOR)
        && uri.getScheme() == null) {
      uri = new URI(DEFAULT_SCHEME, null, uri.getPath(), null, null);
    } else if (uri.getScheme() != null && uri.getScheme().equals(DEFAULT_SCHEME)) {
      // Normalize the URI if it is already a file scheme but has three slashes
      String path = uri.getPath();
      if (uri.toString().startsWith(DEFAULT_SCHEME + ":///")) {
        uri = new URI(DEFAULT_SCHEME, null, path, null, null);
      }
    }

    return uri;
  }

  @SneakyThrows
  private static DatasetIdentifier prepareHiveDatasetIdentifier(
      CatalogTable catalogTable, URI metastoreUri) {
    String qualifiedName = nameFromTableIdentifier(catalogTable.identifier());
    if (!qualifiedName.startsWith(DEFAULT_SEPARATOR)) {
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
    if (!sparkConf.isPresent() && SparkSession.getDefaultSession().isDefined()) {
      sparkConf = Optional.of(SparkSession.getDefaultSession().get().sparkContext().getConf());
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
    if (!setting.isPresent() || !"hive".equals(setting.get())) {
      return Optional.empty();
    }

    return SparkConfUtils.getMetastoreUri(sparkConf.get());
  }

  private static String nameFromTableIdentifier(TableIdentifier identifier) {
    // we create name instead of calling `unquotedString` method which includes spark_catalog
    // for Spark 3.4
    String name;
    if (identifier.database().isDefined()) {
      // include database in name
      name = String.format("%s.%s", identifier.database().get(), identifier.table());
    } else {
      // just table name
      name = identifier.table();
    }

    return name;
  }
}
