/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import java.io.File;
import java.net.URI;
import java.util.Optional;
import java.util.regex.Pattern;
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
  public static final String SPARK_OPENLINEAGE_DATASET_REMOVE_PATH_PATTERN =
      "spark.openlineage.dataset.removePath.pattern";
  public static final String REMOVE_PATTERN_GROUP = "remove";

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

    String name =
        Optional.of(uri.getPath())
            .map(PathUtils::removeFirstSlashIfSingleSlashInString)
            .map(PathUtils::removePathPattern)
            .get();

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

    DatasetIdentifier di;
    if (catalogTable.storage() != null && catalogTable.storage().locationUri().isDefined()) {
      log.error("From Storage");
      di = PathUtils.fromURI(catalogTable.storage().locationUri().get(), DEFAULT_SCHEME);
    } else {
      // try to obtain location
      try {
        log.error("From Default Table Path");
        di = prepareDatasetIdentifierFromDefaultTablePath(catalogTable);
      } catch (IllegalStateException e) {
        // session inactive - no way to find DatasetProvider
        throw new IllegalArgumentException(
            "Unable to extract DatasetIdentifier from a CatalogTable", e);
      }
    }

    if (metastoreUri.isPresent() && metastoreUri.get() != null) {
      log.error("From Metastore");
      // dealing with Hive tables
      DatasetIdentifier symlink = prepareHiveDatasetIdentifier(catalogTable, metastoreUri.get());
      return di.withSymlink(
          symlink.getName(), symlink.getNamespace(), DatasetIdentifier.SymlinkType.TABLE);
    } else {
      log.error("From Table Identifier");
      return di.withSymlink(
          nameFromTableIdentifier(catalogTable.identifier()),
          StringUtils.substringBeforeLast(di.getName(), File.separator),
          DatasetIdentifier.SymlinkType.TABLE);
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

    return PathUtils.fromURI(new URI(DEFAULT_SCHEME, null, path, null, null), DEFAULT_SCHEME);
  }

  @SneakyThrows
  private static DatasetIdentifier prepareHiveDatasetIdentifier(
      CatalogTable catalogTable, URI metastoreUri) {
    String qualifiedName = nameFromTableIdentifier(catalogTable.identifier());
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

  private static String removeFirstSlashIfSingleSlashInString(String name) {
    if (name.chars().filter(x -> x == '/').count() == 1 && name.startsWith("/")) {
      return name.substring(1);
    }
    return name;
  }

  private static String removePathPattern(String datasetName) {
    return loadSparkConf()
        .filter(conf -> conf.contains(SPARK_OPENLINEAGE_DATASET_REMOVE_PATH_PATTERN))
        .map(conf -> conf.get(SPARK_OPENLINEAGE_DATASET_REMOVE_PATH_PATTERN))
        .map(pattern -> Pattern.compile(pattern))
        .map(pattern -> pattern.matcher(datasetName))
        .filter(matcher -> matcher.find())
        .filter(
            matcher -> {
              try {
                matcher.group(REMOVE_PATTERN_GROUP);
                return true;
              } catch (IllegalStateException | IllegalArgumentException e) {
                return false;
              }
            })
        .filter(matcher -> StringUtils.isNotEmpty(matcher.group(REMOVE_PATTERN_GROUP)))
        .map(
            matcher ->
                datasetName.substring(0, matcher.start(REMOVE_PATTERN_GROUP))
                    + datasetName.substring(
                        matcher.end(REMOVE_PATTERN_GROUP), datasetName.length()))
        .orElse(datasetName);
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
