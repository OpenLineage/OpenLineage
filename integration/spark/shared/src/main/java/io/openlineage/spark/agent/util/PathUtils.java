/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.filesystem.FilesystemDatasetUtils;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.internal.StaticSQLConf;

@Slf4j
@SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
public class PathUtils {
  private static final String DEFAULT_DB = "default";
  public static final String GLUE_TABLE_PREFIX = "table/";

  public static DatasetIdentifier fromPath(Path path) {
    return fromURI(path.toUri());
  }

  public static DatasetIdentifier fromURI(URI location) {
    return FilesystemDatasetUtils.fromLocation(location);
  }

  public static List<Path> getDirectoryPaths(
      Collection<Path> paths, Configuration hadoopConf, boolean normalizeHiveStylePartitioning) {
    LinkedHashSet<Path> normalizedPaths = new LinkedHashSet<>();
    for (Path path : collapsePathsWithSameParent(paths)) {
      if (hasNormalizedAncestor(normalizedPaths, path)) {
        continue;
      }

      Optional<Path> directoryPath = getDirectoryPath(path, hadoopConf);
      if (normalizeHiveStylePartitioning) {
        directoryPath = directoryPath.map(PathUtils::normalizeHiveStylePartitioning);
      }
      directoryPath.ifPresent(normalizedPaths::add);
    }
    return new ArrayList<>(normalizedPaths);
  }

  private static List<Path> collapsePathsWithSameParent(Collection<Path> paths) {
    if (paths.size() <= 1) {
      return new ArrayList<>(paths);
    }

    Map<Path, Long> parentCounts =
        paths.stream()
            .map(Path::getParent)
            .filter(parent -> parent != null)
            .collect(
                Collectors.groupingBy(parent -> parent, LinkedHashMap::new, Collectors.counting()));

    List<Path> collapsedPaths = new ArrayList<>();
    Set<Path> emittedParents = new LinkedHashSet<>();
    for (Path path : paths) {
      Path parent = path.getParent();
      if (parent != null && parentCounts.getOrDefault(parent, 0L) > 1) {
        if (emittedParents.add(parent)) {
          collapsedPaths.add(parent);
        }
      } else {
        collapsedPaths.add(path);
      }
    }
    return collapsedPaths;
  }

  private static boolean hasNormalizedAncestor(Set<Path> normalizedPaths, Path path) {
    Path current = path.getParent();
    while (current != null) {
      if (normalizedPaths.contains(current)) {
        return true;
      }
      current = current.getParent();
    }
    return false;
  }

  private static Optional<Path> getDirectoryPath(Path p, Configuration hadoopConf) {
    if (hasGlobPattern(p)) {
      return getPathBeforeGlob(p);
    }

    try {
      if (p.getFileSystem(hadoopConf).getFileStatus(p).isFile()) {
        return Optional.ofNullable(p.getParent());
      }
      return Optional.of(p);
    } catch (IOException e) {
      log.warn("Unable to get file status for path: {}", p, e);
      return Optional.of(p);
    }
  }

  private static boolean hasGlobPattern(Path path) {
    String value = path.toString();
    return value.contains("*")
        || value.contains("?")
        || value.contains("{")
        || value.contains("}")
        || value.contains("[")
        || value.contains("]");
  }

  private static Optional<Path> getPathBeforeGlob(Path path) {
    String value = path.toString();
    Path current = path;
    while (current != null && hasGlobPattern(current)) {
      current = current.getParent();
    }

    if (current == null) {
      log.warn("Unable to determine non-glob parent for path: {}", value);
      return Optional.empty();
    }
    return Optional.of(current);
  }

  public static Path normalizeHiveStylePartitioning(Path path) {
    Path current = path;
    while (current != null && isHivePartitionPath(current.getName())) {
      Path parent = current.getParent();
      if (parent == null) {
        return path;
      }
      current = parent;
    }
    return current == null ? path : current;
  }

  private static boolean isHivePartitionPath(String pathName) {
    int firstEquals = pathName.indexOf('=');
    return firstEquals > 0
        && firstEquals == pathName.lastIndexOf('=')
        && pathName.substring(0, firstEquals).matches("[A-Za-z_][A-Za-z0-9_]*")
        && firstEquals < pathName.length() - 1;
  }

  /**
   * Create DatasetIdentifier from CatalogTable, using storage's locationURI if it exists. In other
   * way, use defaultTablePath.
   */
  public static DatasetIdentifier fromCatalogTable(
      CatalogTable catalogTable, SparkSession sparkSession) {
    URI locationUri;
    locationUri = getLocationUri(catalogTable, sparkSession);
    return fromCatalogTable(catalogTable, sparkSession, locationUri);
  }

  /** Create DatasetIdentifier from CatalogTable, using provided location. */
  @SneakyThrows
  public static DatasetIdentifier fromCatalogTable(
      CatalogTable catalogTable, SparkSession sparkSession, Path location) {
    return fromCatalogTable(catalogTable, sparkSession, location.toUri());
  }

  /** Create DatasetIdentifier from CatalogTable, using provided location. */
  @SneakyThrows
  public static DatasetIdentifier fromCatalogTable(
      CatalogTable catalogTable, SparkSession sparkSession, URI location) {
    return fromTableIdentifier(catalogTable.identifier(), sparkSession.sparkContext(), location);
  }

  @SneakyThrows
  public static DatasetIdentifier fromTableIdentifier(
      TableIdentifier identifier, SparkContext sparkContext, URI location) {
    // perform URL normalization
    DatasetIdentifier locationDataset = fromURI(location);
    URI locationUri = FilesystemDatasetUtils.toLocation(locationDataset);

    Optional<DatasetIdentifier> symlinkDataset = Optional.empty();

    SparkConf sparkConf = sparkContext.getConf();
    Configuration hadoopConf = sparkContext.hadoopConfiguration();

    Optional<URI> metastoreUri = getMetastoreUri(sparkContext);
    Optional<String> glueArn = AwsUtils.getGlueArn(sparkConf, hadoopConf);

    if (glueArn.isPresent()) {
      // Even if glue catalog is used, it will have a hive metastore URI
      // Use ARN format 'arn:aws:glue:{region}:{account_id}:table/{database}/{table}'
      String tableName = nameFromTableIdentifier(identifier, "/");
      symlinkDataset =
          Optional.of(new DatasetIdentifier(GLUE_TABLE_PREFIX + tableName, glueArn.get()));
    } else if (metastoreUri.isPresent()) {
      // dealing with Hive tables
      URI hiveUri = prepareHiveUri(metastoreUri.get());
      String tableName = nameFromTableIdentifier(identifier);
      symlinkDataset = Optional.of(FilesystemDatasetUtils.fromLocationAndName(hiveUri, tableName));
    } else if (DatabricksUtils.isDatabricksUnityCatalogEnabled(sparkConf)) {
      String tableName = nameFromTableIdentifier(identifier);
      String namespace = StringUtils.substringBeforeLast(location.toString(), File.separator);
      symlinkDataset = Optional.of(new DatasetIdentifier(tableName, namespace));
    } else {
      Optional<URI> warehouseLocation =
          getWarehouseLocation(sparkConf, hadoopConf)
              // perform normalization
              .map(FilesystemDatasetUtils::fromLocation)
              .map(FilesystemDatasetUtils::toLocation);

      if (warehouseLocation.isPresent()) {
        URI relativePath = warehouseLocation.get().relativize(locationUri);
        if (!relativePath.equals(locationUri)) {
          // if there is no metastore, and table has custom location,
          // it cannot be accessed via default warehouse location
          String tableName = nameFromTableIdentifier(identifier);
          symlinkDataset =
              Optional.of(
                  FilesystemDatasetUtils.fromLocationAndName(warehouseLocation.get(), tableName));
        }
      }
    }

    if (symlinkDataset.isPresent()) {
      locationDataset.withSymlink(
          symlinkDataset.get().getName(),
          symlinkDataset.get().getNamespace(),
          DatasetIdentifier.SymlinkType.TABLE);
    }

    return locationDataset;
  }

  public static URI getDefaultLocationUri(SparkSession sparkSession, TableIdentifier identifier) {
    return sparkSession.sessionState().catalog().defaultTablePath(identifier);
  }

  public static Path reconstructDefaultLocation(String warehouse, String[] namespace, String name) {
    String database = null;
    if (namespace.length == 1) {
      // {"database"}
      database = namespace[0];
    } else if (namespace.length > 1) {
      // {"spark_catalog", "database"}
      database = namespace[1];
    }

    // /warehouse/mytable
    if (database == null || database.equals(DEFAULT_DB)) {
      return new Path(warehouse, name);
    }

    // /warehouse/mydb.db/mytable
    return new Path(new Path(warehouse, database + ".db"), name);
  }

  public static Optional<URI> getMetastoreUri(SparkContext context) {
    // make sure enableHiveSupport is called
    Optional<String> setting =
        SparkConfUtils.findSparkConfigKey(
            context.getConf(), StaticSQLConf.CATALOG_IMPLEMENTATION().key());
    if (!setting.isPresent() || !"hive".equals(setting.get())) {
      return Optional.empty();
    }
    return SparkConfUtils.getMetastoreUri(context);
  }

  @SneakyThrows
  public static URI prepareHiveUri(URI uri) {
    return new URI("hive", uri.getAuthority(), null, null, null);
  }

  @SneakyThrows
  public static Optional<URI> getWarehouseLocation(SparkConf sparkConf, Configuration hadoopConf) {
    Optional<String> warehouseLocation =
        SparkConfUtils.findSparkConfigKey(sparkConf, StaticSQLConf.WAREHOUSE_PATH().key());
    if (!warehouseLocation.isPresent()) {
      warehouseLocation =
          SparkConfUtils.findHadoopConfigKey(hadoopConf, "hive.metastore.warehouse.dir");
    }
    return warehouseLocation.map(URI::create);
  }

  private static URI getLocationUri(CatalogTable catalogTable, SparkSession sparkSession) {
    URI locationUri;
    if (catalogTable.storage() != null && catalogTable.storage().locationUri().isDefined()) {
      locationUri = catalogTable.storage().locationUri().get();
    } else {
      locationUri = getDefaultLocationUri(sparkSession, catalogTable.identifier());
    }
    return locationUri;
  }

  /** Get DatasetIdentifier name in format database.table or table */
  private static String nameFromTableIdentifier(TableIdentifier identifier) {
    return nameFromTableIdentifier(identifier, ".");
  }

  private static String nameFromTableIdentifier(TableIdentifier identifier, String delimiter) {
    // calling `unquotedString` method includes `spark_catalog`, so instead get proper identifier
    // manually
    String name;
    if (identifier.database().isDefined()) {
      // include database in name
      name = identifier.database().get() + delimiter + identifier.table();
    } else {
      // just table name
      name = identifier.table();
    }

    return name;
  }
}
