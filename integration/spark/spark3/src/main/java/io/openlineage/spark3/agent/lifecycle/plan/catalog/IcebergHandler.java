/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.filesystem.FilesystemDatasetUtils;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.agent.util.SparkConfUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.net.URI;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;

@Slf4j
public class IcebergHandler implements CatalogHandler {

  private final OpenLineageContext context;

  private static final String TYPE = "type";
  private static final String CATALOG_IMPL = "catalog-impl";

  public IcebergHandler(OpenLineageContext context) {
    this.context = context;
  }

  @Override
  public boolean hasClasses() {
    try {
      IcebergHandler.class.getClassLoader().loadClass("org.apache.iceberg.catalog.Catalog");
      return true;
    } catch (Exception e) {
      // swallow- we don't care
    }
    return false;
  }

  @Override
  public boolean isClass(TableCatalog tableCatalog) {
    return (tableCatalog instanceof SparkCatalog) || (tableCatalog instanceof SparkSessionCatalog);
  }

  @Override
  public DatasetIdentifier getDatasetIdentifier(
      SparkSession session,
      TableCatalog tableCatalog,
      Identifier identifier,
      Map<String, String> properties) {
    String catalogName = tableCatalog.name();
    Map<String, String> catalogConf = getCatalogConf(session, catalogName);
    String catalogType = getCatalogType(catalogConf);
    if (catalogType == null) {
      throw new UnsupportedCatalogException(catalogName);
    }

    Path warehouseLocation = new Path(catalogConf.get(CatalogProperties.WAREHOUSE_LOCATION));
    Optional<Table> table = getIcebergTable(tableCatalog, identifier);
    Path tableLocation;
    if (table.isPresent()) {
      tableLocation = new Path(table.get().location());
    } else {
      tableLocation = reconstructDefaultLocation(warehouseLocation, identifier);
    }
    DatasetIdentifier di = PathUtils.fromPath(tableLocation);

    DatasetIdentifier symlink;
    String tableName = identifier.toString();
    if ("hive".equals(catalogType)) {
      symlink = getHiveIdentifier(session, catalogConf.get(CatalogProperties.URI), tableName);
    } else if ("rest".equals(catalogType)) {
      symlink = getRestIdentifier(catalogConf.get(CatalogProperties.URI), tableName);
    } else if ("nessie".equals(catalogType)) {
      symlink = getNessieIdentifier(catalogConf.get(CatalogProperties.URI), tableName);
    } else {
      symlink = FilesystemDatasetUtils.fromLocationAndName(warehouseLocation.toUri(), tableName);
    }

    return di.withSymlink(
        symlink.getName(), symlink.getNamespace(), DatasetIdentifier.SymlinkType.TABLE);
  }

  private Map<String, String> getCatalogConf(SparkSession session, String catalogName) {
    String prefix = String.format("spark.sql.catalog.%s", catalogName);
    Map<String, String> conf =
        ScalaConversionUtils.<String, String>fromMap(session.conf().getAll());
    Map<String, String> catalogConf =
        conf.entrySet().stream()
            .filter(x -> x.getKey().startsWith(prefix))
            .filter(x -> x.getKey().length() > prefix.length())
            .collect(
                Collectors.toMap(
                    x -> x.getKey().substring(prefix.length() + 1), // handle dot after prefix
                    Map.Entry::getValue));
    return catalogConf;
  }

  @SneakyThrows
  private DatasetIdentifier getHiveIdentifier(
      SparkSession session, @Nullable String confUri, String table) {
    URI metastoreUri;
    if (confUri == null) {
      metastoreUri =
          SparkConfUtils.getMetastoreUri(session.sparkContext())
              .orElseThrow(() -> new UnsupportedCatalogException("hive"));
    } else {
      metastoreUri = new URI(confUri);
    }

    return new DatasetIdentifier(table, PathUtils.prepareHiveUri(metastoreUri).toString());
  }

  @SneakyThrows
  private DatasetIdentifier getNessieIdentifier(@Nullable String confUri, String table) {
    String uri = new URI(confUri).toString();
    return new DatasetIdentifier(table, uri);
  }

  @SneakyThrows
  private DatasetIdentifier getRestIdentifier(@Nullable String confUri, String table) {
    String uri = new URI(confUri).toString();
    return new DatasetIdentifier(table, uri);
  }

  @Override
  public Optional<OpenLineage.StorageDatasetFacet> getStorageDatasetFacet(
      Map<String, String> properties) {
    String format = properties.getOrDefault("format", "");
    return Optional.of(
        context.getOpenLineage().newStorageDatasetFacet("iceberg", format.replace("iceberg/", "")));
  }

  @SneakyThrows
  @Override
  public Optional<String> getDatasetVersion(
      TableCatalog tableCatalog, Identifier identifier, Map<String, String> properties) {
    return getIcebergTable(tableCatalog, identifier)
        .map(table -> table.currentSnapshot())
        .map(snapshot -> Long.toString(snapshot.snapshotId()));
  }

  @SneakyThrows
  private Optional<Table> getIcebergTable(TableCatalog tableCatalog, Identifier identifier) {
    try {
      if (tableCatalog instanceof SparkCatalog) {
        SparkCatalog sparkCatalog = (SparkCatalog) tableCatalog;
        SparkTable sparkTable = (SparkTable) sparkCatalog.loadTable(identifier);
        return Optional.ofNullable(sparkTable.table());
      } else {
        TableIdentifier tableIdentifier = TableIdentifier.parse(identifier.toString());
        SparkSessionCatalog sparkCatalog = (SparkSessionCatalog) tableCatalog;
        return Optional.ofNullable(sparkCatalog.icebergCatalog().loadTable(tableIdentifier));
      }
    } catch (NoSuchTableException | ClassCastException e) {
      log.error("Failed to load table from catalog: {}", identifier, e);
      return Optional.empty();
    }
  }

  private Path reconstructDefaultLocation(Path warehouseLocation, Identifier identifier) {
    // namespace1.namespace2.table -> /warehouseLocation/namespace1/namespace2/table
    String[] namespace = identifier.namespace();
    ArrayList<String> pathComponents = new ArrayList(namespace.length + 1);
    for (String component : namespace) {
      pathComponents.add(component);
    }
    pathComponents.add(identifier.name());
    return new Path(warehouseLocation, String.join(Path.SEPARATOR, pathComponents));
  }

  @Override
  public String getName() {
    return "iceberg";
  }

  private String getCatalogType(Map<String, String> catalogConf) {
    if (catalogConf.containsKey(TYPE)) {
      return catalogConf.get(TYPE);
    } else if (catalogConf.containsKey(CATALOG_IMPL)
        && catalogConf.get(CATALOG_IMPL).endsWith("GlueCatalog")) {
      return "glue";
    } else {
      return null;
    }
  }
}
