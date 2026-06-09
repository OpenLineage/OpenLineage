/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import static io.openlineage.spark.agent.util.CatalogDatasetFacetUtils.isHiveCatalog;

import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.DatasetIdentifier.Symlink;
import io.openlineage.client.utils.DatasetIdentifier.SymlinkType;
import io.openlineage.client.utils.filesystem.FilesystemDatasetUtils;
import io.openlineage.spark.agent.util.CatalogDatasetFacetUtils;
import io.openlineage.spark.agent.util.GoogleCloudPlatformUtils;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog;

@Slf4j
public class V2SessionCatalogHandler implements CatalogHandler {
  private OpenLineageContext context;

  public V2SessionCatalogHandler(OpenLineageContext context) {
    this.context = context;
  }

  @Override
  public boolean hasClasses() {
    return true;
  }

  @Override
  public boolean isClass(TableCatalog tableCatalog) {
    return tableCatalog instanceof V2SessionCatalog;
  }

  @Override
  public DatasetIdentifier getDatasetIdentifier(
      SparkSession session,
      TableCatalog tableCatalog,
      Identifier identifier,
      Map<String, String> properties) {
    V2SessionCatalog v2Catalog = (V2SessionCatalog) tableCatalog;

    Map<String, String> namespaceMetadata = v2Catalog.loadNamespaceMetadata(identifier.namespace());
    String namespaceLocation = namespaceMetadata.get("location");

    String tableLocation = properties.get(TableCatalog.PROP_LOCATION);
    DatasetIdentifier di;
    if (tableLocation != null) {
      di = PathUtils.fromPath(new Path(tableLocation));
    } else {
      if (namespaceLocation == null) {
        throw new OpenLineageClientException(
            "Unable to extract DatasetIdentifier from V2SessionCatalog");
      }

      // table is dropped, no information in catalog
      di =
          PathUtils.fromPath(
              PathUtils.reconstructDefaultLocation(
                  namespaceLocation, identifier.namespace(), identifier.name()));
    }
    if (CatalogDatasetFacetUtils.isHiveSupportEnabled(session.sparkContext())
        && isHiveCatalog(tableCatalog)) {
      if (GoogleCloudPlatformUtils.isBigLakeHiveCatalog(session.sparkContext().getConf())) {
        di.withSymlink(new Symlink(identifier.toString(), "gcp_lakehouse", SymlinkType.TABLE));
      } else {
        PathUtils.getMetastoreUri(session.sparkContext())
            .map(
                uri ->
                    FilesystemDatasetUtils.fromLocationAndName(
                        PathUtils.prepareHiveUri(uri), identifier.toString()))
            .ifPresent(
                i -> di.withSymlink(new Symlink(i.getName(), i.getNamespace(), SymlinkType.TABLE)));
      }
    }

    if (namespaceLocation != null && di.getSymlinks().isEmpty()) {
      di.withSymlink(
          new Symlink(identifier.toString(), namespaceMetadata.get("location"), SymlinkType.TABLE));
    }
    return di;
  }

  @Override
  public Optional<CatalogWithAdditionalFacets> getCatalogDatasetFacet(
      TableCatalog tableCatalog, Map<String, String> properties) {
    return context
        .getSparkContext()
        .filter(CatalogDatasetFacetUtils::isHiveSupportEnabled)
        .filter(sc -> CatalogDatasetFacetUtils.isHiveCatalog(tableCatalog))
        .flatMap(c -> CatalogDatasetFacetUtils.getCatalogDatasetFacetForHive(context))
        .map(CatalogWithAdditionalFacets::of);
  }

  @Override
  public String getName() {
    return "session";
  }
}
