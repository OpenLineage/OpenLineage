/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.DatasetIdentifier.Symlink;
import io.openlineage.client.utils.DatasetIdentifier.SymlinkType;
import io.openlineage.spark.agent.util.PathUtils;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog;

@Slf4j
public class V2SessionCatalogHandler implements CatalogHandler {
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

    if (namespaceLocation != null) {
      di.withSymlink(
          new Symlink(identifier.toString(), namespaceMetadata.get("location"), SymlinkType.TABLE));
    }
    return di;
  }

  @Override
  public String getName() {
    return "session";
  }
}
