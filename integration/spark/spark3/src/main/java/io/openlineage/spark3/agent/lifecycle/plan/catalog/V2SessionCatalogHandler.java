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
    V2SessionCatalog catalog = (V2SessionCatalog) tableCatalog;
    Map<String, String> namespaceMetadata = catalog.loadNamespaceMetadata(identifier.namespace());
    if (namespaceMetadata.containsKey("location")) {
      DatasetIdentifier di =
          PathUtils.fromPath(new Path(namespaceMetadata.get("location"), identifier.name()));
      di.withSymlink(
          new Symlink(identifier.toString(), namespaceMetadata.get("location"), SymlinkType.TABLE));
      return di;
    }
    throw new OpenLineageClientException(
        "Unable to extract DatasetIdentifier from V2SessionCatalog");
  }

  @Override
  public String getName() {
    return "session";
  }
}
