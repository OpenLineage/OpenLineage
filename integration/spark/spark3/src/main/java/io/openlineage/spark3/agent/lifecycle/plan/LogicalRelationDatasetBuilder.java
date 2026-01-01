/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.dataset.DatasetCompositeFacetsBuilder;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.CatalogUtils3;
import io.openlineage.spark3.agent.utils.DatasetVersionDatasetFacetUtils;
import java.lang.reflect.InvocationTargetException;
import java.util.NoSuchElementException;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.CatalogManager;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog;
import scala.Option;

/**
 * Class extending {@link io.openlineage.spark.agent.lifecycle.plan.LogicalRelationDatasetBuilder}
 * with methods only available for Spark3. It is required to support datasetVersionFacet for delta
 * provider
 */
@Slf4j
public class LogicalRelationDatasetBuilder<D extends OpenLineage.Dataset>
    extends io.openlineage.spark.agent.lifecycle.plan.LogicalRelationDatasetBuilder<D> {

  public LogicalRelationDatasetBuilder(
      OpenLineageContext context, DatasetFactory datasetFactory, boolean searchDependencies) {
    super(context, datasetFactory, searchDependencies);
  }

  @Override
  public boolean isDefinedAt(SparkListenerEvent event) {
    return true;
  }

  @Override
  protected Optional<String> getDatasetVersion(LogicalRelation x) {
    return DatasetVersionDatasetFacetUtils.extractVersionFromLogicalRelation(x);
  }

  @Override
  protected void addCatalogAndStorageFacets(
      CatalogTable catalogTable, DatasetCompositeFacetsBuilder builder) {
    if (!context.getSparkSession().isPresent()) {
      return;
    }
    if (catalogTable == null || catalogTable.identifier() == null) {
      log.debug("No table identifier, cannot add catalog/storage facets");
      return;
    }

    String catalogName;
    try {
      //noinspection unchecked
      catalogName =
          ((Option<String>) MethodUtils.invokeMethod(catalogTable.identifier(), "catalog")).get();
    } catch (NoSuchMethodException
        | IllegalAccessException
        | InvocationTargetException
        | NoSuchElementException e) {
      log.debug("No catalog name, cannot add catalog/storage facets");
      return;
    }

    CatalogManager catalogManager = context.getSparkSession().get().sessionState().catalogManager();
    Optional.of(catalogManager.catalog(catalogName))
        .filter(catalogPlugin -> !(catalogPlugin instanceof V2SessionCatalog))
        .filter(catalogPlugin -> catalogPlugin instanceof TableCatalog)
        .map(TableCatalog.class::cast)
        .ifPresent(
            tableCatalog ->
                CatalogUtils3.addStorageAndCatalogFacets(
                    context,
                    tableCatalog,
                    ScalaConversionUtils.fromMap(catalogTable.properties()),
                    builder));
  }
}
