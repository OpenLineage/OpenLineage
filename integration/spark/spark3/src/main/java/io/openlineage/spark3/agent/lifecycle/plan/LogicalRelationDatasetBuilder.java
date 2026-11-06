/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.dataset.DatasetCompositeFacetsBuilder;
import io.openlineage.spark.agent.lifecycle.plan.catalog.CatalogUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.agent.util.SparkSessionUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkDatasetBuilder;
import io.openlineage.spark3.agent.utils.DatasetVersionDatasetFacetUtils;
import io.openlineage.spark3.agent.utils.HudiUtils;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
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
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return super.isDefinedAtLogicalPlan(x)
        || (x instanceof LogicalRelation
            && (searchDependencies || !isSingleNodeLogicalPlan(x))
            && HudiUtils.isHudiBaseRelation(((LogicalRelation) x).relation()));
  }

  @Override
  public List<D> apply(SparkListenerEvent event, LogicalRelation logRel) {
    Object relation = logRel.relation();
    boolean hudiBaseRelation = HudiUtils.isHudiBaseRelation(relation);
    boolean hudiFileIndex =
        relation instanceof HadoopFsRelation
            && HudiUtils.isHudiFileIndex(((HadoopFsRelation) relation).location());
    if (!hudiBaseRelation && !hudiFileIndex
        || context.getSparkExtensionVisitorWrapper().isDefinedAt(relation)) {
      return super.apply(event, logRel);
    }
    try {
      return handleHudiRelation(logRel, hudiBaseRelation);
    } catch (Exception e) {
      log.warn("Unable to extract Hudi table metadata, falling back to generic handling", e);
      // Without a catalog table, a HoodieBaseRelation is not a relation the parent can handle
      return hudiBaseRelation && !hasCatalogTable(logRel)
          ? Collections.emptyList()
          : super.apply(event, logRel);
    }
  }

  @Override
  protected Optional<String> getDatasetVersion(LogicalRelation x) {
    return DatasetVersionDatasetFacetUtils.extractVersionFromLogicalRelation(x);
  }

  private List<D> handleHudiRelation(LogicalRelation logRel, boolean hudiBaseRelation) {
    Object relation = logRel.relation();
    Object metaClient =
        hudiBaseRelation
            ? HudiUtils.metaClient(relation)
            : HudiUtils.metaClient(((HadoopFsRelation) relation).location());
    SparkDatasetBuilder<D> builder =
        datasetFactory.sparkDatasetBuilder().datasetType("TABLE", HudiUtils.tableType(metaClient));
    if (hasCatalogTable(logRel)) {
      return handleCatalogTable(logRel, builder);
    }
    if (hudiBaseRelation) {
      return Collections.singletonList(
          builder
              .dataset(HudiUtils.basePath(relation, metaClient))
              .schema(HudiUtils.schema(relation))
              .build());
    }
    return handleHadoopFsRelation(logRel, builder);
  }

  private boolean hasCatalogTable(LogicalRelation logRel) {
    return logRel.catalogTable() != null && logRel.catalogTable().isDefined();
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

    SparkSessionUtils.catalog(context.getSparkSession().get(), catalogName)
        .filter(plugin -> plugin instanceof TableCatalog)
        .map(TableCatalog.class::cast)
        .ifPresent(
            tableCatalog ->
                CatalogUtils.addStorageAndCatalogFacets(
                    context,
                    tableCatalog,
                    ScalaConversionUtils.fromMap(catalogTable.properties()),
                    builder));
  }
}
