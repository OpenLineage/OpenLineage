/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark35.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.dataset.DatasetCompositeFacetsBuilder;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.lifecycle.plan.catalog.CatalogUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.agent.util.SparkSessionUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkDatasetBuilder;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.hudi.HoodieBaseRelation;
import org.apache.hudi.HoodieFileIndex;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import scala.Option;

/**
 * Class extending {@link io.openlineage.spark.agent.lifecycle.plan.LogicalRelationDatasetBuilder}
 * with methods only available for Spark3. It is required to support datasetVersionFacet for delta
 * provider
 */
@Slf4j
public class LogicalRelationDatasetBuilder<D extends OpenLineage.Dataset>
    extends io.openlineage.spark3.agent.lifecycle.plan.LogicalRelationDatasetBuilder<D> {

  public LogicalRelationDatasetBuilder(
      OpenLineageContext context, DatasetFactory datasetFactory, boolean searchDependencies) {
    super(context, datasetFactory, searchDependencies);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    // if a LogicalPlan is a single node plan like `select * from temp`,
    // then it's leaf node and should not be considered output node
    if (x instanceof LogicalRelation && isSingleNodeLogicalPlan(x) && !searchDependencies) {
      return false;
    }

    return x instanceof LogicalRelation
        && (((LogicalRelation) x).relation() instanceof HadoopFsRelation
            || ((LogicalRelation) x).relation() instanceof JDBCRelation
            || ((LogicalRelation) x).relation() instanceof HoodieBaseRelation
            || context
                .getSparkExtensionVisitorWrapper()
                .isDefinedAt(((LogicalRelation) x).relation())
            || ((LogicalRelation) x).catalogTable().isDefined());
  }

  @Override
  public List<D> apply(SparkListenerEvent event, LogicalRelation logRel) {
    if (logRel.relation() instanceof HoodieBaseRelation) {
      return handleHudiBaseRelation((HoodieBaseRelation) logRel.relation());
    } else if (logRel.relation() instanceof HadoopFsRelation
        && ((HadoopFsRelation) logRel.relation()).location() instanceof HoodieFileIndex) {
      HoodieFileIndex index = (HoodieFileIndex) ((HadoopFsRelation) logRel.relation()).location();
      SparkDatasetBuilder<D> builder =
          datasetFactory
              .sparkDatasetBuilder()
              .datasetType("TABLE", index.metaClient().getTableType().name())
              .symlink(getSymlink(index.metaClient()));
      return handleHadoopFsRelation(logRel, builder);
    }
    return super.apply(event, logRel);
  }

  private List<D> handleHudiBaseRelation(HoodieBaseRelation relation) {
    return Collections.singletonList(
        datasetFactory
            .sparkDatasetBuilder()
            .symlink(getSymlink(relation.metaClient()))
            .dataset(resolveRelationUri(relation))
            .schema(relation.schema())
            .datasetType("TABLE", relation.metaClient().getTableType().name())
            .build());
  }

  private java.net.URI resolveRelationUri(HoodieBaseRelation relation) {
    if (relation.basePath() != null) {
      return relation.basePath().toUri();
    }
    return relation.metaClient().getBasePathV2().toUri();
  }

  private static List<DatasetIdentifier.Symlink> getSymlink(HoodieTableMetaClient metaClient) {
    DatasetIdentifier.Symlink hudi =
        new DatasetIdentifier.Symlink(
            metaClient.getBasePathV2().toUri().toString(),
            "hudi",
            DatasetIdentifier.SymlinkType.TABLE);
    return Collections.singletonList(hudi);
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
