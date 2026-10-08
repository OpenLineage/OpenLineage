/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.dataset.DatasetCompositeFacetsBuilder;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.lifecycle.plan.catalog.CatalogUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.agent.util.SparkSessionUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkDatasetBuilder;
import io.openlineage.spark3.agent.utils.DatasetVersionDatasetFacetUtils;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.hadoop.fs.Path;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.apache.spark.sql.types.StructType;
import scala.Option;

/**
 * Class extending {@link io.openlineage.spark.agent.lifecycle.plan.LogicalRelationDatasetBuilder}
 * with methods only available for Spark3. It is required to support datasetVersionFacet for delta
 * provider
 */
@Slf4j
public class LogicalRelationDatasetBuilder<D extends OpenLineage.Dataset>
    extends io.openlineage.spark.agent.lifecycle.plan.LogicalRelationDatasetBuilder<D> {
  private static final String HOODIE_BASE_RELATION_CLASS = "org.apache.hudi.HoodieBaseRelation";
  private static final String HOODIE_FILE_INDEX_CLASS = "org.apache.hudi.HoodieFileIndex";

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
    if (x instanceof LogicalRelation && isSingleNodeLogicalPlan(x) && !searchDependencies) {
      return false;
    }
    if (!(x instanceof LogicalRelation)) {
      return false;
    }

    LogicalRelation logicalRelation = (LogicalRelation) x;
    Object relation = logicalRelation.relation();
    return relation instanceof HadoopFsRelation
        || relation instanceof JDBCRelation
        || isHudiBaseRelation(relation)
        || context.getSparkExtensionVisitorWrapper().isDefinedAt(relation)
        || logicalRelation.catalogTable().isDefined();
  }

  @Override
  public List<D> apply(SparkListenerEvent event, LogicalRelation logRel) {
    Object relation = logRel.relation();
    if (isHudiBaseRelation(relation)) {
      return handleHudiBaseRelation(relation);
    }
    if (relation instanceof HadoopFsRelation && isHudiFileIndex(((HadoopFsRelation) relation))) {
      SparkDatasetBuilder<D> builder =
          datasetFactory
              .sparkDatasetBuilder()
              .datasetType("TABLE", hudiTableType(((HadoopFsRelation) relation)))
              .symlink(hudiSymlink(((HadoopFsRelation) relation)));
      return handleHadoopFsRelation(logRel, builder);
    }
    return super.apply(event, logRel);
  }

  @Override
  protected Optional<String> getDatasetVersion(LogicalRelation x) {
    return DatasetVersionDatasetFacetUtils.extractVersionFromLogicalRelation(x);
  }

  private List<D> handleHudiBaseRelation(Object relation) {
    StructType schema = hudiSchema(relation);
    Object metaClient = hudiMetaClient(relation);
    return Collections.singletonList(
        datasetFactory
            .sparkDatasetBuilder()
            .symlink(hudiSymlink(metaClient))
            .dataset(resolveHudiRelationUri(relation, metaClient))
            .schema(schema)
            .datasetType("TABLE", hudiTableType(metaClient))
            .build());
  }

  private boolean isHudiBaseRelation(Object relation) {
    return isInstanceOf(relation, HOODIE_BASE_RELATION_CLASS);
  }

  private boolean isHudiFileIndex(HadoopFsRelation relation) {
    return isInstanceOf(relation.location(), HOODIE_FILE_INDEX_CLASS);
  }

  private boolean isInstanceOf(Object instance, String className) {
    try {
      return Class.forName(className).isInstance(instance);
    } catch (ClassNotFoundException | LinkageError e) {
      return false;
    }
  }

  private Object hudiMetaClient(Object relation) {
    try {
      return MethodUtils.invokeMethod(relation, "metaClient");
    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new IllegalStateException("Unable to resolve Hudi meta client", e);
    }
  }

  private Object hudiMetaClient(HadoopFsRelation relation) {
    try {
      return MethodUtils.invokeMethod(relation.location(), "metaClient");
    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new IllegalStateException("Unable to resolve Hudi file index meta client", e);
    }
  }

  private URI resolveHudiRelationUri(Object relation, Object metaClient) {
    try {
      Path path = (Path) MethodUtils.invokeMethod(relation, "basePath");
      if (path != null) {
        return path.toUri();
      }
    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      log.debug("Unable to resolve Hudi relation basePath via reflection", e);
    }

    try {
      Object storagePath = MethodUtils.invokeMethod(metaClient, "getBasePathV2");
      return (URI) MethodUtils.invokeMethod(storagePath, "toUri");
    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new IllegalStateException("Unable to resolve Hudi base path", e);
    }
  }

  private StructType hudiSchema(Object relation) {
    try {
      return (StructType) MethodUtils.invokeMethod(relation, "schema");
    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new IllegalStateException("Unable to resolve Hudi schema", e);
    }
  }

  private String hudiTableType(HadoopFsRelation relation) {
    return hudiTableType(hudiMetaClient(relation));
  }

  private String hudiTableType(Object metaClient) {
    try {
      Object tableType = MethodUtils.invokeMethod(metaClient, "getTableType");
      return tableType.toString();
    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new IllegalStateException("Unable to resolve Hudi table type", e);
    }
  }

  private List<DatasetIdentifier.Symlink> hudiSymlink(HadoopFsRelation relation) {
    return hudiSymlink(hudiMetaClient(relation));
  }

  private List<DatasetIdentifier.Symlink> hudiSymlink(Object metaClient) {
    URI uri = hudiBasePathV2Uri(metaClient);
    return Collections.singletonList(
        new DatasetIdentifier.Symlink(uri.toString(), "hudi", DatasetIdentifier.SymlinkType.TABLE));
  }

  private URI hudiBasePathV2Uri(Object metaClient) {
    try {
      Object storagePath = MethodUtils.invokeMethod(metaClient, "getBasePathV2");
      return (URI) MethodUtils.invokeMethod(storagePath, "toUri");
    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new IllegalStateException("Unable to resolve Hudi base path URI", e);
    }
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
