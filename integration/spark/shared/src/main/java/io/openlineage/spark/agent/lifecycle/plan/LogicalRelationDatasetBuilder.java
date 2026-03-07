/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputStatisticsInputDatasetFacetBuilder;
import io.openlineage.client.dataset.DatasetCompositeFacetsBuilder;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.lifecycle.plan.handlers.ExtensionLineageRelationHandler;
import io.openlineage.spark.agent.lifecycle.plan.handlers.JdbcRelationHandler;
import io.openlineage.spark.agent.util.DatasetVersionUtils;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.AbstractQueryPlanDatasetBuilder;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.catalog.CatalogStatistics;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.FileIndex;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.apache.spark.sql.sources.BaseRelation;

/**
 * {@link LogicalPlan} visitor that attempts to extract a {@link OpenLineage.Dataset} from a {@link
 * LogicalRelation}. The {@link org.apache.spark.sql.sources.BaseRelation} is tested for known
 * types, such as {@link HadoopFsRelation} or {@link JDBCRelation}s, as those are easy to extract
 * exact dataset information.
 *
 * <p>For {@link HadoopFsRelation}s, it is assumed that a single directory maps to a single {@link
 * OpenLineage.Dataset}. Any files referenced are replaced by their parent directory and all files
 * in a given directory are assumed to belong to the same {@link OpenLineage.Dataset}. Directory
 * partitioning is currently not addressed.
 *
 * <p>For {@link JDBCRelation}s, {@link OpenLineage.Dataset} naming expects the namespace to be the
 * JDBC connection URL (schema and authority only) and the table name to be the <code>
 * &lt;database&gt;
 * </code>.<code>&lt;tableName&gt;</code>.
 *
 * <p>{@link CatalogTable}s, if present, can be used to describe the {@link OpenLineage.Dataset} if
 * its {@link org.apache.spark.sql.sources.BaseRelation} is unknown.
 *
 * <p>TODO If a user specifies the {@link JDBCOptions#JDBC_QUERY_STRING()} option, we do not parse
 * the sql to determine the specific tables used. Since we return a List of {@link
 * OpenLineage.Dataset}s, we can parse the sql and determine each table referenced to return a
 * complete list of datasets referenced.
 */
@Slf4j
public class LogicalRelationDatasetBuilder<D extends OpenLineage.Dataset>
    extends AbstractQueryPlanDatasetBuilder<SparkListenerEvent, LogicalRelation, D> {

  private final DatasetFactory<D> datasetFactory;

  public LogicalRelationDatasetBuilder(
      OpenLineageContext context, DatasetFactory<D> datasetFactory, boolean searchDependencies) {
    super(context, searchDependencies);
    this.datasetFactory = datasetFactory;
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
            || context
                .getSparkExtensionVisitorWrapper()
                .isDefinedAt(((LogicalRelation) x).relation())
            || ((LogicalRelation) x).catalogTable().isDefined());
  }

  private boolean isSingleNodeLogicalPlan(LogicalPlan x) {
    return context
            .getQueryExecution()
            .map(qe -> qe.optimizedPlan())
            .filter(p -> p.equals(x))
            .isPresent()
        && (x.children() == null || x.children().isEmpty());
  }

  @Override
  public List<D> apply(LogicalRelation logRel) {
    // intentionally unimplemented
    throw new UnsupportedOperationException("apply(LogicalPlay) is not implemented");
  }

  @Override
  public List<D> apply(SparkListenerEvent event, LogicalRelation logRel) {
    if (context.getSparkExtensionVisitorWrapper().isDefinedAt(logRel.relation())) {
      return new ExtensionLineageRelationHandler<>(context, datasetFactory)
          .handleRelation(event, logRel);
    } else if (logRel.catalogTable() != null && logRel.catalogTable().isDefined()) {
      return handleCatalogTable(logRel);
    } else if (logRel.relation() instanceof HadoopFsRelation) {
      return handleHadoopFsRelation(logRel);
    } else if (logRel.relation() instanceof JDBCRelation) {
      return new JdbcRelationHandler<>(datasetFactory).handleRelation(logRel);
    }
    throw new IllegalArgumentException(
        "Expected logical plan to be either HadoopFsRelation, JDBCRelation, "
            + "or CatalogTable but was "
            + logRel);
  }

  private List<D> handleCatalogTable(LogicalRelation logRel) {
    if (!context.getSparkSession().isPresent()) {
      return Collections.emptyList();
    }

    CatalogTable catalogTable = logRel.catalogTable().get();

    DatasetIdentifier di =
        PathUtils.fromCatalogTable(catalogTable, context.getSparkSession().get());

    DatasetCompositeFacetsBuilder datasetFacetsBuilder =
        datasetFactory.createCompositeFacetBuilder();
    datasetFacetsBuilder
        .getFacets()
        .schema(PlanUtils.schemaFacet(context.getOpenLineage(), logRel.schema()))
        .dataSource(PlanUtils.datasourceFacet(context.getOpenLineage(), di.getNamespace()));

    InputStatisticsInputDatasetFacetBuilder statsBuilder =
        context.getOpenLineage().newInputStatisticsInputDatasetFacetBuilder();
    ScalaConversionUtils.asJavaOptional(catalogTable.stats())
        .map(CatalogStatistics::sizeInBytes)
        .ifPresent(
            bytes -> {
              statsBuilder.size(bytes.longValue());
              if (catalogTable.ignoredProperties().contains("numFiles")) {
                statsBuilder.fileCount(
                    Long.valueOf(catalogTable.ignoredProperties().get("numFiles").get()));
              }
              datasetFacetsBuilder.getInputFacets().inputStatistics(statsBuilder.build());
            });
    if (catalogTable.stats() == null || catalogTable.stats().isEmpty()) {
      Optional.ofNullable(logRel.relation())
          .map(BaseRelation::sizeInBytes)
          .ifPresent(
              size -> {
                // https://github.com/apache/spark/blob/v3.5.7/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L2565
                // https://github.com/apache/spark/blob/v3.5.7/sql/core/src/main/scala/org/apache/spark/sql/sources/interfaces.scala#L209
                if (size < Long.MAX_VALUE) {
                  statsBuilder.size(size);
                  datasetFacetsBuilder.getInputFacets().inputStatistics(statsBuilder.build());
                }
              });
    }

    getDatasetVersion(logRel)
        .ifPresent(
            v -> DatasetVersionUtils.buildVersionOutputFacets(context, datasetFacetsBuilder, v));

    addCatalogAndStorageFacets(catalogTable, datasetFacetsBuilder);
    return Collections.singletonList(datasetFactory.getDataset(di, datasetFacetsBuilder));
  }

  private List<D> handleHadoopFsRelation(LogicalRelation x) {
    HadoopFsRelation relation = (HadoopFsRelation) x.relation();
    try {
      return context
          .getSparkSession()
          .map(
              session -> {
                Configuration hadoopConfig =
                    session.sessionState().newHadoopConfWithOptions(relation.options());

                DatasetCompositeFacetsBuilder datasetFacetsBuilder =
                    datasetFactory.createCompositeFacetBuilder();
                getDatasetVersion(x)
                    .map(
                        version ->
                            datasetFacetsBuilder
                                .getFacets()
                                .version(
                                    context
                                        .getOpenLineage()
                                        .newDatasetVersionDatasetFacet(version)));

                FileIndex fileIndex = relation.location();
                Collection<Path> rootPaths = ScalaConversionUtils.fromSeq(fileIndex.rootPaths());

                if (fileIndex instanceof PartitioningAwareFileIndex) {
                  return handlePartitioningAwareRelation(
                      relation,
                      (PartitioningAwareFileIndex) fileIndex,
                      rootPaths,
                      datasetFacetsBuilder);
                }

                // Non-PartitioningAwareFileIndex (e.g. CatalogFileIndex): skip file count
                // to avoid triggering expensive filesystem listings
                if (isSingleFileRelation(rootPaths, hadoopConfig)) {
                  return Collections.singletonList(
                      datasetFactory.getDataset(
                          rootPaths.stream().findFirst().get().toUri(),
                          relation.schema(),
                          datasetFacetsBuilder));
                } else {
                  return PlanUtils.getDirectoryPaths(rootPaths, hadoopConfig).stream()
                      .map(
                          p ->
                              datasetFactory.getDataset(
                                  p.toUri(), relation.schema(), datasetFacetsBuilder))
                      .collect(Collectors.toList());
                }
              })
          .orElse(Collections.emptyList());
    } catch (Exception e) {
      // This is a fallback that can occur when hadoop configurations cannot be
      // reached. This occurs in Azure Databricks when credential passthrough
      // is enabled and you're attempting to get the data lake credentials.
      // The Spark Listener context cannot use the user credentials
      // thus we need a fallback.
      //
      // This can also occur when running on a spark cluster with UnityCatalog
      // enabled.
      // An exception will get thrown claiming there is "no Credential Scope".
      List<D> inputDatasets = new ArrayList<D>();
      List<Path> paths =
          new ArrayList<>(ScalaConversionUtils.fromSeq(relation.location().rootPaths()));
      for (Path p : paths) {
        inputDatasets.add(datasetFactory.getDataset(p.toUri(), relation.schema()));
      }
      if (inputDatasets.isEmpty()) {
        return Collections.emptyList();
      } else {
        return inputDatasets;
      }
    }
  }

  /**
   * Handles HadoopFsRelation backed by a PartitioningAwareFileIndex (the common case for
   * InMemoryFileIndex). Uses the index's cached allFiles() to derive file count and directory paths
   * without any remote filesystem calls or expensive String[] materialization.
   */
  private List<D> handlePartitioningAwareRelation(
      HadoopFsRelation relation,
      PartitioningAwareFileIndex fileIndex,
      Collection<Path> rootPaths,
      DatasetCompositeFacetsBuilder datasetFacetsBuilder) {

    List<FileStatus> allFiles = ScalaConversionUtils.fromSeq(fileIndex.allFiles());

    datasetFacetsBuilder
        .getInputFacets()
        .inputStatistics(
            context
                .getOpenLineage()
                .newInputStatisticsInputDatasetFacetBuilder()
                .size(relation.sizeInBytes())
                .fileCount((long) allFiles.size())
                .build());

    // Single root path pointing directly to a file (e.g. /tmp/data.csv)
    if (rootPaths.size() == 1 && allFiles.size() == 1) {
      Path rootPath = rootPaths.stream().findFirst().get();
      Path filePath = allFiles.get(0).getPath();
      // If the root path matches the only file, this is a single-file relation
      if (rootPath.getName().equals(filePath.getName())) {
        return Collections.singletonList(
            datasetFactory.getDataset(rootPath.toUri(), relation.schema(), datasetFacetsBuilder));
      }
    }

    // Collect unique parent directories from the cached file statuses.
    // This avoids remote getFileStatus() calls that getDirectoryPaths() would make.
    LinkedHashSet<Path> directories = new LinkedHashSet<>();
    for (FileStatus fs : allFiles) {
      Path parent = fs.getPath().getParent();
      if (parent != null) {
        directories.add(parent);
      }
    }

    if (directories.isEmpty()) {
      // Fall back to root paths if no files found
      return rootPaths.stream()
          .map(p -> datasetFactory.getDataset(p.toUri(), relation.schema(), datasetFacetsBuilder))
          .collect(Collectors.toList());
    }

    return directories.stream()
        .map(p -> datasetFactory.getDataset(p.toUri(), relation.schema(), datasetFacetsBuilder))
        .collect(Collectors.toList());
  }

  @SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
  private boolean isSingleFileRelation(Collection<Path> paths, Configuration hadoopConfig) {
    if (paths.size() != 1) {
      return false;
    }

    try {
      Path path = paths.stream().findFirst().get();
      return path.getFileSystem(hadoopConfig).isFile(path);
    } catch (IOException e) {
      return false;
    }
  }

  protected Optional<String> getDatasetVersion(LogicalRelation x) {
    // not implemented
    return Optional.empty();
  }

  protected void addCatalogAndStorageFacets(
      CatalogTable catalogTable, DatasetCompositeFacetsBuilder builder) {
    // not implemented
  }
}
