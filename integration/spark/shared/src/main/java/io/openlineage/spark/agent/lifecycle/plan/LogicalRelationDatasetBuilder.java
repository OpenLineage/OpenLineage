/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.JdbcUtils;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.AbstractQueryPlanDatasetBuilder;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import scala.collection.JavaConversions;

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
    return x instanceof LogicalRelation
        && (((LogicalRelation) x).relation() instanceof HadoopFsRelation
            || ((LogicalRelation) x).relation() instanceof JDBCRelation
            || ((LogicalRelation) x).catalogTable().isDefined());
  }

  @Override
  public List<D> apply(LogicalRelation logRel) {
    if (logRel.catalogTable() != null && logRel.catalogTable().isDefined()) {
      return handleCatalogTable(logRel);
    } else if (logRel.relation() instanceof HadoopFsRelation) {
      return handleHadoopFsRelation(logRel);
    } else if (logRel.relation() instanceof JDBCRelation) {
      return handleJdbcRelation(logRel);
    }
    throw new IllegalArgumentException(
        "Expected logical plan to be either HadoopFsRelation, JDBCRelation, "
            + "or CatalogTable but was "
            + logRel);
  }

  private List<D> handleCatalogTable(LogicalRelation logRel) {
    CatalogTable catalogTable = logRel.catalogTable().get();

    DatasetIdentifier di = PathUtils.fromCatalogTable(catalogTable);

    OpenLineage.DatasetFacetsBuilder datasetFacetsBuilder =
        context.getOpenLineage().newDatasetFacetsBuilder();
    datasetFacetsBuilder.schema(PlanUtils.schemaFacet(context.getOpenLineage(), logRel.schema()));
    datasetFacetsBuilder.dataSource(
        PlanUtils.datasourceFacet(context.getOpenLineage(), di.getNamespace()));

    getDatasetVersion(logRel)
        .map(
            version ->
                datasetFacetsBuilder.version(
                    context.getOpenLineage().newDatasetVersionDatasetFacet(version)));

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

                OpenLineage.DatasetFacetsBuilder datasetFacetsBuilder =
                    context.getOpenLineage().newDatasetFacetsBuilder();
                getDatasetVersion(x)
                    .map(
                        version ->
                            datasetFacetsBuilder.version(
                                context.getOpenLineage().newDatasetVersionDatasetFacet(version)));

                return JavaConversions.asJavaCollection(relation.location().rootPaths()).stream()
                    .map(p -> PlanUtils.getDirectoryPath(p, hadoopConfig))
                    .distinct()
                    .map(
                        p -> {
                          // TODO- refactor this to return a single partitioned dataset based on
                          // static
                          // static partitions in the relation
                          return datasetFactory.getDataset(
                              p.toUri(), relation.schema(), datasetFacetsBuilder);
                        })
                    .collect(Collectors.toList());
              })
          .orElse(Collections.emptyList());
    } catch (Exception e) {
      if ("com.databricks.backend.daemon.data.client.adl.AzureCredentialNotFoundException"
          .equals(e.getClass().getName())) {
        // This is a fallback that can occur when hadoop configurations cannot be
        // reached. This occurs in Azure Databricks when credential passthrough
        // is enabled and you're attempting to get the data lake credentials.
        // The Spark Listener context cannot use the user credentials
        // thus we need a fallback.
        // This is similar to the InsertIntoHadoopRelationVisitor's process for getting
        // Datasets
        List<D> inputDatasets = new ArrayList<D>();
        List<Path> paths =
            JavaConversions.asJavaCollection(relation.location().rootPaths()).stream()
                .collect(Collectors.toList());

        for (Path p : paths) {
          inputDatasets.add(datasetFactory.getDataset(p.toUri(), relation.schema()));
        }
        if (inputDatasets.isEmpty()) {
          return Collections.emptyList();
        } else {
          return inputDatasets;
        }
      } else {
        throw e;
      }
    }
  }

  protected Optional<String> getDatasetVersion(LogicalRelation x) {
    // not implemented
    return Optional.empty();
  }

  private List<D> handleJdbcRelation(LogicalRelation x) {
    JDBCRelation relation = (JDBCRelation) x.relation();
    // TODO- if a relation is composed of a complex sql query, we should attempt to
    // extract the
    // table names so that we can construct a true lineage
    String tableName =
        relation
            .jdbcOptions()
            .parameters()
            .get(JDBCOptions.JDBC_TABLE_NAME())
            .getOrElse(ScalaConversionUtils.toScalaFn(() -> "COMPLEX"));
    // strip the jdbc: prefix from the url. this leaves us with a url like
    // postgresql://<hostname>:<port>/<database_name>?params
    // we don't parse the URI here because different drivers use different
    // connection
    // formats that aren't always amenable to how Java parses URIs. E.g., the oracle
    // driver format looks like oracle:<drivertype>:<user>/<password>@<database>
    // whereas postgres, mysql, and sqlserver use the scheme://hostname:port/db
    // format.
    String url = JdbcUtils.sanitizeJdbcUrl(relation.jdbcOptions().url());
    return Collections.singletonList(datasetFactory.getDataset(tableName, url, relation.schema()));
  }
}
