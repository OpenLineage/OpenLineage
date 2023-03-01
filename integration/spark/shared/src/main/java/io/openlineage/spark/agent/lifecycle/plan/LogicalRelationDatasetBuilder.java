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
import io.openlineage.spark.api.AbstractQueryPlanDatasetBuilder;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import io.openlineage.sql.ColumnMeta;
import io.openlineage.sql.DbTableMeta;
import io.openlineage.sql.ExtractionError;
import io.openlineage.sql.SqlMeta;
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
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
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
    // if a LogicalPlan is a single node plan like `select * from temp`,
    // then it's leaf node and should not be considered output node
    if (x instanceof LogicalRelation && isSingleNodeLogicalPlan(x) && !searchDependencies) {
      return false;
    }

    return x instanceof LogicalRelation
        && (((LogicalRelation) x).relation() instanceof HadoopFsRelation
            || ((LogicalRelation) x).relation() instanceof JDBCRelation
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
    if (logRel.catalogTable() != null && logRel.catalogTable().isDefined()) {
      return handleCatalogTable(logRel);
    } else if (logRel.relation() instanceof HadoopFsRelation) {
      return handleHadoopFsRelation(logRel);
    } else if (logRel.relation() instanceof JDBCRelation) {
      return handleJdbcRelation(logRel, datasetFactory);
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
            new ArrayList<>(JavaConversions.asJavaCollection(relation.location().rootPaths()));
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

  private List<D> handleJdbcRelation(LogicalRelation x, DatasetFactory<D> datasetFactory) {
    // strip the jdbc: prefix from the url. this leaves us with a url like
    // postgresql://<hostname>:<port>/<database_name>?params
    // we don't parse the URI here because different drivers use different
    // connection
    // formats that aren't always amenable to how Java parses URIs. E.g., the oracle
    // driver format looks like oracle:<drivertype>:<user>/<password>@<database>
    // whereas postgres, mysql, and sqlserver use the scheme://hostname:port/db
    // format.
    JDBCRelation relation = (JDBCRelation) x.relation();
    String url = JdbcUtils.sanitizeJdbcUrl(relation.jdbcOptions().url());
    SqlMeta sqlMeta = JdbcUtils.extractQueryFromSpark(relation).get();
    if (!sqlMeta.errors().isEmpty()) { // error return nothing
      log.error(
              String.format(
                      "error while parsing query: %s",
                      sqlMeta.errors().stream()
                              .map(ExtractionError::toString)
                              .collect(Collectors.joining(","))));
    } else if (sqlMeta.inTables().isEmpty()) {
      log.error("no tables defined in query, this should not happen");
    } else if (sqlMeta.columnLineage().isEmpty()) {
      return Collections.singletonList(
              datasetFactory.getDataset(
                      sqlMeta.inTables().get(0).qualifiedName(), url, relation.schema()));
    } else {
      return sqlMeta.inTables().stream()
              .map(
                      dbtm ->
                              datasetFactory.getDataset(
                                      dbtm.qualifiedName(),
                                      url,
                                      generateJDBCSchema(dbtm, relation.schema(), sqlMeta)))
              .collect(Collectors.toList());
    }
    return Collections.emptyList();
  }

  private static StructType generateJDBCSchema(
          DbTableMeta origin, StructType schema, SqlMeta sqlMeta) {
    StructType originSchema = new StructType();
    for (StructField f : schema.fields()) {
      List<ColumnMeta> fields =
              sqlMeta.columnLineage().stream()
                      .filter(cl -> cl.descendant().name().equals(f.name()))
                      .flatMap(
                              cl ->
                                      cl.lineage().stream()
                                              .filter(
                                                      cm -> cm.origin().isPresent() && cm.origin().get().equals(origin)))
                      .collect(Collectors.toList());
      for (ColumnMeta cm : fields) {
        originSchema = originSchema.add(cm.name(), f.dataType());
      }
    }

    return originSchema;
  }

  protected Optional<String> getDatasetVersion(LogicalRelation x) {
    // not implemented
    return Optional.empty();
  }
}
