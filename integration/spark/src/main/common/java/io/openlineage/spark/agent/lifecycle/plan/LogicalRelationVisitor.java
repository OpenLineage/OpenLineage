package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.PlanUtils;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import scala.collection.JavaConversions;
import scala.runtime.AbstractFunction0;

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
 * <p>{@link org.apache.spark.sql.catalyst.catalog.CatalogTable}s, if present, can be used to
 * describe the {@link OpenLineage.Dataset} if its {@link org.apache.spark.sql.sources.BaseRelation}
 * is unknown.
 *
 * <p>TODO If a user specifies the {@link JDBCOptions#JDBC_QUERY_STRING()} option, we do not parse
 * the sql to determine the specific tables used. Since we return a List of {@link
 * OpenLineage.Dataset}s, we can parse the sql and determine each table referenced to return a
 * complete list of datasets referenced.
 */
@Slf4j
public class LogicalRelationVisitor extends QueryPlanVisitor<LogicalRelation, OpenLineage.Dataset> {
  private final SparkContext context;
  private final String jobNamespace;

  public LogicalRelationVisitor(SparkContext context, String jobNamespace) {
    this.context = context;
    this.jobNamespace = jobNamespace;
  }

  @Override
  public boolean isDefinedAt(LogicalPlan x) {
    return x instanceof LogicalRelation
        && (((LogicalRelation) x).relation() instanceof HadoopFsRelation
            || ((LogicalRelation) x).relation() instanceof JDBCRelation
            || ((LogicalRelation) x).catalogTable().isDefined());
  }

  @Override
  public List<OpenLineage.Dataset> apply(LogicalPlan x) {
    LogicalRelation logRel = (LogicalRelation) x;
    if (logRel.relation() instanceof HadoopFsRelation) {
      return handleHadoopFsRelation((LogicalRelation) x);
    } else if (logRel.relation() instanceof JDBCRelation) {
      return handleJdbcRelation((LogicalRelation) x);
    } else if (logRel.catalogTable().isDefined()) {
      return handleCatalogTable(logRel);
    }
    throw new IllegalArgumentException(
        "Expected logical plan to be either HadoopFsRelation, JDBCRelation, "
            + "or CatalogTable but was "
            + x);
  }

  private List<OpenLineage.Dataset> handleCatalogTable(LogicalRelation logRel) {
    CatalogTable catalogTable = logRel.catalogTable().get();
    return Collections.singletonList(
        PlanUtils.getDataset(catalogTable.location(), catalogTable.schema()));
  }

  private List<OpenLineage.Dataset> handleHadoopFsRelation(LogicalRelation x) {
    HadoopFsRelation relation = (HadoopFsRelation) x.relation();
    return JavaConversions.asJavaCollection(relation.location().rootPaths()).stream()
        .map(p -> PlanUtils.getDirectoryPath(p, context.hadoopConfiguration()))
        .distinct()
        .map(
            p -> {
              // TODO- refactor this to return a single partitioned dataset based on static
              // static partitions in the relation
              return PlanUtils.getDataset(p.toUri(), relation.schema());
            })
        .collect(Collectors.toList());
  }

  private List<OpenLineage.Dataset> handleJdbcRelation(LogicalRelation x) {
    JDBCRelation relation = (JDBCRelation) x.relation();
    // TODO- if a relation is composed of a complex sql query, we should attempt to
    // extract the
    // table names so that we can construct a true lineage
    String tableName =
        relation
            .jdbcOptions()
            .parameters()
            .get(JDBCOptions.JDBC_TABLE_NAME())
            .getOrElse(
                new AbstractFunction0<String>() {
                  @Override
                  public String apply() {
                    return "COMPLEX";
                  }
                });
    // strip the jdbc: prefix from the url. this leaves us with a url like
    // postgresql://<hostname>:<port>/<database_name>?params
    // we don't parse the URI here because different drivers use different connection
    // formats that aren't always amenable to how Java parses URIs. E.g., the oracle
    // driver format looks like oracle:<drivertype>:<user>/<password>@<database>
    // whereas postgres, mysql, and sqlserver use the scheme://hostname:port/db format.
    String url = relation.jdbcOptions().url().replaceFirst("jdbc:", "");
    OpenLineage.DatasetFacets datasetFacet = PlanUtils.datasetFacet(relation.schema(), url);
    return Collections.singletonList(PlanUtils.getDataset(tableName, url, datasetFacet));
  }
}
