package marquez.spark.agent.lifecycle.plan;

import java.util.Collections;
import java.util.List;
import marquez.spark.agent.client.LineageEvent.Dataset;
import marquez.spark.agent.client.LineageEvent.DatasetFacet;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractPartialFunction;

/**
 * {@link LogicalPlan} visitor that extracts a {@link Dataset} from a {@link JDBCRelation} or a
 * {@link SaveIntoDataSourceCommand} that writes using a {@link JdbcRelationProvider}. {@link
 * Dataset} naming expects the namespace to be the JDBC connection URL (schema and authority only)
 * and the table name to be the <code>&lt;database&gt;</code>.<code>&lt;tableName&gt;</code>.
 *
 * <p>TODO If a user specifies the {@link JDBCOptions#JDBC_QUERY_STRING()} option, we do not parse
 * the sql to determine the specific tables used. Since we return a List of {@link Dataset}s, we can
 * parse the sql and determine each table referenced to return a complete list of datasets
 * referenced.
 */
public class JDBCRelationVisitor extends AbstractPartialFunction<LogicalPlan, List<Dataset>> {
  private final SQLContext sqlContext;

  public JDBCRelationVisitor(SQLContext sqlContext) {
    this.sqlContext = sqlContext;
  }

  @Override
  public boolean isDefinedAt(LogicalPlan x) {
    return x instanceof LogicalRelation && ((LogicalRelation) x).relation() instanceof JDBCRelation;
  }

  @Override
  public List<Dataset> apply(LogicalPlan x) {
    JDBCRelation relation = (JDBCRelation) ((LogicalRelation) x).relation();
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
    DatasetFacet datasetFacet = PlanUtils.datasetFacet(relation.schema(), url);
    return Collections.singletonList(
        Dataset.builder().namespace(url).name(tableName).facets(datasetFacet).build());
  }
}
