package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.sources.BaseRelation;

/**
 * {@link LogicalPlan} visitor that matches SqlDWRelation that comes from an Azure Databricks
 * environment. This function extracts a {@link OpenLineage.Dataset} from the SQL DW/ Synapse table
 * referenced by the relation. The convention used for a namespace is a URI of <code>
 * sqlserver://&lt;server&gt;.&lt;.datasetId&gt;.&lt;tableName&gt;</code> . The name for Sql Dw
 * tables may be table name (e.g. "exampleInputA") or a multi-part name (e.g.
 * "[dbo].[exampleInputA]"). If the data source is a query (e.g. <code>
 * ((select \"id\" FROM dbo.exampleInputA WHERE postalCode != '55555') q)</code>) then the name will
 * be <code>COMPLEX</code>.
 */
@Slf4j
public class SqlDWDatabricksVisitor<D extends OpenLineage.Dataset>
    extends QueryPlanVisitor<LogicalPlan, D> {
  private final DatasetFactory<D> factory;
  private static final Pattern dbJdbcPattern = Pattern.compile("database=([^;]*);?");
  private static final Pattern serverJdbcPattern = Pattern.compile("jdbc:([^;]*);?");
  private static final String DATABRICKS_CLASS_NAME = "com.databricks.spark.sqldw.SqlDWRelation";

  public SqlDWDatabricksVisitor(OpenLineageContext context, DatasetFactory<D> factory) {
    super(context);
    this.factory = factory;
  }

  public static boolean hasSqlDWDatabricksClasses() {
    try {
      SqlDWDatabricksVisitor.class.getClassLoader().loadClass(DATABRICKS_CLASS_NAME);
      return true;
    } catch (Exception e) {
      // swallow- we don't care
    }
    return false;
  }

  protected boolean isSqlDwRelationClass(LogicalPlan plan) {
    return plan instanceof LogicalRelation
        && ((LogicalRelation) plan).relation().getClass().getName().equals(DATABRICKS_CLASS_NAME);
  }

  @Override
  public boolean isDefinedAt(LogicalPlan plan) {
    return isSqlDwRelationClass(plan);
  }

  private Optional<String> getName(BaseRelation relation) {
    String tableName;
    try {
      tableName = (String) FieldUtils.readField(relation, "tableNameOrSubquery", true);
    } catch (IllegalAccessException | IllegalArgumentException e) {
      try {
        // In the Spark 2 version of this package, they used a more complex
        // name rather than tableNameOrSubquery
        log.debug("tableNameOrSubquery is not found. Trying Spark2 field");
        tableName =
            (String)
                FieldUtils.readField(
                    relation,
                    "com$databricks$spark$sqldw$SqlDWRelation$$tableNameOrSubquery",
                    true);
      } catch (IllegalAccessException | IllegalArgumentException secondException) {
        log.warn("Unable to discover SQLDW tableNameOrSubquery property");
        return Optional.empty();
      }
    }
    // The Synapse connector will return a table name wrapped in double quotes
    // or you could have a query string (e.g. (SELECT * FROM table)q)
    if (tableName.startsWith("\"") && tableName.endsWith("\"")) {
      tableName = tableName.replace("\"", "");
    }
    // TODO If there is a query, we should ultimately parse the SQL but
    // returning COMPLEX to be consistent with other implementations.
    if (tableName.startsWith("(")) {
      tableName = "COMPLEX";
    }
    return Optional.of(tableName);
  }

  private Optional<String> getNameSpace(BaseRelation relation) {

    String jdbcUrl;
    try {
      Object fieldDetails = FieldUtils.readField(relation, "params", true);
      jdbcUrl = (String) MethodUtils.invokeMethod(fieldDetails, true, "jdbcUrl");
    } catch (IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
      log.warn("Unable to discover SQLDW jdbcUrl Parameters");
      return Optional.empty();
    }

    Matcher serverMatcher = serverJdbcPattern.matcher(jdbcUrl);
    boolean serverIsFound = serverMatcher.find();
    Matcher dbMatcher = dbJdbcPattern.matcher(jdbcUrl);
    boolean dbIsFound = dbMatcher.find();

    if (!(dbIsFound && serverIsFound)) {
      log.warn("Unable to discover SQLDW database name or server name from jdbc url");
      return Optional.empty();
    }

    String databaseSubString = dbMatcher.group(1);
    String serverSubString = serverMatcher.group(1);
    String output = String.format("%s;database=%s;", serverSubString, databaseSubString);

    return Optional.of(output);
  }

  @Override
  public List<D> apply(LogicalPlan x) {
    BaseRelation relation = ((LogicalRelation) x).relation();
    List<D> output;
    Optional<String> name = getName(relation);
    Optional<String> namespace = getNameSpace(relation);
    if (name.isPresent() && namespace.isPresent()) {
      output =
          Collections.singletonList(
              factory.getDataset(name.get(), namespace.get(), relation.schema()));
    } else {
      output = Collections.emptyList();
    }
    return output;
  }
}
