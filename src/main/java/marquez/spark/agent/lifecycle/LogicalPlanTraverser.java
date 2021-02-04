package marquez.spark.agent.lifecycle;

import static scala.collection.JavaConversions.asJavaCollection;

import java.util.Collection;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.CatalogTableType;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand;
import org.apache.spark.sql.execution.datasources.FileIndex;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public abstract class LogicalPlanTraverser {
  public Object visit(LogicalPlan plan) {
    if (plan == null) return null;

    switch (plan.getClass().getSimpleName()) {
      case "LogicalRelation":
        if (plan instanceof LogicalRelation) {
          visit((LogicalRelation) plan);
        }
        break;
      case "InsertIntoHadoopFsRelationCommand":
        if (plan instanceof InsertIntoHadoopFsRelationCommand) {
          visit((InsertIntoHadoopFsRelationCommand) plan);
        }
        break;
      case "CreateDataSourceTableAsSelectCommand":
        if (plan instanceof CreateDataSourceTableAsSelectCommand) {
          visit((CreateDataSourceTableAsSelectCommand) plan);
        }
        break;
    }

    visitChildren(asJavaCollection(plan.children()));

    return null;
  }

  protected Object visit(
      CreateDataSourceTableAsSelectCommand createDataSourceTableAsSelectCommand) {
    visit(createDataSourceTableAsSelectCommand.table());
    visit(createDataSourceTableAsSelectCommand.query());
    visit(createDataSourceTableAsSelectCommand.mode());
    return null;
  }

  protected Object visit(SaveMode mode) {
    if (mode != null) {
      return mode.name();
    }
    return null;
  }

  protected Object visit(CatalogTable table) {
    visit(table.identifier());
    visit(table.tableType());
    visit(table.storage());
    visit(table.schema());
    return null;
  }

  protected Object visit(CatalogStorageFormat storage) {
    return null;
  }

  protected Object visit(CatalogTableType tableType) {
    return null;
  }

  protected Object visit(TableIdentifier identifier) {
    return null;
  }

  protected Object visit(InsertIntoHadoopFsRelationCommand insertIntoHadoopFsRelationCommand) {
    visit(insertIntoHadoopFsRelationCommand.outputPath());
    visit(insertIntoHadoopFsRelationCommand.query());
    return null;
  }

  protected Object visit(LogicalRelation logicalRelation) {
    visitRelation(logicalRelation.relation());
    return null;
  }

  protected Object visitRelation(BaseRelation relation) {
    switch (relation.getClass().getSimpleName()) {
      case "JDBCRelation":
        visit((JDBCRelation) relation);
        break;
      case "HadoopFsRelation":
        visit((HadoopFsRelation) relation);
        break;
    }
    visit(relation.schema());
    return null;
  }

  protected Object visit(JDBCRelation relation) {
    visit(relation.jdbcOptions());
    return null;
  }

  protected Object visit(JDBCOptions jdbcOptions) {
    return null;
  }

  protected Object visit(StructType structType) {
    visit(structType.fields());
    return null;
  }

  protected Object visit(StructField[] fields) {
    for (StructField field : fields) {
      visit(field);
    }
    return null;
  }

  protected Object visit(StructField field) {
    visit(field.dataType());
    return null;
  }

  protected Object visit(DataType dataType) {
    return null;
  }

  protected Object visit(HadoopFsRelation relation) {

    visit(relation.location());
    return null;
  }

  protected Object visit(FileIndex fileIndex) {

    visitPaths(asJavaCollection(fileIndex.rootPaths()));
    return null;
  }

  protected Object visitPaths(Collection<Path> paths) {
    for (Path path : paths) {
      visit(path);
    }
    return null;
  }

  protected Object visit(Path path) {
    return null;
  }

  protected Object visitChildren(Collection<LogicalPlan> children) {
    for (LogicalPlan plan : children) {
      visit(plan);
    }
    return null;
  }
}
