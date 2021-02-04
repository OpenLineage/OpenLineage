package marquez.spark.agent.lifecycle;

import static scala.collection.JavaConversions.asJavaCollection;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

public class LogicalPlanFacetTraverser extends LogicalPlanTraverser {
  @Override
  public Map<String, Object> visit(LogicalPlan plan) {
    if (plan == null) return null;

    Map<String, Object> map = new LinkedHashMap<>();
    switch (plan.getClass().getSimpleName()) {
      case "LogicalRelation":
        if (plan instanceof LogicalRelation) {
          map.put(plan.getClass().getName(), visit((LogicalRelation) plan));
        }
        break;
      case "InsertIntoHadoopFsRelationCommand":
        if (plan instanceof InsertIntoHadoopFsRelationCommand) {
          map.put(plan.getClass().getName(), visit((InsertIntoHadoopFsRelationCommand) plan));
        }
        break;
      case "CreateDataSourceTableAsSelectCommand":
        if (plan instanceof CreateDataSourceTableAsSelectCommand) {
          map.put(plan.getClass().getName(), visit((CreateDataSourceTableAsSelectCommand) plan));
        }
        break;
    }
    map.put("type", plan.getClass().getName());
    map.put("name", plan.nodeName());
    List children = visitChildren(asJavaCollection(plan.children()));
    if (children.size() != 0) {
      map.put("children", children);
    }
    return map;
  }

  @Override
  protected Object visit(
      CreateDataSourceTableAsSelectCommand createDataSourceTableAsSelectCommand) {
    Map map = new LinkedHashMap<>();
    map.put("table", visit(createDataSourceTableAsSelectCommand.table()));
    map.put("query", visit(createDataSourceTableAsSelectCommand.query()));
    map.put("mode", visit(createDataSourceTableAsSelectCommand.mode()));
    return map;
  }

  @Override
  protected Object visit(SaveMode mode) {
    if (mode != null) {
      return mode.name();
    }
    return null;
  }

  @Override
  protected Object visit(CatalogTable table) {
    Map map = new LinkedHashMap<>();
    map.put("identifier", visit(table.identifier()));
    map.put("tableType", visit(table.tableType()));
    map.put("storage", visit(table.storage()));
    map.put("schema", visit(table.schema()));
    return map;
  }

  @Override
  protected Object visit(CatalogStorageFormat storage) {
    Map map = new LinkedHashMap<>();
    map.put("locationUri", storage.locationUri());
    map.put("inputFormat", storage.inputFormat());
    map.put("outputFormat", storage.outputFormat());
    map.put("serde", storage.serde());
    map.put("compressed", storage.compressed());
    return map;
  }

  @Override
  protected Object visit(CatalogTableType tableType) {
    return tableType.name();
  }

  @Override
  protected Object visit(TableIdentifier identifier) {
    return identifier.identifier();
  }

  @Override
  protected Object visit(InsertIntoHadoopFsRelationCommand insertIntoHadoopFsRelationCommand) {
    Map map = new LinkedHashMap<>();
    map.put("outputPath", visit(insertIntoHadoopFsRelationCommand.outputPath()));
    map.put("query", visit(insertIntoHadoopFsRelationCommand.query()));
    map.put("options", asJavaCollection(insertIntoHadoopFsRelationCommand.options()));
    return map;
  }

  @Override
  protected Object visit(LogicalRelation logicalRelation) {
    Map<String, Object> map = new HashMap<>();
    map.put("relation", visitRelation(logicalRelation.relation()));
    return map;
  }

  @Override
  protected Object visitRelation(BaseRelation relation) {
    Map map = new LinkedHashMap();
    switch (relation.getClass().getSimpleName()) {
      case "JDBCRelation":
        map = visit((JDBCRelation) relation);
        break;
      case "HadoopFsRelation":
        map = visit((HadoopFsRelation) relation);
        break;
    }
    map.put("schema", visit(relation.schema()));
    return map;
  }

  @Override
  protected Map visit(JDBCRelation relation) {
    Map map = new LinkedHashMap<>();
    map.put("jdbcOptions", visit(relation.jdbcOptions()));
    return map;
  }

  @Override
  protected Object visit(JDBCOptions jdbcOptions) {
    return jdbcOptions.asProperties();
  }

  @Override
  protected Object visit(StructType structType) {
    Map map = new LinkedHashMap();
    map.put("fields", visit(structType.fields()));
    return map;
  }

  @Override
  protected Object visit(StructField[] fields) {
    List list = new ArrayList();
    for (StructField field : fields) {
      list.add(visit(field));
    }
    return list;
  }

  @Override
  protected Object visit(StructField field) {
    Map map = new LinkedHashMap();
    map.put("name", field.name());
    map.put("type", visit(field.dataType()));
    map.put("nullable", field.nullable());
    return map;
  }

  @Override
  protected Object visit(DataType dataType) {
    if (dataType != null) {
      return dataType.simpleString();
    }
    return null;
  }

  @Override
  protected Map visit(HadoopFsRelation relation) {
    Map map = new LinkedHashMap<>();
    map.put("location", visit(relation.location()));
    return map;
  }

  @Override
  protected Object visit(FileIndex fileIndex) {
    Map map = new LinkedHashMap<>();
    map.put("rootPaths", visitPaths(asJavaCollection(fileIndex.rootPaths())));
    return map;
  }

  @Override
  protected Object visitPaths(Collection<Path> paths) {
    List list = new ArrayList();
    for (Path path : paths) {
      list.add(visit(path));
    }
    return list;
  }

  @Override
  protected Object visit(Path path) {
    Map map = new LinkedHashMap<>();
    map.put("name", path.getName());
    map.put("uri", visitPathUri(path.toUri()));
    return map;
  }

  protected Object visitPathUri(URI uri) {
    return uri.toASCIIString();
  }

  protected List visitChildren(Collection<LogicalPlan> children) {
    List list = new ArrayList<>();
    for (LogicalPlan plan : children) {
      list.add(visit(plan));
    }
    return list;
  }
}
