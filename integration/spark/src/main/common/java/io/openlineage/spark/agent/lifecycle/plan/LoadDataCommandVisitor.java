package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.PlanUtils;

import java.util.Collections;
import java.util.List;

import lombok.SneakyThrows;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.LoadDataCommand;

/**
 * {@link LogicalPlan} visitor that matches an {@link LoadDataCommandVisitor} and
 * extracts the output {@link OpenLineage.Dataset} being written.
 */
public class LoadDataCommandVisitor
  extends QueryPlanVisitor<LoadDataCommand, OpenLineage.Dataset> {

  private final SparkSession sparkSession;
  
  public LoadDataCommandVisitor(SparkSession sparkSession) {
    this.sparkSession = sparkSession;
  }
  
  @SneakyThrows
  @Override
  public List<OpenLineage.Dataset> apply(LogicalPlan x) {
    LoadDataCommand command = (LoadDataCommand) x;
    CatalogTable table = sparkSession.sessionState().catalog().getTableMetadata(command.table());
    return Collections.singletonList(PlanUtils.getDataset(table));
  }

//  @SneakyThrows
//  private Path getHivePath(LoadDataCommand command) {
//    // This logic is copied and translated from Scala from the LoadDataCommand itself
//    if (command.isLocal()) {
//      FileContext localFS = FileContext.getLocalFSFileContext();
//      return LoadDataCommand.makeQualified(FsConstants.LOCAL_FS_URI, localFS.getWorkingDirectory(),
//        new Path(command.path()));
//    } else {
//      Path loadPath = new Path(command.path());
//      // Follow Hive's behavior:
//      // If no schema or authority is provided with non-local inpath,
//      // we will use hadoop configuration "fs.defaultFS".
//      String defaultFSConf = SparkSession.active().sessionState().newHadoopConf().get("fs.defaultFS")
//      URI defaultFS;
//      if (defaultFSConf == null) {
//        defaultFS = new URI("");
//      } else {
//        defaultFS = new URI(defaultFSConf);
//      }
//      // Follow Hive's behavior:
//      // If LOCAL is not specified, and the path is relative,
//      // then the path is interpreted relative to "/user/<username>"
//      Path uriPath = new Path(String.format("/user/%s/", System.getProperty("user.name")));
//      // makeQualified() will ignore the query parameter part while creating a path, so the
//      // entire  string will be considered while making a Path instance,this is mainly done
//      // by considering the wild card scenario in mind.as per old logic query param  is
//      // been considered while creating URI instance and if path contains wild card char '?'
//      // the remaining characters after '?' will be removed while forming URI instance
//      return LoadDataCommand.makeQualified(defaultFS, uriPath, loadPath);
//    }
//  }
}
