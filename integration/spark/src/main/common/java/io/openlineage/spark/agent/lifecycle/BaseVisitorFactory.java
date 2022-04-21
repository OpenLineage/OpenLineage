/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent.lifecycle;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.Dataset;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.spark.agent.lifecycle.plan.AlterTableAddColumnsCommandVisitor;
import io.openlineage.spark.agent.lifecycle.plan.AlterTableRenameCommandVisitor;
import io.openlineage.spark.agent.lifecycle.plan.BigQueryNodeVisitor;
import io.openlineage.spark.agent.lifecycle.plan.CreateDataSourceTableAsSelectCommandVisitor;
import io.openlineage.spark.agent.lifecycle.plan.CreateDataSourceTableCommandVisitor;
import io.openlineage.spark.agent.lifecycle.plan.CreateHiveTableAsSelectCommandVisitor;
import io.openlineage.spark.agent.lifecycle.plan.CreateTableCommandVisitor;
import io.openlineage.spark.agent.lifecycle.plan.DropTableCommandVisitor;
import io.openlineage.spark.agent.lifecycle.plan.InsertIntoDataSourceDirVisitor;
import io.openlineage.spark.agent.lifecycle.plan.InsertIntoDataSourceVisitor;
import io.openlineage.spark.agent.lifecycle.plan.InsertIntoDirVisitor;
import io.openlineage.spark.agent.lifecycle.plan.InsertIntoHadoopFsRelationVisitor;
import io.openlineage.spark.agent.lifecycle.plan.InsertIntoHiveDirVisitor;
import io.openlineage.spark.agent.lifecycle.plan.InsertIntoHiveTableVisitor;
import io.openlineage.spark.agent.lifecycle.plan.KafkaRelationVisitor;
import io.openlineage.spark.agent.lifecycle.plan.LoadDataCommandVisitor;
import io.openlineage.spark.agent.lifecycle.plan.LogicalRDDVisitor;
import io.openlineage.spark.agent.lifecycle.plan.OptimizedCreateHiveTableAsSelectCommandVisitor;
import io.openlineage.spark.agent.lifecycle.plan.SqlDWDatabricksVisitor;
import io.openlineage.spark.agent.lifecycle.plan.SqlExecutionRDDVisitor;
import io.openlineage.spark.agent.lifecycle.plan.TruncateTableCommandVisitor;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.PartialFunction;

abstract class BaseVisitorFactory implements VisitorFactory {

  protected <D extends Dataset> List<PartialFunction<LogicalPlan, List<D>>> getBaseCommonVisitors(
      OpenLineageContext context, DatasetFactory<D> factory) {
    List<PartialFunction<LogicalPlan, List<D>>> list = new ArrayList<>();
    list.add(new LogicalRDDVisitor(context, factory));
    if (BigQueryNodeVisitor.hasBigQueryClasses()) {
      list.add(new BigQueryNodeVisitor(context, factory));
    }
    if (KafkaRelationVisitor.hasKafkaClasses()) {
      list.add(new KafkaRelationVisitor(context, factory));
    }
    if (SqlDWDatabricksVisitor.hasSqlDWDatabricksClasses()) {
      list.add(new SqlDWDatabricksVisitor(context, factory));
    }
    return list;
  }

  public abstract <D extends Dataset> List<PartialFunction<LogicalPlan, List<D>>> getCommonVisitors(
      OpenLineageContext context, DatasetFactory<D> factory);

  @Override
  public List<PartialFunction<LogicalPlan, List<InputDataset>>> getInputVisitors(
      OpenLineageContext context) {
    List<PartialFunction<LogicalPlan, List<OpenLineage.InputDataset>>> inputVisitors =
        new ArrayList<>(getCommonVisitors(context, DatasetFactory.input(context)));
    if (VisitorFactory.classPresent("org.apache.spark.sql.execution.SQLExecutionRDD")) {
      inputVisitors.add(new SqlExecutionRDDVisitor(context));
    }
    return inputVisitors;
  }

  @Override
  public List<PartialFunction<LogicalPlan, List<OpenLineage.OutputDataset>>> getOutputVisitors(
      OpenLineageContext context) {
    DatasetFactory<OpenLineage.OutputDataset> factory = DatasetFactory.output(context);

    List<PartialFunction<LogicalPlan, List<OpenLineage.OutputDataset>>> outputCommonVisitors =
        getCommonVisitors(context, factory);
    List<PartialFunction<LogicalPlan, List<OpenLineage.OutputDataset>>> list =
        new ArrayList<>(outputCommonVisitors);

    list.add(new InsertIntoDataSourceDirVisitor(context));
    list.add(new InsertIntoDataSourceVisitor(context));
    list.add(new InsertIntoHadoopFsRelationVisitor(context));
    list.add(new CreateDataSourceTableAsSelectCommandVisitor(context));
    list.add(new InsertIntoDirVisitor(context));
    if (InsertIntoHiveTableVisitor.hasHiveClasses()) {
      list.add(new InsertIntoHiveTableVisitor(context));
      list.add(new InsertIntoHiveDirVisitor(context));
      list.add(new CreateHiveTableAsSelectCommandVisitor(context));
    }
    if (OptimizedCreateHiveTableAsSelectCommandVisitor.hasClasses()) {
      list.add(new OptimizedCreateHiveTableAsSelectCommandVisitor(context));
    }
    list.add(new CreateDataSourceTableCommandVisitor(context));
    list.add(new LoadDataCommandVisitor(context));
    list.add(new AlterTableRenameCommandVisitor(context));
    list.add(new AlterTableAddColumnsCommandVisitor(context));
    list.add(new CreateTableCommandVisitor(context));
    list.add(new DropTableCommandVisitor(context));
    list.add(new TruncateTableCommandVisitor(context));
    return list;
  }
}
