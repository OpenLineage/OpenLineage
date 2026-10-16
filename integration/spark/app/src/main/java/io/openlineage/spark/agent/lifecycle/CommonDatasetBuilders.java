/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import com.google.common.collect.ImmutableList;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.plan.AlterTableAddColumnsCommandDatasetBuilder;
import io.openlineage.spark.agent.lifecycle.plan.AlterTableAddPartitionCommandDatasetBuilder;
import io.openlineage.spark.agent.lifecycle.plan.AlterTableRenameCommandDatasetBuilder;
import io.openlineage.spark.agent.lifecycle.plan.AlterTableSetLocationCommandDatasetBuilder;
import io.openlineage.spark.agent.lifecycle.plan.BigQueryInputDatasetBuilder;
import io.openlineage.spark.agent.lifecycle.plan.BigQueryOutputDatasetBuilder;
import io.openlineage.spark.agent.lifecycle.plan.CreateDataSourceTableAsSelectCommandDatasetBuilder;
import io.openlineage.spark.agent.lifecycle.plan.CreateDataSourceTableCommandDatasetBuilder;
import io.openlineage.spark.agent.lifecycle.plan.CreateHiveTableAsSelectCommandDatasetBuilder;
import io.openlineage.spark.agent.lifecycle.plan.CreateTableCommandDatasetBuilder;
import io.openlineage.spark.agent.lifecycle.plan.DropTableCommandDatasetBuilder;
import io.openlineage.spark.agent.lifecycle.plan.ExternalRDDDatasetBuilder;
import io.openlineage.spark.agent.lifecycle.plan.HiveTableRelationDatasetBuilder;
import io.openlineage.spark.agent.lifecycle.plan.InsertIntoDataSourceDatasetBuilder;
import io.openlineage.spark.agent.lifecycle.plan.InsertIntoDataSourceDirDatasetBuilder;
import io.openlineage.spark.agent.lifecycle.plan.InsertIntoDirDatasetBuilder;
import io.openlineage.spark.agent.lifecycle.plan.InsertIntoHadoopFsRelationDatasetBuilder;
import io.openlineage.spark.agent.lifecycle.plan.InsertIntoHiveDirDatasetBuilder;
import io.openlineage.spark.agent.lifecycle.plan.InsertIntoHiveTableDatasetBuilder;
import io.openlineage.spark.agent.lifecycle.plan.KafkaStreamingWriteDatasetBuilder;
import io.openlineage.spark.agent.lifecycle.plan.LoadDataCommandDatasetBuilder;
import io.openlineage.spark.agent.lifecycle.plan.LoadDataCommandInputDatasetBuilder;
import io.openlineage.spark.agent.lifecycle.plan.LogicalRDDDatasetBuilder;
import io.openlineage.spark.agent.lifecycle.plan.OptimizedCreateHiveTableAsSelectCommandDatasetBuilder;
import io.openlineage.spark.agent.lifecycle.plan.StreamingDataSourceV2RelationDatasetBuilder;
import io.openlineage.spark.agent.lifecycle.plan.TruncateTableCommandDatasetBuilder;
import io.openlineage.spark.agent.util.BigQueryUtils;
import io.openlineage.spark.agent.util.ReflectionUtils;
import io.openlineage.spark.api.AbstractQueryPlanDatasetBuilder;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.CreateTableLikeCommandDatasetBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.DropTableDatasetBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.RefreshTableCommandDatasetBuilder;
import io.openlineage.spark32.agent.lifecycle.plan.RepairTableCommandDatasetBuilder;
import java.util.List;
import org.apache.spark.scheduler.SparkListenerEvent;

/**
 * Dataset builders registered for every Spark version. Each {@link DatasetBuilderFactory} appends
 * them after its version specific builders, so builders listed here resolve a plan node or a job
 * name suffix only when no version specific builder does. The order within the list is the order of
 * the query plan visitors they were migrated from.
 */
final class CommonDatasetBuilders {

  private CommonDatasetBuilders() {}

  static List<AbstractQueryPlanDatasetBuilder<SparkListenerEvent, ?, OpenLineage.InputDataset>>
      inputs(OpenLineageContext context) {
    ImmutableList.Builder<
            AbstractQueryPlanDatasetBuilder<SparkListenerEvent, ?, OpenLineage.InputDataset>>
        builders = ImmutableList.builder();
    DatasetFactory<OpenLineage.InputDataset> factory = DatasetFactory.input(context);
    builders.add(new LogicalRDDDatasetBuilder<>(context, factory, true));
    if (InsertIntoHiveTableDatasetBuilder.hasHiveClasses()) {
      builders.add(new HiveTableRelationDatasetBuilder<>(context, factory, true));
    }
    if (BigQueryUtils.hasBigQueryClasses()) {
      builders.add(new BigQueryInputDatasetBuilder(context));
    }
    builders.add(
        new ExternalRDDDatasetBuilder(context),
        new LoadDataCommandInputDatasetBuilder(context),
        new StreamingDataSourceV2RelationDatasetBuilder(context),
        new RefreshTableCommandDatasetBuilder(context));
    return builders.build();
  }

  static List<AbstractQueryPlanDatasetBuilder<SparkListenerEvent, ?, OpenLineage.OutputDataset>>
      outputs(OpenLineageContext context) {
    ImmutableList.Builder<
            AbstractQueryPlanDatasetBuilder<SparkListenerEvent, ?, OpenLineage.OutputDataset>>
        builders = ImmutableList.builder();
    DatasetFactory<OpenLineage.OutputDataset> factory = DatasetFactory.output(context);
    builders.add(new LogicalRDDDatasetBuilder<>(context, factory, false));
    if (InsertIntoHiveTableDatasetBuilder.hasHiveClasses()) {
      builders.add(new HiveTableRelationDatasetBuilder<>(context, factory, false));
    }
    if (BigQueryUtils.hasBigQueryClasses()) {
      builders.add(new BigQueryOutputDatasetBuilder(context));
    }
    builders.add(
        new InsertIntoDataSourceDirDatasetBuilder(context),
        new InsertIntoDataSourceDatasetBuilder(context),
        new InsertIntoHadoopFsRelationDatasetBuilder(context),
        new CreateDataSourceTableAsSelectCommandDatasetBuilder(context),
        new InsertIntoDirDatasetBuilder(context));
    if (InsertIntoHiveTableDatasetBuilder.hasHiveClasses()) {
      builders.add(
          new InsertIntoHiveTableDatasetBuilder(context),
          new InsertIntoHiveDirDatasetBuilder(context),
          new CreateHiveTableAsSelectCommandDatasetBuilder(context));
    }
    if (OptimizedCreateHiveTableAsSelectCommandDatasetBuilder.hasClasses()) {
      builders.add(new OptimizedCreateHiveTableAsSelectCommandDatasetBuilder(context));
    }
    builders.add(
        new CreateDataSourceTableCommandDatasetBuilder(context),
        new LoadDataCommandDatasetBuilder(context),
        new AlterTableRenameCommandDatasetBuilder(context),
        new AlterTableAddColumnsCommandDatasetBuilder(context),
        new CreateTableCommandDatasetBuilder(context),
        new DropTableCommandDatasetBuilder(context),
        new TruncateTableCommandDatasetBuilder(context),
        new AlterTableSetLocationCommandDatasetBuilder(context),
        new AlterTableAddPartitionCommandDatasetBuilder(context),
        new CreateTableLikeCommandDatasetBuilder(context),
        new DropTableDatasetBuilder(context),
        new KafkaStreamingWriteDatasetBuilder(context));
    // RepairTableCommand replaced AlterTableRecoverPartitionsCommand in Spark 3.2
    if (ReflectionUtils.hasClass("org.apache.spark.sql.execution.command.RepairTableCommand")) {
      builders.add(new RepairTableCommandDatasetBuilder(context));
    }
    return builders.build();
  }
}
