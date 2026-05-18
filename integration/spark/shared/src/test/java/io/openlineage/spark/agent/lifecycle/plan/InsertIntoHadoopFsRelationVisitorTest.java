/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import scala.Option;

class InsertIntoHadoopFsRelationVisitorTest {
  @Test
  void applyNormalizesHiveStylePartitionedOutputPathWhenEnabled() {
    OpenLineageContext context = mock(OpenLineageContext.class);
    SparkSession sparkSession = mock(SparkSession.class);
    SparkOpenLineageConfig config = new SparkOpenLineageConfig();
    config.setNormalizeHiveStylePartitioning(true);

    when(context.getOpenLineage()).thenReturn(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI));
    when(context.getOpenLineageConfig()).thenReturn(config);
    when(context.getSparkSession()).thenReturn(Optional.of(sparkSession));

    LogicalPlan query = mock(LogicalPlan.class);
    when(query.schema()).thenReturn(new StructType().add("id", StringType$.MODULE$));

    InsertIntoHadoopFsRelationCommand command = mock(InsertIntoHadoopFsRelationCommand.class);
    when(command.catalogTable()).thenReturn(Option.empty());
    when(command.outputPath()).thenReturn(new Path("s3://bucket/table/dt=20260516"));
    when(command.query()).thenReturn(query);
    when(command.mode()).thenReturn(SaveMode.Append);

    List<OpenLineage.OutputDataset> datasets =
        new InsertIntoHadoopFsRelationVisitor(context).apply(command);

    assertThat(datasets).hasSize(1);
    assertThat(datasets.get(0).getNamespace()).isEqualTo("s3://bucket");
    assertThat(datasets.get(0).getName()).isEqualTo("table");
  }
}
