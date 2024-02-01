/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.snowflake.lifecycle.plan;

import static io.openlineage.spark.agent.vendor.snowflake.SnowflakeVendor.hasSnowflakeClasses;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.agent.vendor.snowflake.lifecycle.SnowflakeDataset;
import io.openlineage.spark.api.AbstractQueryPlanDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import net.snowflake.spark.snowflake.DefaultSource;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.apache.spark.sql.types.StructType;

/** Look to {@link io.openlineage.spark.agent.lifecycle.plan.SaveIntoDataSourceCommandVisitor} */
public class SnowflakeSaveIntoDataSourceCommandDatasetBuilder
    extends AbstractQueryPlanDatasetBuilder<
        SparkListenerEvent, SaveIntoDataSourceCommand, OpenLineage.OutputDataset> {

  public SnowflakeSaveIntoDataSourceCommandDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(SaveIntoDataSourceCommand command) {
    if (isSnowflakeSource(command.dataSource())) {
      // Called from SaveIntoDataSourceCommandVisitor on Snowflake write operations.

      Map<String, String> options = ScalaConversionUtils.<String, String>fromMap(command.options());
      Optional<String> dbtable = Optional.ofNullable(options.get("dbtable"));
      String sfSchema = options.get("sfschema");
      String sfUrl = options.get("sfurl");
      String sfDatabase = options.get("sfdatabase");

      return Collections.singletonList(
          // Similar to Kafka, Snowflake also has some special handling. So we use the method
          // below for extracting the dataset from Snowflake write operations.
          SnowflakeDataset.getDataset(
              outputDataset(), sfUrl, sfDatabase, sfSchema, dbtable, getSchema(command)
              // command.schema() doesn't seem to contain the schema when tested with Azure
              // Snowflake,
              // so we use the helper to extract it from the logical plan.
              ));
    } else {
      return Collections.emptyList();
    }
  }

  public static boolean isSnowflakeSource(CreatableRelationProvider provider) {
    return hasSnowflakeClasses() && provider instanceof DefaultSource;
  }

  /**
   * Taken from {@link
   * io.openlineage.spark.agent.lifecycle.plan.SaveIntoDataSourceCommandVisitor#getSchema(org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand)}
   *
   * @param command
   * @return
   */
  private StructType getSchema(SaveIntoDataSourceCommand command) {
    StructType schema = command.schema();
    if ((schema == null || schema.fields() == null || schema.fields().length == 0)
        && command.query() != null
        && command.query().output() != null) {
      // get schema from logical plan's output
      schema = PlanUtils.toStructType(ScalaConversionUtils.fromSeq(command.query().output()));
    }
    return schema;
  }
}
