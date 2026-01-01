/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.snowflake.lifecycle;

import static io.openlineage.spark.agent.vendor.snowflake.Constants.SNOWFLAKE_CLASS_NAME;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.agent.vendor.snowflake.lifecycle.plan.SnowflakeSaveIntoDataSourceCommandDatasetBuilder;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import net.snowflake.spark.snowflake.Parameters.MergedParameters;
import net.snowflake.spark.snowflake.SnowflakeRelation;
import net.snowflake.spark.snowflake.TableName;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.LogicalRelation;

/**
 * {@link LogicalPlan} visitor that matches the SnowflakeRelation that comes from Snowflake The
 * SnowflakeRelation is used to extract the table name and database name to populate the {@link
 * OpenLineage.Dataset} during Snowflake read operations. Writes are routed here from {@link
 * SnowflakeSaveIntoDataSourceCommandDatasetBuilder}.
 */
@Slf4j
public class SnowflakeRelationVisitor<D extends OpenLineage.Dataset>
    extends QueryPlanVisitor<LogicalRelation, D> {

  private final DatasetFactory<D> factory;

  public SnowflakeRelationVisitor(OpenLineageContext context, DatasetFactory<D> factory) {
    super(context);
    this.factory = factory;
  }

  protected boolean isSnowflakeClass(LogicalPlan plan) {
    try {
      Class c = Thread.currentThread().getContextClassLoader().loadClass(SNOWFLAKE_CLASS_NAME);
      return (plan instanceof LogicalRelation
          && c.isAssignableFrom(((LogicalRelation) plan).relation().getClass()));
    } catch (Exception e) {
      // swallow - not a snowflake class
    }
    return false;
  }

  @Override
  public boolean isDefinedAt(LogicalPlan plan) {
    return isSnowflakeClass(plan);
  }

  @Override
  public List<D> apply(LogicalPlan x) {
    SnowflakeRelation relation = (SnowflakeRelation) ((LogicalRelation) x).relation();

    MergedParameters params = relation.params();
    String sfDatabase = params.sfDatabase();
    String sfSchema = params.sfSchema();
    String sfFullURL = params.sfFullURL();
    Optional<String> dbtable =
        ScalaConversionUtils.asJavaOptional(params.table()).map(TableName::toString);
    Optional<String> query = ScalaConversionUtils.asJavaOptional(params.query());

    return SnowflakeDataset.getDatasets(
        factory, sfFullURL, sfDatabase, sfSchema, dbtable, query, relation.schema());
  }
}
