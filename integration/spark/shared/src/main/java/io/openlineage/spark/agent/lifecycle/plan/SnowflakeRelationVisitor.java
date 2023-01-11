/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import net.snowflake.spark.snowflake.DefaultSource;
import net.snowflake.spark.snowflake.Parameters.MergedParameters;
import net.snowflake.spark.snowflake.SnowflakeRelation;
import net.snowflake.spark.snowflake.TableName;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.apache.spark.sql.types.StructType;
import scala.Option;

/**
 * {@link LogicalPlan} visitor that matches the SnowflakeRelation that comes from Snowflake The
 * SnowflakeRelation is used to extract the table name and database name to populate the {@link
 * OpenLineage.Dataset} during Snowflake read operations. Writes are routed here from
 * SaveIntoDataSourceCommandVisitor.
 */
@Slf4j
public class SnowflakeRelationVisitor<D extends OpenLineage.Dataset>
    extends QueryPlanVisitor<LogicalRelation, D> {

  private final DatasetFactory<D> factory;

  private static final String SNOWFLAKE_CLASS_NAME =
      "net.snowflake.spark.snowflake.SnowflakeRelation";

  private static final String SNOWFLAKE_PROVIDER_CLASS_NAME =
      "net.snowflake.spark.snowflake.DefaultSource";

  private static final String SNOWFLAKE_PREFIX = "snowflake://";

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

  public static boolean hasSnowflakeClasses() {
    /**
     * Checking the Snowflake class with both
     * SnowflakeRelationVisitor.class.getClassLoader.loadClass and
     * Thread.currentThread().getContextClassLoader().loadClass. The first checks if the class is
     * present on the classpath, and the second one is a catchall which captures if the class has
     * been installed. This is relevant for Azure Databricks where jars can be installed and
     * accessible to the user, even if they are not present on the classpath.
     */
    try {
      SnowflakeRelationVisitor.class.getClassLoader().loadClass(SNOWFLAKE_PROVIDER_CLASS_NAME);
      return true;
    } catch (Exception e) {
      // swallow - we don't care
    }
    try {
      Thread.currentThread().getContextClassLoader().loadClass(SNOWFLAKE_PROVIDER_CLASS_NAME);
      return true;
    } catch (Exception e) {
      // swallow - we don't care
    }
    return false;
  }

  public static boolean isSnowflakeSource(CreatableRelationProvider provider) {
    return hasSnowflakeClasses() && provider instanceof DefaultSource;
  }

  @Override
  public boolean isDefinedAt(LogicalPlan plan) {
    return isSnowflakeClass(plan);
  }

  @Override
  public List<D> apply(LogicalPlan x) {
    SnowflakeRelation relation = (SnowflakeRelation) ((LogicalRelation) x).relation();

    List<D> output;

    MergedParameters params = relation.params();
    String sfDatabase = params.sfDatabase();
    String sfSchema = params.sfSchema();
    String url = params.sfFullURL();
    String tableName = "COMPLEX";
    Option<TableName> table = params.table();

    try {
      TableName tableNameObj = table.getOrElse(null);

      if (tableNameObj != null) {
        tableName = tableNameObj.toString();
      }
    } catch (NullPointerException e) {
      //  already initialized table name to COMPLEX
      log.warn("Unable to discover Snowflake table property");
    }

    url = url.replace("https://", "");
    String name = String.format("%s.%s.%s", sfDatabase, sfSchema, tableName);
    String namespace = String.format("%s%s", SNOWFLAKE_PREFIX, url);

    output = Collections.singletonList(factory.getDataset(name, namespace, relation.schema()));

    return output;
  }

  public static <D extends OpenLineage.Dataset> List<D> createSnowflakeDatasets(
      DatasetFactory<D> datasetFactory,
      scala.collection.immutable.Map<String, String> options,
      StructType schema) {
    // Called from SaveIntoDataSourceCommandVisitor on Snowflake write operations.

    List<D> output;

    Map<String, String> javaOptions =
        io.openlineage.spark.agent.util.ScalaConversionUtils.fromMap(options);

    String tableName = javaOptions.get("dbtable");
    if (tableName == null) {
      tableName = "COMPLEX";
    }

    String sfSchema = javaOptions.get("sfschema");
    String sfUrl = javaOptions.get("sfurl");
    String sfDatabase = javaOptions.get("sfdatabase");

    sfUrl = sfUrl.replace("https://", "");
    String name = String.format("%s.%s.%s", sfDatabase, sfSchema, tableName);
    String namespace = String.format("%s%s", SNOWFLAKE_PREFIX, sfUrl);

    output = Collections.singletonList(datasetFactory.getDataset(name, namespace, schema));
    return output;
  }
}
