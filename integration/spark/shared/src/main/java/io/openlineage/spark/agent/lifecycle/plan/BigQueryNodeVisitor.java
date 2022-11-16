/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.cloud.spark.bigquery.BigQueryRelation;
import com.google.cloud.spark.bigquery.BigQueryRelationProvider;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import scala.Option;

/**
 * {@link LogicalPlan} visitor that matches {@link BigQueryRelation}s or {@link
 * SaveIntoDataSourceCommand}s that use a {@link BigQueryRelationProvider}. This function extracts a
 * {@link OpenLineage.Dataset} from the BigQuery table referenced by the relation. The convention
 * used for naming is a URI of <code>
 * bigquery://&lt;projectId&gt;.&lt;.datasetId&gt;.&lt;tableName&gt;</code> . The namespace for
 * bigquery tables is always <code>bigquery</code> and the name is the FQN.
 *
 * @param <D> the type of {@link OpenLineage.Dataset} created by this visitor
 */
@Slf4j
public class BigQueryNodeVisitor<D extends OpenLineage.Dataset>
    extends QueryPlanVisitor<LogicalPlan, D> {
  private static final String BIGQUERY_NAMESPACE = "bigquery";
  private final DatasetFactory<D> factory;

  public BigQueryNodeVisitor(OpenLineageContext context, DatasetFactory<D> factory) {
    super(context);
    this.factory = factory;
  }

  public static boolean hasBigQueryClasses() {
    try {
      BigQueryNodeVisitor.class
          .getClassLoader()
          .loadClass("com.google.cloud.spark.bigquery.BigQueryRelation");
      return true;
    } catch (Exception e) {
      // swallow- we don't care
    }
    return false;
  }

  @Override
  public boolean isDefinedAt(LogicalPlan plan) {
    if ((plan instanceof LogicalRelation
            && ((LogicalRelation) plan).relation() instanceof BigQueryRelation)
        || plan instanceof SaveIntoDataSourceCommand
            && ((SaveIntoDataSourceCommand) plan).dataSource()
                instanceof BigQueryRelationProvider) {
      return true;
    }
    return false;
  }

  private Optional<Supplier<BigQueryRelation>> bigQuerySupplier(LogicalPlan plan) {
    // SaveIntoDataSourceCommand is a special case because it references a CreatableRelationProvider
    // Every other write instance references a LogicalRelation(BigQueryRelation, _, _, _)
    if (plan instanceof LogicalRelation
        && ((LogicalRelation) plan).relation() instanceof BigQueryRelation) {
      return Optional.of(() -> (BigQueryRelation) ((LogicalRelation) plan).relation());
    }
    return Optional.empty();
  }

  private String getFromSaveIntoDataSourceCommand(SaveIntoDataSourceCommand saveCommand) {
    CreatableRelationProvider relationProvider = saveCommand.dataSource();
    SQLContext sqlContext = context.getSparkSession().get().sqlContext();
    BigQueryRelationProvider bqRelationProvider = (BigQueryRelationProvider) relationProvider;
    SparkBigQueryConfig config =
        bqRelationProvider.createSparkBigQueryConfig(
            sqlContext, saveCommand.options(), Option.apply(saveCommand.schema()));
    return BigQueryUtil.friendlyTableName(config.getTableIdWithoutThePartition());
  }

  @Override
  public List<D> apply(LogicalPlan plan) {
    if (plan instanceof SaveIntoDataSourceCommand) {
      SaveIntoDataSourceCommand saveCommand = (SaveIntoDataSourceCommand) plan;
      return Collections.singletonList(
          factory.getDataset(
              getFromSaveIntoDataSourceCommand(saveCommand),
              BIGQUERY_NAMESPACE,
              saveCommand.schema()));
    } else {
      return bigQuerySupplier(plan)
          .map(s -> s.get())
          .filter(relation -> getBigQueryTableName(relation).isPresent())
          .map(
              relation ->
                  Collections.singletonList(
                      factory.getDataset(
                          getBigQueryTableName(relation).get(),
                          BIGQUERY_NAMESPACE,
                          relation.schema())))
          .orElse(null);
    }
  }

  /**
   * Versions of spark-bigquery-connector differ in implementation of {@link BigQueryRelation}
   * class. Previously `tableName` method was available in Scala class, recent java implementation
   * contains `getTableName` instead. This method retrieves the table name regardless of connector
   * version.
   *
   * @return
   */
  private Optional<String> getBigQueryTableName(BigQueryRelation relation) {
    if (MethodUtils.getAccessibleMethod(relation.getClass(), "tableName") != null) {
      try {
        return Optional.of((String) MethodUtils.invokeMethod(relation, "tableName"));
      } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
        log.error("Could not invoke method", e);
      }
    } else if (MethodUtils.getAccessibleMethod(relation.getClass(), "getTableName") != null) {
      try {
        return Optional.of((String) MethodUtils.invokeMethod(relation, "getTableName"));
      } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
        log.error("Could not invoke method", e);
      }
    }
    return Optional.empty();
  }
}
