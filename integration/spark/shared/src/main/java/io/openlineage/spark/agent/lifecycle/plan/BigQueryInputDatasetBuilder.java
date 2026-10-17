/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import com.google.cloud.spark.bigquery.BigQueryRelation;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.direct.DirectBigQueryRelation;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.ReflectionUtils;
import io.openlineage.spark.agent.util.SqlUtils;
import io.openlineage.spark.api.AbstractQueryPlanInputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.LogicalRelation;

/**
 * Extracts an input {@link OpenLineage.Dataset} from a {@link LogicalRelation} over a {@link
 * BigQueryRelation}: the BigQuery table referenced by the relation, or the tables read by its
 * query. The convention used for naming is a URI of <code>
 * bigquery://&lt;projectId&gt;.&lt;.datasetId&gt;.&lt;tableName&gt;</code> . The namespace for
 * bigquery tables is always <code>bigquery</code> and the name is the FQN.
 */
@Slf4j
public class BigQueryInputDatasetBuilder extends AbstractQueryPlanInputDatasetBuilder<LogicalPlan> {
  private static final String BIGQUERY_NAMESPACE = "bigquery";

  public BigQueryInputDatasetBuilder(OpenLineageContext context) {
    super(context, true);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan plan) {
    return plan instanceof LogicalRelation
        && ReflectionUtils.hasClass("com.google.cloud.spark.bigquery.BigQueryRelation")
        && ((LogicalRelation) plan).relation() instanceof BigQueryRelation;
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

  @Override
  public List<OpenLineage.InputDataset> apply(LogicalPlan plan) {
    return bigQuerySupplier(plan)
        .map(s -> s.get())
        .map(
            relation -> {
              if (relation instanceof DirectBigQueryRelation) {
                List<OpenLineage.InputDataset> datasets =
                    tryGetFromQuery((DirectBigQueryRelation) relation);
                if (!datasets.isEmpty()) {
                  return datasets;
                }
              }
              return Collections.singletonList(
                  inputDataset()
                      .sparkDatasetBuilder()
                      .dataset(getBigQueryTableName(relation).get(), BIGQUERY_NAMESPACE)
                      .schema(relation.schema())
                      .build());
            })
        .orElse(Collections.emptyList());
  }

  @SuppressWarnings("PMD.AvoidCatchingNPE")
  private List<OpenLineage.InputDataset> tryGetFromQuery(DirectBigQueryRelation relation) {
    try {
      SparkBigQueryConfig config =
          (SparkBigQueryConfig) FieldUtils.readField(relation, "options", true);
      if (config.getQuery().isPresent()) {
        return SqlUtils.getDatasets(
            inputDataset(), config.getQuery().get(), "bigquery", "bigquery");
      }
    } catch (IllegalAccessException | IllegalArgumentException | NullPointerException e) {
      log.error("Could not invoke method", e);
    }
    return Collections.emptyList();
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
