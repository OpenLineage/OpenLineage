/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import static io.openlineage.spark.agent.util.ReflectionUtils.tryExecuteMethod;

import com.google.cloud.spark.bigquery.BigQueryRelationProvider;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.ReflectionUtils;
import io.openlineage.spark.agent.util.SparkSessionUtils;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import scala.Option;

/**
 * Extracts the output {@link OpenLineage.Dataset} of a {@link SaveIntoDataSourceCommand} that uses
 * a {@link BigQueryRelationProvider}: the BigQuery table it writes to. The convention used for
 * naming is a URI of <code>
 * bigquery://&lt;projectId&gt;.&lt;.datasetId&gt;.&lt;tableName&gt;</code> . The namespace for
 * bigquery tables is always <code>bigquery</code> and the name is the FQN.
 */
@Slf4j
public class BigQueryOutputDatasetBuilder
    extends AbstractQueryPlanOutputDatasetBuilder<SaveIntoDataSourceCommand> {
  private static final String BIGQUERY_NAMESPACE = "bigquery";

  public BigQueryOutputDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan plan) {
    return plan instanceof SaveIntoDataSourceCommand
        && ReflectionUtils.hasClass("com.google.cloud.spark.bigquery.BigQueryRelationProvider")
        && ((SaveIntoDataSourceCommand) plan).dataSource() instanceof BigQueryRelationProvider;
  }

  private String getFromSaveIntoDataSourceCommand(
      SaveIntoDataSourceCommand saveCommand, SparkSession session) {
    SQLContext sqlContext = session.sqlContext();
    BigQueryRelationProvider bqRelationProvider =
        (BigQueryRelationProvider) saveCommand.dataSource();
    SparkBigQueryConfig config =
        bqRelationProvider.createSparkBigQueryConfig(
            sqlContext, saveCommand.options(), Option.apply(saveCommand.schema()));
    return getBigQueryTableName(config).get();
  }

  private static Optional<Object> extractDatasetIdentifierFromTableId(Object tableId) {
    return Stream.of(
            ReflectionUtils.tryExecuteStaticMethodForClassName(
                "com.google.cloud.bigquery.connector.common.BigQueryUtil",
                "friendlyTableName",
                tableId),
            ReflectionUtils.tryExecuteStaticMethodForClassName(
                "com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.connector.common.BigQueryUtil",
                "friendlyTableName",
                tableId))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .findFirst();
  }

  private Optional<String> getBigQueryTableName(SparkBigQueryConfig config) {
    return tryExecuteMethod(config, "getTableId")
        .flatMap(BigQueryOutputDatasetBuilder::extractDatasetIdentifierFromTableId)
        .map(x -> (String) x);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(SaveIntoDataSourceCommand saveCommand) {
    Optional<SparkSession> session = SparkSessionUtils.activeSession();
    return session
        .map(
            sparkSession ->
                Collections.singletonList(
                    outputDataset()
                        .sparkDatasetBuilder()
                        .dataset(
                            getFromSaveIntoDataSourceCommand(saveCommand, sparkSession),
                            BIGQUERY_NAMESPACE)
                        .schema(saveCommand.schema())
                        .build()))
        .orElse(Collections.emptyList());
  }
}
