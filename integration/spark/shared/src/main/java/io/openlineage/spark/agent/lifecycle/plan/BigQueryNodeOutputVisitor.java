/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import static io.openlineage.spark.agent.util.ReflectionUtils.tryExecuteMethod;

import com.google.cloud.spark.bigquery.BigQueryRelation;
import com.google.cloud.spark.bigquery.BigQueryRelationProvider;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.ReflectionUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.agent.util.SparkSessionUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.catalyst.expressions.Attribute;

/**
 * {@link LogicalPlan} visitor that matches {@link BigQueryRelation}s or {@link
 * SaveIntoDataSourceCommand}s that use a {@link BigQueryRelationProvider}. This function extracts a
 * {@link OpenLineage.Dataset} from the BigQuery table referenced by the relation. The convention
 * used for naming is a URI of <code>
 * bigquery://&lt;projectId&gt;.&lt;.datasetId&gt;.&lt;tableName&gt;</code> . The namespace for
 * bigquery tables is always <code>bigquery</code> and the name is the FQN.
 */
@Slf4j
public class BigQueryNodeOutputVisitor
    extends QueryPlanVisitor<LogicalPlan, OpenLineage.OutputDataset> {
  private static final String BIGQUERY_NAMESPACE = "bigquery";
  private final DatasetFactory<OpenLineage.OutputDataset> factory;

  public BigQueryNodeOutputVisitor(
      OpenLineageContext context, DatasetFactory<OpenLineage.OutputDataset> factory) {
    super(context);
    this.factory = factory;
  }

  @Override
  public boolean isDefinedAt(LogicalPlan plan) {
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
        .flatMap(BigQueryNodeOutputVisitor::extractDatasetIdentifierFromTableId)
        .map(x -> (String) x);
  }

    @Override
    public List<OpenLineage.OutputDataset> apply(LogicalPlan plan) {
        SaveIntoDataSourceCommand saveCommand = (SaveIntoDataSourceCommand) plan;
        Optional<SparkSession> session = SparkSessionUtils.activeSession();
        if (session.isPresent()) {
            StructType schema = saveCommand.schema();
            if (schema == null || schema.isEmpty()) {
                List<StructField> fields =
                        ScalaConversionUtils.<Attribute>fromSeq(saveCommand.query().output()).stream()
                                .map(attr ->
                                        new StructField(
                                                attr.name(),
                                                attr.dataType(),
                                                attr.nullable(),
                                                Metadata.empty()))
                                .collect(Collectors.toList());

                schema = new StructType(fields.toArray(new StructField[0]));
            }
            return Collections.singletonList(
                    factory
                            .sparkDatasetBuilder()
                            .dataset(
                                    getFromSaveIntoDataSourceCommand(saveCommand, session.get()),
                                    BIGQUERY_NAMESPACE)
                            .schema(schema)
                            .build());
        }
        return Collections.emptyList();
    }
}
