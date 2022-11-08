/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery;

import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.spark.bigquery.BigQueryRelation;
import com.google.cloud.spark.bigquery.BigQueryRelationProvider;
import com.google.cloud.spark.bigquery.DataSourceVersion;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.SparkBigQueryConnectorModule;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.common.MockBigQueryClientModule;
import com.google.cloud.spark.bigquery.repackaged.com.google.inject.Binder;
import com.google.cloud.spark.bigquery.repackaged.com.google.inject.Guice;
import com.google.cloud.spark.bigquery.repackaged.com.google.inject.Injector;
import com.google.cloud.spark.bigquery.repackaged.com.google.inject.Key;
import com.google.cloud.spark.bigquery.repackaged.com.google.inject.Module;
import java.math.BigInteger;
import java.util.Collections;
import java.util.Optional;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.TableScan;
import org.apache.spark.sql.types.StructType;
import org.mockito.Mockito;
import scala.Option;
import scala.collection.JavaConversions;
import scala.collection.immutable.Map;
import scala.runtime.AbstractFunction0;

/**
 * Mock relation provider that uses a {@link Mockito} mock instance of {@link
 * com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.BigQuery} for testing.
 */
public class MockBigQueryRelationProvider extends BigQueryRelationProvider {
  public static final BigQuery BIG_QUERY = Mockito.mock(BigQuery.class);
  public static final MockInjector INJECTOR = new MockInjector();

  public static Table makeTable(TableId id, StandardTableDefinition tableDefinition) {
    return new Table.Builder(BIG_QUERY, id, tableDefinition)
        .setNumBytes(tableDefinition.getNumBytes())
        .setNumRows(BigInteger.valueOf(tableDefinition.getNumRows()))
        .build();
  }

  @Override
  public BigQueryRelation createRelationInternal(
      SQLContext sqlContext, Map<String, String> parameters, Option<StructType> schema) {
    Injector injector = INJECTOR.createGuiceInjector(sqlContext, parameters, schema);
    SparkBigQueryConfig config = injector.getInstance(SparkBigQueryConfig.class);
    BigQueryClient bigQueryClient = injector.getInstance(BigQueryClient.class);
    TableInfo tableInfo = bigQueryClient.getReadTable(config.toReadTableOptions());
    Dataset<Row> testRecords = injector.getInstance(new Key<Dataset<Row>>() {});
    return new MockBigQueryRelation(config, tableInfo, sqlContext, testRecords);
  }

  public static class MockBigQueryRelation extends BigQueryRelation implements TableScan {
    private final Dataset<Row> testRecords;

    public MockBigQueryRelation(
        SparkBigQueryConfig options,
        TableInfo table,
        SQLContext sqlContext,
        Dataset<Row> testRecords) {
      super(options, table, sqlContext);
      this.testRecords = testRecords;
    }

    @Override
    public RDD<Row> buildScan() {
      return testRecords.rdd();
    }
  }

  public static class MockInjector {
    private Module testModule = new EmptyModule();

    public Injector createGuiceInjector(
        SQLContext sqlContext, Map<String, String> parameters, Option<StructType> schema) {
      final MockBigQueryClientModule bqModule = new MockBigQueryClientModule(BIG_QUERY);
      SparkSession sparkSession = sqlContext.sparkSession();
      return Guice.createInjector(
          testModule,
          bqModule,
          new SparkBigQueryConnectorModule(
              sparkSession,
              JavaConversions.<String, String>mapAsJavaMap(parameters),
              Collections.emptyMap(),
              Optional.ofNullable(
                  schema.getOrElse(
                      new AbstractFunction0<StructType>() {
                        @Override
                        public StructType apply() {
                          return null;
                        }
                      })),
              DataSourceVersion.V1,
              true));
    }

    public void setTestModule(Module testModule) {
      this.testModule = testModule;
    }
  }

  private static class EmptyModule implements Module {
    private EmptyModule() {}

    @Override
    public void configure(Binder binder) {}
  }
}
