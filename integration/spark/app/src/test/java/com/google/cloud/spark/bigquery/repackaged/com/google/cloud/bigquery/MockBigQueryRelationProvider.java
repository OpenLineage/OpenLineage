/* SPDX-License-Identifier: Apache-2.0 */

package com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery;

import com.google.cloud.spark.bigquery.BigQueryRelation;
import com.google.cloud.spark.bigquery.BigQueryRelationProvider;
import com.google.cloud.spark.bigquery.DataSourceVersion;
import com.google.cloud.spark.bigquery.GuiceInjectorCreator;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.connector.common.MockBigQueryClientModule;
import com.google.cloud.spark.bigquery.repackaged.com.google.inject.Binder;
import com.google.cloud.spark.bigquery.repackaged.com.google.inject.Guice;
import com.google.cloud.spark.bigquery.repackaged.com.google.inject.Injector;
import com.google.cloud.spark.bigquery.repackaged.com.google.inject.Key;
import com.google.cloud.spark.bigquery.repackaged.com.google.inject.Module;
import com.google.cloud.spark.bigquery.v2.SparkBigQueryConnectorModule;
import java.math.BigInteger;
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

  public MockBigQueryRelationProvider() {
    super(
        new AbstractFunction0<GuiceInjectorCreator>() {
          @Override
          public GuiceInjectorCreator apply() {
            return new MockInjector();
          }
        });
  }

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

    @Override
    public boolean canEqual(Object that) {
      return that instanceof MockBigQueryRelation;
    }

    @Override
    public Object productElement(int n) {
      return null;
    }

    @Override
    public int productArity() {
      return 0;
    }
  }

  public static class MockInjector implements GuiceInjectorCreator {
    private Module testModule = new EmptyModule();

    @Override
    public Injector createGuiceInjector(
        SQLContext sqlContext, Map<String, String> parameters, Option<StructType> schema) {
      final MockBigQueryClientModule bqModule = new MockBigQueryClientModule(BIG_QUERY);
      SparkSession sparkSession = sqlContext.sparkSession();
      return Guice.createInjector(
          testModule,
          bqModule,
          new SparkBigQueryConnectorModule(
              sparkSession,
              JavaConversions.mapAsJavaMap(parameters),
              Optional.ofNullable(
                  schema.getOrElse(
                      new AbstractFunction0<StructType>() {
                        @Override
                        public StructType apply() {
                          return null;
                        }
                      })),
              DataSourceVersion.V1));
    }

    public void setTestModule(Module testModule) {
      this.testModule = testModule;
    }

    @Override
    public Option<StructType> createGuiceInjector$default$3() {
      return Option.empty();
    }
  }

  private static class EmptyModule implements Module {
    private EmptyModule() {}

    public void configure(Binder binder) {}
  }
}
