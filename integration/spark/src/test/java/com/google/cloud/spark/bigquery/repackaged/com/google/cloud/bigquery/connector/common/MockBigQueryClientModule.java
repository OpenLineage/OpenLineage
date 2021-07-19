package com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.connector.common;

import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.BigQuery;
import com.google.cloud.spark.bigquery.repackaged.com.google.inject.Binder;
import com.google.cloud.spark.bigquery.repackaged.com.google.inject.Module;
import com.google.cloud.spark.bigquery.repackaged.com.google.inject.Provides;
import com.google.cloud.spark.bigquery.repackaged.com.google.inject.Singleton;
import java.util.Optional;

public class MockBigQueryClientModule implements Module {
  private final BigQuery bq;

  public MockBigQueryClientModule(BigQuery bq) {
    this.bq = bq;
  }

  @Override
  public void configure(Binder binder) {}

  @Provides
  @Singleton
  public BigQueryCredentialsSupplier provideBigQueryCredentialsSupplier(BigQueryConfig config) {
    return new BigQueryCredentialsSupplier(
        Optional.of("not a real access token"), Optional.empty(), Optional.empty());
  }

  @Provides
  @Singleton
  public BigQueryClient provideBigQueryClient() {
    return new BigQueryClient(
        bq, Optional.of("materializationProject"), Optional.of("materializationDataset"));
  }
}
