/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;
import org.mockito.MockedStatic;

class AwsUtilsTest {

  private SparkContext sparkContext;

  @AfterEach
  void tearDown() {
    if (sparkContext != null) {
      ApplicationMetadataCache.invalidate(sparkContext);
    }
  }

  @Test
  @SetEnvironmentVariable(key = "AWS_DEFAULT_REGION", value = "eu-central-1")
  void cachesGlueCatalogIdentityForTheApplication() {
    sparkContext = glueSparkContext();

    try (MockedStatic<AwsAccountIdFetcher> accountIdFetcher =
        mockStatic(AwsAccountIdFetcher.class)) {
      accountIdFetcher
          .when(() -> AwsAccountIdFetcher.getAccountIdOptional(sparkContext))
          .thenReturn(Optional.of("123456789012"));

      assertThat(AwsUtils.getGlueArn(sparkContext))
          .contains("arn:aws:glue:eu-central-1:123456789012");
      assertThat(AwsUtils.getGlueArn(sparkContext))
          .contains("arn:aws:glue:eu-central-1:123456789012");

      accountIdFetcher.verify(
          () -> AwsAccountIdFetcher.getAccountIdOptional(sparkContext), times(1));
    }
  }

  @Test
  @SetEnvironmentVariable(key = "AWS_DEFAULT_REGION", value = "eu-central-1")
  void cachesUnavailableGlueCatalogIdentityForTheApplication() {
    sparkContext = glueSparkContext();

    try (MockedStatic<AwsAccountIdFetcher> accountIdFetcher =
        mockStatic(AwsAccountIdFetcher.class)) {
      accountIdFetcher
          .when(() -> AwsAccountIdFetcher.getAccountIdOptional(sparkContext))
          .thenReturn(Optional.empty());

      assertThat(AwsUtils.getGlueArn(sparkContext)).isEmpty();
      assertThat(AwsUtils.getGlueArn(sparkContext)).isEmpty();

      accountIdFetcher.verify(
          () -> AwsAccountIdFetcher.getAccountIdOptional(sparkContext), times(1));
    }
  }

  private static SparkContext glueSparkContext() {
    SparkConf sparkConf = new SparkConf();
    Configuration hadoopConf = new Configuration();
    hadoopConf.set(
        AwsUtils.HIVE_METASTORE_CLIENT_FACTORY_CLASS, AwsUtils.AWS_GLUE_HIVE_FACTORY_CLASS);
    SparkContext context = mock(SparkContext.class);
    when(context.getConf()).thenReturn(sparkConf);
    when(context.hadoopConfiguration()).thenReturn(hadoopConf);
    return context;
  }
}
