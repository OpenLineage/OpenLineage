/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

class AwsUtilsTest {

  @Test
  @SetEnvironmentVariable(key = "AWS_DEFAULT_REGION", value = "us-west-2")
  void getGlueArn_typeGlueWithHadoopCatalogImpl_returnsEmpty() {
    SparkConf sparkConf =
        new SparkConf(false)
            .set("spark.sql.catalog.iceberg.type", "glue")
            .set(
                "spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog")
            .set("spark.glue.accountId", "123456789012");

    assertThat(AwsUtils.getGlueArn(sparkConf, new Configuration())).isEmpty();
  }

  @Test
  @SetEnvironmentVariable(key = "AWS_DEFAULT_REGION", value = "us-west-2")
  void getGlueArn_typeGlueShorthandOnly_returnsArn() {
    SparkConf sparkConf =
        new SparkConf(false)
            .set("spark.sql.catalog.iceberg.type", "GLUE")
            .set("spark.glue.accountId", "123456789012");

    assertThat(AwsUtils.getGlueArn(sparkConf, new Configuration()))
        .contains("arn:aws:glue:us-west-2:123456789012");
  }

  @Test
  void getGlueArn_deltaSparkCatalog_doesNotThrow() {
    SparkConf sparkConf =
        new SparkConf(false)
            .set(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog");

    assertThat(AwsUtils.getGlueArn(sparkConf, new Configuration())).isEmpty();
  }

  @Test
  @SetEnvironmentVariable(key = "AWS_DEFAULT_REGION", value = "us-west-2")
  void getGlueArn_glueCatalogImpl_returnsArn() {
    SparkConf sparkConf =
        new SparkConf(false)
            .set(
                "spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
            .set("spark.glue.accountId", "123456789012");

    assertThat(AwsUtils.getGlueArn(sparkConf, new Configuration()))
        .contains("arn:aws:glue:us-west-2:123456789012");
  }
}
