/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mockStatic;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;
import org.mockito.MockedStatic;

class S3TablesUtilsTest {
  private static final String CATALOG_IMPL = "catalog-impl";
  private static final String WAREHOUSE = "warehouse";
  private static final String HIVE_METASTORE_GLUE_CATALOG_ID = "hive.metastore.glue.catalogid";

  @Test
  void matchesS3TablesCatalogConfig_ModeA_S3TablesCatalogImpl() {
    Map<String, String> conf = new HashMap<>();
    conf.put(CATALOG_IMPL, "software.amazon.s3tables.iceberg.S3TablesCatalog");
    assertThat(S3TablesUtils.matchesS3TablesCatalogConfig(conf)).isTrue();
  }

  @Test
  void matchesS3TablesCatalogConfig_S3TablesArnWarehouse() {
    Map<String, String> conf = new HashMap<>();
    conf.put(WAREHOUSE, "arn:aws:s3tables:eu-central-1:557690578487:bucket/my-bucket");
    assertThat(S3TablesUtils.matchesS3TablesCatalogConfig(conf)).isTrue();
  }

  @Test
  void matchesS3TablesCatalogConfig_ModeB_RestEndpoint() {
    Map<String, String> conf = new HashMap<>();
    conf.put("type", "rest");
    conf.put("uri", "https://s3tables.eu-central-1.amazonaws.com/iceberg");
    assertThat(S3TablesUtils.matchesS3TablesCatalogConfig(conf)).isTrue();
  }

  @Test
  void matchesS3TablesCatalogConfig_ModeB_SigningName() {
    Map<String, String> conf = new HashMap<>();
    conf.put("signing-name", "s3tables");
    assertThat(S3TablesUtils.matchesS3TablesCatalogConfig(conf)).isTrue();
  }

  @Test
  void matchesS3TablesCatalogConfig_ModeC_FederationWarehouse() {
    Map<String, String> conf = new HashMap<>();
    conf.put("type", "rest");
    conf.put(WAREHOUSE, "557690578487:s3tablescatalog/my-bucket");
    assertThat(S3TablesUtils.matchesS3TablesCatalogConfig(conf)).isTrue();
  }

  @Test
  void matchesS3TablesCatalogConfig_ModeD_GlueImplPlusFederationId() {
    Map<String, String> conf = new HashMap<>();
    conf.put(CATALOG_IMPL, "org.apache.iceberg.aws.glue.GlueCatalog");
    conf.put("glue.id", "557690578487:s3tablescatalog/my-bucket");
    assertThat(S3TablesUtils.matchesS3TablesCatalogConfig(conf)).isTrue();
  }

  @Test
  void matchesS3TablesCatalogConfig_PlainGlue_DoesNotMatch() {
    Map<String, String> conf = new HashMap<>();
    conf.put(CATALOG_IMPL, "org.apache.iceberg.aws.glue.GlueCatalog");
    conf.put(WAREHOUSE, "s3://my-glue-warehouse/");
    assertThat(S3TablesUtils.matchesS3TablesCatalogConfig(conf)).isFalse();
  }

  @Test
  void matchesS3TablesCatalogConfig_EmptyConf_DoesNotMatch() {
    assertThat(S3TablesUtils.matchesS3TablesCatalogConfig(new HashMap<>())).isFalse();
  }

  @Test
  @SetEnvironmentVariable(key = "AWS_DEFAULT_REGION", value = "eu-central-1")
  void buildS3TablesArnFromCatalogConf_PrefersWarehouseArn() {
    SparkConf sparkConf = new SparkConf();
    Configuration hadoopConf = new Configuration();
    Map<String, String> catalogConf = new HashMap<>();
    catalogConf.put(WAREHOUSE, "arn:aws:s3tables:eu-central-1:557690578487:bucket/my-bucket");
    catalogConf.put(CATALOG_IMPL, "software.amazon.s3tables.iceberg.S3TablesCatalog");

    String arn = S3TablesUtils.buildS3TablesArnFromCatalogConf(sparkConf, hadoopConf, catalogConf);
    assertThat(arn).isEqualTo("arn:aws:s3tables:eu-central-1:557690578487:bucket/my-bucket");
  }

  @Test
  @SetEnvironmentVariable(key = "AWS_DEFAULT_REGION", value = "eu-central-1")
  void buildS3TablesArnFromCatalogConf_PrefersGlueIdOverWarehouse() {
    SparkConf sparkConf = new SparkConf();
    Configuration hadoopConf = new Configuration();
    Map<String, String> catalogConf = new HashMap<>();
    catalogConf.put(CATALOG_IMPL, "org.apache.iceberg.aws.glue.GlueCatalog");
    catalogConf.put("glue.id", "557690578487:s3tablescatalog/federated-bucket");
    catalogConf.put(WAREHOUSE, "s3://some-other/warehouse/");

    String arn = S3TablesUtils.buildS3TablesArnFromCatalogConf(sparkConf, hadoopConf, catalogConf);
    assertThat(arn).isEqualTo("arn:aws:s3tables:eu-central-1:557690578487:bucket/federated-bucket");
  }

  @Test
  @SetEnvironmentVariable(key = "AWS_DEFAULT_REGION", value = "eu-central-1")
  void buildS3TablesArnFromCatalogConf_AccountFallbackOrder() {
    Map<String, String> catalogConf = new HashMap<>();

    SparkConf sparkConf = new SparkConf();
    sparkConf.set(HIVE_METASTORE_GLUE_CATALOG_ID, "111111111111");
    sparkConf.set("spark.glue.accountId", "222222222222");
    Configuration hadoopConf = new Configuration();
    hadoopConf.set(HIVE_METASTORE_GLUE_CATALOG_ID, "333333333333");

    assertThat(S3TablesUtils.buildS3TablesArnFromCatalogConf(sparkConf, hadoopConf, catalogConf))
        .isEqualTo("arn:aws:s3tables:eu-central-1:111111111111");

    sparkConf.remove(HIVE_METASTORE_GLUE_CATALOG_ID);
    assertThat(S3TablesUtils.buildS3TablesArnFromCatalogConf(sparkConf, hadoopConf, catalogConf))
        .isEqualTo("arn:aws:s3tables:eu-central-1:222222222222");

    sparkConf.remove("spark.glue.accountId");
    assertThat(S3TablesUtils.buildS3TablesArnFromCatalogConf(sparkConf, hadoopConf, catalogConf))
        .isEqualTo("arn:aws:s3tables:eu-central-1:333333333333");

    hadoopConf.unset(HIVE_METASTORE_GLUE_CATALOG_ID);
    try (MockedStatic<AwsAccountIdFetcher> mocked = mockStatic(AwsAccountIdFetcher.class)) {
      mocked.when(AwsAccountIdFetcher::getAccountId).thenReturn("444444444444");
      assertThat(S3TablesUtils.buildS3TablesArnFromCatalogConf(sparkConf, hadoopConf, catalogConf))
          .isEqualTo("arn:aws:s3tables:eu-central-1:444444444444");
    }
  }
}
