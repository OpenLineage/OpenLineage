/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mockStatic;

import io.openlineage.spark.agent.util.S3TablesUtils.BucketInfo;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;
import org.mockito.MockedStatic;

class S3TablesUtilsTest {

  @Test
  void isS3TablesStorage_matchesPhysicalBucket() {
    assertThat(S3TablesUtils.isS3TablesStorage(URI.create("s3://abc12345--table-s3/"))).isTrue();
    assertThat(S3TablesUtils.isS3TablesStorage(URI.create("s3://abc12345--table-s3"))).isTrue();
    assertThat(
            S3TablesUtils.isS3TablesStorage(
                URI.create("s3://abcd1234-ef56-7890--table-s3/data/file.parquet")))
        .isTrue();
  }

  @Test
  void isS3TablesStorage_rejectsRegularS3Buckets() {
    assertThat(S3TablesUtils.isS3TablesStorage(URI.create("s3://my-bucket/warehouse/tbl")))
        .isFalse();
    assertThat(S3TablesUtils.isS3TablesStorage(URI.create("s3://my-bucket--table-s3-extra")))
        .isFalse();
    assertThat(S3TablesUtils.isS3TablesStorage(URI.create("file:///tmp/foo"))).isFalse();
  }

  @Test
  void isS3TablesNamespace_handlesInvalidAndNonPhysicalNamespaces() {
    assertThat(S3TablesUtils.isS3TablesNamespace("s3://abc12345--table-s3")).isTrue();
    assertThat(S3TablesUtils.isS3TablesNamespace("s3://regular-bucket")).isFalse();
    assertThat(S3TablesUtils.isS3TablesNamespace("not a uri")).isFalse();
    assertThat(S3TablesUtils.isS3TablesNamespace(null)).isFalse();
  }

  @Test
  void matchesS3TablesCatalogConfig_ModeA_S3TablesCatalogImpl() {
    Map<String, String> conf = new HashMap<>();
    conf.put("catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog");
    assertThat(S3TablesUtils.matchesS3TablesCatalogConfig(conf)).isTrue();
  }

  @Test
  void matchesS3TablesCatalogConfig_S3TablesArnWarehouse() {
    Map<String, String> conf = new HashMap<>();
    conf.put("warehouse", "arn:aws:s3tables:eu-central-1:557690578487:bucket/my-bucket");
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
    conf.put("warehouse", "557690578487:s3tablescatalog/my-bucket");
    assertThat(S3TablesUtils.matchesS3TablesCatalogConfig(conf)).isTrue();
  }

  @Test
  void matchesS3TablesCatalogConfig_ModeD_GlueImplPlusFederationId() {
    Map<String, String> conf = new HashMap<>();
    conf.put("catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog");
    conf.put("glue.id", "557690578487:s3tablescatalog/my-bucket");
    assertThat(S3TablesUtils.matchesS3TablesCatalogConfig(conf)).isTrue();
  }

  @Test
  void matchesS3TablesCatalogConfig_PlainGlue_DoesNotMatch() {
    Map<String, String> conf = new HashMap<>();
    conf.put("catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog");
    conf.put("warehouse", "s3://my-glue-warehouse/");
    assertThat(S3TablesUtils.matchesS3TablesCatalogConfig(conf)).isFalse();
  }

  @Test
  void matchesS3TablesCatalogConfig_EmptyConf_DoesNotMatch() {
    assertThat(S3TablesUtils.matchesS3TablesCatalogConfig(new HashMap<>())).isFalse();
  }

  @Test
  void parseS3TablesArn_extractsAllParts() {
    Optional<BucketInfo> info =
        S3TablesUtils.parseS3TablesArn(
            "arn:aws:s3tables:eu-central-1:557690578487:bucket/my-bucket");
    assertThat(info).isPresent();
    assertThat(info.get().getRegion()).contains("eu-central-1");
    assertThat(info.get().getAccount()).contains("557690578487");
    assertThat(info.get().getBucketName()).isEqualTo("my-bucket");
  }

  @Test
  void parseFederationId_extractsAccountAndBucket() {
    Optional<BucketInfo> info =
        S3TablesUtils.parseFederationId("557690578487:s3tablescatalog/my-bucket");
    assertThat(info).isPresent();
    assertThat(info.get().getRegion()).isEmpty();
    assertThat(info.get().getAccount()).contains("557690578487");
    assertThat(info.get().getBucketName()).isEqualTo("my-bucket");
  }

  @Test
  void parseFederationId_rejectsInvalid() {
    assertThat(S3TablesUtils.parseFederationId("garbage")).isEmpty();
    assertThat(S3TablesUtils.parseFederationId("123456:notacatalog/foo")).isEmpty();
    assertThat(S3TablesUtils.parseFederationId(null)).isEmpty();
  }

  @Test
  @SetEnvironmentVariable(key = "AWS_DEFAULT_REGION", value = "eu-central-1")
  void buildS3TablesArnFromCatalogConf_PrefersWarehouseArn() {
    SparkConf sparkConf = new SparkConf();
    Configuration hadoopConf = new Configuration();
    Map<String, String> catalogConf = new HashMap<>();
    catalogConf.put("warehouse", "arn:aws:s3tables:eu-central-1:557690578487:bucket/my-bucket");
    catalogConf.put("catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog");

    String arn = S3TablesUtils.buildS3TablesArnFromCatalogConf(sparkConf, hadoopConf, catalogConf);
    assertThat(arn).isEqualTo("arn:aws:s3tables:eu-central-1:557690578487:bucket/my-bucket");
  }

  @Test
  @SetEnvironmentVariable(key = "AWS_DEFAULT_REGION", value = "eu-central-1")
  void buildS3TablesArnFromCatalogConf_PrefersGlueIdOverWarehouse() {
    SparkConf sparkConf = new SparkConf();
    Configuration hadoopConf = new Configuration();
    Map<String, String> catalogConf = new HashMap<>();
    catalogConf.put("catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog");
    catalogConf.put("glue.id", "557690578487:s3tablescatalog/federated-bucket");
    catalogConf.put("warehouse", "s3://some-other/warehouse/");

    String arn = S3TablesUtils.buildS3TablesArnFromCatalogConf(sparkConf, hadoopConf, catalogConf);
    assertThat(arn).isEqualTo("arn:aws:s3tables:eu-central-1:557690578487:bucket/federated-bucket");
  }

  @Test
  @SetEnvironmentVariable(key = "AWS_DEFAULT_REGION", value = "eu-central-1")
  void buildS3TablesArnFromCatalogConf_AccountFallbackOrder() {
    Map<String, String> catalogConf = new HashMap<>();

    SparkConf sparkConf = new SparkConf();
    sparkConf.set("hive.metastore.glue.catalogid", "111111111111");
    sparkConf.set("spark.glue.accountId", "222222222222");
    Configuration hadoopConf = new Configuration();
    hadoopConf.set("hive.metastore.glue.catalogid", "333333333333");

    assertThat(S3TablesUtils.buildS3TablesArnFromCatalogConf(sparkConf, hadoopConf, catalogConf))
        .isEqualTo("arn:aws:s3tables:eu-central-1:111111111111");

    sparkConf.remove("hive.metastore.glue.catalogid");
    assertThat(S3TablesUtils.buildS3TablesArnFromCatalogConf(sparkConf, hadoopConf, catalogConf))
        .isEqualTo("arn:aws:s3tables:eu-central-1:222222222222");

    sparkConf.remove("spark.glue.accountId");
    assertThat(S3TablesUtils.buildS3TablesArnFromCatalogConf(sparkConf, hadoopConf, catalogConf))
        .isEqualTo("arn:aws:s3tables:eu-central-1:333333333333");

    hadoopConf.unset("hive.metastore.glue.catalogid");
    try (MockedStatic<AwsAccountIdFetcher> mocked = mockStatic(AwsAccountIdFetcher.class)) {
      mocked.when(AwsAccountIdFetcher::getAccountId).thenReturn("444444444444");
      assertThat(S3TablesUtils.buildS3TablesArnFromCatalogConf(sparkConf, hadoopConf, catalogConf))
          .isEqualTo("arn:aws:s3tables:eu-central-1:444444444444");
    }
  }
}
