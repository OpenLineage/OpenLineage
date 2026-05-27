/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Value;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;

/**
 * Utilities for detecting and building dataset identifiers for AWS S3 Tables. S3 Tables can be
 * reached from Spark in several configurations (native {@code S3TablesCatalog}, Iceberg REST
 * pointing at the S3 Tables endpoint, Iceberg REST pointing at the Glue Iceberg endpoint with S3
 * Tables federation, and Iceberg {@code GlueCatalog} against the federated catalog). This utility
 * builds an {@code arn:aws:s3tables:<region>:<account>:bucket/<bucket-name>} identifier instead of
 * exposing internal catalog implementation details.
 */
@Slf4j
@UtilityClass
public class S3TablesUtils {
  private static final String CATALOG_IMPL_KEY = "catalog-impl";
  private static final String WAREHOUSE_KEY = "warehouse";
  private static final String URI_KEY = "uri";
  private static final String GLUE_ID_KEY = "glue.id";
  private static final String SIGNING_NAME_KEY = "signing-name";
  private static final String REST_SIGNING_NAME_KEY = "rest.signing-name";

  private static final String S3TABLES_CATALOG_SUFFIX = "S3TablesCatalog";
  private static final String GLUE_CATALOG_SUFFIX = "GlueCatalog";
  private static final String S3TABLES_ARN_PREFIX = "arn:aws:s3tables:";
  private static final Pattern FEDERATION_ID = Pattern.compile("^(\\d+):s3tablescatalog/(.+)$");
  private static final Pattern S3TABLES_ARN =
      Pattern.compile("^arn:aws:s3tables:([^:]+):([^:]+):bucket/(.+)$");
  private static final Pattern S3TABLES_HOST =
      Pattern.compile("^s3tables\\.[^.]+\\.amazonaws\\.com$");

  /** Bucket info extracted from a warehouse / glue.id property. */
  @Value
  private static class BucketInfo {
    Optional<String> region;
    Optional<String> account;
    String bucketName;
  }

  /**
   * Returns true if the given per-catalog config (already stripped of {@code
   * spark.sql.catalog.<name>.} prefix) looks like an S3 Tables catalog.
   */
  public static boolean matchesS3TablesCatalogConfig(Map<String, String> catalogConf) {
    if (catalogConf == null || catalogConf.isEmpty()) {
      return false;
    }
    String impl = catalogConf.get(CATALOG_IMPL_KEY);
    if (impl != null && impl.endsWith(S3TABLES_CATALOG_SUFFIX)) {
      return true;
    }

    String warehouse = catalogConf.get(WAREHOUSE_KEY);
    if (warehouse != null) {
      if (warehouse.startsWith(S3TABLES_ARN_PREFIX)) {
        return true;
      }
      if (FEDERATION_ID.matcher(warehouse).matches()) {
        return true;
      }
    }

    if (impl != null && impl.endsWith(GLUE_CATALOG_SUFFIX)) {
      String glueId = catalogConf.get(GLUE_ID_KEY);
      if (glueId != null && FEDERATION_ID.matcher(glueId).matches()) {
        return true;
      }
    }

    String uri = catalogConf.get(URI_KEY);
    if (uri != null) {
      try {
        URI parsed = URI.create(uri);
        if (parsed.getHost() != null && S3TABLES_HOST.matcher(parsed.getHost()).matches()) {
          return true;
        }
      } catch (IllegalArgumentException ignored) {
        // not a valid URI
      }
    }

    String signing =
        catalogConf.getOrDefault(REST_SIGNING_NAME_KEY, catalogConf.get(SIGNING_NAME_KEY));
    if ("s3tables".equalsIgnoreCase(signing)) {
      return true;
    }

    return false;
  }

  /**
   * Builds an S3 Tables ARN namespace (full ARN when bucket name is recoverable, account-level
   * otherwise) from a per-catalog config.
   */
  public static String buildS3TablesArnFromCatalogConf(
      SparkConf sparkConf, Configuration hadoopConf, Map<String, String> catalogConf) {
    Optional<BucketInfo> info = extractBucketFromCatalogConf(catalogConf);
    return composeArn(sparkConf, hadoopConf, info);
  }

  // ----- internals -----

  private static Optional<BucketInfo> extractBucketFromCatalogConf(Map<String, String> conf) {
    if (conf == null) {
      return Optional.empty();
    }
    Optional<BucketInfo> fromFederation = parseFederationId(conf.get(GLUE_ID_KEY));
    if (fromFederation.isPresent()) {
      return fromFederation;
    }
    String warehouse = conf.get(WAREHOUSE_KEY);
    Optional<BucketInfo> fromWarehouseFederation = parseFederationId(warehouse);
    if (fromWarehouseFederation.isPresent()) {
      return fromWarehouseFederation;
    }
    Optional<BucketInfo> fromArn = parseS3TablesArn(warehouse);
    if (fromArn.isPresent()) {
      return fromArn;
    }
    return Optional.empty();
  }

  private static String composeArn(
      SparkConf sparkConf, Configuration hadoopConf, Optional<BucketInfo> info) {
    Optional<String> resolvedRegion = info.flatMap(b -> b.region);
    if (!resolvedRegion.isPresent()) {
      resolvedRegion = AwsUtils.awsRegion();
    }
    if (!resolvedRegion.isPresent()) {
      log.warn("Unable to resolve AWS region for S3 Tables namespace; using 'unknown'.");
    }

    Optional<String> resolvedAccount = info.flatMap(b -> b.account);
    if (!resolvedAccount.isPresent()) {
      resolvedAccount = resolveAccountId(sparkConf, hadoopConf);
    }
    if (!resolvedAccount.isPresent()) {
      log.warn("Unable to resolve AWS account ID for S3 Tables namespace; using 'unknown'.");
    }

    String prefix =
        S3TABLES_ARN_PREFIX
            + resolvedRegion.orElse("unknown")
            + ":"
            + resolvedAccount.orElse("unknown");
    return info.map(b -> prefix + ":bucket/" + b.bucketName).orElse(prefix);
  }

  private static Optional<String> resolveAccountId(SparkConf sparkConf, Configuration hadoopConf) {
    // Mirror the order AwsUtils.getGlueCatalogId uses: explicit catalog id wins, then EMR/Glue job
    // account, then STS. Keeps behavior consistent across V1/V2 paths and avoids a network call in
    // test environments that set the account explicitly.
    if (sparkConf != null) {
      Optional<String> explicit =
          SparkConfUtils.findSparkConfigKey(sparkConf, "hive.metastore.glue.catalogid");
      if (explicit.isPresent()) {
        return explicit;
      }
      Optional<String> glueJob =
          SparkConfUtils.findSparkConfigKey(sparkConf, "spark.glue.accountId");
      if (glueJob.isPresent()) {
        return glueJob;
      }
    }
    if (hadoopConf != null) {
      Optional<String> hadoopExplicit =
          SparkConfUtils.findHadoopConfigKey(hadoopConf, "hive.metastore.glue.catalogid");
      if (hadoopExplicit.isPresent()) {
        return hadoopExplicit;
      }
    }
    try {
      return Optional.ofNullable(AwsAccountIdFetcher.getAccountId());
    } catch (Exception e) {
      log.debug("Could not retrieve AWS account ID", e);
      return Optional.empty();
    }
  }

  private static Optional<BucketInfo> parseFederationId(String value) {
    if (value == null) {
      return Optional.empty();
    }
    Matcher m = FEDERATION_ID.matcher(value);
    if (m.matches()) {
      return Optional.of(new BucketInfo(Optional.empty(), Optional.of(m.group(1)), m.group(2)));
    }
    return Optional.empty();
  }

  private static Optional<BucketInfo> parseS3TablesArn(String value) {
    if (value == null) {
      return Optional.empty();
    }
    Matcher m = S3TABLES_ARN.matcher(value);
    if (m.matches()) {
      return Optional.of(
          new BucketInfo(Optional.of(m.group(1)), Optional.of(m.group(2)), m.group(3)));
    }
    return Optional.empty();
  }
}
