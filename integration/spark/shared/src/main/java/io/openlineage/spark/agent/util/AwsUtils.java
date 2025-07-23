/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.dataset.Naming;
import java.util.Optional;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.jetbrains.annotations.NotNull;

@Slf4j
@UtilityClass
public class AwsUtils {

  public static final String HIVE_METASTORE_CLIENT_FACTORY_CLASS =
      "hive.metastore.client.factory.class";
  public static final String AWS_GLUE_HIVE_FACTORY_CLASS =
      "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory";
  private static final String HIVE_METASTORE_GLUE_CATALOG_ID_KEY = "hive.metastore.glue.catalogid";

  public static Optional<Naming.AWSGlue> getGlueName(
      SparkConf sparkConf, Configuration hadoopConf, CatalogTable catalogTable) {
    try {
      if (!isHiveUsingGlue(sparkConf, hadoopConf)) {
        return Optional.empty();
      }
      if (catalogTable.identifier().database().isEmpty()) {
        throw new IllegalStateException("Database identifier is required");
      }
      return createAWSGlueNaming(
          sparkConf,
          hadoopConf,
          catalogTable.identifier().database().get(),
          catalogTable.identifier().table());
    } catch (Exception e) {
      log.error("Failed to retrieve Glue", e);
      return Optional.empty();
    }
  }

  public static Optional<Naming.AWSGlue> getGlueName(
      SparkConf sparkConf, Configuration hadoopConf, String database, String table) {
    try {
      if (!isHiveUsingGlue(sparkConf, hadoopConf)) {
        return Optional.empty();
      }
      return createAWSGlueNaming(sparkConf, hadoopConf, database, table);
    } catch (Exception e) {
      log.error("Failed to retrieve Glue", e);
      return Optional.empty();
    }
  }

  private static Optional<Naming.AWSGlue> createAWSGlueNaming(
      SparkConf sparkConf, Configuration hadoopConf, String database, String table) {
    return awsRegion()
        .flatMap(
            region ->
                getGlueCatalogId(sparkConf, hadoopConf)
                    .map(
                        glueCatalogId ->
                            new Naming.AWSGlue(region, glueCatalogId, database, table)));
  }

  /**
   * Obtains the Glue catalog ID.
   *
   * <p>There is no single place where Glue catalog ID is located. It depends on the environment
   * where the application is running and optional, extra configuration.
   */
  private static @NotNull Optional<String> getGlueCatalogId(
      SparkConf sparkConf, Configuration hadoopConf) {
    /*
    The ID of the Glue catalog can be specified explicitly. If it is not, then the account ID of the current account
    is used.

    To specify the catalog ID directly, the property "hive.metastore.glue.catalogid" is used. This method is useful
    in scenarios when the application should use the organization's Glue catalog instead of the current account catalog.

    When the catalog ID is not specified, there are different ways to determine the account ID. In environments like
    Glue jobs, it is conveniently available as a Spark property. In other environments (like EMR), we have to use
    AWS SDK to determine the current account ID.
     */

    Optional<String> explicitGlueCatalogId = getExplicitGlueCatalogId(sparkConf, hadoopConf);
    if (explicitGlueCatalogId.isPresent()) {
      return explicitGlueCatalogId;
    }

    Optional<String> glueJobAccountId =
        SparkConfUtils.findSparkConfigKey(sparkConf, "spark.glue.accountId");
    if (glueJobAccountId.isPresent()) {
      log.debug("Using [spark.glue.account] property [{}] as catalog ID.", glueJobAccountId.get());
      return glueJobAccountId;
    } else {
      log.debug("Fetching current account ID to use as the catalog ID.");
      return Optional.of(AwsAccountIdFetcher.getAccountId());
    }
  }

  /** Obtains the Glue catalog id when it is specified explicitly. */
  private static Optional<String> getExplicitGlueCatalogId(
      SparkConf sparkConf, Configuration hadoopConf) {
    /*
    For environments like EMR the catalog ID is specified in Spark properties.
    For other environments like Athena it is specified as Hadoop properties.

     Note for Athena: Even though the catalog ID is specified with prefix "spark.hadoop", it is removed by SparkHadoopUtil
     */
    Optional<String> sparkPropertyCatalogId =
        SparkConfUtils.findSparkConfigKey(sparkConf, HIVE_METASTORE_GLUE_CATALOG_ID_KEY);
    if (sparkPropertyCatalogId.isPresent()) {
      log.debug(
          "There is an explicit catalog ID [{}} passed as [{}] Spark property.",
          sparkPropertyCatalogId.get(),
          HIVE_METASTORE_GLUE_CATALOG_ID_KEY);
      return sparkPropertyCatalogId;
    }
    Optional<String> hadoopPropertyCatalogId =
        SparkConfUtils.findHadoopConfigKey(hadoopConf, HIVE_METASTORE_GLUE_CATALOG_ID_KEY);
    hadoopPropertyCatalogId.ifPresent(
        s ->
            log.debug(
                "There is an explicit catalog ID [{}} passed as [{}] Hadoop property.",
                s,
                HIVE_METASTORE_GLUE_CATALOG_ID_KEY));
    return hadoopPropertyCatalogId;
  }

  private static @NotNull Optional<String> awsRegion() {
    return Optional.ofNullable(System.getenv("AWS_DEFAULT_REGION"))
        .filter(s -> !s.isEmpty())
        .map(Optional::of)
        .orElseGet(() -> Optional.ofNullable(System.getenv("AWS_REGION")));
  }

  private static boolean isHiveUsingGlue(SparkConf sparkConf, Configuration hadoopConf) {
    Optional<String> hadoopFactoryClass =
        SparkConfUtils.findHadoopConfigKey(hadoopConf, HIVE_METASTORE_CLIENT_FACTORY_CLASS);
    Optional<String> sparkFactoryClass =
        SparkConfUtils.findSparkConfigKey(sparkConf, HIVE_METASTORE_CLIENT_FACTORY_CLASS);
    return AWS_GLUE_HIVE_FACTORY_CLASS.equals(
        hadoopFactoryClass.orElse(sparkFactoryClass.orElse(null)));
  }
}
