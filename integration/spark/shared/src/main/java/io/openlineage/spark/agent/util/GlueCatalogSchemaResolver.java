/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.utils.DatasetIdentifier;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.types.StructField;

/**
 * Utility class to resolve original column names from Glue Catalog for LogicalRDD operations.
 *
 * <p>When DynamicFrame operations create a LogicalRDD, the column names in the RDD may be different
 * from the original Glue Catalog table columns due to transformations. This resolver attempts to
 * retrieve the original schema from the Glue Catalog to preserve accurate column-level lineage.
 */
@Slf4j
public class GlueCatalogSchemaResolver {

  /**
   * Attempts to resolve the original column names from Glue Catalog for a given dataset.
   *
   * @param datasetIdentifier The dataset identifier extracted from the LogicalRDD
   * @param sparkSession The Spark session for catalog access
   * @return Optional list of original column names from Glue Catalog, empty if not resolvable
   */
  public static Optional<List<String>> resolveOriginalColumns(
      DatasetIdentifier datasetIdentifier, SparkSession sparkSession) {

    if (sparkSession == null) {
      log.debug("SparkSession is null, cannot resolve Glue Catalog schema");
      return Optional.empty();
    }

    SparkContext sparkContext = sparkSession.sparkContext();
    SparkConf sparkConf = sparkContext.getConf();
    Configuration hadoopConf = sparkContext.hadoopConfiguration();

    // Check if Hive is using Glue as the metastore
    if (!isHiveUsingGlue(sparkConf, hadoopConf)) {
      log.debug("Hive is not using Glue metastore, skipping Glue schema resolution");
      return Optional.empty();
    }

    // Try to find the table in the Spark catalog
    return findTableInCatalog(datasetIdentifier, sparkSession);
  }

  /**
   * Searches for a table in the Spark catalog based on the dataset identifier path.
   *
   * @param datasetIdentifier The dataset identifier from LogicalRDD
   * @param sparkSession The Spark session
   * @return Optional list of column names from the catalog table
   */
  private static Optional<List<String>> findTableInCatalog(
      DatasetIdentifier datasetIdentifier, SparkSession sparkSession) {

    try {
      // The dataset name from S3 path often matches the table name
      // e.g., "reproduce_issues/source_isolated" -> table might be "source_isolated"
      String datasetName = datasetIdentifier.getName();

      // Extract potential table name from the path
      String[] pathParts = datasetName.split("/");
      String potentialTableName = pathParts[pathParts.length - 1];

      log.debug(
          "Attempting to find Glue table for dataset: {}, potential table: {}",
          datasetName,
          potentialTableName);

      // Try to get the table from the catalog
      // First try with default database
      Optional<List<String>> columns =
          tryGetTableColumns(sparkSession, "default", potentialTableName);
      if (columns.isPresent()) {
        return columns;
      }

      // If the path has multiple parts, try using the second-to-last part as database
      if (pathParts.length > 1) {
        String potentialDatabase = pathParts[pathParts.length - 2];
        columns = tryGetTableColumns(sparkSession, potentialDatabase, potentialTableName);
        if (columns.isPresent()) {
          return columns;
        }
      }

      log.debug("Could not find Glue table for dataset: {}", datasetName);
      return Optional.empty();

    } catch (Exception e) {
      log.debug(
          "Error resolving Glue Catalog schema for dataset {}: {}",
          datasetIdentifier.getName(),
          e.getMessage());
      return Optional.empty();
    }
  }

  /**
   * Attempts to retrieve column names from a specific table in the Spark catalog.
   *
   * @param sparkSession The Spark session
   * @param database The database name
   * @param tableName The table name
   * @return Optional list of column names if the table exists
   */
  private static Optional<List<String>> tryGetTableColumns(
      SparkSession sparkSession, String database, String tableName) {

    try {
      // Check if the table exists
      if (!sparkSession.catalog().tableExists(database, tableName)) {
        log.debug("Table {}.{} does not exist in catalog", database, tableName);
        return Optional.empty();
      }

      // Get the table metadata
      CatalogTable catalogTable =
          sparkSession
              .sessionState()
              .catalog()
              .getTableMetadata(
                  sparkSession
                      .sessionState()
                      .sqlParser()
                      .parseTableIdentifier(database + "." + tableName));

      // Extract column names from the schema
      List<String> columnNames =
          ScalaConversionUtils.fromSeq(catalogTable.schema()).stream()
              .map(StructField::name)
              .collect(Collectors.toList());

      log.debug(
          "Retrieved {} columns from Glue table {}.{}: {}",
          columnNames.size(),
          database,
          tableName,
          columnNames);

      return Optional.of(columnNames);

    } catch (Exception e) {
      log.debug("Could not get columns for table {}.{}: {}", database, tableName, e.getMessage());
      return Optional.empty();
    }
  }

  /**
   * Checks if Hive is configured to use Glue as the metastore.
   *
   * @param sparkConf The Spark configuration
   * @param hadoopConf The Hadoop configuration
   * @return true if Glue is being used as the metastore
   */
  private static boolean isHiveUsingGlue(SparkConf sparkConf, Configuration hadoopConf) {
    Optional<String> hadoopFactoryClass =
        SparkConfUtils.findHadoopConfigKey(
            hadoopConf, AwsUtils.HIVE_METASTORE_CLIENT_FACTORY_CLASS);
    Optional<String> sparkFactoryClass =
        SparkConfUtils.findSparkConfigKey(sparkConf, AwsUtils.HIVE_METASTORE_CLIENT_FACTORY_CLASS);
    return AwsUtils.AWS_GLUE_HIVE_FACTORY_CLASS.equals(
        hadoopFactoryClass.orElse(sparkFactoryClass.orElse(null)));
  }

  /**
   * Returns the original catalog column names when DynamicFrame has renamed columns but preserved
   * their order. Falls back to RDD column names when:
   *
   * <ul>
   *   <li>Column counts differ (columns were added or dropped)
   *   <li>Column name sets are identical (columns were reordered, not renamed)
   * </ul>
   *
   * @param rddColumnNames The column names from the LogicalRDD
   * @param catalogColumnNames The original column names from the Glue Catalog
   * @return Catalog column names if columns were renamed, RDD column names otherwise
   */
  public static List<String> mapColumnsByPosition(
      List<String> rddColumnNames, List<String> catalogColumnNames) {

    if (rddColumnNames.size() != catalogColumnNames.size()) {
      log.warn(
          "Column count mismatch: RDD has {} columns, Catalog has {} columns. "
              + "Using RDD column names.",
          rddColumnNames.size(),
          catalogColumnNames.size());
      return rddColumnNames;
    }

    // If the column name sets are identical, columns were reordered not renamed.
    // The RDD names are already correct in that case.
    if (new HashSet<>(rddColumnNames).equals(new HashSet<>(catalogColumnNames))) {
      log.debug(
          "RDD and catalog columns have the same names (possibly reordered). "
              + "Using RDD column names.");
      return rddColumnNames;
    }

    log.debug("Mapping {} RDD columns to catalog columns by position", rddColumnNames.size());
    return catalogColumnNames;
  }
}
