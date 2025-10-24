/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.snowflake.lifecycle;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.SqlUtils;
import io.openlineage.spark.api.DatasetFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnowflakeDataset {
  public static final String SNOWFLAKE_PREFIX = "snowflake://";

  private static final Logger logger = LoggerFactory.getLogger(SnowflakeDataset.class);

  public static <D extends OpenLineage.Dataset> List<D> getDatasets(
      DatasetFactory<D> factory,
      String sfFullURL,
      String sfDatabase,
      String sfSchema,
      Optional<String> dbtable,
      Optional<String> query,
      StructType schema) {

    final String namespace =
        String.format("%s%s", SNOWFLAKE_PREFIX, parseAccountIdentifier(sfFullURL));
    final String tableName;
    // https://docs.snowflake.com/en/user-guide/spark-connector-use#moving-data-from-snowflake-to-spark
    // > Specify one of the following options for the table data to be read:
    // >    - `dbtable`: The name of the table to be read. All columns and records are retrieved
    // >      (i.e. it is equivalent to SELECT * FROM db_table).
    // >    - `query`: The exact query (SELECT statement) to run.
    // If dbtable is null it will be replaced with the string `complex` and it means the query
    // option was used.
    // An improvement could be put the query string in the `DatasetFacets`
    StructType normalizedSchema = stripQuotesFromSchema(schema);
    if (dbtable.isPresent()) {
      tableName = dbtable.get();
      String name =
          String.format(
              "%s.%s.%s", stripQuotes(sfDatabase), stripQuotes(sfSchema), stripQuotes(tableName));
      return Collections.singletonList(factory.getDataset(name, namespace, normalizedSchema));
    } else if (query.isPresent()) {
      return SqlUtils.getDatasets(
          factory,
          query.get(),
          "snowflake",
          namespace,
          stripQuotes(sfDatabase),
          stripQuotes(sfSchema));
    } else {
      logger.warn(
          "Unable to discover Snowflake table property - neither \"dbtable\" nor \"query\" option present");
    }
    return Collections.emptyList();
  }

  /**
   * Parses the Snowflake full URL to extract the account identifier according to OpenLineage naming
   * specification.
   *
   * <p>Snowflake URLs follow the format:
   * https://&lt;account_identifier&gt;.&lt;region&gt;.&lt;cloud&gt;.snowflakecomputing.com
   *
   * <p>The account identifier can be: - orgname-accountname (preferred format) - account_locator
   * (legacy format)
   *
   * <p>This method extracts only the account identifier part, stripping the region, cloud provider,
   * and domain suffix to comply with OpenLineage naming: snowflake://&lt;account_identifier&gt;
   *
   * @param sfFullURL The full Snowflake URL
   * @return The account identifier (organization-account or account locator)
   */
  static String parseAccountIdentifier(String sfFullURL) {
    String url = sfFullURL;

    // Remove protocol if present
    if (url.startsWith("https://")) {
      url = url.substring(8);
    } else if (url.startsWith("http://")) {
      url = url.substring(7);
    }

    // Extract account identifier (first segment before the first dot)
    int firstDotIndex = url.indexOf('.');
    if (firstDotIndex > 0) {
      return url.substring(0, firstDotIndex);
    }

    return url;
  }

  /**
   * Strips surrounding double quotes from Snowflake identifiers if present.
   *
   * <p>In Snowflake and other SQL databases, double quotes are used as delimiters to preserve case
   * sensitivity or allow special characters in identifiers. The quotes are not part of the actual
   * identifier name and should be removed for normalized dataset and column names.
   *
   * <p>Examples:
   *
   * <ul>
   *   <li>"MyTable" → MyTable
   *   <li>"my_database" → my_database
   *   <li>normal_table → normal_table (no change)
   *   <li>"" → (empty string)
   *   <li>" → " (single quote, not stripped)
   * </ul>
   *
   * @param identifier The identifier that may have surrounding quotes
   * @return The identifier with surrounding quotes removed if present
   */
  static String stripQuotes(String identifier) {
    if (identifier == null || identifier.length() < 2) {
      return identifier;
    }

    if (identifier.startsWith("\"") && identifier.endsWith("\"")) {
      return identifier.substring(1, identifier.length() - 1);
    }

    return identifier;
  }

  /**
   * Strips surrounding double quotes from all field names in a Spark StructType schema.
   *
   * <p>When Snowflake tables use quoted identifiers to preserve case sensitivity, those quotes
   * appear in the Spark schema field names. This method creates a new StructType with normalized
   * field names (quotes removed) while preserving all other field properties (type, nullable,
   * metadata).
   *
   * @param schema The original schema with potentially quoted field names
   * @return A new StructType with quotes stripped from all field names
   */
  static StructType stripQuotesFromSchema(StructType schema) {
    if (schema == null) {
      return null;
    }

    StructField[] normalizedFields =
        Arrays.stream(schema.fields())
            .map(
                field ->
                    new StructField(
                        stripQuotes(field.name()),
                        field.dataType(),
                        field.nullable(),
                        field.metadata()))
            .toArray(StructField[]::new);

    return new StructType(normalizedFields);
  }
}
