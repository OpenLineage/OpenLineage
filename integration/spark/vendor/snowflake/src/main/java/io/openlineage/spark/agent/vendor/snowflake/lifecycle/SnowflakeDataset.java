/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.snowflake.lifecycle;

import static io.openlineage.client.utils.SnowflakeUtils.stripQuotes;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.SnowflakeUtils;
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
        String.format("%s%s", SNOWFLAKE_PREFIX, SnowflakeUtils.parseAccountIdentifier(sfFullURL));
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
          stripQuotes(sfSchema),
          normalizedSchema);
    } else {
      logger.warn(
          "Unable to discover Snowflake table property - neither \"dbtable\" nor \"query\" option present");
    }
    return Collections.emptyList();
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
  public static StructType stripQuotesFromSchema(StructType schema) {
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
