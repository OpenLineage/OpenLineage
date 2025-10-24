/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.snowflake.lifecycle;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.SqlUtils;
import io.openlineage.spark.api.DatasetFactory;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
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
    if (dbtable.isPresent()) {
      tableName = dbtable.get();
      String name = String.format("%s.%s.%s", sfDatabase, sfSchema, tableName);
      return Collections.singletonList(factory.getDataset(name, namespace, schema));
    } else if (query.isPresent()) {
      return SqlUtils.getDatasets(
          factory, query.get(), "snowflake", namespace, sfDatabase, sfSchema);
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
}
