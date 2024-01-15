/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.snowflake.lifecycle;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.DatasetFactory;
import java.util.Optional;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnowflakeDataset {
  public static final String SNOWFLAKE_PREFIX = "snowflake://";

  private static final Logger logger = LoggerFactory.getLogger(SnowflakeDataset.class);

  public static <D extends OpenLineage.Dataset> D getDataset(
      DatasetFactory<D> factory,
      String sfFullURL,
      String sfDatabase,
      String sfSchema,
      Optional<String> dbtable,
      StructType schema) {
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
    } else {
      tableName = "COMPLEX";
      logger.warn("Unable to discover Snowflake table property");
    }

    String name = String.format("%s.%s.%s", sfDatabase, sfSchema, tableName);
    String namespace = String.format("%s%s", SNOWFLAKE_PREFIX, sfFullURL.replace("https://", ""));
    return factory.getDataset(name, namespace, schema);
  }
}
