/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.snowflake.lifecycle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class SnowflakeDatasetTest {

  @ParameterizedTest
  @CsvSource({
    // Modern organization-account format on Azure
    "https://myorg-myaccount.east-us-2.azure.snowflakecomputing.com, myorg-myaccount",

    // Modern organization-account format on AWS
    "https://myorg-myaccount.us-west-2.aws.snowflakecomputing.com, myorg-myaccount",

    // Modern organization-account format on GCP
    "https://myorg-myaccount.us-central1.gcp.snowflakecomputing.com, myorg-myaccount",

    // Legacy account locator format
    "https://xy12345.snowflakecomputing.com, xy12345",
    "https://xy12345.us-west-2.snowflakecomputing.com, xy12345",

    // Without https protocol
    "http://myorg-myaccount.east-us-2.azure.snowflakecomputing.com, myorg-myaccount",

    // Edge case: just account identifier (no domain)
    "myorg-myaccount, myorg-myaccount",
    "accountlocator, accountlocator",

    // Complex organization names with multiple hyphens
    "https://my-org-name-my-account.region.cloud.snowflakecomputing.com, my-org-name-my-account",

    // Numeric account names
    "https://org123-account456.us-east-1.aws.snowflakecomputing.com, org123-account456"
  })
  void testParseAccountIdentifier(String input, String expected) {
    String result = SnowflakeDataset.parseAccountIdentifier(input);
    assertEquals(
        expected, result, String.format("Failed to parse account identifier from URL: %s", input));
  }

  @ParameterizedTest
  @CsvSource({
    // Quoted identifiers (should strip quotes)
    "\"MyTable\", MyTable",
    "\"my_database\", my_database",
    "\"UPPERCASE_TABLE\", UPPERCASE_TABLE",
    "\"table_with_special$chars\", table_with_special$chars",

    // Unquoted identifiers (should remain unchanged)
    "normal_table, normal_table",
    "TableName, TableName",
    "TABLE123, TABLE123",

    // Edge cases
    "\"\", ''", // Empty quotes should result in empty string (represented as '' in CSV)
    "single, single", // No quotes

    // Null and short strings
    "null, null", // Null input (will be handled separately)
  })
  void testStripQuotes(String input, String expected) {
    // Handle null case
    if ("null".equals(input)) {
      assertNull(SnowflakeDataset.stripQuotes(null));
      return;
    }

    // Handle empty string representation in CSV
    if ("''".equals(expected)) {
      expected = "";
    }

    String result = SnowflakeDataset.stripQuotes(input);
    assertEquals(expected, result, String.format("Failed to strip quotes from: %s", input));
  }

  @Test
  void testStripQuotesFromSchema() {
    // Create schema with quoted field names
    StructType originalSchema =
        new StructType(
            new StructField[] {
              new StructField("\"report_date\"", DataTypes.DateType, false, Metadata.empty()),
              new StructField("\"model_portfolio\"", DataTypes.StringType, true, Metadata.empty()),
              new StructField("unquoted_field", DataTypes.IntegerType, true, Metadata.empty()),
              new StructField("\"bor_nav\"", DataTypes.DoubleType, true, Metadata.empty())
            });

    // Strip quotes from schema
    StructType normalizedSchema = SnowflakeDataset.stripQuotesFromSchema(originalSchema);

    // Verify field names are normalized
    assertEquals(4, normalizedSchema.fields().length);
    assertEquals("report_date", normalizedSchema.fields()[0].name());
    assertEquals("model_portfolio", normalizedSchema.fields()[1].name());
    assertEquals("unquoted_field", normalizedSchema.fields()[2].name());
    assertEquals("bor_nav", normalizedSchema.fields()[3].name());

    // Verify data types and nullable are preserved
    assertEquals(DataTypes.DateType, normalizedSchema.fields()[0].dataType());
    assertEquals(false, normalizedSchema.fields()[0].nullable());
    assertEquals(DataTypes.StringType, normalizedSchema.fields()[1].dataType());
    assertEquals(true, normalizedSchema.fields()[1].nullable());
    assertEquals(DataTypes.IntegerType, normalizedSchema.fields()[2].dataType());
    assertEquals(DataTypes.DoubleType, normalizedSchema.fields()[3].dataType());
  }

  @Test
  void testStripQuotesFromSchemaWithNull() {
    assertNull(SnowflakeDataset.stripQuotesFromSchema(null));
  }

  @Test
  void testStripQuotesFromSchemaEmpty() {
    StructType emptySchema = new StructType(new StructField[] {});
    StructType result = SnowflakeDataset.stripQuotesFromSchema(emptySchema);
    assertEquals(0, result.fields().length);
  }
}
