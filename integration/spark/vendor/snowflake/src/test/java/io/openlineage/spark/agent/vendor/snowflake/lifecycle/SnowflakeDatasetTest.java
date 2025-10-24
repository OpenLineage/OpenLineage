/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.snowflake.lifecycle;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
}
