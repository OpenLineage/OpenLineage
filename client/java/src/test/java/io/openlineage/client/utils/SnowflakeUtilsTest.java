/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class SnowflakeUtilsTest {

  @ParameterizedTest
  @CsvSource({
    // Organization-account format (preferred) - never includes region/cloud in URL
    "https://myorg-myaccount.snowflakecomputing.com, myorg-myaccount",
    "http://myorg-myaccount.snowflakecomputing.com, myorg-myaccount",
    "myorg-myaccount, myorg-myaccount",
    "https://my-org-name-my-account.snowflakecomputing.com, my-org-name-my-account",
    "https://org123-account456.snowflakecomputing.com, org123-account456",
    // Legacy account locator format - return with region.cloud
    "https://xy12345.snowflakecomputing.com, xy12345.us-west-1.aws",
    "https://xy12345.us-west-2.snowflakecomputing.com, xy12345.us-west-2.aws",
    "https://xy12345.us-west-2.aws.snowflakecomputing.com, xy12345.us-west-2.aws",
    "https://xy12345.east-us-2.azure.snowflakecomputing.com, xy12345.east-us-2.azure",
    "https://abc123.us-central1.gcp.snowflakecomputing.com, abc123.us-central1.gcp",
    "accountlocator, accountlocator.us-west-1.aws",
    "xy12345.us-east-1, xy12345.us-east-1.aws",
    "xy12345.us-east-1.gcp, xy12345.us-east-1.gcp"
  })
  void testParseAccountIdentifier(String input, String expected) {
    String result = SnowflakeUtils.parseAccountIdentifier(input);
    assertEquals(
        expected, result, String.format("Failed to parse account identifier from URL: %s", input));
  }
}
