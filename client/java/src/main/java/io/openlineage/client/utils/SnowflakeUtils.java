/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils;

public class SnowflakeUtils {
  /**
   * Parses the Snowflake full URL to extract the account identifier according to OpenLineage naming
   * specification.
   *
   * <p>Snowflake supports two URL formats:
   *
   * <p>1. Organization-account format (preferred):
   *
   * <ul>
   *   <li>https://orgname-accountname.snowflakecomputing.com → orgname-accountname
   * </ul>
   *
   * <p>Note: Organization-account URLs never include region or cloud information.
   *
   * <p>2. Legacy account locator format:
   *
   * <ul>
   *   <li>https://xy12345.snowflakecomputing.com → xy12345.us-west-1.aws (defaults added for AWS US
   *       West Oregon)
   *   <li>https://xy12345.us-east-1.snowflakecomputing.com → xy12345.us-east-1.aws (cloud defaults
   *       to aws)
   *   <li>https://xy12345.us-east-2.aws.snowflakecomputing.com → xy12345.us-east-2.aws
   *   <li>https://xy12345.east-us-2.azure.snowflakecomputing.com → xy12345.east-us-2.azure
   * </ul>
   *
   * <p>This method returns the namespace part for OpenLineage:
   *
   * <ul>
   *   <li>Organization-account format: snowflake://orgname-accountname
   *   <li>Account locator format: snowflake://account_locator.region.cloud
   * </ul>
   *
   * @param sfFullURL The full Snowflake URL or account identifier
   * @return The account identifier according to OpenLineage spec
   */
  @SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
  public static String parseAccountIdentifier(String sfFullURL) {
    String url = sfFullURL;

    // Remove protocol if present
    if (url.startsWith("https://")) {
      url = url.substring(8);
    } else if (url.startsWith("http://")) {
      url = url.substring(7);
    }

    // Remove snowflakecomputing.com domain if present
    int domainIndex = url.indexOf(".snowflakecomputing.com");
    if (domainIndex > 0) {
      url = url.substring(0, domainIndex);
    }

    // Split by dots to analyze the structure
    String[] parts = url.split("\\.");

    // First part is the account identifier (either orgname-accountname or account_locator)
    String accountPart = parts[0];

    // Check if it's organization-account format (contains hyphen)
    // Organization-account format: orgname-accountname (never has region/cloud in URL)
    // Account locator format: accountlocator[.region[.cloud]]
    if (accountPart.contains("-")) {
      // Organization-account format - return as-is
      return accountPart;
    }

    // Account locator format - need to include region and cloud
    if (parts.length == 1) {
      // Just account locator, add default region and cloud (AWS US West Oregon)
      return accountPart + ".us-west-1.aws";
    } else if (parts.length == 2) {
      // account_locator.region, add default cloud (aws)
      return accountPart + "." + parts[1] + ".aws";
    } else {
      // Full format: account_locator.region.cloud
      return url;
    }
  }
}
