/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

class JdbcUrlSanitizer {
  private static final String SLASH_DELIMITER_USER_PASSWORD_REGEX =
      "[A-Za-z0-9_%]+//?[A-Za-z0-9_%]*@";
  private static final String COLON_DELIMITER_USER_PASSWORD_REGEX =
      "([/|,])[A-Za-z0-9_%]+:?[A-Za-z0-9_%]*@";
  private static final String PARAMS_USER_PASSWORD_REGEX =
      "(?i)[,;&:]?(?:user|username|password)=[^,;&:()]+[,;&:]?";
  private static final String DUPLICATED_DELIMITERS = "(\\(\\)){2,}|[,;&:]{2,}";
  private static final String QUERY_PARAMS_REGEX = "\\?.*$";

  /**
   * JdbcUrl can contain username and password this method clean-up credentials from jdbcUrl. Also
   * drop query params as they include a lot of useless options, like timeout
   *
   * @param jdbcUrl url to database
   * @return String
   */
  public static String dropSensitiveData(String jdbcUrl) {
    return jdbcUrl
        .replaceAll(SLASH_DELIMITER_USER_PASSWORD_REGEX, "@")
        .replaceAll(COLON_DELIMITER_USER_PASSWORD_REGEX, "$1")
        .replaceAll(PARAMS_USER_PASSWORD_REGEX, "")
        .replaceAll(DUPLICATED_DELIMITERS, "")
        .replaceAll(QUERY_PARAMS_REGEX, "");
  }
}
