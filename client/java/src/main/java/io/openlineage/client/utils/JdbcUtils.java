/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcUtils {
  public static final String SLASH_DELIMITER_USER_PASSWORD_REGEX =
      "[A-Za-z0-9_%]+//?[A-Za-z0-9_%]*@";
  public static final String COLON_DELIMITER_USER_PASSWORD_REGEX =
      "([/|,])[A-Za-z0-9_%]+:?[A-Za-z0-9_%]*@";
  public static final String ALPHANUMERIC = "[A-Za-z0-9]+";

  /**
   * JdbcUrl can contain username and password this method clean-up credentials from jdbcUrl and
   * strip the jdbc prefix from the url
   *
   * @param jdbcUrl url to database
   * @return String
   */
  public static String sanitizeJdbcUrl(String jdbcUrl) {
    return jdbcUrl
        .replaceFirst("^jdbc:", "")
        .replaceFirst("^postgresql:", "postgres:")
        .replaceAll(SLASH_DELIMITER_USER_PASSWORD_REGEX, "@")
        .replaceAll(COLON_DELIMITER_USER_PASSWORD_REGEX, "$1")
        .replaceAll("(?<=[?,;&:)=])\\(?(?i)(?:user|username|password)=[^;&,)]+(?:[;&;)]|$)", "")
        .replaceAll("\\?.*$", "");
  }

  public static DatasetIdentifier getDatasetIdentifierFromJdbcUrl(String jdbcUrl, String name) {
    List<String> parts = Arrays.stream(name.split("\\.")).collect(Collectors.toList());
    return getDatasetIdentifierFromJdbcUrl(jdbcUrl, parts);
  }

  /**
   * The algorithm for this method is as follows. First we parse URI and check if it includes path
   * part of URI. If yes, then we check if it contains database. Database is the first part after
   * slash in URI - the "db" in something like postgres://host:5432/db. If it does contain it, and
   * provided parts list has less than three elements, then we use it as database part of name -
   * this indicates that database is the default one in this context. Otherwise, we take database
   * from parts list.
   *
   * @param jdbcUrl String URI we want to take dataset identifier from
   * @param parts Provided list of delimited parts of table qualified name parts. Can include
   *     database name.
   * @return DatasetIdentifier
   */
  public static DatasetIdentifier getDatasetIdentifierFromJdbcUrl(
      String jdbcUrl, List<String> parts) {
    String namespace = sanitizeJdbcUrl(jdbcUrl);
    String urlDatabase = null;

    try {
      URI uri = new URI(namespace);
      String path = uri.getPath();
      if (path != null) {
        namespace = String.format("%s://%s", uri.getScheme(), uri.getAuthority());

        if (path.startsWith("/")) {
          path = path.substring(1);
        }

        if (path.length() > 1 && path.matches(ALPHANUMERIC)) {
          urlDatabase = path;
        }
      }
    } catch (URISyntaxException ignored) {
      // If URI parsing fails, we can't do anything smart - let's return provided URI
      // as a dataset namespace
    }

    if (urlDatabase != null && parts.size() <= 3) {
      parts.add(0, urlDatabase);
    }

    String name = String.join(".", parts);

    return new DatasetIdentifier(name, namespace);
  }
}
