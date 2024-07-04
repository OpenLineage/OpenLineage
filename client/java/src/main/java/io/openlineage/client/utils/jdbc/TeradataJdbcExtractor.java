/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import java.net.URISyntaxException;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;

public class TeradataJdbcExtractor implements JdbcExtractor {
  // https://teradata-docs.s3.amazonaws.com/doc/connectivity/jdbc/reference/current/jdbcug_chapter_2.html#URL_DATABASE

  private static final String SCHEME = "teradata";
  private static final String PORT_PROPERTY = "DBS_PORT";
  private static final String DEFAULT_PORT = "1025";
  private static final String DATABASE_PROPERTY = "DATABASE";

  private static final Pattern URL =
      Pattern.compile("(?:\\w+)://(?<host>[\\w\\d\\.\\[\\]:]+)?/?(?<params>.*)");

  @Override
  public boolean isDefinedAt(String jdbcUri) {
    return jdbcUri.startsWith(SCHEME);
  }

  @Override
  @SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
  public JdbcLocation extract(String rawUri, Properties properties) throws URISyntaxException {
    Matcher matcher = URL.matcher(rawUri);
    if (!matcher.matches()) {
      throw new URISyntaxException(rawUri, "Failed to parse jdbc url");
    }

    String host = matcher.group("host");
    if (host == null) {
      throw new URISyntaxException(rawUri, "Missing host");
    }

    String[] rawParams = StringUtils.defaultString(matcher.group("params")).split(",");

    Properties params = new Properties();
    for (String urlParam : rawParams) {
      String[] parts = urlParam.split("=");
      if (parts.length == 2) {
        params.setProperty(parts[0], parts[1]);
      }
    }

    String port = Optional.ofNullable(params.getProperty(PORT_PROPERTY)).orElse(DEFAULT_PORT);
    String authority = host + ":" + port;

    Optional<String> database = Optional.ofNullable(params.getProperty(DATABASE_PROPERTY));

    return new JdbcLocation(SCHEME, authority, Optional.empty(), database);
  }
}
