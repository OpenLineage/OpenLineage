/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GenericJdbcExtractor implements JdbcExtractor {
  private static Pattern URL_FORMAT =
      Pattern.compile(
          "^(?<scheme>\\w+)://(?<authority>[\\w\\d\\.\\[\\]:,-]+)/?(?<database>[\\w\\d.]+)?(?:\\?.*)?");

  @Override
  public boolean isDefinedAt(String jdbcUri) {
    return true;
  }

  @Override
  public JdbcLocation extract(String rawUri, Properties properties) throws URISyntaxException {
    if (!rawUri.contains(",")) {
      return extractOneHost(rawUri);
    }
    return extractMultipleHosts(rawUri);
  }

  private JdbcLocation extractOneHost(String rawUri) throws URISyntaxException {
    URI uri = new URI(rawUri);

    if (uri.getHost() == null) {
      throw new URISyntaxException(rawUri, "Missing host");
    }

    String scheme = uri.getScheme();
    String host = uri.getHost();
    String authority;
    if (uri.getPort() > 0) {
      authority = String.format("%s:%d", host, uri.getPort());
    } else {
      authority = host;
    }

    Optional<String> database =
        Optional.ofNullable(uri.getPath())
            .map(db -> db.replaceFirst("/", ""))
            .filter(db -> !db.isEmpty());

    return new JdbcLocation(scheme, Optional.of(authority), Optional.empty(), database);
  }

  private JdbcLocation extractMultipleHosts(String rawUri) throws URISyntaxException {
    // new URI() parses 'scheme://host1,host2' syntax as scheme-specific part instead of authority
    // Using regex to extract URI components

    Matcher matcher = URL_FORMAT.matcher(rawUri);
    if (!matcher.matches()) {
      throw new URISyntaxException(rawUri, "Failed to parse jdbc url");
    }

    String scheme = matcher.group("scheme");
    String authority = matcher.group("authority");
    String database = matcher.group("database");
    return new JdbcLocation(
        scheme, Optional.ofNullable(authority), Optional.empty(), Optional.ofNullable(database));
  }
}
