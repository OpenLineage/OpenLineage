/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import java.net.URISyntaxException;
import java.util.Locale;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OverridingJdbcExtractor extends GenericJdbcExtractor implements JdbcExtractor {
  private String overrideScheme;
  private String defaultPort;
  private static Pattern HOST_PORT_FORMAT =
      Pattern.compile("^(?<host>[\\[\\]\\w\\d.-]+):(?<port>\\d+)?");

  public OverridingJdbcExtractor(String overrideScheme) {
    this(overrideScheme, null);
  }

  public OverridingJdbcExtractor(String overrideScheme, String defaultPort) {
    this.overrideScheme = overrideScheme;
    this.defaultPort = defaultPort;
  }

  @Override
  public boolean isDefinedAt(String jdbcUri) {
    return jdbcUri.toLowerCase(Locale.ROOT).startsWith(overrideScheme);
  }

  @Override
  public JdbcLocation extract(String rawUri, Properties properties) throws URISyntaxException {
    JdbcLocation result = super.extract(rawUri, properties);

    Optional<String> authority = result.getAuthority();
    if (authority.isPresent() && defaultPort != null) {
      authority = authority.map(this::appendDefaultPort);
    }

    return new JdbcLocation(overrideScheme, authority, result.getInstance(), result.getDatabase());
  }

  private String appendDefaultPort(String authority) {
    String[] hosts = authority.split(",");
    for (int i = 0; i < hosts.length; i++) {
      String host = hosts[i];
      Matcher hostPortMatcher = HOST_PORT_FORMAT.matcher(host);
      if (!hostPortMatcher.matches()) {
        hosts[i] = host + ":" + defaultPort;
      }
    }
    return String.join(",", hosts);
  }
}
