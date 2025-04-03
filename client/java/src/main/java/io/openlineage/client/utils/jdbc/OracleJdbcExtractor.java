/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

public class OracleJdbcExtractor implements JdbcExtractor {
  // https://docs.oracle.com/en/database/oracle/oracle-database/21/jjdbc/data-sources-and-URLs.html#GUID-EF07727C-50AB-4DCE-8EDC-57F0927FF61A

  private static final String SCHEME = "oracle";
  private static final String DEFAULT_PORT = "1521";
  private static final String URI_START = "^.*@(//)?";
  private static final String URI_END = "\\?.*$";
  private static final String PROTOCOL_PART = "^\\w+://";

  @Override
  public boolean isDefinedAt(String jdbcUri) {
    return jdbcUri.toLowerCase(Locale.ROOT).startsWith(SCHEME);
  }

  @Override
  public JdbcLocation extract(String rawUri, Properties properties) throws URISyntaxException {
    // oracle:thin:@//host:1521:sid?... -> host:1521:sid
    String uri = rawUri.replaceFirst(URI_START, "").replaceAll(URI_END, "");

    if (uri.contains("(")) {
      throw new URISyntaxException(uri, "TNS format is unsupported for now");
    }
    return extractUri(uri, properties);
  }

  private JdbcLocation extractUri(String uri, Properties properties) throws URISyntaxException {
    // Handle LDAP connection strings
    if (uri.toLowerCase(Locale.ROOT).startsWith("ldap://")) {
      String[] ldapParts = uri.split("/", 4);
      if (ldapParts.length >= 3) {
        String hostPort = ldapParts[2];
        String dbName = ldapParts.length > 3 ? ldapParts[3].split(",")[0] : "";
        // Convert to a normalized form that can be processed by OverridingJdbcExtractor
        uri = hostPort + "/" + dbName;
      }
    }

    // convert 'tcp://'' protocol to 'oracle://'', convert ':sid' format to '/sid'
    String normalizedUri = uri.replaceFirst(PROTOCOL_PART, "");
    normalizedUri = SCHEME + "://" + fixSidFormat(normalizedUri);

    return new OverridingJdbcExtractor(SCHEME, DEFAULT_PORT).extract(normalizedUri, properties);
  }

  private String fixSidFormat(String uri) {
    if (!uri.contains(":")) {
      return uri;
    }
    List<String> components = Arrays.stream(uri.split(":")).collect(Collectors.toList());
    String last = components.remove(components.size() - 1);
    if (last.contains("]") || last.matches("^\\d+$") || last.contains("/")) {
      // '[ip:v:6]' or 'host:1521' or 'host:1521/serviceName'
      return uri;
    }
    // 'host:1521:sid' -> 'host:1521/sid'
    return StringUtils.join(components, ":") + "/" + last;
  }
}
