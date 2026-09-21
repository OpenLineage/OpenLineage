/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
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
      return extractTns(uri, properties);
    }
    return extractUri(uri, properties);
  }

  /**
   * Handles TNS connect descriptors, e.g. {@code
   * (DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=h)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=s)))}.
   * Pulls out every ADDRESS (host/port) and the CONNECT_DATA identifier, then reuses
   * OverridingJdbcExtractor for the actual formatting instead of duplicating it here.
   */
  private JdbcLocation extractTns(String uri, Properties properties) throws URISyntaxException {
    TnsDescriptor root = TnsDescriptor.parse(uri);

    List<String> hostPorts = new ArrayList<>();
    for (TnsDescriptor address : root.findAll("ADDRESS")) {
      Optional<String> host = address.childValue("HOST");
      if (!host.isPresent()) {
        continue;
      }
      String port = address.childValue("PORT").orElse(DEFAULT_PORT);
      hostPorts.add(host.get() + ":" + port);
    }
    if (hostPorts.isEmpty()) {
      throw new URISyntaxException(uri, "No ADDRESS entries found in TNS descriptor");
    }

    String identifier =
        root.findAll("CONNECT_DATA").stream()
            .findFirst()
            .flatMap(
                cd ->
                    cd.childValue("SERVICE_NAME")
                        .map(Optional::of)
                        .orElseGet(
                            () ->
                                cd.childValue("SID")
                                    .map(Optional::of)
                                    .orElseGet(() -> cd.childValue("INSTANCE_NAME"))))
            .orElse(null);

    String normalizedUri =
        SCHEME
            + "://"
            + StringUtils.join(hostPorts, ",")
            + (identifier != null ? "/" + identifier : "");

    return new OverridingJdbcExtractor(SCHEME, DEFAULT_PORT).extract(normalizedUri, properties);
  }

  @SuppressWarnings("PMD")
  private JdbcLocation extractUri(String uri, Properties properties) throws URISyntaxException {
    // Handle LDAP connection strings, including cases with multiple LDAP URIs
    if (uri.toLowerCase(Locale.ROOT).contains("ldap")) {
      uri = handleLdapConnectionString(uri);
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

  /**
   * Handles LDAP connection strings by extracting and normalizing the host/port and database name.
   * Supports multiple LDAP URIs and different LDAP prefix formats.
   *
   * @param uri The original URI string that might contain LDAP connection information
   * @return A normalized URI with host/port and database name extracted from the LDAP string
   */
  @SuppressWarnings("PMD")
  private String handleLdapConnectionString(String uri) {
    // Split original URI by whitespace to cover scenarios with multiple LDAP URIs
    String[] tokens = uri.split("\\\\s+");
    String firstLdapUri = null;
    for (String token : tokens) {
      String lowerToken = token.toLowerCase(Locale.ROOT);
      if (lowerToken.contains("ldap:") || lowerToken.contains("ldaps:")) {
        // In case a prefix like "jdbc:oracle:thin:@" exists, take the substring starting from
        // "ldap"
        int idx = lowerToken.indexOf("ldap");
        firstLdapUri = token.substring(idx);
        break;
      }
    }

    if (firstLdapUri != null) {
      String withoutPrefix = firstLdapUri.replaceFirst("(?i)ldaps?:(//)?", "");

      // Split the string by "/" into all segments.
      // The first segment is the host:port and the last segment is the database name.
      String[] parts = withoutPrefix.split("/");
      if (parts.length >= 1) {
        String hostPort = parts[0];
        String dbName = "";
        if (parts.length > 1) {
          // Use the last segment as the database name.
          dbName = parts[parts.length - 1].split(",")[0];
        }
        // Normalize the URI for further processing.
        return hostPort + "/" + dbName;
      }
    }

    return uri;
  }
}
