/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.Properties;

public class GenericJdbcExtractor implements JdbcExtractor {
  @Override
  public boolean isDefinedAt(String jdbcUri) {
    return true;
  }

  @Override
  public JdbcLocation extract(String rawUri, Properties properties) throws URISyntaxException {
    URI uri = new URI(rawUri);

    if (uri.getHost() == null) {
      throw new URISyntaxException(rawUri, "Missing host");
    }

    String scheme = uri.getScheme();
    String host = uri.getHost();
    Optional<String> port =
        uri.getPort() > 0 ? Optional.of(String.format("%d", uri.getPort())) : Optional.empty();
    Optional<String> database =
        Optional.ofNullable(uri.getPath())
            .map(db -> db.replaceFirst("/", ""))
            .filter(db -> !db.isEmpty());
    return new JdbcLocation(scheme, host, port, Optional.empty(), database);
  }
}
