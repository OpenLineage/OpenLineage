/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DerbyJdbcExtractor implements JdbcExtractor {
  private static final Pattern DATABASE_NAME_PATTERN =
      Pattern.compile(".*databaseName=(?<databaseName>[^;]+).*");

  @Override
  public boolean isDefinedAt(String jdbcUri) {
    return jdbcUri.startsWith("derby:");
  }

  @Override
  public JdbcLocation extract(String rawUri, Properties properties) throws URISyntaxException {
    Matcher databaseNameMatcher = DATABASE_NAME_PATTERN.matcher(rawUri);

    String databaseName = "metastore_db";
    if (databaseNameMatcher.find()) {
      databaseName = databaseNameMatcher.group("databaseName");
    }

    // get Derby location, e.g. /tmp or /current/dir
    // https://db.apache.org/derby/docs/10.3/tuning/rtunproper32066.html
    String derbyHome =
        Optional.ofNullable(System.getProperty("derby.system.home"))
            .orElse(System.getProperty("user.dir"));

    // databaseName could be a relative location. Normalize '/some/path/../abc' to '/some/abc'
    String derbyLocation = new URI(derbyHome + File.separator + databaseName).normalize().getPath();

    return new JdbcLocation("file", Optional.empty(), Optional.of(derbyLocation), Optional.empty());
  }
}
