/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import java.net.URISyntaxException;
import java.util.Properties;

public interface JdbcExtractor {
  public abstract boolean isDefinedAt(String jdbcUri);

  public abstract JdbcLocation extract(String rawUri, Properties properties)
      throws URISyntaxException;
}
