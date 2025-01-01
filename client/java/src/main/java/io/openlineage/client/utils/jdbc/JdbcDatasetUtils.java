/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import io.openlineage.client.utils.DatasetIdentifier;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcDatasetUtils {
  private static final JdbcExtractor[] extractors = {
    new PostgresJdbcExtractor(),
    new OracleJdbcExtractor(),
    new MySqlJdbcExtractor(),
    new SqlServerJdbcExtractor(),
    new TeradataJdbcExtractor(),
    new DerbyJdbcExtractor(),
    new GenericJdbcExtractor()
  };

  private static JdbcExtractor getExtractor(String jdbcUri) throws URISyntaxException {
    for (JdbcExtractor extractor : extractors) {
      if (extractor.isDefinedAt(jdbcUri)) {
        return extractor;
      }
    }
    throw new URISyntaxException(jdbcUri, "Unsupported JDBC URL");
  }

  /**
   * Get DatasetIdentifier from JdbcUrl and JDBC properties for specified table.
   *
   * @param jdbcUrl url to database
   * @param name table name
   * @param properties JDBC properties
   * @return DatasetIdentifier
   */
  public static DatasetIdentifier getDatasetIdentifier(
      String jdbcUrl, String name, Properties properties) {
    List<String> parts = Arrays.stream(name.split("\\.")).collect(Collectors.toList());
    return getDatasetIdentifier(jdbcUrl, parts, properties);
  }

  @SneakyThrows
  public static DatasetIdentifier getDatasetIdentifier(
      String jdbcUrl, List<String> parts, Properties properties) {

    String uri = jdbcUrl.replaceAll("^(?i)jdbc:", "");
    try {
      JdbcExtractor extractor = getExtractor(uri);
      JdbcLocation location = extractor.extract(uri, properties);
      return new DatasetIdentifier(location.toName(parts), location.toNamespace());
    } catch (URISyntaxException e) {
      log.debug("Failed to parse jdbc url", e);
      return new DatasetIdentifier(
          String.join(".", parts), JdbcUrlSanitizer.dropSensitiveData(uri));
    }
  }
}
