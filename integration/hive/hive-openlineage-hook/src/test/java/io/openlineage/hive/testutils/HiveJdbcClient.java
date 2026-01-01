/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.testutils;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.Statement;
import lombok.SneakyThrows;

public class HiveJdbcClient {
  private final HikariDataSource dataSource;

  private HiveJdbcClient(int hiveContainerPort) {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl("jdbc:hive2://localhost:" + hiveContainerPort);
    config.setUsername("hive");
    config.setPassword("");
    config.setMaximumPoolSize(2);
    config.setMinimumIdle(1);
    config.setConnectionTimeout(30000); // 30 seconds
    config.setIdleTimeout(60000); // 1 minute
    this.dataSource = new HikariDataSource(config);
  }

  @SneakyThrows
  public boolean execute(String sql) {
    try (Connection conn = dataSource.getConnection();
        Statement stmt = conn.createStatement()) {
      return stmt.execute(sql);
    }
  }

  public void close() {
    if (dataSource != null && !dataSource.isClosed()) {
      dataSource.close();
    }
  }

  public static HiveJdbcClient create(int hiveContainerPort) {
    return new HiveJdbcClient(hiveContainerPort);
  }
}
