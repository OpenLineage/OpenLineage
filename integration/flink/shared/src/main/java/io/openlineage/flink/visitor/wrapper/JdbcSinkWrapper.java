/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.wrapper;

import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.internal.GenericJdbcSinkFunction;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.xa.JdbcXaSinkFunction;

@Slf4j
public class JdbcSinkWrapper {
  private Object sink;

  public <T> JdbcSinkWrapper(T sink) {
    this.sink = sink;
  }

  public static <T> JdbcSinkWrapper of(T sink) {
    return new JdbcSinkWrapper(sink);
  }

  public String getConnectionUrl() {
    Optional<JdbcConnectionOptions> connectionOptionsOpt = getConnectionOptions();
    return connectionOptionsOpt
        .map(connectionOptions -> connectionOptions.getDbURL())
        .orElseThrow();
  }

  public Optional<String> getTableName() {
    Optional<JdbcConnectionOptions> connectionOptionsOpt = getConnectionOptions();
    return connectionOptionsOpt
        .map(
            connectionOptions ->
                WrapperUtils.<String>getFieldValue(
                    connectionOptions.getClass(), connectionOptions, "tableName"))
        .orElse(Optional.of(""));
  }

  private Optional<JdbcConnectionOptions> getConnectionOptions() {
    Optional jdbcOutputFormatOpt = Optional.empty();
    if (sink instanceof JdbcOutputFormat) {
      jdbcOutputFormatOpt = Optional.of((JdbcOutputFormat) sink);
    } else if (sink instanceof GenericJdbcSinkFunction) {
      jdbcOutputFormatOpt =
          WrapperUtils.<JdbcOutputFormat>getFieldValue(
              GenericJdbcSinkFunction.class, sink, "outputFormat");
    } else if (sink instanceof JdbcXaSinkFunction) {
      jdbcOutputFormatOpt =
          WrapperUtils.<JdbcOutputFormat>getFieldValue(
              JdbcXaSinkFunction.class, sink, "outputFormat");
    }

    if (jdbcOutputFormatOpt.isPresent()) {
      Optional<JdbcConnectionProvider> providerOpt =
          WrapperUtils.<JdbcConnectionProvider>getFieldValue(
              JdbcOutputFormat.class, jdbcOutputFormatOpt.get(), "connectionProvider");
      return providerOpt
          .map(
              provider ->
                  WrapperUtils.<JdbcConnectionOptions>getFieldValue(
                      SimpleJdbcConnectionProvider.class, provider, "jdbcOptions"))
          .get();
    }

    return Optional.empty();
  }
}
