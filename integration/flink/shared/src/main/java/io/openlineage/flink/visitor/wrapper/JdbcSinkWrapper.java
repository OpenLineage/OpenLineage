/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.wrapper;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.internal.GenericJdbcSinkFunction;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.xa.JdbcXaSinkFunction;

import java.util.Optional;

@Slf4j
public class JdbcSinkWrapper {
  private Object sink;
  private Class sinkClass;

  public <T> JdbcSinkWrapper(T sink, Class sinkClass) {
    this.sink = sink;
    this.sinkClass = sinkClass;
  }

  public static <T> JdbcSinkWrapper of(T sink, Class sourceClass) {
    return new JdbcSinkWrapper(sink, sourceClass);
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
            connectionOptions -> WrapperUtils.<String>getFieldValue(connectionOptions.getClass(), connectionOptions, "tableName"))
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
