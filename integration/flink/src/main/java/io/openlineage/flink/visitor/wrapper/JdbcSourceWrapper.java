/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.wrapper;

import io.openlineage.sql.OpenLineageSql;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.options.InternalJdbcConnectionOptions;
import org.apache.flink.connector.jdbc.table.JdbcRowDataInputFormat;
import org.apache.flink.connector.jdbc.table.JdbcRowDataLookupFunction;

@Slf4j
public class JdbcSourceWrapper {
  private Object source;
  private Class sourceClass;

  public <T> JdbcSourceWrapper(T source, Class sourceClass) {
    this.source = source;
    this.sourceClass = sourceClass;
  }

  public static <T> JdbcSourceWrapper of(T source, Class sourceClass) {
    return new JdbcSourceWrapper(source, sourceClass);
  }

  public String getConnectionUrl() {
    Optional<JdbcConnectionOptions> connectionOptionsOpt = getConnectionOptions();
    return connectionOptionsOpt
        .map(connectionOptions -> connectionOptions.getDbURL())
        .orElseThrow();
  }

  public String getTableName() {
    Optional<String> queryOpt = Optional.empty();
    if (source instanceof JdbcRowDataLookupFunction) {
      Optional<JdbcConnectionOptions> connectionOptionsOpt = getConnectionOptions();
      return connectionOptionsOpt
          .map(
              connectionOptions ->
                  ((InternalJdbcConnectionOptions) connectionOptions).getTableName())
          .orElseThrow();
    } else if (source instanceof JdbcInputFormat) {
      queryOpt = WrapperUtils.<String>getFieldValue(JdbcInputFormat.class, source, "queryTemplate");
    } else if (source instanceof JdbcRowDataInputFormat) {
      queryOpt =
          WrapperUtils.<String>getFieldValue(JdbcRowDataInputFormat.class, source, "queryTemplate");
    }

    Optional<String> tableNameOpt =
        queryOpt
            .flatMap(query -> OpenLineageSql.parse(List.of(query)))
            .map(sqlMeta -> sqlMeta.inTables().isEmpty() ? "" : sqlMeta.inTables().get(0).name());

    return tableNameOpt.isPresent() ? tableNameOpt.get() : "";
  }

  private Optional<JdbcConnectionOptions> getConnectionOptions() {
    if (source instanceof JdbcInputFormat
        || source instanceof JdbcRowDataInputFormat
        || source instanceof JdbcRowDataLookupFunction) {
      Optional<JdbcConnectionProvider> providerOpt =
          WrapperUtils.<JdbcConnectionProvider>getFieldValue(
              source.getClass(), source, "connectionProvider");
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
