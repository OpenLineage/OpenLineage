/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.wrapper;

import static org.apache.flink.util.Preconditions.checkState;

import com.datastax.driver.mapping.annotations.Table;
import io.openlineage.flink.utils.CassandraUtils;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CassandraSourceWrapper<T> {
  private static final Pattern SELECT_REGEXP =
      Pattern.compile("(?i)select .+ from (\\w+)\\.(\\w+).*;$");
  private static final String POJO_CLASS_FIELD_NAME = "inputClass";
  private static final String QUERY_FIELD_NAME = "query";

  private T source;
  private Class sourceClass;
  private boolean hasQuery;

  public CassandraSourceWrapper(T source, Class sourceClass, boolean hasQuery) {
    this.source = source;
    this.sourceClass = sourceClass;
    this.hasQuery = hasQuery;
  }

  public static <T> CassandraSourceWrapper of(T source, Class sourceClass, boolean hasQuery) {
    return new CassandraSourceWrapper(source, sourceClass, hasQuery);
  }

  public String getKeyspace() {
    if (hasQuery) {
      return extractFromQuery(1);
    } else {
      Class pojoClass = getField(POJO_CLASS_FIELD_NAME);
      Optional<Table> table = CassandraUtils.extractTableAnnotation(pojoClass);
      return table.map(t -> t.keyspace()).orElseThrow();
    }
  }

  public String getTableName() {
    if (hasQuery) {
      return extractFromQuery(2);
    } else {
      Class pojoClass = getField(POJO_CLASS_FIELD_NAME);
      Optional<Table> table = CassandraUtils.extractTableAnnotation(pojoClass);
      return table.map(t -> t.name()).orElseThrow();
    }
  }

  private String extractFromQuery(int index) {
    String query = getField(QUERY_FIELD_NAME);
    final Matcher queryMatcher = SELECT_REGEXP.matcher(query);
    checkState(
        queryMatcher.matches(), "Query must be of the form select ... from keyspace.table ...;");
    return queryMatcher.group(index);
  }

  private <T> T getField(String name) {
    return WrapperUtils.<T>getFieldValue(sourceClass, source, name).get();
  }
}
