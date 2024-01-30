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

public class CassandraSinkWrapper<T> {
  public static final String POJO_OUTPUT_CLASS_FIELD_NAME = "outputClass";
  public static final String POJO_CLASS_FIELD_NAME = "clazz";
  public static final String INSERT_QUERY_FIELD_NAME = "insertQuery";

  private static final Pattern INSERT_REGEXP =
      Pattern.compile("(?i)insert.+into (\\w+)\\.(\\w+).*;$");
  private String fieldName;
  private T sink;
  private Class sinkClass;
  private boolean hasInsertQuery;

  public static <T> CassandraSinkWrapper of(
      T sink, Class sinkClass, String fieldName, boolean hasInsertQuery) {
    return new CassandraSinkWrapper(sink, sinkClass, fieldName, hasInsertQuery);
  }

  public CassandraSinkWrapper(T sink, Class sinkClass, String fieldName, boolean hasInsertQuery) {
    this.sink = sink;
    this.sinkClass = sinkClass;
    this.hasInsertQuery = hasInsertQuery;
    this.fieldName = fieldName;
  }

  public String getKeySpace() {
    if (hasInsertQuery) {
      return extractFromQuery(1);
    } else {
      Class pojoClass = getField(fieldName);
      Optional<Table> table = CassandraUtils.extractTableAnnotation(pojoClass);
      return table.map(t -> t.keyspace()).orElseThrow();
    }
  }

  public String getTableName() {
    if (hasInsertQuery) {
      return extractFromQuery(2);
    } else {
      Class pojoClass = getField(fieldName);
      Optional<Table> table = CassandraUtils.extractTableAnnotation(pojoClass);
      return table.map(t -> t.name()).orElseThrow();
    }
  }

  private String extractFromQuery(int index) {
    String query = getField(fieldName);
    final Matcher queryMatcher = INSERT_REGEXP.matcher(query);
    checkState(
        queryMatcher.matches(), "Insert query must be of the form insert into keyspace.table ...;");
    return queryMatcher.group(index);
  }

  private <T> T getField(String name) {
    return WrapperUtils.<T>getFieldValue(sinkClass, sink, name).get();
  }
}
