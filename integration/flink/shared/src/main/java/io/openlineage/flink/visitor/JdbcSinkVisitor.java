/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.visitor.wrapper.JdbcSinkWrapper;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.jdbc.JdbcRowOutputFormat;
import org.apache.flink.connector.jdbc.internal.GenericJdbcSinkFunction;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.table.JdbcRowDataLookupFunction;
import org.apache.flink.connector.jdbc.xa.JdbcXaSinkFunction;

import java.util.Collections;
import java.util.List;

@Slf4j
public class JdbcSinkVisitor extends Visitor<OpenLineage.OutputDataset> {

  public JdbcSinkVisitor(@NonNull OpenLineageContext context) {
    super(context);
  }

  @Override
  public boolean isDefinedAt(Object sink) {
    if (sink instanceof JdbcOutputFormat
        || sink instanceof JdbcRowOutputFormat
        || sink instanceof GenericJdbcSinkFunction
        || sink instanceof JdbcXaSinkFunction) {
      return true;
    }
    return false;
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(Object object) {
    log.debug("Apply sink {} in JdbcSinkVisitor", object);
    JdbcSinkWrapper sinkWrapper;
    if (object instanceof JdbcRowOutputFormat) {
      sinkWrapper = JdbcSinkWrapper.of(object, JdbcRowOutputFormat.class);
    } else if (object instanceof JdbcOutputFormat) {
      sinkWrapper = JdbcSinkWrapper.of(object, JdbcOutputFormat.class);
    } else if (object instanceof GenericJdbcSinkFunction) {
      sinkWrapper = JdbcSinkWrapper.of(object, GenericJdbcSinkFunction.class);
    } else if (object instanceof JdbcXaSinkFunction) {
      sinkWrapper = JdbcSinkWrapper.of(object, JdbcRowDataLookupFunction.class);
    } else {
      throw new UnsupportedOperationException(
          String.format("Unsupported JDBC sink type %s", object.getClass().getCanonicalName()));
    }

    return Collections.singletonList(
        createOutputDataset(context, sinkWrapper.getConnectionUrl(), sinkWrapper.getTableName().get()));
  }
}
