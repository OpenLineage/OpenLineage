/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.jdbc.JdbcDatasetUtils;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.visitor.wrapper.JdbcSinkWrapper;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.jdbc.JdbcRowOutputFormat;
import org.apache.flink.connector.jdbc.internal.GenericJdbcSinkFunction;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.xa.JdbcXaSinkFunction;

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
      sinkWrapper = JdbcSinkWrapper.of(object);
    } else if (object instanceof JdbcOutputFormat) {
      sinkWrapper = JdbcSinkWrapper.of(object);
    } else if (object instanceof GenericJdbcSinkFunction) {
      sinkWrapper = JdbcSinkWrapper.of(object);
    } else if (object instanceof JdbcXaSinkFunction) {
      sinkWrapper = JdbcSinkWrapper.of(object);
    } else {
      throw new UnsupportedOperationException(
          String.format("Unsupported JDBC sink type %s", object.getClass().getCanonicalName()));
    }

    DatasetIdentifier di =
        JdbcDatasetUtils.getDatasetIdentifier(
            sinkWrapper.getConnectionUrl(), sinkWrapper.getTableName().get(), new Properties());
    return Collections.singletonList(outputDataset().getDataset(di.getName(), di.getNamespace()));
  }
}
