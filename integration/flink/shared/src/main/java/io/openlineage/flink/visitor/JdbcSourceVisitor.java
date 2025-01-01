/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.jdbc.JdbcDatasetUtils;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.visitor.wrapper.JdbcSourceWrapper;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.connector.jdbc.table.JdbcRowDataInputFormat;
import org.apache.flink.connector.jdbc.table.JdbcRowDataLookupFunction;

@Slf4j
public class JdbcSourceVisitor extends Visitor<OpenLineage.InputDataset> {

  public JdbcSourceVisitor(@NonNull OpenLineageContext context) {
    super(context);
  }

  @Override
  public boolean isDefinedAt(Object source) {
    if (source instanceof JdbcInputFormat
        || source instanceof JdbcRowDataInputFormat
        || source instanceof JdbcRowDataLookupFunction) {
      return true;
    }
    return false;
  }

  @Override
  public List<OpenLineage.InputDataset> apply(Object object) {
    log.debug("Apply source {} in JdbcSourceVisitor", object);
    JdbcSourceWrapper sourceWrapper;
    if (object instanceof JdbcInputFormat) {
      sourceWrapper = JdbcSourceWrapper.of(object);
    } else if (object instanceof JdbcRowDataInputFormat) {
      sourceWrapper = JdbcSourceWrapper.of(object);
    } else if (object instanceof JdbcRowDataLookupFunction) {
      sourceWrapper = JdbcSourceWrapper.of(object);
    } else {
      throw new UnsupportedOperationException(
          String.format("Unsupported JDBC Source type %s", object.getClass().getCanonicalName()));
    }

    // TODO: implement namespace resolver
    DatasetIdentifier di =
        JdbcDatasetUtils.getDatasetIdentifier(
            sourceWrapper.getConnectionUrl(), sourceWrapper.getTableName().get(), new Properties());
    return Collections.singletonList(inputDataset().getDataset(di.getName(), di.getNamespace()));
  }
}
