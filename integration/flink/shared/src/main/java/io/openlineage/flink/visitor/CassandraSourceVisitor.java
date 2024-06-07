/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.visitor.wrapper.CassandraSourceWrapper;
import java.util.Collections;
import java.util.List;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.batch.connectors.cassandra.CassandraInputFormat;
import org.apache.flink.batch.connectors.cassandra.CassandraPojoInputFormat;

@Slf4j
public class CassandraSourceVisitor extends Visitor<OpenLineage.InputDataset> {

  public CassandraSourceVisitor(@NonNull OpenLineageContext context) {
    super(context);
  }

  @Override
  public boolean isDefinedAt(Object object) {
    return object instanceof CassandraInputFormat || object instanceof CassandraPojoInputFormat;
  }

  @Override
  public List<OpenLineage.InputDataset> apply(Object object) {
    log.debug("Apply source {} in CassandraSourceVisitor", object);
    CassandraSourceWrapper sourceWrapper;
    if (object instanceof CassandraInputFormat) {
      sourceWrapper = CassandraSourceWrapper.of(object, CassandraInputFormat.class, true);
    } else if (object instanceof CassandraPojoInputFormat) {
      sourceWrapper = CassandraSourceWrapper.of(object, CassandraPojoInputFormat.class, true);
    } else {
      throw new UnsupportedOperationException(
          String.format(
              "Unsupported Cassandra Source type %s", object.getClass().getCanonicalName()));
    }

    return Collections.singletonList(
        inputDataset()
            .getDataset(sourceWrapper.getName(), (String) sourceWrapper.getNamespace().get()));
  }
}
