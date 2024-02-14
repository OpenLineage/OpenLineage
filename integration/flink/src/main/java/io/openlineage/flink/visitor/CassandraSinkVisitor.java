/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import static io.openlineage.flink.visitor.wrapper.CassandraSinkWrapper.INSERT_QUERY_FIELD_NAME;
import static io.openlineage.flink.visitor.wrapper.CassandraSinkWrapper.POJO_CLASS_FIELD_NAME;
import static io.openlineage.flink.visitor.wrapper.CassandraSinkWrapper.POJO_OUTPUT_CLASS_FIELD_NAME;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.visitor.wrapper.CassandraSinkWrapper;
import java.util.Collections;
import java.util.List;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.batch.connectors.cassandra.CassandraPojoOutputFormat;
import org.apache.flink.batch.connectors.cassandra.CassandraRowOutputFormat;
import org.apache.flink.batch.connectors.cassandra.CassandraTupleOutputFormat;
import org.apache.flink.streaming.connectors.cassandra.CassandraPojoSink;
import org.apache.flink.streaming.connectors.cassandra.CassandraRowSink;
import org.apache.flink.streaming.connectors.cassandra.CassandraRowWriteAheadSink;
import org.apache.flink.streaming.connectors.cassandra.CassandraScalaProductSink;
import org.apache.flink.streaming.connectors.cassandra.CassandraTupleSink;
import org.apache.flink.streaming.connectors.cassandra.CassandraTupleWriteAheadSink;

@Slf4j
public class CassandraSinkVisitor extends Visitor<OpenLineage.OutputDataset> {

  public CassandraSinkVisitor(@NonNull OpenLineageContext context) {
    super(context);
  }

  @Override
  public boolean isDefinedAt(Object object) {
    return object instanceof CassandraPojoOutputFormat
        || object instanceof CassandraRowOutputFormat
        || object instanceof CassandraTupleOutputFormat
        || object instanceof CassandraPojoSink
        || object instanceof CassandraRowSink
        || object instanceof CassandraTupleSink
        || object instanceof CassandraRowWriteAheadSink
        || object instanceof CassandraScalaProductSink
        || object instanceof CassandraTupleWriteAheadSink;
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(Object object) {
    log.debug("Apply sink {} in CassandraSinkVisitor", object);
    CassandraSinkWrapper sinkWrapper;
    if (object instanceof RichOutputFormat) {
      sinkWrapper = createWrapperForOutputFormat(object);
    } else {
      sinkWrapper = createWrapperForSink(object);
    }

    if (sinkWrapper == null) {
      throw new UnsupportedOperationException(
          String.format(
              "Unsupported Cassandra sink type %s", object.getClass().getCanonicalName()));
    }

    return Collections.singletonList(
        getDataset(context, sinkWrapper.getKeySpace(), sinkWrapper.getTableName()));
  }

  private CassandraSinkWrapper createWrapperForSink(Object object) {
    CassandraSinkWrapper sinkWrapper = null;
    if (object instanceof CassandraRowSink) {
      sinkWrapper =
          CassandraSinkWrapper.of(object, CassandraRowSink.class, INSERT_QUERY_FIELD_NAME, true);
    } else if (object instanceof CassandraPojoSink) {
      sinkWrapper =
          CassandraSinkWrapper.of(object, CassandraPojoSink.class, POJO_CLASS_FIELD_NAME, false);
    } else if (object instanceof CassandraTupleSink) {
      sinkWrapper =
          CassandraSinkWrapper.of(object, CassandraTupleSink.class, INSERT_QUERY_FIELD_NAME, true);
    } else if (object instanceof CassandraRowWriteAheadSink) {
      sinkWrapper =
          CassandraSinkWrapper.of(
              object, CassandraRowWriteAheadSink.class, INSERT_QUERY_FIELD_NAME, true);
    } else if (object instanceof CassandraScalaProductSink) {
      sinkWrapper =
          CassandraSinkWrapper.of(
              object, CassandraScalaProductSink.class, INSERT_QUERY_FIELD_NAME, true);
    } else if (object instanceof CassandraTupleWriteAheadSink) {
      sinkWrapper =
          CassandraSinkWrapper.of(
              object, CassandraTupleWriteAheadSink.class, INSERT_QUERY_FIELD_NAME, true);
    }
    return sinkWrapper;
  }

  private CassandraSinkWrapper createWrapperForOutputFormat(Object object) {
    CassandraSinkWrapper sinkWrapper = null;
    if (object instanceof CassandraPojoOutputFormat) {
      sinkWrapper =
          CassandraSinkWrapper.of(
              object, CassandraPojoOutputFormat.class, POJO_OUTPUT_CLASS_FIELD_NAME, false);
    } else if (object instanceof CassandraRowOutputFormat) {
      sinkWrapper =
          CassandraSinkWrapper.of(
              object, CassandraRowOutputFormat.class, INSERT_QUERY_FIELD_NAME, true);
    } else if (object instanceof CassandraTupleOutputFormat) {
      sinkWrapper =
          CassandraSinkWrapper.of(
              object, CassandraTupleOutputFormat.class, INSERT_QUERY_FIELD_NAME, true);
    }
    return sinkWrapper;
  }

  private OpenLineage.OutputDataset getDataset(
      OpenLineageContext context, String keySpace, String tableName) {
    OpenLineage openLineage = context.getOpenLineage();
    return openLineage.newOutputDatasetBuilder().name(tableName).namespace(keySpace).build();
  }
}
