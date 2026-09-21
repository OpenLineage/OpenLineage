/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package org.apache.spark.sql.kafka010;

import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;

/** Test double with the canonical class name handled by the shared Kafka visitor. */
public class KafkaStreamingWrite implements StreamingWrite {

  @Override
  public StreamingDataWriterFactory createStreamingWriterFactory(PhysicalWriteInfo info) {
    return null;
  }

  @Override
  public void commit(long epochId, WriterCommitMessage[] messages) {}

  @Override
  public void abort(long epochId, WriterCommitMessage[] messages) {}
}
