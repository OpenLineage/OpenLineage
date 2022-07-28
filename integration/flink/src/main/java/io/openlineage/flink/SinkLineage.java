/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink;

import java.util.List;
import lombok.Value;

@Value
public class SinkLineage {
  // Can be implementation of org.apache.flink.api.connector.source.Source
  // or org.apache.flink.streaming.api.functions.source.SourceFunction
  List<Object> sources;

  // Can be implementation of org.apache.flink.api.connector.sink2.Sink
  // or org.apache.flink.streaming.api.functions.sink.SinkFunction
  Object sink;
}
