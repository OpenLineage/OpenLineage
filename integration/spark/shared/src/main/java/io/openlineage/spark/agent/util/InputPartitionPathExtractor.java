/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.connector.read.InputPartition;

public interface InputPartitionPathExtractor {
  boolean isDefinedAt(InputPartition inputPartition);

  List<Path> extract(Configuration conf, InputPartition inputPartition);
}
