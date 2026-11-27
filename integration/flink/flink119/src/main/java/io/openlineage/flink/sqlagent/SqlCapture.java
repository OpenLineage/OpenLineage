/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.sqlagent;

import java.util.Map;
import org.apache.flink.configuration.Configuration;

/** Dataset metadata and configuration for one submission call. */
final class SqlCapture {
  final Map<String, Object> datasets;
  final Configuration configuration;
  final String statement;

  SqlCapture(Map<String, Object> datasets, Configuration configuration) {
    this(datasets, configuration, null);
  }

  SqlCapture(Map<String, Object> datasets, Configuration configuration, String statement) {
    this.datasets = datasets;
    this.configuration = configuration;
    this.statement = statement;
  }
}
