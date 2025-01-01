/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark32.agent.lifecycle.plan;

import io.openlineage.spark.agent.lifecycle.SQLQueryRecorder;
import java.util.Optional;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

public class OpenLineageSQLQueryRecorder implements SQLQueryRecorder {
  @Override
  public Optional<String> record(LogicalPlan plan) {
    // spark 3.2 and below does not keep this attribute
    return Optional.empty();
  }
}
