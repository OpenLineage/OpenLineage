/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import java.util.Optional;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

public interface SQLQueryRecorder {
  Optional<String> record(LogicalPlan plan);
}
