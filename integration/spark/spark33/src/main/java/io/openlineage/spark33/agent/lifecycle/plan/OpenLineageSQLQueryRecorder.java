/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark33.agent.lifecycle.plan;

import io.openlineage.spark.agent.lifecycle.SQLQueryRecorder;
import java.util.Optional;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.Option;

public class OpenLineageSQLQueryRecorder implements SQLQueryRecorder {
  @Override
  public Optional<String> record(LogicalPlan plan) {
    if (plan == null || plan.origin() == null) {
      return Optional.empty();
    }

    Option<String> sqlQuery = plan.origin().sqlText();

    if (sqlQuery.isEmpty() || sqlQuery.get() == null) {
      return Optional.empty();
    }

    return Optional.of(sqlQuery.get());
  }
}
