/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import java.util.Optional;
import org.apache.spark.package$;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

public class SparkSQLQueryParser {
  static final String SPARK_3_3_OR_ABOVE =
      "io.openlineage.spark33.agent.lifecycle.plan.OpenLineageSQLQueryRecorder";
  static final String SPARK_3_2_OR_BELOW =
      "io.openlineage.spark32.agent.lifecycle.plan.OpenLineageSQLQueryRecorder";

  public Optional<String> parse(LogicalPlan plan) {
    SQLQueryRecorder queryRecorder = resolveQueryRecorder();
    return queryRecorder.record(plan);
  }

  SQLQueryRecorder resolveQueryRecorder() {
    String version = package$.MODULE$.SPARK_VERSION();
    String parserClassName = resolveParserClass(version);
    try {
      return (SQLQueryRecorder) Class.forName(parserClassName).newInstance();
    } catch (Exception e) {

      throw new RuntimeException(
          String.format("Failed to instantiate parser class: %s", version), e);
    }
  }

  String resolveParserClass(String version) {
    if (version.startsWith("2.")) {
      return SPARK_3_2_OR_BELOW;
    } else if (version.startsWith("3.0")) {
      return SPARK_3_2_OR_BELOW;
    } else if (version.startsWith("3.1")) {
      return SPARK_3_2_OR_BELOW;
    } else if (version.startsWith("3.2")) {
      return SPARK_3_2_OR_BELOW;
    } else if (version.startsWith("3.3")) {
      return SPARK_3_3_OR_ABOVE;
    } else if (version.startsWith("3.4")) {
      return SPARK_3_3_OR_ABOVE;
    } else if (version.startsWith("3.5")) {
      return SPARK_3_3_OR_ABOVE;
    } else if (version.startsWith("4")) {
      return SPARK_3_3_OR_ABOVE;
    } else {
      return SPARK_3_3_OR_ABOVE;
    }
  }
}
