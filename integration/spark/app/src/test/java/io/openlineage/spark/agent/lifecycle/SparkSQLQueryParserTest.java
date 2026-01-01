/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class SparkSQLQueryParserTest {

  SparkSQLQueryParser parser = new SparkSQLQueryParser();

  @ParameterizedTest
  @CsvSource({
    "2.4.8,io.openlineage.spark32.agent.lifecycle.plan.OpenLineageSQLQueryRecorder",
    "3.0.1,io.openlineage.spark32.agent.lifecycle.plan.OpenLineageSQLQueryRecorder",
    "3.1.0,io.openlineage.spark32.agent.lifecycle.plan.OpenLineageSQLQueryRecorder",
    "3.2.0,io.openlineage.spark32.agent.lifecycle.plan.OpenLineageSQLQueryRecorder",
    "3.3.0,io.openlineage.spark33.agent.lifecycle.plan.OpenLineageSQLQueryRecorder",
    "3.4.0,io.openlineage.spark33.agent.lifecycle.plan.OpenLineageSQLQueryRecorder",
    "3.5.0,io.openlineage.spark33.agent.lifecycle.plan.OpenLineageSQLQueryRecorder",
    "4.0.0,io.openlineage.spark33.agent.lifecycle.plan.OpenLineageSQLQueryRecorder"
  })
  void testResolveParserClass(String sparkVersion, String expectedParserClass) {
    assertThat(parser.resolveParserClass(sparkVersion)).isEqualTo(expectedParserClass);
  }
}
