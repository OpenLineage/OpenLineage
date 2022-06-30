/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class VisitorFactoryProviderTest {

  private static final String IO_OPENLINEAGE_SPARK_AGENT_LIFECYCLE_SPARK_32_VISITOR_FACTORY_IMPL =
      "io.openlineage.spark.agent.lifecycle.Spark32VisitorFactoryImpl";
  private static final String IO_OPENLINEAGE_SPARK_AGENT_LIFECYCLE_SPARK_3_VISITOR_FACTORY_IMPL =
      "io.openlineage.spark.agent.lifecycle.Spark3VisitorFactoryImpl";
  private static final String IO_OPENLINEAGE_SPARK_AGENT_LIFECYCLE_SPARK_2_VISITOR_FACTORY_IMPL =
      "io.openlineage.spark.agent.lifecycle.Spark2VisitorFactoryImpl";

  @ParameterizedTest
  @MethodSource("provideVersionFactory")
  void getInstance(String version, String expectedClass) {
    String className = VisitorFactoryProvider.getVisitorFactoryForVersion(version);
    assertThat(className).isEqualTo(expectedClass);
  }

  private static Stream<Arguments> provideVersionFactory() {
    return Stream.of(
        Arguments.of("3.1", IO_OPENLINEAGE_SPARK_AGENT_LIFECYCLE_SPARK_3_VISITOR_FACTORY_IMPL),
        Arguments.of("3.2", IO_OPENLINEAGE_SPARK_AGENT_LIFECYCLE_SPARK_32_VISITOR_FACTORY_IMPL),
        Arguments.of("2.4", IO_OPENLINEAGE_SPARK_AGENT_LIFECYCLE_SPARK_2_VISITOR_FACTORY_IMPL));
  }
}
