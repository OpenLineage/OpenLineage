/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent.lifecycle;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class VisitorFactoryProviderTest {

  @ParameterizedTest
  @MethodSource("provideVersionFactory")
  void getInstance(String version, String expectedClass) {
    String className = VisitorFactoryProvider.getVersion(version);
    assertThat(className).isEqualTo(expectedClass);
  }

  private static Stream<Arguments> provideVersionFactory() {
    return Stream.of(
        Arguments.of("3.1", "io.openlineage.spark.agent.lifecycle.Spark3VisitorFactoryImpl"),
        Arguments.of("3.0", "io.openlineage.spark.agent.lifecycle.Spark3VisitorFactoryImpl"),
        Arguments.of("4.0", "io.openlineage.spark.agent.lifecycle.Spark3VisitorFactoryImpl"),
        Arguments.of("1.0", "io.openlineage.spark.agent.lifecycle.Spark3VisitorFactoryImpl"),
        Arguments.of("2.4", "io.openlineage.spark.agent.lifecycle.Spark2VisitorFactoryImpl"),
        Arguments.of("2.7", "io.openlineage.spark.agent.lifecycle.Spark2VisitorFactoryImpl"));
  }
}
