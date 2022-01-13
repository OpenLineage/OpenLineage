package io.openlineage.spark.agent.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class VisitorFactoryProviderTest {

  @ParameterizedTest
  @MethodSource("provideVersionFactory")
  void getInstance(String version, String expectedClass) {
    VisitorFactory factory = VisitorFactoryProvider.getInstance(version);
    assertThat(factory.getClass().getName()).isEqualTo(expectedClass);
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
